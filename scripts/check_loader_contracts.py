#!/usr/bin/env python3
"""Static pre-flight for the TransactionFraud_GNN loader contracts.

No database and no TigerGraph needed. Run this BEFORE
`tf-gnn-load install-jobs`. Every check catches a class of failure that
otherwise surfaces only on the server, minutes into a run, with an error
message that does not name the file that caused it:

   1. VALUES arity vs schema    TigerGraph rejects a positional VALUES
                                list whose length differs from the
                                vertex/edge attribute count. Appending one
                                attribute to the schema silently breaks
                                every job that targets that type.
   2. PSV columns vs $N         A job referencing $27 against a 27-column
                                view reads past the end of the row. This
                                is the check that guards the one place the
                                two sides deliberately disagree ---
                                22_transactions carries four endpoint keys
                                the vertex does not.
   3. Duplicate PRINT keys      GSQL SEM-1415, "Expression key 'X' is
                                duplicated". Install-time only, and the
                                message gives a line number in a long
                                query.
  4/5. PRINT <-> Python         The verifier asks the query for keys by
                                name. A key it wants and the query does
                                not print is a hard error at verify time;
                                a key printed and never read is dead
                                weight.
  6/7. Prepared relations       Every relation postgres/verify.py requires
                                must be created by some SQL file, and
                                every FROM/JOIN must resolve.
   8. Accumulator hygiene       Used-but-undeclared fails at install.
   9. Paren balance             Cheap syntax smoke test on the SQL.
  10. Job <-> dataset pairing   Every loading job is claimed by exactly
                                one export spec and vice versa. An orphan
                                job installs fine and loads nothing, which
                                is the failure mode with no symptom.
  11. No forbidden attributes   The schema declares no split, protected or
                                response-time attribute, so no loading job
                                may name one.
  12. Manifest format version   export.py WRITES the version that
                                loading.py READS. They live in different
                                modules, and they drifted once already:
                                the retarget bumped the writer to 2 and
                                left the reader at 1, so `load` rejected
                                the manifest `export` had just written ---
                                after prepare, audit and export had all
                                succeeded.

Exit status is non-zero if any check fails, so this is CI-usable.
"""

from __future__ import annotations

import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]

SCHEMA = ROOT / "gsql" / "schema" / "schema.gsql"
JOBS = ROOT / "gsql" / "loading_jobs.gsql"
VERIFY_QUERY = ROOT / "gsql" / "verify_load.gsql"
LOAD_VIEWS = ROOT / "sql" / "postgres" / "080_create_load_views.sql"
EXPORT_PY = ROOT / "src" / "tf_gnn_loader" / "postgres" / "export.py"
TG_VERIFY_PY = ROOT / "src" / "tf_gnn_loader" / "tigergraph" / "verify.py"
TG_LOADING_PY = ROOT / "src" / "tf_gnn_loader" / "tigergraph" / "loading.py"
PG_VERIFY_PY = ROOT / "src" / "tf_gnn_loader" / "postgres" / "verify.py"
SQL_DIR = ROOT / "sql" / "postgres"

# PostgreSQL functions live in tf_gnn_prep too, so a FROM/JOIN check has to
# know they are not relations.
PREP_FUNCTIONS = frozenset(
    {
        "clean_text",
        "boolean_text",
        "to_ms",
        "event_seq_after",
        "hmac_pads",
        "pii_token",
        "email_domain",
        "postal_prefix",
        "funding_account_id",
        "card_generation",
        "instrument_type",
    }
)

# The tuples in tigergraph/verify.py that name keys the query must PRINT.
# _ZERO_INVARIANTS is a subset of _INVARIANT_KEYS and is listed anyway, so
# that a key added there and nowhere else still has to exist.
_TYPE_TUPLES = frozenset(
    {
        "_LOADED_VERTEX_TYPES",
        "_LOADED_EDGE_TYPES",
        "_INVARIANT_KEYS",
        "_ZERO_INVARIANTS",
        "_AMOUNT_KEYS",
    }
)

# Attributes the TransactionFraud_GNN schema deliberately does not
# declare. A loading job naming one is either targeting the superseded
# TF_GNN schema or reintroducing a leak the schema removed on purpose.
FORBIDDEN_JOB_TOKENS = (
    "split_id",
    "causal_fold",
    "is_train",
    "is_val",
    "is_test",
    "label_resolution_status",
    "is_blocked",
    "blocked_unix_time",
    "Merchant_Category",
    "Full_Name",
    "Birthdate",
    "Card_Merchant_Transaction",
)

failures: list[str] = []


def fail(check: str, detail: str) -> None:
    failures.append(f"{check}: {detail}")
    print(f"  FAIL  {detail}")


def strip_comments(text: str) -> str:
    """Remove /* */ and // comments so declarations are not read from prose."""

    return re.sub(r"//.*", "", re.sub(r"/\*.*?\*/", "", text, flags=re.S))


def schema_attribute_counts(schema: str) -> dict[str, tuple[str, int]]:
    result: dict[str, tuple[str, int]] = {}

    pattern = re.compile(
        r"ADD\s+(?:DIRECTED|UNDIRECTED)?\s*(VERTEX|EDGE)\s+(\w+)\s*\((.*?)\)\s*(?:WITH|;)",
        re.S,
    )

    for match in pattern.finditer(schema):
        kind, name, body = match.groups()

        # DISCRIMINATOR(x STRING) is one VALUES slot, not one per inner field.
        body = re.sub(r"DISCRIMINATOR\s*\([^)]*\)", "DISCRIMINATOR", body)

        count = 0

        for part in body.split(","):
            part = part.strip()

            if not part or re.match(r"^(FROM|TO)\b", part):
                continue

            count += 1

        result[name] = (kind, count)

    return result


def select_list_width(view_sql: str, view: str) -> int | None:
    match = re.search(
        r"CREATE (?:OR REPLACE )?VIEW\s+tf_gnn_prep\."
        + view
        + r"\s+AS\s+SELECT\s+(.*?)\nFROM ",
        view_sql,
        re.S,
    )

    if match is None:
        return None

    body = re.sub(r"--.*", "", match.group(1))

    depth = 0
    width = 1

    for char in body:
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif char == "," and depth == 0:
            width += 1

    return width


def python_requested_keys(source: str) -> set[str]:
    keys: set[str] = set()

    for node in ast.parse(source).body:
        if not isinstance(node, ast.AnnAssign):
            continue

        if not isinstance(node.target, ast.Name):
            continue

        if node.target.id not in _TYPE_TUPLES:
            continue

        if not isinstance(node.value, ast.Tuple):
            continue

        for element in node.value.elts:
            if isinstance(element, ast.Constant) and isinstance(element.value, str):
                keys.add(element.value)

    return keys


def main() -> int:
    schema = strip_comments(SCHEMA.read_text(encoding="utf-8"))
    jobs = strip_comments(JOBS.read_text(encoding="utf-8"))
    verify_raw = VERIFY_QUERY.read_text(encoding="utf-8")
    verify = strip_comments(verify_raw)
    load_views = LOAD_VIEWS.read_text(encoding="utf-8")
    export_py = EXPORT_PY.read_text(encoding="utf-8")
    tg_verify = TG_VERIFY_PY.read_text(encoding="utf-8")

    attrs = schema_attribute_counts(schema)

    # ---- 1. VALUES arity vs schema ----
    print("1. loading-job VALUES arity vs schema")

    for job_match in re.finditer(
        r"CREATE LOADING JOB (\w+).*?(?=CREATE LOADING JOB|\Z)", jobs, re.S
    ):
        job = job_match.group(1)

        for target in re.finditer(
            r"TO (VERTEX|EDGE) (\w+) VALUES \((.*?)\)", job_match.group(0), re.S
        ):
            kind, name, values = target.groups()

            if name not in attrs:
                fail("arity", f"{job} targets {name}, which is not in the schema")
                continue

            supplied = len([v for v in values.split(",") if v.strip()])

            # An edge VALUES list is FROM, TO, then the edge's attributes.
            expected = attrs[name][1] + (2 if kind == "EDGE" else 0)

            if supplied != expected:
                fail(
                    "arity",
                    f"{job}: {name} supplies {supplied} values, schema expects {expected}",
                )

    # ---- 2. PSV width vs highest $N ----
    print("2. PSV view columns vs loading-job $N")

    for spec in re.finditer(
        r'"name":\s*"([^"]+)",\s*"view":\s*"([^"]+)",\s*"loading_job":\s*"([^"]+)"',
        export_py,
        re.S,
    ):
        name, view, job = spec.groups()

        width = select_list_width(load_views, view)

        job_body = re.search(
            r"CREATE LOADING JOB " + job + r"\b.*?(?=CREATE LOADING JOB|\Z)", jobs, re.S
        )

        if width is None or job_body is None:
            continue

        referenced = {int(n) for n in re.findall(r"\$(\d+)", job_body.group(0))}

        if not referenced:
            continue

        needed = max(referenced) + 1

        if needed != width:
            fail(
                "psv-width",
                f"{name}: view has {width} columns, {job} references up to ${needed - 1}",
            )

    # ---- 3. duplicate PRINT keys (GSQL SEM-1415) ----
    print("3. duplicate PRINT keys within one statement (SEM-1415)")

    for index, statement in enumerate(re.finditer(r"\bPRINT\b(.*?);", verify, re.S), 1):
        keys = re.findall(r"AS\s+(\w+)", statement.group(1))

        duplicated = sorted({k for k in keys if keys.count(k) > 1})

        if duplicated:
            fail("sem-1415", f"PRINT statement #{index} duplicates {duplicated}")

    # ---- 4/5. PRINT <-> Python contract ----
    print("4. Python-requested keys are PRINTed / 5. PRINTed keys are read")

    printed = set(re.findall(r"AS\s+(\w+)\s*[,;]", verify))
    requested = python_requested_keys(tg_verify)

    for key in sorted(requested - printed):
        fail(
            "print-contract",
            f"tigergraph/verify.py wants {key!r}, query never PRINTs it",
        )

    for key in sorted(printed - requested):
        fail("print-contract", f"query PRINTs {key!r}, nothing reads it")

    # ---- 6/7. prepared relations ----
    print("6. required prepared relations exist / 7. FROM+JOIN targets resolve")

    all_sql = "".join(
        p.read_text(encoding="utf-8") + "\n" for p in sorted(SQL_DIR.glob("*.sql"))
    )

    created: set[str] = set()

    for pattern in (
        r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+tf_gnn_prep\.(\w+)",
        r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?tf_gnn_prep\.(\w+)",
    ):
        created |= set(re.findall(pattern, all_sql, re.I))

    required = re.search(
        r"_REQUIRED_RELATIONS:.*?=\s*\((.*?)\n\)",
        PG_VERIFY_PY.read_text(encoding="utf-8"),
        re.S,
    )

    if required is not None:
        for name in re.findall(r'"(\w+)"', required.group(1)):
            if name not in created:
                fail(
                    "prepared",
                    f"postgres/verify.py requires {name!r}, no SQL file creates it",
                )

    for name in sorted(set(re.findall(r"(?:FROM|JOIN)\s+tf_gnn_prep\.(\w+)", all_sql))):
        if name not in created and name not in PREP_FUNCTIONS:
            fail(
                "prepared", f"SQL references tf_gnn_prep.{name}, which is never created"
            )

    # ---- 8. accumulator hygiene ----
    print("8. verify_load accumulator hygiene")

    declared = set(
        re.findall(r"(?:Sum|Min|Max|List|Set|Map)Accum<[^>]*>\s+@@(\w+)", verify)
    )
    used = re.findall(r"@@(\w+)", verify)

    for name in sorted(set(used) - declared):
        fail("accum", f"@@{name} is used but never declared")

    for name in sorted(n for n in declared if used.count(n) < 2):
        fail("accum", f"@@{name} is declared but never used")

    # ---- 9. paren balance ----
    print("9. SQL parenthesis balance")

    for path in sorted(SQL_DIR.glob("*.sql")):
        text = re.sub(r"--.*", "", path.read_text(encoding="utf-8"))

        # Dollar-quoted bodies contain their own parens and predicates.
        text = re.sub(r"\$[a-z_]*\$.*?\$[a-z_]*\$", "", text, flags=re.S)

        if text.count("(") != text.count(")"):
            fail(
                "parens",
                f"{path.name}: {text.count('(')} open vs {text.count(')')} close",
            )

    # ---- 10. loading job <-> export dataset pairing ----
    print("10. every loading job is claimed by exactly one export spec")

    declared_jobs = set(re.findall(r"CREATE LOADING JOB (\w+)", jobs))

    claimed_jobs = set(re.findall(r'"loading_job":\s*"([^"]+)"', export_py))

    for job in sorted(declared_jobs - claimed_jobs):
        fail(
            "job-pairing",
            f"loading job {job!r} is declared but no export spec uses it; "
            "it would install and load nothing",
        )

    for job in sorted(claimed_jobs - declared_jobs):
        fail(
            "job-pairing",
            f"export.py names loading job {job!r}, which loading_jobs.gsql "
            "does not declare",
        )

    # ---- 11. no forbidden attributes in the loading jobs ----
    print("11. loading jobs name no attribute the schema removed")

    for token in FORBIDDEN_JOB_TOKENS:
        if re.search(r"\b" + re.escape(token) + r"\b", jobs):
            fail(
                "forbidden",
                f"loading_jobs.gsql mentions {token!r}, which the "
                "TransactionFraud_GNN schema does not declare",
            )

    # ---- 12. manifest format version: writer vs reader ----
    print("12. export.py manifest format_version matches loading.py")

    written = re.search(r'"format_version":\s*(\d+)', export_py)
    expected = re.search(
        r"_EXPECTED_MANIFEST_FORMAT_VERSION\s*=\s*(\d+)",
        TG_LOADING_PY.read_text(encoding="utf-8"),
    )

    if written is None:
        fail("manifest-version", "export.py writes no format_version")
    elif expected is None:
        fail(
            "manifest-version",
            "loading.py declares no _EXPECTED_MANIFEST_FORMAT_VERSION",
        )
    elif written.group(1) != expected.group(1):
        fail(
            "manifest-version",
            f"export.py writes format_version {written.group(1)}, "
            f"loading.py expects {expected.group(1)}; `load` would reject "
            "the manifest `export` produces",
        )

    print()

    if failures:
        print(f"{len(failures)} contract failure(s). Do not install.")
        return 1

    print("All loader contracts hold. Safe to install.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
