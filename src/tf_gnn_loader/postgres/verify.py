"""
Verify the prepared PostgreSQL state before anything is exported.

Two kinds of check, and the difference is the whole point:

    GATES    every row of tf_gnn_prep.audit_failures must be zero. A
             non-zero row stops the export, because the defect it
             describes would corrupt the graph silently rather than
             loudly.
    REPORTS  coverage, fanout and vocabulary distributions, printed and
             recorded. Their right value is not known in advance, so
             asserting one would produce a gate that fires on a
             legitimate corpus --- worse than no gate at all.

The gates live in sql/postgres/090_create_audit_views.sql, not here. This
module reads them, adds the handful of cross-checks that need Python
(dataset coverage against the export contract, policy presence), writes
postgres_verification.json, and refuses to return quietly on a failure.

Run directly with:

    python -m tf_gnn_loader.postgres.verify
"""

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, TypedDict

from psycopg import sql

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.contract import PREP_SCHEMA as _PREP_SCHEMA
from tf_gnn_loader.postgres.export import EXPORT_DATASET_NAMES
from tf_gnn_loader.postgres.settings import Settings


_LABEL_POLICY_ID = "phantomledger_synthetic_v1"
_PII_TOKEN_POLICY_ID = "hmac_sha256_v1"


_REQUIRED_RELATIONS: tuple[str, ...] = (
    # Policies
    "label_policy",
    "pii_token_policy",
    # The temporal contract
    "transaction_event_seq",
    "event_seq_by_time",
    "observation_bounds",
    "first_seen_floor",
    "card_first_seen",
    "account_first_seen",
    "merchant_first_seen",
    "party_first_seen",
    "device_first_seen",
    "ip_first_seen",
    # Geography
    "merchant_geography",
    "merchant_is_online",
    "merchant_locations",
    "party_home_area",
    # Entities and their relations
    "card_account",
    "merchant_category",
    "loaded_parties",
    "loaded_accounts",
    "loaded_cards",
    "loaded_merchants",
    "loaded_party_owns_account",
    "loaded_account_has_card",
    "loaded_party_operates_merchant",
    "loaded_merchant_has_location",
    # Tokenised PII
    "email_values",
    "phone_values",
    "address_values",
    "document_values",
    "device_values",
    "ip_values",
    "loaded_party_has_email",
    "loaded_party_has_phone",
    "loaded_party_has_address",
    "loaded_party_has_identity_document",
    "loaded_party_has_device",
    "loaded_party_has_ip",
    # Transactions
    "transaction_manifest",
    "loaded_transaction_used_device",
    "loaded_transaction_from_ip",
    # Load views. One per exported dataset; the names come from
    # tf_gnn_loader.postgres.export so the two lists cannot drift.
    "load_parties",
    "load_accounts",
    "load_cards",
    "load_merchants",
    "load_merchant_locations",
    "load_devices",
    "load_ip_addresses",
    "load_emails",
    "load_phones",
    "load_addresses",
    "load_identity_documents",
    "load_party_owns_account",
    "load_account_has_card",
    "load_party_has_email",
    "load_party_has_phone",
    "load_party_has_address",
    "load_party_has_identity_document",
    "load_party_has_device",
    "load_party_has_ip",
    "load_party_operates_merchant",
    "load_merchant_has_location",
    "load_transactions",
    "load_transaction_used_device",
    "load_transaction_from_ip",
    # Audits
    "audit_source_counts",
    "audit_event_sequence",
    "audit_label_maturity",
    "audit_endpoint_integrity",
    "audit_first_seen",
    "audit_scd_intervals",
    "audit_geography",
    "audit_coordinates",
    "audit_category_consistency",
    "audit_vocabularies",
    "audit_pii_tokens",
    "audit_forbidden_columns",
    "audit_identity_fanout",
    "audit_pii_orphans",
    "audit_load_counts",
    "audit_failures",
)


# Single-row audit views, read wholesale. Adding a column to one of these
# in 090 makes it appear in the report with no change here, which is the
# point: the SQL is the contract and this module is a reader.
_SINGLE_ROW_AUDITS: tuple[str, ...] = (
    "audit_source_counts",
    "audit_event_sequence",
    "audit_label_maturity",
    "audit_endpoint_integrity",
    "audit_first_seen",
    "audit_scd_intervals",
    "audit_geography",
    "audit_coordinates",
    "audit_category_consistency",
    "audit_vocabularies",
    "audit_pii_tokens",
    "audit_forbidden_columns",
    "audit_pii_orphans",
)


class VerificationReport(TypedDict):
    created_at: str
    database: str
    preparation_schema: str
    label_policy: dict[str, Any]
    pii_token_policy: dict[str, Any]
    audits: dict[str, dict[str, Any]]
    identity_fanout: list[dict[str, Any]]
    load_counts: dict[str, int]
    audit_failures: dict[str, int]
    additional_failures: dict[str, str]
    passed: bool


def _as_json_value(value: object) -> Any:
    """Coerce a psycopg value into something json.dumps accepts."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    return str(value)


def _rows(
    settings: Settings,
    relation: str,
) -> list[dict[str, Any]]:
    """Read a whole tf_gnn_prep relation as dictionaries.

    `relation` always comes from a module-level tuple in this file, never
    from user input, and is composed with psycopg's sql.Identifier so it
    cannot be an injection site either way.
    """

    statement = sql.SQL("SELECT * FROM {}.{}").format(
        sql.Identifier(_PREP_SCHEMA),
        sql.Identifier(relation),
    )

    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(statement)

            description = cursor.description

            if description is None:
                raise RuntimeError(f"{relation} returned no column description")

            names = [column.name for column in description]

            return [
                {
                    name: _as_json_value(value)
                    for name, value in zip(names, row, strict=True)
                }
                for row in cursor.fetchall()
            ]


def _single_row(
    settings: Settings,
    relation: str,
) -> dict[str, Any]:
    rows = _rows(settings, relation)

    if len(rows) != 1:
        raise RuntimeError(
            f"{_PREP_SCHEMA}.{relation} returned {len(rows)} rows; "
            + "it is a single-row audit view"
        )

    return rows[0]


def _current_database(settings: Settings) -> str:
    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(b"SELECT current_database()")

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL did not return current_database()")

    return str(row[0])


def _assert_required_relations_exist(settings: Settings) -> None:
    missing: list[str] = []

    with connect(settings) as conn:
        with conn.cursor() as cursor:
            for relation in _REQUIRED_RELATIONS:
                _ = cursor.execute(
                    b"SELECT to_regclass(%s)",
                    (f"{_PREP_SCHEMA}.{relation}",),
                )

                row = cursor.fetchone()

                if row is None or row[0] is None:
                    missing.append(f"{_PREP_SCHEMA}.{relation}")

    if missing:
        raise RuntimeError(
            "Prepared PostgreSQL relations are missing: "
            + ", ".join(missing)
            + ". Run `tf-gnn-load prepare` first."
        )


def _read_policy(
    settings: Settings,
    relation: str,
    policy_id: str,
) -> dict[str, Any]:
    rows = [
        row for row in _rows(settings, relation) if row.get("policy_id") == policy_id
    ]

    if not rows:
        raise RuntimeError(
            f"{_PREP_SCHEMA}.{relation} has no row for {policy_id!r}. "
            + "Run `tf-gnn-load prepare`."
        )

    return rows[0]


def _read_load_counts(settings: Settings) -> dict[str, int]:
    counts: dict[str, int] = {}

    for row in _rows(settings, "audit_load_counts"):
        dataset = str(row["dataset"])
        counts[dataset] = int(row["rows"] or 0)

    return counts


def _read_audit_failures(settings: Settings) -> dict[str, int]:
    failures: dict[str, int] = {}

    for row in _rows(settings, "audit_failures"):
        failures[str(row["check_name"])] = int(row["failure_count"] or 0)

    return dict(sorted(failures.items()))


def _additional_failures(
    load_counts: dict[str, int],
    audits: dict[str, dict[str, Any]],
) -> dict[str, str]:
    """Cross-checks that need the Python side of the contract.

    Everything a single SQL view can see is already a gate in 090. What is
    left is agreement BETWEEN artifacts: the audit's dataset list against
    the exporter's, and the policy rows the tokenisation depends on.
    """

    failures: dict[str, str] = {}

    audited = set(load_counts)
    expected = set(EXPORT_DATASET_NAMES)

    for missing in sorted(expected - audited):
        failures[f"dataset_not_audited:{missing}"] = (
            "tf_gnn_prep.audit_load_counts has no row for this dataset, "
            "so its row count is never compared against the manifest. "
            "Add it in sql/postgres/090_create_audit_views.sql."
        )

    for extra in sorted(audited - expected):
        failures[f"dataset_not_exported:{extra}"] = (
            "tf_gnn_prep.audit_load_counts names a dataset the exporter "
            "does not produce. Remove it from 090 or add it to "
            "tf_gnn_loader.postgres.export."
        )

    # The tokenisation gate deserves its own message: audit_failures
    # reports it as a count, and a count of 3 does not say which views.
    forbidden = audits.get("audit_forbidden_columns", {})

    if int(forbidden.get("forbidden_columns") or 0) != 0:
        failures["raw_pii_or_forbidden_columns_in_load_views"] = str(
            forbidden.get("detail") or ""
        )

    return failures


def _write_report(
    output_directory: Path,
    report: VerificationReport,
) -> Path:
    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = output_directory / "postgres_verification.json"

    temporary_path = path.with_suffix(".json.tmp")

    _ = temporary_path.write_text(
        json.dumps(
            report,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    temporary_path.replace(path)

    return path


def _print_reports(
    audits: dict[str, dict[str, Any]],
    identity_fanout: list[dict[str, Any]],
    load_counts: dict[str, int],
) -> None:
    counts = audits["audit_source_counts"]
    sequence = audits["audit_event_sequence"]
    labels = audits["audit_label_maturity"]
    vocab = audits["audit_vocabularies"]
    tokens = audits["audit_pii_tokens"]
    coordinates = audits["audit_coordinates"]
    intervals = audits["audit_scd_intervals"]
    first_seen = audits["audit_first_seen"]

    print(
        "source: "
        + f"{counts['transactions']:,} transactions, "
        + f"{counts['accounts']:,} accounts, "
        + f"{counts['cards']:,} cards, "
        + f"{counts['merchants']:,} merchants, "
        + f"{counts['parties']:,} parties"
    )

    print(
        "event_seq: "
        + f"1..{sequence['maximum_event_seq']:,} over "
        + f"{sequence['transaction_count']:,} rows "
        + f"(out of order: {sequence['out_of_order_rows']:,})"
    )

    print(
        "labels: "
        + f"{labels['fraud_rows']:,} fraud, "
        + f"{labels['known_rows']:,} known, "
        + f"{labels['unknown_rows']:,} unresolved "
        + "(unresolved rows are NOT usable negatives)"
    )

    print(
        "channels: "
        + f"{vocab['transactions_card_present']:,} card-present, "
        + f"{vocab['transactions_ecommerce']:,} ecommerce, "
        + f"{vocab['transactions_unknown_channel']:,} unknown"
    )

    print(
        "cards: "
        + f"{vocab['cards_credit']:,} credit, "
        + f"{vocab['cards_debit']:,} debit, "
        + f"{vocab['cards_unknown_type']:,} unknown type"
    )

    print(
        "accounts: "
        + f"{intervals['accounts_with_multiple_cards']:,} with more than "
        + "one card generation, "
        + f"{intervals['closed_card_tenures']:,} closed card tenures "
        + "(reissues)"
    )

    print(
        "first_seen: "
        + f"{first_seen['parties_at_floor']:,} parties, "
        + f"{first_seen['cards_at_floor']:,} cards and "
        + f"{first_seen['merchants_at_floor']:,} merchants took the "
        + "observation-window floor rather than a transaction"
    )

    print(
        "tokens: "
        + f"{tokens['emails']:,} emails, "
        + f"{tokens['phones']:,} phones, "
        + f"{tokens['addresses']:,} addresses, "
        + f"{tokens['documents']:,} documents, "
        + f"{tokens['devices']:,} devices, "
        + f"{tokens['ip_addresses']:,} IPs"
    )

    print(
        "unenrolled endpoints: "
        + f"{tokens['devices_transacting_without_party']:,} devices and "
        + f"{tokens['ips_transacting_without_party']:,} IPs transacted "
        + "with no party on file"
    )

    print(
        "geography: "
        + f"{coordinates['locations_with_coordinates']:,}"
        + f"/{coordinates['merchant_locations']:,} outlets with "
        + "coordinates, "
        + f"{coordinates['transactions_with_event_location']:,} "
        + "transactions carry an event location, "
        + f"{coordinates['addresses_masked_ambiguous']:,} addresses "
        + "masked as ambiguous"
    )

    for fanout in identity_fanout:
        print(
            f"fanout {fanout['pii_kind']}: "
            + f"{fanout['distinct_values']:,} values, "
            + f"{fanout['shared_values']:,} shared, "
            + f"mean {fanout['mean_parties_per_value']} parties/value, "
            + f"max {fanout['max_parties_per_value']:,}"
        )

    print(
        "datasets: "
        + f"{len(load_counts)} views, "
        + f"{sum(load_counts.values()):,} rows total"
    )


def verify(
    settings: Settings,
) -> VerificationReport:
    """Verify every prepared PostgreSQL audit before export."""

    _assert_required_relations_exist(settings)

    database_name = _current_database(settings)

    label_policy = _read_policy(settings, "label_policy", _LABEL_POLICY_ID)

    pii_token_policy = _read_policy(
        settings,
        "pii_token_policy",
        _PII_TOKEN_POLICY_ID,
    )

    # The padded HMAC keys are the salt XOR a public constant. They belong
    # in the database, where the raw PII already is, and emphatically not
    # in a report file that gets copied around.
    pii_token_policy = {
        key: value
        for key, value in pii_token_policy.items()
        if key not in {"key_ipad", "key_opad"}
    }

    audits = {name: _single_row(settings, name) for name in _SINGLE_ROW_AUDITS}

    identity_fanout = _rows(settings, "audit_identity_fanout")
    load_counts = _read_load_counts(settings)
    audit_failures = _read_audit_failures(settings)

    additional_failures = _additional_failures(
        load_counts=load_counts,
        audits=audits,
    )

    failed_audits = {
        name: count for name, count in audit_failures.items() if count != 0
    }

    passed = not failed_audits and not additional_failures

    report: VerificationReport = {
        "created_at": datetime.now(UTC).isoformat(),
        "database": database_name,
        "preparation_schema": _PREP_SCHEMA,
        "label_policy": label_policy,
        "pii_token_policy": pii_token_policy,
        "audits": audits,
        "identity_fanout": identity_fanout,
        "load_counts": load_counts,
        "audit_failures": audit_failures,
        "additional_failures": additional_failures,
        "passed": passed,
    }

    report_path = _write_report(
        settings.export_dir,
        report,
    )

    print("PostgreSQL verification report written to " + str(report_path))

    _print_reports(
        audits=audits,
        identity_fanout=identity_fanout,
        load_counts=load_counts,
    )

    if failed_audits:
        details = ", ".join(f"{name}={count}" for name, count in failed_audits.items())

        raise RuntimeError("PostgreSQL audit failures: " + details)

    if additional_failures:
        details = "; ".join(
            f"{name}: {message}" for name, message in additional_failures.items()
        )

        raise RuntimeError("Additional PostgreSQL verification failures: " + details)

    print("PostgreSQL preparation verification passed.")

    return report


def main() -> int:
    """Run verification without requiring the project CLI."""

    settings = Settings()
    report = verify(settings)

    print(
        json.dumps(
            report,
            indent=2,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
