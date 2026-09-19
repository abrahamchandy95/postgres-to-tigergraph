"""Validate and export the canonical temporal staging feed in one snapshot.

Only temporary views are created. Source tables, IDs, clocks, labels and
tenures are never repaired, inferred, renumbered or overwritten here.
"""

from datetime import UTC, datetime
from time import monotonic
from typing import Any
from uuid import uuid4

from psycopg import Connection

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.export import ShardWriter
from tf_gnn_loader.postgres.settings import Settings
from tf_gnn_loader.tigergraph.loading import write_json_atomic
from .contract import (
    BY_NAME,
    DATASETS,
    FORMAT_VERSION,
    GRAPH,
    SOURCE_SCHEMA,
    VERTICES,
)


def _execute(conn: Connection[Any], query: str) -> Any:
    return conn.execute(query.encode())


def current_database(conn: Connection[Any]) -> str:
    row = conn.execute("SELECT current_database()").fetchone()
    if row is None:
        raise RuntimeError("PostgreSQL returned no database name")
    return str(row[0])


def prepare_snapshot(conn: Connection[Any]) -> None:
    columns = conn.execute(
        "SELECT table_name,column_name FROM information_schema.columns "
        "WHERE table_schema=%s ORDER BY table_name,ordinal_position",
        (SOURCE_SCHEMA,),
    ).fetchall()
    actual: dict[str, list[str]] = {}
    for table, column in columns:
        actual.setdefault(table, []).append(column)
    for d in DATASETS:
        if actual.get("mt_" + d.name) != list(d.columns):
            raise RuntimeError(
                f"Source contract mismatch for {d.table}; expected columns {d.columns}. "
                "Set MULE_PG_DSN to the database containing the mule_temporal export."
            )
        expressions = []
        for name, kind in d.fields:
            pg_type = {
                "UINT": "numeric(20,0)",
                "INT": "integer",
                "DOUBLE": "double precision",
                "FLOAT": "real",
                "BOOL": "boolean",
                "DATETIME": "timestamp",
            }.get(kind, "text")
            expressions.append(f'"{name}"::{pg_type} AS "{name}"')
        _execute(
            conn,
            f'CREATE TEMP VIEW "mt_{d.name}" AS SELECT '
            + ",".join(expressions)
            + f" FROM {d.table}",
        )
    conn.commit()
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    conn.execute("SET LOCAL TIME ZONE 'UTC'")
    conn.execute("SET LOCAL work_mem = '128MB'")


def audit_snapshot(conn: Connection[Any]) -> dict[str, Any]:
    checks: dict[str, int] = {}
    counts: dict[str, int] = {}

    def check(label: str, query: str) -> None:
        print(f"  Checking {label}...", flush=True)
        started = monotonic()
        n = int(_execute(conn, query).fetchone()[0])
        print(f"  {label}: {n:,} violations ({monotonic() - started:.1f}s)", flush=True)
        checks[label] = n
        if n:
            raise RuntimeError(
                f"Temporal source audit failed: {label} ({n:,} rows); nothing was uploaded"
            )

    for d in DATASETS:
        print(f"Auditing {d.name}...", flush=True)
        counts[d.name] = int(
            _execute(conn, f"SELECT count(*) FROM {d.table}").fetchone()[0]
        )
        # TEXT COPY is safe only for single-line, unescaped PSV values.
        invalid = []
        for name, kind in d.fields:
            col = f'"{name}"'
            invalid.append(f"{col} IS NULL")
            if kind == "UINT":
                invalid.append(f"{col} !~ '^[0-9]+$'")
            elif kind == "BOOL":
                invalid.append(f"lower({col}) NOT IN ('true','false')")
            elif kind == "INT":
                invalid.append(f"{col} !~ '^-?[0-9]+$'")
            elif kind == "STRING":
                invalid.append(f"{col} ~ E'[|\\\\\\\\\\r\\n\\t\"]'")
        check(
            d.name + ": source values",
            f"SELECT count(*) FROM {d.table} WHERE " + " OR ".join(invalid),
        )
        ranges = [
            f'"{name}" < 0 OR "{name}" > 18446744073709551615'
            for name, kind in d.fields
            if kind == "UINT"
        ]
        if ranges:
            check(
                d.name + ": UINT range",
                f"SELECT count(*) FROM {d.view} WHERE " + " OR ".join(ranges),
            )
        if d.vertex:
            primary = d.columns[0]
            check(
                d.name + ": identity",
                f"SELECT count(*) - count(DISTINCT {primary}) + count(*) FILTER (WHERE {primary}='') FROM {d.view}",
            )
            if "first_seen_seq" in d.columns:
                check(
                    d.name + ": first observation",
                    f"SELECT count(*) FROM {d.view} WHERE first_seen_seq <= 0 OR first_seen_ts_ms <= 0",
                )
                check(
                    d.name + ": opaque IDs",
                    f"SELECT count(*) FROM {d.view} WHERE {primary} !~ '^[patdih]_[0-9a-f]{{32}}$'",
                )
            else:
                check(
                    d.name + ": event values",
                    f"""SELECT count(*) FROM {d.view}
                    WHERE event_seq <= 0 OR event_ts_ms <= 0
                    OR floor(extract(epoch FROM event_time)*1000) <> event_ts_ms
                    OR amount < 0 OR amount::text IN ('NaN','Infinity','-Infinity')
                    OR (NOT amount_present AND amount <> 0)""",
                )
        else:
            source, target = BY_NAME[d.source], BY_NAME[d.target]
            start = "valid_from_seq" if d.association else "event_seq"
            source_clock = "first_seen_seq" if d.association else "event_seq"
            check(
                d.name + ": endpoints",
                f"""SELECT count(*) FROM {d.view} r
                LEFT JOIN {source.view} f ON r.from_id=f.{source.columns[0]}
                LEFT JOIN {target.view} t ON r.to_id=t.{target.columns[0]}
                WHERE f.{source.columns[0]} IS NULL OR t.{target.columns[0]} IS NULL
                OR f.{source_clock} > r.{start} OR t.first_seen_seq > r.{start}""",
            )
            key = "from_id,to_id,valid_from_seq" if d.association else "from_id"
            check(
                d.name + ": unique identity/role",
                f"SELECT count(*) FROM (SELECT {key} FROM {d.view} GROUP BY {key} HAVING count(*)>1) bad",
            )
            if d.association:
                check(
                    d.name + ": intervals",
                    f"SELECT count(*) FROM {d.view} WHERE valid_from_seq<=0 OR (valid_to_seq<>0 AND valid_to_seq<=valid_from_seq) OR confidence<0 OR confidence>1 OR confidence::text='NaN' OR source_system=''",
                )
                check(
                    d.name + ": overlapping tenures",
                    f"""SELECT count(*) FROM (
                    SELECT valid_from_seq, max(CASE WHEN valid_to_seq=0 THEN 18446744073709551616 ELSE valid_to_seq END)
                    OVER (PARTITION BY from_id,to_id ORDER BY valid_from_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) prior_end
                    FROM {d.view}) r WHERE valid_from_seq < prior_end""",
                )
            else:
                check(
                    d.name + ": repeated clocks",
                    f"SELECT count(*) FROM {d.view} r JOIN {source.view} f ON r.from_id=f.{source.columns[0]} WHERE r.event_seq<>f.event_seq OR r.event_ts_ms<>f.event_ts_ms",
                )
    p, z = BY_NAME["Payment_Transaction"].view, BY_NAME["Zelle_Transfer"].view
    if counts["Payment_Transaction"] + counts["Zelle_Transfer"] == 0:
        raise RuntimeError("Temporal source contains no payments")
    check(
        "exclusive payment identities",
        f"SELECT count(*) FROM {p} p JOIN {z} z ON p.transaction_id=z.transfer_id",
    )
    check(
        "non-Zelle rail", f"SELECT count(*) FROM {p} WHERE lower(payment_rail)='zelle'"
    )
    events = f"SELECT event_seq,event_ts_ms FROM {p} UNION ALL SELECT event_seq,event_ts_ms FROM {z}"
    check(
        "shared event sequence",
        f"SELECT count(*)-count(DISTINCT event_seq) FROM ({events}) e",
    )
    check(
        "chronological events",
        f"SELECT count(*) FROM (SELECT event_ts_ms,lag(event_ts_ms) OVER (ORDER BY event_seq) prior FROM ({events}) e) t WHERE event_ts_ms<prior",
    )
    check(
        "label availability",
        f"""SELECT count(*) FROM {z} WHERE
        (label_known AND (fraud_label NOT IN (0,1) OR label_available_seq<=event_seq OR label_available_ts_ms<event_ts_ms))
        OR (NOT label_known AND (fraud_label<>-1 OR label_available_seq<>0 OR label_available_ts_ms<>0))""",
    )
    witnesses = [events]
    witnesses += [
        f"SELECT first_seen_seq,first_seen_ts_ms FROM {d.view}" for d in VERTICES[:6]
    ]
    witnesses += [
        f"SELECT label_available_seq,label_available_ts_ms FROM {z} WHERE label_known"
    ]
    clock = " UNION ALL ".join(witnesses)
    check(
        "common clock witnesses",
        f"SELECT count(*) FROM (SELECT event_seq FROM ({clock}) w GROUP BY event_seq HAVING min(event_ts_ms)<>max(event_ts_ms)) bad",
    )
    check(
        "chronological clock witnesses",
        f"SELECT count(*) FROM (SELECT event_ts_ms,lag(event_ts_ms) OVER (ORDER BY event_seq) prior FROM (SELECT DISTINCT event_seq,event_ts_ms FROM ({clock}) w) u) t WHERE event_ts_ms<prior",
    )
    for event_type, prefix in (
        ("Zelle_Transfer", "Transfer"),
        ("Payment_Transaction", "Transaction"),
    ):
        e = BY_NAME[event_type]
        for role in ("From", "To"):
            a = BY_NAME[f"{prefix}_{role}_Account"].view
            t = BY_NAME[f"{prefix}_{role}_Token"].view
            check(
                f"{prefix}: {role} role present",
                f"SELECT count(*) FROM {e.view} e LEFT JOIN {a} a ON a.from_id=e.{e.columns[0]} LEFT JOIN {t} t ON t.from_id=e.{e.columns[0]} WHERE a.from_id IS NULL AND t.from_id IS NULL",
            )
    binding = BY_NAME["Token_Bound_To_Account"].view
    check(
        "exclusive token account tenures",
        f"""SELECT count(*) FROM {binding} a JOIN {binding} b
        ON a.from_id=b.from_id AND a.to_id<b.to_id
        AND (a.valid_to_seq=0 OR b.valid_from_seq<a.valid_to_seq)
        AND (b.valid_to_seq=0 OR a.valid_from_seq<b.valid_to_seq)""",
    )
    for role in ("From", "To"):
        t, a = (
            BY_NAME[f"Transfer_{role}_Token"].view,
            BY_NAME[f"Transfer_{role}_Account"].view,
        )
        check(
            f"Zelle {role}: visible binding",
            f"""SELECT count(*) FROM {t} t JOIN {a} a ON a.from_id=t.from_id
            WHERE NOT EXISTS (SELECT 1 FROM {binding} b WHERE b.from_id=t.to_id AND b.to_id=a.to_id
            AND b.valid_from_seq<=t.event_seq AND (b.valid_to_seq=0 OR t.event_seq<b.valid_to_seq))""",
        )
    account = BY_NAME["Account"].view
    check(
        "account mule flag",
        f"SELECT count(*) FROM {account} WHERE is_mule NOT IN (-1,0,1)",
    )
    mule_flags = {
        str(flag): int(count)
        for flag, count in _execute(
            conn, f"SELECT is_mule,count(*) FROM {account} GROUP BY is_mule"
        ).fetchall()
    }
    return {
        "passed": True,
        "counts": counts,
        "checks": checks,
        "account_mule_flags": mule_flags,
    }


def audit(settings: Settings) -> dict[str, Any]:
    with connect(settings, autocommit=False) as conn:
        prepare_snapshot(conn)
        return audit_snapshot(conn)


def export(settings: Settings) -> dict[str, Any]:
    directory = settings.export_dir.resolve()
    manifest_path = directory / "export_manifest.json"
    if manifest_path.exists():
        raise RuntimeError(
            "An immutable mule export already exists. Resume with load/push, or choose a new MULE_EXPORT_DIR for a new empty target."
        )
    shards_dir = directory / "shards" / uuid4().hex
    shards_dir.mkdir(parents=True)
    datasets: dict[str, Any] = {}
    with connect(settings, autocommit=False) as conn:
        prepare_snapshot(conn)
        report = audit_snapshot(conn)
        database = current_database(conn)
        for index, d in enumerate(DATASETS, 1):
            print(f"Exporting {d.name}...", flush=True)
            writer = ShardWriter(
                shards_dir, f"{index:02d}_{d.name.lower()}", settings.shard_bytes
            )
            columns = []
            for name, kind in d.fields:
                if kind == "BOOL":
                    columns.append(f"CASE WHEN {name} THEN 'true' ELSE 'false' END")
                elif kind == "DATETIME":
                    columns.append(f"to_char({name},'YYYY-MM-DD HH24:MI:SS')")
                else:
                    columns.append(name)
            query = f"COPY (SELECT {','.join(columns)} FROM {d.view}) TO STDOUT WITH (FORMAT TEXT, DELIMITER '|', NULL '', ENCODING 'UTF8')"
            try:
                with conn.cursor().copy(query.encode()) as copy:
                    for chunk in copy:
                        writer.write_copy_chunk(bytes(chunk))
            finally:
                shards = writer.finish()
            datasets[d.name] = {
                "view": d.table,
                "loading_job": d.job,
                "rows": sum(s["rows"] for s in shards),
                "bytes": sum(s["bytes"] for s in shards),
                "shards": shards,
            }
            if datasets[d.name]["rows"] != report["counts"][d.name]:
                raise RuntimeError(f"Export row count mismatch for {d.name}")
    manifest: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "graphname": GRAPH,
        "source_database": database,
        "source_schema": SOURCE_SCHEMA,
        "created_at": datetime.now(UTC).isoformat(),
        "separator": "|",
        "header": False,
        "datasets": datasets,
        "audit": report,
    }
    write_json_atomic(manifest_path, manifest)
    return manifest
