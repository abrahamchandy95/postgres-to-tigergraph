"""
Inspect the PhantomLedger PostgreSQL source database.

This module inventories the card_fraud schema without changing it.

By default, PostgreSQL catalog estimates are used for row counts. Pass
exact=True to run COUNT(*) against every source table, which can take
considerably longer on the transaction tables.
"""

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import TypedDict

from psycopg import sql

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.settings import Settings


_SOURCE_SCHEMA = "card_fraud"
_PREPARATION_SCHEMA = "tf_gnn_prep"


# PhantomLedger's full card_fraud export, all 43 tables. The
# authoritative list is the generator's own
# include/phantomledger/exporter/card_fraud/schema.hpp (kTableCount = 43).
#
# The point of listing ALL of them, including the ones this loader does
# not read, is that a table going missing and a table being deliberately
# skipped must not look the same. `unread_source_tables` in
# tf_gnn_loader.postgres.contract records WHY each skipped one is skipped.
_EXPECTED_SOURCE_TABLES: tuple[str, ...] = (
    "cf_Address",
    "cf_Assigned_To",
    "cf_Card",
    "cf_Card_Send_Transaction",
    "cf_City",
    "cf_DOB",
    "cf_Device",
    "cf_Email",
    "cf_Email_Minhash",
    "cf_Full_Name",
    "cf_Ground_Truth_Label",
    "cf_Has_Address",
    "cf_Has_City",
    "cf_Has_DOB",
    "cf_Has_Device",
    "cf_Has_Email",
    "cf_Has_Email_Minhash",
    "cf_Has_Full_Name",
    "cf_Has_ID",
    "cf_Has_IP",
    "cf_Has_Phone",
    "cf_Has_State",
    # cf_Has_Std_* carry the party's home area with a since_unix_time.
    # The EARLIEST tenure is what gives an Address its immutable
    # first-observation region and postcode prefix, so a source that
    # stopped exporting them would show up here as a missing table rather
    # than as Address attributes that quietly went blank.
    "cf_Has_Std_City",
    "cf_Has_Std_Postcode",
    "cf_Has_Std_State",
    "cf_Has_Zip",
    "cf_ID",
    "cf_IP",
    "cf_Is_Merchant",
    "cf_Located_In",
    "cf_Merchant",
    "cf_Merchant_Assigned",
    "cf_Merchant_Category",
    "cf_Merchant_Location",
    "cf_Merchant_Receive_Transaction",
    "cf_Party",
    "cf_Party_Has_Card",
    "cf_Payment_Transaction",
    "cf_Phone",
    "cf_State",
    # THE EVENT-TIME ENDPOINTS. These two are what let
    # Transaction_Used_Device and Transaction_From_IP exist at all: they
    # say which device and which source IP the authorization came from,
    # where cf_Has_Device / cf_Has_IP only say what the institution has on
    # file for the party. A source without them loads a graph with no
    # per-transaction endpoint evidence.
    "cf_Transaction_Uses_Device",
    "cf_Transaction_Uses_IP",
    "cf_Zipcode",
)


_RELATION_KINDS: dict[str, str] = {
    "r": "table",
    "p": "partitioned_table",
    "v": "view",
    "m": "materialized_view",
    "f": "foreign_table",
}


class ColumnInspection(TypedDict):
    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int


class TableInspection(TypedDict):
    name: str
    relation_kind: str
    estimated_rows: int | None
    exact_rows: int | None
    size_bytes: int
    size_pretty: str
    columns: list[ColumnInspection]


class InspectionSummary(TypedDict):
    expected_table_count: int
    discovered_table_count: int
    missing_expected_table_count: int
    unexpected_table_count: int
    estimated_source_rows: int
    exact_source_rows: int | None


class InspectionReport(TypedDict):
    created_at: str
    database: str
    database_user: str
    server_version: str
    database_size_bytes: int
    database_size_pretty: str
    source_schema: str
    source_schema_exists: bool
    preparation_schema_exists: bool
    exact_counts_requested: bool
    protected_columns_present: list[str]
    missing_expected_tables: list[str]
    unexpected_tables: list[str]
    summary: InspectionSummary
    tables: dict[str, TableInspection]


def _as_string(
    value: object,
    context: str,
) -> str:
    if not isinstance(value, str):
        raise RuntimeError(
            f"Expected text for {context}, got " + f"{type(value).__name__}: {value!r}"
        )

    return value


def _as_integer(
    value: object,
    context: str,
) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"Expected integer for {context}, got boolean")

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(
                f"Expected integer for {context}, got {value!r}"
            ) from exc

    raise RuntimeError(
        f"Expected integer for {context}, got " + f"{type(value).__name__}: {value!r}"
    )


def _schema_exists(
    settings: Settings,
    schema_name: str,
) -> bool:
    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = %s
                )
                """,
                (schema_name,),
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL did not return a schema-existence result")

    result = row[0]

    if not isinstance(result, bool):
        raise RuntimeError(
            "Unexpected schema-existence result for " + f"{schema_name!r}: {result!r}"
        )

    return result


def _database_metadata(
    settings: Settings,
) -> tuple[str, str, str, int, str]:
    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SELECT
                    current_database(),
                    current_user,
                    current_setting('server_version'),
                    pg_database_size(current_database()),
                    pg_size_pretty(
                        pg_database_size(current_database())
                    )
                """
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL did not return database metadata")

    return (
        _as_string(row[0], "current database"),
        _as_string(row[1], "current user"),
        _as_string(row[2], "server version"),
        _as_integer(row[3], "database size"),
        _as_string(row[4], "pretty database size"),
    )


def _relation_inventory(
    settings: Settings,
) -> dict[str, TableInspection]:
    tables: dict[str, TableInspection] = {}

    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SELECT
                    relation.relname,
                    relation.relkind::text,
                    relation.reltuples::bigint,
                    pg_total_relation_size(relation.oid),
                    pg_size_pretty(
                        pg_total_relation_size(relation.oid)
                    )
                FROM pg_catalog.pg_class AS relation
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = %s
                  AND relation.relkind IN (
                      'r',
                      'p',
                      'v',
                      'm',
                      'f'
                  )
                ORDER BY relation.relname
                """,
                (_SOURCE_SCHEMA,),
            )

            rows = cursor.fetchall()

    for row in rows:
        table_name = _as_string(
            row[0],
            "relation name",
        )

        relation_code = _as_string(
            row[1],
            f"{table_name} relation kind",
        )

        estimated_value = _as_integer(
            row[2],
            f"{table_name} estimated rows",
        )

        estimated_rows: int | None

        if estimated_value < 0:
            estimated_rows = None
        else:
            estimated_rows = estimated_value

        tables[table_name] = {
            "name": table_name,
            "relation_kind": _RELATION_KINDS.get(
                relation_code,
                relation_code,
            ),
            "estimated_rows": estimated_rows,
            "exact_rows": None,
            "size_bytes": _as_integer(
                row[3],
                f"{table_name} size",
            ),
            "size_pretty": _as_string(
                row[4],
                f"{table_name} pretty size",
            ),
            "columns": [],
        }

    return tables


def _attach_columns(
    settings: Settings,
    tables: dict[str, TableInspection],
) -> None:
    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY
                    table_name,
                    ordinal_position
                """,
                (_SOURCE_SCHEMA,),
            )

            rows = cursor.fetchall()

    for row in rows:
        table_name = _as_string(
            row[0],
            "column table name",
        )

        table = tables.get(table_name)

        if table is None:
            continue

        nullable_text = _as_string(
            row[3],
            f"{table_name} nullable value",
        )

        table["columns"].append(
            {
                "name": _as_string(
                    row[1],
                    f"{table_name} column name",
                ),
                "data_type": _as_string(
                    row[2],
                    f"{table_name} data type",
                ),
                "is_nullable": nullable_text == "YES",
                "ordinal_position": _as_integer(
                    row[4],
                    f"{table_name} ordinal position",
                ),
            }
        )


def _attach_exact_counts(
    settings: Settings,
    tables: dict[str, TableInspection],
) -> None:
    with connect(settings) as conn:
        for table_name, table in tables.items():
            relation_kind = table["relation_kind"]

            if relation_kind not in {
                "table",
                "partitioned_table",
                "materialized_view",
                "foreign_table",
            }:
                continue

            print("Counting " + f"{_SOURCE_SCHEMA}.{table_name}...")

            statement = sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(_SOURCE_SCHEMA),
                sql.Identifier(table_name),
            )

            with conn.cursor() as cursor:
                _ = cursor.execute(statement)
                row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "PostgreSQL did not return a count for "
                    + f"{_SOURCE_SCHEMA}.{table_name}"
                )

            exact_rows = _as_integer(
                row[0],
                f"{table_name} exact rows",
            )

            table["exact_rows"] = exact_rows

            print(f"  {exact_rows:,} rows")


def _protected_columns_present(
    settings: Settings,
) -> list[str]:
    """
    Report protected and response-time columns the source still carries.

    None of these is a defect and none of them blocks anything. They are
    reported because the TransactionFraud_GNN schema declares no home for
    any of them and this loader never reads them, so the only way anyone
    finds out they exist is by looking:

        cf_Party.name / .gender / .dob   protected attributes
        cf_Payment_Transaction.error     an authorization RESPONSE, and
                                         the scoring moment is the request
        cf_IP.is_blocked                 a timeless blocklist flag, which
        cf_Device.is_blocked             is written after fraud is
                                         confirmed and is therefore a
                                         function of the label

    Seeing them listed here is the reminder that skipping them was a
    decision. Wiring one up needs a schema change, not an edit to a view.
    """

    with connect(settings) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SELECT table_name || '.' || column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND (
                      (table_name = 'cf_Party'
                       AND column_name IN ('name', 'gender', 'dob'))
                   OR (table_name = 'cf_Payment_Transaction'
                       AND column_name = 'error')
                   OR (table_name IN ('cf_IP', 'cf_Device')
                       AND column_name = 'is_blocked')
                  )
                ORDER BY table_name, column_name
                """,
                (_SOURCE_SCHEMA,),
            )

            rows = cursor.fetchall()

    return [
        _as_string(
            row[0],
            "protected or response-time column",
        )
        for row in rows
    ]


def _summary(
    tables: dict[str, TableInspection],
    missing_expected_tables: list[str],
    unexpected_tables: list[str],
    exact: bool,
) -> InspectionSummary:
    estimated_source_rows = sum(
        table["estimated_rows"] or 0 for table in tables.values()
    )

    exact_source_rows: int | None = None

    if exact:
        exact_source_rows = sum(table["exact_rows"] or 0 for table in tables.values())

    return {
        "expected_table_count": len(_EXPECTED_SOURCE_TABLES),
        "discovered_table_count": len(tables),
        "missing_expected_table_count": len(missing_expected_tables),
        "unexpected_table_count": len(unexpected_tables),
        "estimated_source_rows": estimated_source_rows,
        "exact_source_rows": exact_source_rows,
    }


def _write_report(
    output_directory: Path,
    report: InspectionReport,
) -> Path:
    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = output_directory / "postgres_inspection.json"

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


def inspect(
    settings: Settings,
    *,
    exact: bool = False,
) -> InspectionReport:
    """
    Inspect the source database and write postgres_inspection.json.

    This function does not replace 001_validate_sources.sql. Inspection
    produces a readable inventory; validation enforces the source contract.
    """

    (
        database_name,
        database_user,
        server_version,
        database_size_bytes,
        database_size_pretty,
    ) = _database_metadata(settings)

    source_schema_exists = _schema_exists(
        settings,
        _SOURCE_SCHEMA,
    )

    preparation_schema_exists = _schema_exists(
        settings,
        _PREPARATION_SCHEMA,
    )

    if not source_schema_exists:
        raise RuntimeError(f"Required source schema {_SOURCE_SCHEMA!r} does not exist")

    tables = _relation_inventory(settings)
    _attach_columns(settings, tables)

    if exact:
        _attach_exact_counts(
            settings,
            tables,
        )

    expected = set(_EXPECTED_SOURCE_TABLES)
    discovered = set(tables)

    missing_expected_tables = sorted(expected - discovered)

    unexpected_tables = sorted(discovered - expected)

    protected_columns_present = _protected_columns_present(settings)

    report: InspectionReport = {
        "created_at": datetime.now(UTC).isoformat(),
        "database": database_name,
        "database_user": database_user,
        "server_version": server_version,
        "database_size_bytes": database_size_bytes,
        "database_size_pretty": database_size_pretty,
        "source_schema": _SOURCE_SCHEMA,
        "source_schema_exists": source_schema_exists,
        "preparation_schema_exists": (preparation_schema_exists),
        "exact_counts_requested": exact,
        "protected_columns_present": protected_columns_present,
        "missing_expected_tables": missing_expected_tables,
        "unexpected_tables": unexpected_tables,
        "summary": _summary(
            tables,
            missing_expected_tables,
            unexpected_tables,
            exact,
        ),
        "tables": tables,
    }

    path = _write_report(
        settings.export_dir,
        report,
    )

    print("PostgreSQL inspection written to " + str(path))

    print("Database size: " + database_size_pretty)

    print(f"Source tables: {len(tables)}")

    if missing_expected_tables:
        print("Missing expected tables: " + ", ".join(missing_expected_tables))

    if unexpected_tables:
        print("Unexpected source tables: " + ", ".join(unexpected_tables))

    if protected_columns_present:
        print(
            "Present in the source and deliberately not read: "
            + ", ".join(protected_columns_present)
        )

    return report
