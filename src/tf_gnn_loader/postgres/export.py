"""
Export prepared PostgreSQL views into resumable TigerGraph PSV shards.

All label-maturity, entity-derivation, geography, tokenisation and
value-conversion logic lives in sql/postgres/*.sql. This module is
plumbing:

1. Verifies that the prepared load views exist.
2. Refuses to export when a PostgreSQL audit gate has failed.
3. Streams each load view with PostgreSQL COPY.
4. Splits the stream into size-bounded PSV files without splitting rows.
5. Records row counts and SHA-256 hashes in export_manifest.json.

DATASET NUMBERING IS THE LOAD ORDER, AND IT IS LOAD-BEARING.
tigergraph/loading.py uploads in sorted(dataset_name) order, so the
two-digit zero-padded prefixes are what enforce it.

TigerGraph AUTO-CREATES a missing edge endpoint with schema defaults
rather than failing. A vertex created that way carries first_seen_seq = 0,
and 0 passes every

    neighbour.first_seen_seq <= seed.event_seq

predicate --- including cutoffs from before the entity existed. No error,
no null, a plausible number, and every admission filter quietly opens. So
the order is:

    01-11  every vertex type
    12-21  the slowly changing relations between them
    22     Payment_Transaction, which also creates its four
           participation edges from the same PSV line
    23-24  the optional event-time endpoints, which need both the
           transaction and the Device / IP_Address to exist

By the time 22 runs, every vertex it references already exists, so it
upserts nothing. That is the invariant the ordering buys.

ONE PSV ROW, FIVE GRAPH OBJECTS. 22_transactions feeds
Payment_Transaction plus Transaction_From_Account, Transaction_Used_Card,
Transaction_At_Merchant and Transaction_At_Location in a single loading
job. That is what makes "exactly one Account and Merchant per
authorization" true by construction rather than by assertion, and it
saves four full copies of a 27M-row edge file.

NO RAW PII LEAVES POSTGRESQL. Every Device, IP_Address, Email, Phone,
Address and Identity_Document id in these shards is an HMAC-SHA256 token
built in sql/postgres/020_create_policies.sql. The gate is
tf_gnn_prep.audit_forbidden_columns, which reads the PostgreSQL catalogue
rather than the data, so a load view that started selecting a raw column
would fail the export even if every row in it happened to look tokenised.
"""

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import BinaryIO, TypedDict

from psycopg import Connection

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.contract import (
    PREP_SCHEMA as _PREP_SCHEMA,
    SOURCE_SCHEMA,
    TARGET_GRAPH as _TARGET_GRAPH,
)
from tf_gnn_loader.postgres.settings import Settings


class ShardRecord(TypedDict):
    path: str
    bytes: int
    rows: int
    sha256: str


class DatasetRecord(TypedDict):
    view: str
    loading_job: str
    rows: int
    bytes: int
    shards: list[ShardRecord]


class ExportManifest(TypedDict):
    format_version: int
    created_at: str
    graphname: str
    source_database: str
    source_schema: str
    preparation_schema: str
    separator: str
    header: bool
    null_representation: str
    datasets: dict[str, DatasetRecord]


class ExportSpec(TypedDict):
    name: str
    view: str
    loading_job: str


_EXPORT_SPECS: tuple[ExportSpec, ...] = (
    # ---------------------------------------------------------------
    # 01-11  Vertices. Every one of them before any relation.
    # ---------------------------------------------------------------
    {
        "name": "01_parties",
        "view": "load_parties",
        "loading_job": "tfgnn_load_parties",
    },
    {
        "name": "02_accounts",
        "view": "load_accounts",
        "loading_job": "tfgnn_load_accounts",
    },
    {
        "name": "03_cards",
        "view": "load_cards",
        "loading_job": "tfgnn_load_cards",
    },
    {
        "name": "04_merchants",
        "view": "load_merchants",
        "loading_job": "tfgnn_load_merchants",
    },
    {
        "name": "05_merchant_locations",
        "view": "load_merchant_locations",
        "loading_job": "tfgnn_load_merchant_locations",
    },
    {
        "name": "06_devices",
        "view": "load_devices",
        "loading_job": "tfgnn_load_devices",
    },
    {
        "name": "07_ip_addresses",
        "view": "load_ip_addresses",
        "loading_job": "tfgnn_load_ip_addresses",
    },
    {
        "name": "08_emails",
        "view": "load_emails",
        "loading_job": "tfgnn_load_emails",
    },
    {
        "name": "09_phones",
        "view": "load_phones",
        "loading_job": "tfgnn_load_phones",
    },
    {
        "name": "10_addresses",
        "view": "load_addresses",
        "loading_job": "tfgnn_load_addresses",
    },
    {
        "name": "11_identity_documents",
        "view": "load_identity_documents",
        "loading_job": "tfgnn_load_identity_documents",
    },
    # ---------------------------------------------------------------
    # 12-21  Slowly changing relations. Both endpoints already exist.
    # ---------------------------------------------------------------
    {
        "name": "12_party_owns_account",
        "view": "load_party_owns_account",
        "loading_job": "tfgnn_load_party_owns_account",
    },
    {
        "name": "13_account_has_card",
        "view": "load_account_has_card",
        "loading_job": "tfgnn_load_account_has_card",
    },
    {
        "name": "14_party_has_email",
        "view": "load_party_has_email",
        "loading_job": "tfgnn_load_party_has_email",
    },
    {
        "name": "15_party_has_phone",
        "view": "load_party_has_phone",
        "loading_job": "tfgnn_load_party_has_phone",
    },
    {
        "name": "16_party_has_address",
        "view": "load_party_has_address",
        "loading_job": "tfgnn_load_party_has_address",
    },
    {
        "name": "17_party_has_identity_document",
        "view": "load_party_has_identity_document",
        "loading_job": "tfgnn_load_party_has_identity_document",
    },
    {
        "name": "18_party_has_device",
        "view": "load_party_has_device",
        "loading_job": "tfgnn_load_party_has_device",
    },
    {
        "name": "19_party_has_ip",
        "view": "load_party_has_ip",
        "loading_job": "tfgnn_load_party_has_ip",
    },
    {
        "name": "20_party_operates_merchant",
        "view": "load_party_operates_merchant",
        "loading_job": "tfgnn_load_party_operates_merchant",
    },
    {
        "name": "21_merchant_has_location",
        "view": "load_merchant_has_location",
        "loading_job": "tfgnn_load_merchant_has_location",
    },
    # ---------------------------------------------------------------
    # 22  The event vertex and its four participation edges.
    # ---------------------------------------------------------------
    {
        "name": "22_transactions",
        "view": "load_transactions",
        "loading_job": "tfgnn_load_transactions",
    },
    # ---------------------------------------------------------------
    # 23-24  Optional event-time endpoints. After 22, because both ends
    #        --- the Payment_Transaction and the Device / IP_Address ---
    #        must already exist.
    # ---------------------------------------------------------------
    {
        "name": "23_transaction_used_device",
        "view": "load_transaction_used_device",
        "loading_job": "tfgnn_load_transaction_used_device",
    },
    {
        "name": "24_transaction_from_ip",
        "view": "load_transaction_from_ip",
        "loading_job": "tfgnn_load_transaction_from_ip",
    },
)


EXPORT_DATASET_NAMES: frozenset[str] = frozenset(spec["name"] for spec in _EXPORT_SPECS)

# Datasets that may legitimately export zero rows.
#
# The schema calls Device and IP_Address OPTIONAL endpoints --- "at most
# one canonical Device and canonical source IP_Address" --- so a corpus
# whose generator never emitted cf_Transaction_Uses_* is a corpus without
# those relations, not a broken export. 001 raises a NOTICE in that case
# and says what it costs (the account-takeover and source-IP arms).
#
# Nothing else is exempt. Every other dataset documents something that
# must transfer, and a zero-row export would otherwise verify as
# expected == actual == 0 with the missing data never flagged.
ALLOWED_EMPTY_DATASETS: frozenset[str] = frozenset(
    {
        "23_transaction_used_device",
        "24_transaction_from_ip",
    }
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(8 * 1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


class _ShardWriter:
    """Write complete PSV rows into approximately size-bounded files."""

    _directory: Path
    _stem: str
    _target_bytes: int

    _index: int
    _handle: BinaryIO | None
    _path: Path | None
    _current_bytes: int
    _current_rows: int
    _pending: bytes

    _records: list[ShardRecord]

    def __init__(
        self,
        directory: Path,
        stem: str,
        target_bytes: int,
    ) -> None:
        self._directory = directory
        self._stem = stem
        self._target_bytes = target_bytes

        self._index = 0
        self._handle = None
        self._path = None
        self._current_bytes = 0
        self._current_rows = 0
        self._pending = b""

        self._records = []

    def _open_shard(self) -> None:
        self._index += 1

        self._path = self._directory / (f"{self._stem}_{self._index:05d}.psv")

        self._handle = self._path.open("wb")
        self._current_bytes = 0
        self._current_rows = 0

    def _close_shard(self) -> None:
        if self._handle is None or self._path is None:
            return

        self._handle.flush()
        self._handle.close()

        size = self._path.stat().st_size

        self._records.append(
            {
                "path": str(self._path.resolve()),
                "bytes": size,
                "rows": self._current_rows,
                "sha256": _sha256(self._path),
            }
        )

        self._handle = None
        self._path = None
        self._current_bytes = 0
        self._current_rows = 0

    def _write_line(self, line: bytes) -> None:
        if self._handle is None:
            self._open_shard()

        if (
            self._current_rows > 0
            and self._current_bytes + len(line) > self._target_bytes
        ):
            self._close_shard()
            self._open_shard()

        if self._handle is None:
            raise RuntimeError("Shard file was not opened")

        self._handle.write(line)
        self._current_bytes += len(line)
        self._current_rows += 1

    def write_copy_chunk(self, chunk: bytes) -> None:
        """
        PostgreSQL COPY chunks do not necessarily end at row boundaries.

        Preserve the unfinished final row and write only complete rows.
        """

        combined = self._pending + chunk
        pieces = combined.split(b"\n")

        for complete_line in pieces[:-1]:
            self._write_line(complete_line + b"\n")

        self._pending = pieces[-1]

    def finish(self) -> list[ShardRecord]:
        if self._pending:
            self._write_line(self._pending + b"\n")

            self._pending = b""

        self._close_shard()

        return self._records.copy()


def _assert_prepared_views_exist(
    conn: Connection[tuple[object, ...]],
) -> None:
    missing: list[str] = []

    with conn.cursor() as cursor:
        for spec in _EXPORT_SPECS:
            qualified_name = f"{_PREP_SCHEMA}.{spec['view']}"

            _ = cursor.execute(
                b"SELECT to_regclass(%s)",
                (qualified_name,),
            )

            row = cursor.fetchone()

            if row is None or row[0] is None:
                missing.append(qualified_name)

    if missing:
        raise RuntimeError(
            "Prepared PostgreSQL load views are missing: "
            + ", ".join(missing)
            + ". Run `tf-gnn-load prepare` first."
        )


def _assert_audits_pass(
    conn: Connection[tuple[object, ...]],
) -> None:
    with conn.cursor() as cursor:
        _ = cursor.execute(
            b"""
            SELECT
                check_name,
                failure_count
            FROM tf_gnn_prep.audit_failures
            WHERE failure_count <> 0
            ORDER BY check_name
            """
        )

        rows = cursor.fetchall()

    if not rows:
        return

    failures: list[str] = []

    for row in rows:
        check_name = str(row[0])
        failure_count = int(str(row[1]))

        failures.append(f"{check_name}={failure_count}")

    raise RuntimeError(
        "PostgreSQL audit failures prevent export: " + ", ".join(failures)
    )


def _assert_no_empty_datasets(
    datasets: dict[str, DatasetRecord],
) -> None:
    empty = sorted(
        name
        for name, record in datasets.items()
        if record["rows"] == 0 and name not in ALLOWED_EMPTY_DATASETS
    )

    if not empty:
        return

    raise RuntimeError(
        "Datasets exported zero rows: "
        + ", ".join(empty)
        + ". Their vertices and edges would be silently missing from "
        + _TARGET_GRAPH
        + ". Fix the source tables or prepared views, then re-run "
        + "`tf-gnn-load export`."
    )


def _current_database(
    conn: Connection[tuple[object, ...]],
) -> str:
    with conn.cursor() as cursor:
        _ = cursor.execute(b"SELECT current_database()")

        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL did not return current_database()")

    return str(row[0])


def _remove_old_shards(
    shard_directory: Path,
    stem: str,
) -> None:
    for path in shard_directory.glob(f"{stem}_*.psv"):
        path.unlink()


def _export_one(
    conn: Connection[tuple[object, ...]],
    settings: Settings,
    shard_directory: Path,
    spec: ExportSpec,
) -> DatasetRecord:
    name = spec["name"]
    view = spec["view"]
    loading_job = spec["loading_job"]

    _remove_old_shards(
        shard_directory,
        name,
    )

    writer = _ShardWriter(
        directory=shard_directory,
        stem=name,
        target_bytes=settings.shard_bytes,
    )

    # All identifiers come from the fixed _EXPORT_SPECS tuple above.
    # No user-supplied identifier is interpolated into this query.
    copy_statement = (
        "COPY ("
        + f"SELECT * FROM {_PREP_SCHEMA}.{view}"
        + ") TO STDOUT WITH ("
        + "FORMAT TEXT, "
        + "DELIMITER '|', "
        + "NULL '', "
        + "ENCODING 'UTF8'"
        + ")"
    ).encode("utf-8")

    with conn.cursor() as cursor:
        with cursor.copy(copy_statement) as copy:
            for chunk in copy:
                writer.write_copy_chunk(bytes(chunk))

    shards = writer.finish()

    total_rows = sum(shard["rows"] for shard in shards)

    total_bytes = sum(shard["bytes"] for shard in shards)

    return {
        "view": f"{_PREP_SCHEMA}.{view}",
        "loading_job": loading_job,
        "rows": total_rows,
        "bytes": total_bytes,
        "shards": shards,
    }


def _write_manifest(
    path: Path,
    manifest: ExportManifest,
) -> None:
    temporary_path = path.with_suffix(".json.tmp")

    _ = temporary_path.write_text(
        json.dumps(
            manifest,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    temporary_path.replace(path)


def export_all(
    settings: Settings,
) -> ExportManifest:
    """
    Export every prepared TransactionFraud_GNN dataset.

    All datasets are exported under one PostgreSQL REPEATABLE READ,
    READ ONLY transaction. This gives every shard a consistent view of
    the source database even though the exports run sequentially --- which
    matters here more than it looks: the vertex datasets and the
    27M-row fact table must agree about which cards and merchants exist,
    or the fact table references an endpoint no vertex dataset carried.
    """

    export_directory = settings.export_dir
    shard_directory = export_directory / "shards"

    export_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    shard_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    datasets: dict[str, DatasetRecord] = {}

    with connect(
        settings,
        autocommit=False,
    ) as conn:
        with conn.cursor() as cursor:
            _ = cursor.execute(
                b"""
                SET TRANSACTION
                    ISOLATION LEVEL REPEATABLE READ,
                    READ ONLY
                """
            )

        _assert_prepared_views_exist(conn)
        _assert_audits_pass(conn)

        database_name = _current_database(conn)

        for spec in _EXPORT_SPECS:
            name = spec["name"]

            print(f"Exporting {name} " + f"from {_PREP_SCHEMA}.{spec['view']}...")

            dataset = _export_one(
                conn=conn,
                settings=settings,
                shard_directory=shard_directory,
                spec=spec,
            )

            datasets[name] = dataset

            gibibytes = dataset["bytes"] / (1024**3)

            print(
                f"  rows={dataset['rows']:,}, "
                + f"shards={len(dataset['shards'])}, "
                + f"size={gibibytes:.3f} GiB"
            )

        _assert_no_empty_datasets(datasets)

        conn.commit()

    manifest: ExportManifest = {
        "format_version": 2,
        "created_at": datetime.now(UTC).isoformat(),
        "graphname": _TARGET_GRAPH,
        "source_database": database_name,
        "source_schema": SOURCE_SCHEMA,
        "preparation_schema": _PREP_SCHEMA,
        "separator": "|",
        "header": False,
        "null_representation": "",
        "datasets": datasets,
    }

    manifest_path = export_directory / "export_manifest.json"

    _write_manifest(
        manifest_path,
        manifest,
    )

    print("Export manifest written to " + str(manifest_path))

    return manifest
