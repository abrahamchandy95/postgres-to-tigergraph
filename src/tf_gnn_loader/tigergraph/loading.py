"""Upload exported PSV shards and run TransactionFraud_GNN loading jobs.

The PostgreSQL exporter creates:

    artifacts/tf_gnn_load/
    ├── export_manifest.json
    └── shards/
        └── *.psv

This module reads that manifest, validates every local shard, uploads the
shards sequentially, executes the corresponding TigerGraph loading job, and
records completed shards in tigergraph_load_state.json.

A completed shard is skipped on later runs. If execution is interrupted
during a shard, that shard is attempted again on the next run.
"""

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Any, TypedDict, cast

import requests

from tf_gnn_loader.postgres.export import (
    ALLOWED_EMPTY_DATASETS,
    EXPORT_DATASET_NAMES,
)
from tf_gnn_loader.postgres.settings import (
    Settings as PostgresSettings,
)
from tf_gnn_loader.tigergraph.client import Client
from tf_gnn_loader.tigergraph.gsql_paths import (
    gsql_path,
)
from tf_gnn_loader.tigergraph.settings import (
    Settings as TigerGraphSettings,
)


_GRAPHNAME = "TransactionFraud_GNN"

_MANIFEST_FILENAME = "export_manifest.json"
_STATE_FILENAME = "tigergraph_load_state.json"

_SEPARATOR = "|"
_END_OF_LINE = "\n"

# TigerGraph REST++ execution deadline sent through GSQL-TIMEOUT.
#
# The unit is milliseconds:
#   86,400,000 ms = 24 hours
#
# This applies independently to every uploaded shard.
_GSQL_TIMEOUT_MILLISECONDS = 86_400_000

# Disable Requests' client-side connect/read/socket timeout for shard uploads.
#
# The server-side operation remains bounded by GSQL-TIMEOUT above. A genuine
# connection failure can still raise ConnectionError and enter the retry path.
_HTTP_TIMEOUT: None = None

_MAX_UPLOAD_ATTEMPTS = 5

_RETRY_BACKOFF_SECONDS = (
    5.0,
    15.0,
    30.0,
    60.0,
)

_RETRYABLE_HTTP_STATUS_CODES = frozenset(
    {
        # Savanna's nginx front proxy answers 499 when it cuts a
        # long-running /ddl upload (upstream slow under rebuild/Kafka
        # pressure). Same transient family as the gateway trio below:
        # the shard is not recorded, RESTPP wrote nothing, re-send is
        # safe (observed 2026-07-30, shard 7/56 of 06_transactions).
        499,
        502,
        503,
        504,
    }
)

_MINIMUM_SIZE_LIMIT_BYTES = 128_000_000
_SIZE_LIMIT_PADDING_BYTES = 1_000_000

_EXPECTED_MANIFEST_FORMAT_VERSION = 1

# A fresh export must not be appended to a graph that already contains
# loaded data. This is every vertex type the schema declares --- all 12 ---
# because the schema carries no derived or build-output types at all: "no
# corpus-wide count, fraud rate, risk score derived from future labels,
# embedding, or model output is persisted on an entity vertex". A non-empty
# graph therefore means a previous load, never a topology build.
_LOAD_TARGET_VERTEX_TYPES: tuple[str, ...] = (
    "Party",
    "Account",
    "Card",
    "Merchant",
    "Merchant_Location",
    "Device",
    "IP_Address",
    "Email",
    "Phone",
    "Address",
    "Identity_Document",
    "Payment_Transaction",
)

_FAILURE_STATISTIC_KEYS = frozenset(
    {
        "invalidline",
        "errorline",
        "rejectedline",
        "invalidobject",
        "rejectedobject",
    }
)


_LOADING_JOB_PATTERN = re.compile(
    r"""
    CREATE\s+LOADING\s+JOB
    \s+
    (?P<job_name>[A-Za-z_][A-Za-z0-9_]*)
    \s+
    FOR\s+GRAPH\s+TransactionFraud_GNN
    \s*
    \{
    (?P<body>.*?)
    \}
    """,
    flags=(re.IGNORECASE | re.DOTALL | re.VERBOSE),
)


_FILENAME_PATTERN = re.compile(
    r"""
    DEFINE\s+FILENAME
    \s+
    (?P<file_tag>[A-Za-z_][A-Za-z0-9_]*)
    \s*
    ;
    """,
    flags=(re.IGNORECASE | re.VERBOSE),
)


class LoadReport(TypedDict):
    graphname: str
    manifest_path: str
    state_path: str
    datasets: int
    loaded_shards: int
    skipped_shards: int
    loaded_rows: int
    loaded_bytes: int


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256(
    path: Path,
) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(8 * 1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def _read_json(
    path: Path,
) -> dict[str, object]:
    if not path.is_file():
        raise RuntimeError("Required JSON file does not exist: " + str(path))

    try:
        raw: object = json.loads(
            path.read_text(
                encoding="utf-8",
            )
        )

    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {path}: {exc}") from exc

    return _mapping(
        raw,
        str(path),
    )


def _write_json_atomic(
    path: Path,
    value: dict[str, object],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = path.with_suffix(path.suffix + ".tmp")

    _ = temporary_path.write_text(
        json.dumps(
            value,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    temporary_path.replace(path)


def _mapping(
    value: object,
    context: str,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(
            f"Expected an object for {context}, got " + type(value).__name__
        )

    result: dict[str, object] = {}

    for key, item in value.items():
        if not isinstance(key, str):
            raise RuntimeError(f"Expected string keys for {context}")

        result[key] = item

    return result


def _list(
    value: object,
    context: str,
) -> list[object]:
    if not isinstance(value, list):
        raise RuntimeError(
            f"Expected a list for {context}, got " + type(value).__name__
        )

    return value


def _string(
    value: object,
    context: str,
) -> str:
    if not isinstance(value, str):
        raise RuntimeError(
            f"Expected text for {context}, got " + f"{type(value).__name__}: {value!r}"
        )

    return value


def _integer(
    value: object,
    context: str,
) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"Expected an integer for {context}, " + "got boolean")

    if isinstance(value, int):
        return value

    raise RuntimeError(
        f"Expected an integer for {context}, got "
        + f"{type(value).__name__}: {value!r}"
    )


def _read_loading_job_file_tags() -> dict[str, str]:
    """Discover the FILENAME variable used by every loading job."""

    path = gsql_path("loading_jobs")

    source = path.read_text(
        encoding="utf-8",
    )

    if not source.strip():
        raise RuntimeError(f"GSQL file is empty: {path}")

    # The superseded graph. TF_GNN declared Merchant_Category, City,
    # State, Zipcode, Full_Name, Birthdate and the split attributes, none
    # of which exist now, so a job left targeting it would install
    # against a graph this loader can no longer produce data for.
    if "TF_GNN" in source.replace("TransactionFraud_GNN", ""):
        raise RuntimeError(
            "Superseded graph name TF_GNN found in "
            + f"{path}; the target is TransactionFraud_GNN"
        )

    if "USE GRAPH TransactionFraud_GNN" not in source:
        raise RuntimeError(f"{path} must contain 'USE GRAPH TransactionFraud_GNN'")

    result: dict[str, str] = {}

    for job_match in _LOADING_JOB_PATTERN.finditer(source):
        job_name = job_match.group("job_name")

        body = job_match.group("body")

        file_tags = [
            match.group("file_tag") for match in (_FILENAME_PATTERN.finditer(body))
        ]

        if len(file_tags) != 1:
            raise RuntimeError(
                f"Loading job {job_name!r} must "
                + "define exactly one FILENAME "
                + "variable; found "
                + str(len(file_tags))
            )

        if job_name in result:
            raise RuntimeError("Duplicate loading job declaration: " + job_name)

        result[job_name] = file_tags[0]

    if not result:
        raise RuntimeError(
            "No TransactionFraud_GNN loading jobs were found " + f"in {path}"
        )

    return result


def _manifest_hash(
    manifest_path: Path,
) -> str:
    return _sha256(manifest_path)


def _new_state(
    manifest_digest: str,
) -> dict[str, object]:
    return {
        "format_version": 1,
        "graphname": _GRAPHNAME,
        "manifest_sha256": manifest_digest,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "completed_shards": {},
    }


def _read_or_create_state(
    state_path: Path,
    manifest_digest: str,
) -> dict[str, object]:
    if not state_path.exists():
        return _new_state(manifest_digest)

    state = _read_json(state_path)

    graphname = _string(
        state.get("graphname"),
        "load-state graphname",
    )

    if graphname != _GRAPHNAME:
        raise RuntimeError(
            f"{state_path} belongs to graph " + f"{graphname!r}, not {_GRAPHNAME!r}"
        )

    stored_manifest_digest = _string(
        state.get("manifest_sha256"),
        "load-state manifest SHA-256",
    )

    if stored_manifest_digest != manifest_digest:
        raise RuntimeError(
            f"{state_path} belongs to a different "
            + "export manifest. Remove the state "
            + "file only after deciding whether "
            + "the previous partially loaded graph "
            + "should be cleared."
        )

    _ = _mapping(
        state.get("completed_shards"),
        "completed_shards",
    )

    return state


def _resolve_shard_path(
    export_directory: Path,
    manifest_path_value: str,
) -> Path:
    path = Path(manifest_path_value).expanduser()

    if not path.is_absolute():
        path = export_directory / path

    return path.resolve()


def _response_text(
    response: object,
) -> str:
    if isinstance(response, str):
        return response

    return json.dumps(
        response,
        indent=2,
        default=str,
    )


def _contains_explicit_error(
    value: object,
) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).strip().lower() == "error":
                if item in (
                    False,
                    None,
                    0,
                    "",
                    "0",
                    "false",
                    "False",
                ):
                    continue

                return True

            if _contains_explicit_error(item):
                return True

        return False

    if isinstance(value, list):
        return any(_contains_explicit_error(item) for item in value)

    return False


def _normalized_key(
    value: object,
) -> str:
    return re.sub(
        r"[^a-z0-9]",
        "",
        str(value).lower(),
    )


def _collect_integer_statistics(
    value: object,
    key_name: str,
) -> list[int]:
    target = _normalized_key(key_name)

    result: list[int] = []

    if isinstance(value, dict):
        for key, item in value.items():
            if (
                _normalized_key(key) == target
                and isinstance(item, int)
                and not isinstance(item, bool)
            ):
                result.append(item)

            result.extend(
                _collect_integer_statistics(
                    item,
                    key_name,
                )
            )

    elif isinstance(value, list):
        for item in value:
            result.extend(
                _collect_integer_statistics(
                    item,
                    key_name,
                )
            )

    return result


def _run_loading_job_with_file(
    client: Client,
    session: requests.Session,
    shard_path: Path,
    file_tag: str,
    loading_job: str,
    size_limit: int,
) -> object:
    """Upload one shard using a 24-hour TigerGraph deadline.

    The request uses TigerGraph's POST /ddl/{graph_name} loading endpoint.
    Requests' own timeout is disabled, preventing the old 30-second socket
    timeout from terminating a large request-body upload.

    TigerGraph itself receives GSQL-TIMEOUT=86,400,000 milliseconds.
    """

    conn = cast(
        Any,
        client.conn,
    )

    url = f"{conn.restppUrl}" + f"/ddl/{conn.graphname}"

    params = {
        "tag": loading_job,
        "filename": file_tag,
        "sep": _SEPARATOR,
        "eol": _END_OF_LINE,
    }

    requested_headers = {
        "Content-Type": ("application/x-www-form-urlencoded; " + "Charset=utf-8"),
        "RESPONSE-LIMIT": str(size_limit),
        "GSQL-TIMEOUT": str(_GSQL_TIMEOUT_MILLISECONDS),
    }

    prepared_headers, _, verify = conn._prep_req(
        "token",
        requested_headers,
        url,
        "POST",
        None,
    )

    last_error: BaseException | None = None

    for attempt in range(
        1,
        _MAX_UPLOAD_ATTEMPTS + 1,
    ):
        try:
            with shard_path.open("rb") as data:
                response = session.post(
                    url,
                    params=params,
                    data=data,
                    headers=prepared_headers,
                    verify=verify,
                    timeout=_HTTP_TIMEOUT,
                )

            if (
                response.status_code in _RETRYABLE_HTTP_STATUS_CODES
                and attempt < _MAX_UPLOAD_ATTEMPTS
            ):
                delay = _RETRY_BACKOFF_SECONDS[
                    min(
                        attempt - 1,
                        len(_RETRY_BACKOFF_SECONDS) - 1,
                    )
                ]

                print(
                    "  transient TigerGraph HTTP "
                    + f"{response.status_code}; "
                    + f"retrying in {delay:.0f}s "
                    + f"(attempt {attempt + 1}/"
                    + f"{_MAX_UPLOAD_ATTEMPTS})"
                )

                time.sleep(delay)

                continue

            response.raise_for_status()

            try:
                payload: object = response.json()

            except requests.exceptions.JSONDecodeError as exc:
                raise RuntimeError(
                    "TigerGraph returned a non-JSON "
                    + "loading response: "
                    + response.text[:2_000]
                ) from exc

            if isinstance(payload, dict):
                if payload.get("error") not in (
                    False,
                    None,
                    0,
                    "",
                    "0",
                    "false",
                    "False",
                ):
                    raise RuntimeError(
                        "TigerGraph loading request "
                        + "failed:\n"
                        + _response_text(payload)
                    )

                results = payload.get("results")

                if results is not None:
                    return results

            return payload

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            last_error = exc

            if attempt >= _MAX_UPLOAD_ATTEMPTS:
                break

            delay = _RETRY_BACKOFF_SECONDS[
                min(
                    attempt - 1,
                    len(_RETRY_BACKOFF_SECONDS) - 1,
                )
            ]

            print("  transient upload failure: " + f"{type(exc).__name__}: {exc}")

            print(
                f"  retrying in {delay:.0f}s "
                + f"(attempt {attempt + 1}/"
                + f"{_MAX_UPLOAD_ATTEMPTS})"
            )

            time.sleep(delay)

    raise RuntimeError(
        f"Unable to upload {shard_path.name} "
        + f"after {_MAX_UPLOAD_ATTEMPTS} "
        + "attempts"
    ) from last_error


def _validate_loading_response(
    response: object,
    expected_rows: int,
    shard_path: Path,
) -> None:
    if _contains_explicit_error(response):
        raise RuntimeError(
            "TigerGraph reported an error while "
            + f"loading {shard_path}:\n"
            + _response_text(response)
        )

    failure_statistics: dict[
        str,
        int,
    ] = {}

    for key in _FAILURE_STATISTIC_KEYS:
        values = _collect_integer_statistics(
            response,
            key,
        )

        total = sum(values)

        if total:
            failure_statistics[key] = total

    if failure_statistics:
        details = ", ".join(
            f"{key}={value}" for key, value in sorted(failure_statistics.items())
        )

        raise RuntimeError(
            "TigerGraph reported rejected data "
            + f"for {shard_path}: "
            + details
            + "\n"
            + _response_text(response)
        )

    valid_line_counts = _collect_integer_statistics(
        response,
        "validLine",
    )

    if (
        valid_line_counts
        and expected_rows not in valid_line_counts
        and sum(valid_line_counts) != expected_rows
    ):
        raise RuntimeError(
            "TigerGraph valid-line count "
            + f"mismatch for {shard_path}: "
            + f"expected={expected_rows}, "
            + f"reported={valid_line_counts}\n"
            + _response_text(response)
        )


def _validate_manifest(
    manifest: dict[str, object],
) -> dict[str, object]:
    format_version = _integer(
        manifest.get("format_version"),
        "manifest format_version",
    )

    if format_version != _EXPECTED_MANIFEST_FORMAT_VERSION:
        raise RuntimeError(
            "Unsupported export manifest " + "format_version: " + str(format_version)
        )

    graphname = _string(
        manifest.get("graphname"),
        "manifest graphname",
    )

    if graphname != _GRAPHNAME:
        raise RuntimeError(
            "Export manifest targets "
            + f"{graphname!r}, but this loader "
            + f"targets {_GRAPHNAME!r}"
        )

    separator = _string(
        manifest.get("separator"),
        "manifest separator",
    )

    if separator != _SEPARATOR:
        raise RuntimeError(
            "Expected manifest separator " + f"{_SEPARATOR!r}, got " + f"{separator!r}"
        )

    header_value = manifest.get("header")

    if header_value is not False:
        raise RuntimeError(
            "TransactionFraud_GNN shard files must not " + "contain a header row"
        )

    datasets = _mapping(
        manifest.get("datasets"),
        "manifest datasets",
    )

    if not datasets:
        raise RuntimeError("Export manifest contains no datasets")

    return datasets


def _validate_remote_loading_jobs(
    client: Client,
    loading_jobs: set[str],
) -> None:
    response = client.gsql(f"USE GRAPH {_GRAPHNAME}\n" + "SHOW JOB *")

    missing = sorted(
        job_name
        for job_name in loading_jobs
        if re.search(
            rf"\b{re.escape(job_name)}\b",
            response,
        )
        is None
    )

    if missing:
        raise RuntimeError(
            "TigerGraph is missing installed " + "loading jobs: " + ", ".join(missing)
        )


def _assert_fresh_graph_is_empty(
    client: Client,
    state_exists: bool,
) -> None:
    if state_exists:
        return

    raw_counts: object = client.conn.getVertexCount(
        "*",
        realtime=True,
    )

    counts = _mapping(
        raw_counts,
        "TigerGraph vertex counts",
    )

    nonzero: dict[str, int] = {}

    for vertex_type in _LOAD_TARGET_VERTEX_TYPES:
        value = counts.get(
            vertex_type,
            0,
        )

        if not isinstance(value, int) or isinstance(value, bool):
            raise RuntimeError(
                "Unexpected TigerGraph count for " + f"{vertex_type}: {value!r}"
            )

        if value:
            nonzero[vertex_type] = value

    if nonzero:
        details = ", ".join(
            f"{name}={count:,}" for name, count in sorted(nonzero.items())
        )

        raise RuntimeError(
            "No tigergraph_load_state.json "
            + "exists, but TransactionFraud_GNN "
            + "already "
            + "contains source-loaded vertices. "
            + "Refusing to append a fresh export "
            + "to a non-empty graph: "
            + details
        )


def _preflight(
    export_directory: Path,
    datasets: dict[str, object],
    loading_job_file_tags: dict[
        str,
        str,
    ],
) -> None:
    """Validate all jobs and local shards before contacting TigerGraph."""

    missing_datasets = sorted(EXPORT_DATASET_NAMES - set(datasets))

    if missing_datasets:
        raise RuntimeError(
            "The export manifest is missing datasets: "
            + ", ".join(missing_datasets)
            + ". Re-run `tf-gnn-load export`."
        )

    missing_jobs: set[str] = set()

    for dataset_name in sorted(datasets):
        dataset = _mapping(
            datasets[dataset_name],
            f"dataset {dataset_name}",
        )

        loading_job = _string(
            dataset.get("loading_job"),
            f"{dataset_name} loading job",
        )

        if loading_job not in loading_job_file_tags:
            missing_jobs.add(loading_job)

        dataset_rows = _integer(
            dataset.get("rows"),
            f"{dataset_name} rows",
        )

        dataset_bytes = _integer(
            dataset.get("bytes"),
            f"{dataset_name} bytes",
        )

        if dataset_rows < 0 or dataset_bytes < 0:
            raise RuntimeError(f"{dataset_name} has negative " + "row or byte totals")

        if dataset_rows == 0 and dataset_name not in ALLOWED_EMPTY_DATASETS:
            raise RuntimeError(
                f"{dataset_name} has zero rows "
                + "in the export manifest; its "
                + "vertices and edges would be "
                + "missing from "
                + _GRAPHNAME
                + ". Re-run `tf-gnn-load export` "
                + "against corrected source data."
            )

        shards = _list(
            dataset.get("shards"),
            f"{dataset_name} shards",
        )

        if dataset_rows > 0 and not shards:
            raise RuntimeError(
                f"{dataset_name} has " + f"{dataset_rows} rows but " + "no shards"
            )

        shard_row_total = 0
        shard_byte_total = 0

        for shard_index, shard_value in enumerate(
            shards,
            start=1,
        ):
            shard = _mapping(
                shard_value,
                f"{dataset_name} shard " + str(shard_index),
            )

            shard_path = _resolve_shard_path(
                export_directory,
                _string(
                    shard.get("path"),
                    f"{dataset_name} " + "shard path",
                ),
            )

            if not shard_path.is_file():
                raise RuntimeError("Missing export shard: " + str(shard_path))

            expected_rows = _integer(
                shard.get("rows"),
                f"{dataset_name} " + "shard rows",
            )

            expected_bytes = _integer(
                shard.get("bytes"),
                f"{dataset_name} " + "shard bytes",
            )

            if expected_rows <= 0:
                raise RuntimeError(f"{shard_path} has a " + "non-positive row count")

            if expected_bytes <= 0:
                raise RuntimeError(f"{shard_path} has a " + "non-positive byte count")

            shard_row_total += expected_rows
            shard_byte_total += expected_bytes

            actual_bytes = shard_path.stat().st_size

            if actual_bytes != expected_bytes:
                raise RuntimeError(
                    "Shard size mismatch for "
                    + f"{shard_path}: "
                    + f"manifest={expected_bytes}, "
                    + f"actual={actual_bytes}"
                )

        if shard_row_total != dataset_rows:
            raise RuntimeError(
                f"{dataset_name} row total "
                + "mismatch: "
                + f"dataset={dataset_rows}, "
                + f"shards={shard_row_total}"
            )

        if shard_byte_total != dataset_bytes:
            raise RuntimeError(
                f"{dataset_name} byte total "
                + "mismatch: "
                + f"dataset={dataset_bytes}, "
                + f"shards={shard_byte_total}"
            )

    if missing_jobs:
        raise RuntimeError(
            "The export manifest references "
            + "loading jobs that are missing "
            + "from gsql/loading_jobs.gsql: "
            + ", ".join(sorted(missing_jobs))
        )


def load(
    tigergraph_settings: TigerGraphSettings,
    export_directory: Path,
) -> LoadReport:
    """Load every exported PSV shard into TransactionFraud_GNN."""

    if tigergraph_settings.graphname != _GRAPHNAME:
        raise RuntimeError(
            "GRAPHNAME must be "
            + f"{_GRAPHNAME!r}, got "
            + repr(tigergraph_settings.graphname)
        )

    export_directory = export_directory.expanduser().resolve()

    manifest_path = export_directory / _MANIFEST_FILENAME

    state_path = export_directory / _STATE_FILENAME

    manifest = _read_json(manifest_path)

    datasets = _validate_manifest(manifest)

    loading_job_file_tags = _read_loading_job_file_tags()

    _preflight(
        export_directory=export_directory,
        datasets=datasets,
        loading_job_file_tags=(loading_job_file_tags),
    )

    manifest_digest = _manifest_hash(manifest_path)

    state_exists = state_path.exists()

    client = Client(tigergraph_settings)

    _validate_remote_loading_jobs(
        client,
        set(loading_job_file_tags),
    )

    _assert_fresh_graph_is_empty(
        client,
        state_exists=state_exists,
    )

    state = _read_or_create_state(
        state_path=state_path,
        manifest_digest=manifest_digest,
    )

    if not state_exists:
        _write_json_atomic(
            state_path,
            state,
        )

    completed_shards = _mapping(
        state.get("completed_shards"),
        "completed_shards",
    )

    loaded_shards = 0
    skipped_shards = 0
    loaded_rows = 0
    loaded_bytes = 0

    upload_session = requests.Session()

    try:
        for dataset_name in sorted(datasets):
            dataset = _mapping(
                datasets[dataset_name],
                f"dataset {dataset_name}",
            )

            loading_job = _string(
                dataset.get("loading_job"),
                f"{dataset_name} " + "loading job",
            )

            file_tag = loading_job_file_tags[loading_job]

            shards = _list(
                dataset.get("shards"),
                f"{dataset_name} shards",
            )

            print(
                f"{dataset_name}: " + f"job={loading_job}, " + f"shards={len(shards)}"
            )

            for (
                shard_index,
                shard_value,
            ) in enumerate(
                shards,
                start=1,
            ):
                shard = _mapping(
                    shard_value,
                    f"{dataset_name} shard " + str(shard_index),
                )

                shard_path = _resolve_shard_path(
                    export_directory,
                    _string(
                        shard.get("path"),
                        f"{dataset_name} " + "shard path",
                    ),
                )

                expected_sha256 = _string(
                    shard.get("sha256"),
                    f"{dataset_name} " + "shard SHA-256",
                )

                shard_rows = _integer(
                    shard.get("rows"),
                    f"{dataset_name} " + "shard rows",
                )

                shard_bytes = _integer(
                    shard.get("bytes"),
                    f"{dataset_name} " + "shard bytes",
                )

                shard_key = dataset_name + "/" + shard_path.name

                if shard_key in completed_shards:
                    skipped_shards += 1

                    print("  skipping completed " + "shard " + shard_path.name)

                    continue

                print(
                    "  validating shard "
                    + f"{shard_index}/"
                    + f"{len(shards)}: "
                    + shard_path.name
                )

                actual_sha256 = _sha256(shard_path)

                if actual_sha256 != expected_sha256:
                    raise RuntimeError(
                        "SHA-256 mismatch for "
                        + f"{shard_path}: "
                        + "manifest="
                        + expected_sha256
                        + ", actual="
                        + actual_sha256
                    )

                size_limit = max(
                    _MINIMUM_SIZE_LIMIT_BYTES,
                    shard_bytes + _SIZE_LIMIT_PADDING_BYTES,
                )

                print("  uploading " + shard_path.name + f" ({shard_rows:,} rows)")

                response = _run_loading_job_with_file(
                    client=client,
                    session=upload_session,
                    shard_path=shard_path,
                    file_tag=file_tag,
                    loading_job=loading_job,
                    size_limit=size_limit,
                )

                _validate_loading_response(
                    response=response,
                    expected_rows=shard_rows,
                    shard_path=shard_path,
                )

                completed_shards[shard_key] = {
                    "dataset": dataset_name,
                    "path": str(shard_path),
                    "loading_job": loading_job,
                    "file_tag": file_tag,
                    "rows": shard_rows,
                    "bytes": shard_bytes,
                    "sha256": actual_sha256,
                    "loaded_at": _utc_now(),
                    "response": _response_text(response),
                }

                state["completed_shards"] = completed_shards

                state["updated_at"] = _utc_now()

                _write_json_atomic(
                    state_path,
                    state,
                )

                loaded_shards += 1
                loaded_rows += shard_rows
                loaded_bytes += shard_bytes

                print("  completed " + shard_path.name)

    finally:
        upload_session.close()

    report: LoadReport = {
        "graphname": _GRAPHNAME,
        "manifest_path": str(manifest_path),
        "state_path": str(state_path),
        "datasets": len(datasets),
        "loaded_shards": loaded_shards,
        "skipped_shards": skipped_shards,
        "loaded_rows": loaded_rows,
        "loaded_bytes": loaded_bytes,
    }

    print(
        "TigerGraph loading complete: "
        + f"loaded_shards={loaded_shards}, "
        + f"skipped_shards={skipped_shards}, "
        + f"loaded_rows={loaded_rows:,}"
    )

    return report


def main() -> int:
    """Run the loader without requiring cli.py."""

    postgres_settings = PostgresSettings()

    tigergraph_settings = TigerGraphSettings()

    report = load(
        tigergraph_settings=(tigergraph_settings),
        export_directory=(postgres_settings.export_dir),
    )

    print(
        json.dumps(
            report,
            indent=2,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
