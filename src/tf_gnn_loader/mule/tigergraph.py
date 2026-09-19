"""Schema-gated, resumable temporal uploads and exact post-load validation."""

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from typing import Any, Iterator

import requests

from tf_gnn_loader.postgres.export import ShardWriter
from tf_gnn_loader.tigergraph.admin import assert_gsql_succeeded
from tf_gnn_loader.tigergraph.client import Client
from tf_gnn_loader.tigergraph.loading import (
    read_json,
    run_loading_job_with_file,
    sha256,
    validate_loading_response,
    write_json_atomic,
)
from tf_gnn_loader.tigergraph.settings import Settings
from .contract import DATASETS, FORMAT_VERSION, GRAPH, GSQL, VERTICES
from .gsql import QUERY, loading_jobs, verify_query
from .settings import MuleSettings


@contextmanager
def directory_lock(directory: Path) -> Iterator[None]:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".pipeline.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                "Another temporal pipeline is using this export directory"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def validate_schema(schema: dict[str, Any]) -> None:
    if schema.get("GraphName") != GRAPH:
        raise RuntimeError(f"The target schema must belong to {GRAPH}")
    vertices = {v["Name"]: v for v in schema["VertexTypes"]}
    edges = {e["Name"]: e for e in schema["EdgeTypes"]}
    if set(vertices) != {d.name for d in VERTICES}:
        raise RuntimeError(
            "Temporal vertex types do not match the requested schema; no automatic migration is permitted"
        )
    required_edges = {d.name for d in DATASETS if not d.vertex}
    permitted_edges = required_edges | {d.reverse for d in DATASETS if not d.vertex}
    if not required_edges <= set(edges) or set(edges) - permitted_edges:
        raise RuntimeError("Temporal edge types do not match the requested schema")
    for d in DATASETS:
        item = vertices[d.name] if d.vertex else edges[d.name]
        attributes = list(item["Attributes"])
        if d.vertex:
            primary = item["PrimaryId"]
            if primary.get("PrimaryIdAsAttribute") is not True:
                raise RuntimeError(f"Missing primary ID attribute on {d.name}")
            attributes.insert(0, primary)
            expected = d.graph_fields
        else:
            expected = d.fields[2:]
            if (
                item.get("FromVertexTypeName"),
                item.get("ToVertexTypeName"),
                item.get("Config", {}).get("REVERSE_EDGE"),
            ) != (d.source, d.target, d.reverse):
                raise RuntimeError(f"Endpoint or reverse-edge mismatch for {d.name}")
            discriminators = [
                a["AttributeName"] for a in attributes if a.get("IsDiscriminator")
            ]
            if discriminators != (["valid_from_seq"] if d.association else []):
                raise RuntimeError(f"Discriminator mismatch for {d.name}")
        actual = tuple(
            (
                a["AttributeName"],
                a["AttributeType"]["Name"]
                + (
                    "<" + a["AttributeType"]["ValueTypeName"] + ">"
                    if a["AttributeType"]["Name"] == "LIST"
                    else ""
                ),
            )
            for a in attributes
        )
        if actual != expected:
            raise RuntimeError(
                f"Attribute type/order mismatch for {d.name}: expected {expected}, got {actual}"
            )


def checked_client(settings: Settings) -> Client:
    if settings.graphname != GRAPH:
        raise RuntimeError(f"The mule-temporal use case requires GRAPHNAME={GRAPH}")
    client = Client(settings)
    validate_schema(client.conn.getSchema(force=True))
    return client


def _gsql(client: Client, source: str) -> str:
    response = client.gsql(source)
    assert_gsql_succeeded(response, "Temporal GSQL installation")
    if re.search(
        r"(?im)^.*(?:error:|exception:|syntax error|installation failed)", response
    ):
        raise RuntimeError(response)
    return response


def _show_jobs(client: Client) -> str:
    response = client.gsql(f"USE GRAPH {GRAPH}\nSHOW JOB *")
    # TigerGraph 4.2 reports an empty job catalog as a semantic error.
    if "The job * doesn't exist!" in response:
        return ""
    assert_gsql_succeeded(response, "Listing temporal loading jobs")
    return response


def install(settings: Settings) -> dict[str, Any]:
    client = checked_client(settings)
    if (GSQL / "loading_jobs.gsql").read_text() != loading_jobs() or (
        GSQL / "verify_load.gsql"
    ).read_text() != verify_query():
        raise RuntimeError("Generated GSQL is stale; run scripts/generate_mule_gsql.py")
    installed = _show_jobs(client)
    existing = [d.job for d in DATASETS if re.search(rf"\b{d.job}\b", installed)]
    if existing:
        _gsql(client, f"USE GRAPH {GRAPH}\nDROP JOB " + ", ".join(existing))
    print("Installing 27 temporal loading jobs...", flush=True)
    _gsql(client, loading_jobs())
    installed = _show_jobs(client)
    if any(not re.search(rf"\b{d.job}\b", installed) for d in DATASETS):
        raise RuntimeError("Not every temporal loading job was installed")
    print("Compiling temporal load validation...", flush=True)
    _gsql(client, verify_query())
    return {"graphname": GRAPH, "loading_jobs": len(DATASETS), "query": QUERY}


def read_manifest(directory: Path, *, check_files: bool = True) -> dict[str, Any]:
    manifest: dict[str, Any] = read_json(directory / "export_manifest.json")
    if (
        manifest.get("format_version"),
        manifest.get("graphname"),
        manifest.get("separator"),
        manifest.get("header"),
    ) != (FORMAT_VERSION, GRAPH, "|", False):
        raise RuntimeError("Not a temporal mule export manifest")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != {d.name for d in DATASETS}:
        raise RuntimeError("Temporal manifest must contain exactly 27 datasets")
    if (
        not isinstance(manifest.get("audit"), dict)
        or manifest["audit"].get("passed") is not True
    ):
        raise RuntimeError("Temporal manifest has no passing source audit")
    seen: set[Path] = set()
    for d in DATASETS:
        data = datasets[d.name]
        if data.get("loading_job") != d.job:
            raise RuntimeError(f"Wrong loading job for {d.name}")
        total_rows = total_bytes = 0
        for shard in data["shards"]:
            path = Path(shard["path"]).resolve()
            if not path.is_relative_to(directory.resolve()) or path in seen:
                raise RuntimeError(
                    "Shard is outside this export or used more than once"
                )
            seen.add(path)
            if (
                not isinstance(shard["rows"], int)
                or shard["rows"] <= 0
                or shard["bytes"] <= 0
            ):
                raise RuntimeError(f"Invalid shard totals for {d.name}")
            if check_files and (
                path.stat().st_size != shard["bytes"] or sha256(path) != shard["sha256"]
            ):
                raise RuntimeError(f"Temporal shard was changed: {path.name}")
            total_rows += shard["rows"]
            total_bytes += shard["bytes"]
        if (total_rows, total_bytes) != (
            data["rows"],
            data["bytes"],
        ) or total_rows != manifest["audit"]["counts"][d.name]:
            raise RuntimeError(f"Manifest row/byte totals disagree for {d.name}")
    return manifest


def upload_shard(
    client: Client,
    session: requests.Session,
    path: Path,
    rows: int,
    job: str,
    upload_bytes: int,
) -> None:
    """Bound gateway requests without changing the immutable source manifest.

    Completion remains per original shard. A partially sent shard can be
    replayed because vertex IDs and edge discriminators are stable, and every
    attribute is identical to the original audited snapshot.
    """

    def send(part: Path, part_rows: int) -> None:
        try:
            response = run_loading_job_with_file(
                client,
                session,
                part,
                "data",
                job,
                max(128_000_000, part.stat().st_size + 1_000_000),
            )
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                try:
                    payload = exc.response.json()
                except ValueError:
                    payload = {}
                if (
                    isinstance(payload, dict)
                    and "exceeds the license limit"
                    in str(payload.get("message", "")).lower()
                ):
                    raise RuntimeError(
                        "TigerGraph rejected loading because it exceeds the license limit. "
                        "Increase the instance's licensed capacity, then resume with the same "
                        "MULE_EXPORT_DIR. Completed shards are retained; do not clear the graph again."
                    ) from exc
            raise
        validate_loading_response(response, part_rows, part)

    if path.stat().st_size <= upload_bytes:
        send(path, rows)
        return
    with TemporaryDirectory(prefix="mt-upload-", dir=path.parent) as temporary:
        writer = ShardWriter(Path(temporary), path.stem, upload_bytes)
        try:
            with path.open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    writer.write_copy_chunk(block)
        finally:
            parts = writer.finish()
        if sum(p["rows"] for p in parts) != rows:
            raise RuntimeError("Upload partitioning changed the source row count")
        for index, part in enumerate(parts, 1):
            if part["bytes"] > upload_bytes:
                raise RuntimeError("A single source row exceeds MULE_UPLOAD_BYTES")
            print(f"  request {index}/{len(parts)}: {part['rows']:,} rows", flush=True)
            send(Path(part["path"]), part["rows"])


def load(settings: Settings, directory: Path) -> dict[str, Any]:
    directory = directory.resolve()
    with directory_lock(directory):
        manifest = read_manifest(directory)
        identity = {
            "host": settings.host,
            "graphname": GRAPH,
            "manifest_sha256": sha256(directory / "export_manifest.json"),
            "loading_jobs_sha256": sha256(GSQL / "loading_jobs.gsql"),
        }
        state_path = directory / "tigergraph_load_state.json"
        state: dict[str, Any] = (
            read_json(state_path)
            if state_path.exists()
            else {**identity, "completed_shards": {}}
        )
        if any(state.get(k) != v for k, v in identity.items()):
            raise RuntimeError(
                "Load state belongs to another host, export, or loading contract"
            )
        client = checked_client(settings)
        counts = client.conn.getVertexCount("*", realtime=True)
        if not isinstance(counts, dict):
            raise RuntimeError("TigerGraph returned invalid vertex counts")
        if not state_path.exists() and any(counts.values()):
            raise RuntimeError(
                "Refusing a fresh load into a populated graph; entity metadata is immutable"
            )
        jobs = _show_jobs(client)
        if any(not re.search(rf"\b{d.job}\b", jobs) for d in DATASETS):
            raise RuntimeError("Install the temporal loading jobs before loading")
        completed = state["completed_shards"]
        expected_keys = {
            d.name + "/" + Path(s["path"]).name
            for d in DATASETS
            for s in manifest["datasets"][d.name]["shards"]
        }
        if set(completed) - expected_keys:
            raise RuntimeError("Load state contains shards outside this export")
        for d in VERTICES:
            data = manifest["datasets"][d.name]
            minimum = sum(
                s["rows"]
                for s in data["shards"]
                if d.name + "/" + Path(s["path"]).name in completed
            )
            if d.name not in counts or not minimum <= counts[d.name] <= data["rows"]:
                raise RuntimeError(
                    f"Remote {d.name} count is inconsistent with resumable load state"
                )
        write_json_atomic(state_path, state)
        loaded = skipped = 0
        options = MuleSettings()

        def upload(
            d_name: str, shard: dict[str, Any], job: str
        ) -> tuple[str, dict[str, Any]]:
            path = Path(shard["path"])
            print(f"Uploading {path.name}: {shard['rows']:,} rows", flush=True)
            with requests.Session() as session:
                upload_shard(
                    client, session, path, shard["rows"], job, options.mule_upload_bytes
                )
            return d_name + "/" + path.name, {
                "sha256": shard["sha256"],
                "rows": shard["rows"],
                "loaded_at": datetime.now(UTC).isoformat(),
            }

        with ThreadPoolExecutor(max_workers=options.mule_upload_workers) as pool:
            for d in DATASETS:
                pending: list[dict[str, Any]] = []
                for shard in manifest["datasets"][d.name]["shards"]:
                    path = Path(shard["path"])
                    key = d.name + "/" + path.name
                    if key in completed:
                        if completed[key]["sha256"] != shard["sha256"]:
                            raise RuntimeError("Completed shard digest mismatch")
                        skipped += 1
                        continue
                    pending.append(shard)
                # Bounded batches and a dataset barrier preserve endpoint-safe
                # ordering. Only the main thread commits successful shard state.
                for start in range(0, len(pending), options.mule_upload_workers):
                    futures = [
                        pool.submit(upload, d.name, shard, d.job)
                        for shard in pending[
                            start : start + options.mule_upload_workers
                        ]
                    ]
                    errors: list[Exception] = []
                    for future in as_completed(futures):
                        try:
                            key, result = future.result()
                        except Exception as exc:
                            errors.append(exc)
                            continue
                        completed[key] = result
                        write_json_atomic(state_path, state)
                        loaded += 1
                    if errors:
                        raise errors[0]
        return {"loaded_shards": loaded, "skipped_shards": skipped, "graphname": GRAPH}


def verify(settings: Settings, directory: Path) -> dict[str, Any]:
    manifest = read_manifest(directory, check_files=False)
    client = checked_client(settings)
    raw = client.run_installed_with_timeout(QUERY, {})
    merged: dict[str, Any] = {}
    for part in raw:
        if isinstance(part, dict):
            merged.update(part)
    if not {"counts", "violations", "account_mule_flags"} <= merged.keys():
        raise RuntimeError("Temporal verification query returned an incomplete result")
    expected = {d.name: manifest["datasets"][d.name]["rows"] for d in DATASETS}
    expected.update({d.reverse: expected[d.name] for d in DATASETS if not d.vertex})
    mismatches = {
        k: {"expected": n, "actual": merged["counts"].get(k, 0)}
        for k, n in expected.items()
        if merged["counts"].get(k, 0) != n
    }
    expected_flags = manifest["audit"]["account_mule_flags"]
    if merged["account_mule_flags"] != expected_flags:
        mismatches["account_mule_flags"] = {
            "expected": expected_flags,
            "actual": merged["account_mule_flags"],
        }
    report: dict[str, Any] = {
        "passed": not mismatches and merged["violations"] == 0,
        "graphname": GRAPH,
        "verified_at": datetime.now(UTC).isoformat(),
        "manifest_sha256": sha256(directory / "export_manifest.json"),
        "counts": merged["counts"],
        "account_mule_flags": merged["account_mule_flags"],
        "violations": merged["violations"],
        "mismatches": mismatches,
    }
    write_json_atomic(directory / "tigergraph_verification.json", report)
    if not report["passed"]:
        raise RuntimeError("Temporal graph verification failed: " + json.dumps(report))
    return report
