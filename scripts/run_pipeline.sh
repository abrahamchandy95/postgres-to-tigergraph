#!/usr/bin/env bash
set -Eeuo pipefail

MODE="${1:-full}"

case "$MODE" in
  full|from-export|load|verify|jobs)
    ;;
  *)
    echo "Usage: $0 {full|from-export|load|verify|jobs}" >&2
    exit 64
    ;;
esac

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd
)"
if [[ -f "${SCRIPT_DIR}/pyproject.toml" ]]; then
  ROOT_DIR="$SCRIPT_DIR"
elif [[ -f "${SCRIPT_DIR}/../pyproject.toml" ]]; then
  ROOT_DIR="$(
    cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1
    pwd
  )"
else
  echo "Could not locate the repository root from: $SCRIPT_DIR" >&2
  exit 1
fi

cd "$ROOT_DIR"

on_error() {
  local exit_code=$?
  echo
  echo "PIPELINE FAILED"
  echo "  mode:    $MODE"
  echo "  line:    ${BASH_LINENO[0]}"
  echo "  command: ${BASH_COMMAND}"
  echo "  code:    $exit_code"
  exit "$exit_code"
}
trap on_error ERR
trap 'echo; echo "Pipeline interrupted."; exit 130' INT TERM

if [[ ! -f "pyproject.toml" ]]; then
  echo "pyproject.toml was not found in: $ROOT_DIR" >&2
  exit 1
fi

if [[ ! -f ".env" ]]; then
  echo ".env was not found in: $ROOT_DIR" >&2
  exit 1
fi

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

if ! command -v python >/dev/null 2>&1; then
  echo "Python is not available." >&2
  exit 1
fi

# Install/update the editable package only when the console command is absent.
if ! command -v tf-gnn-load >/dev/null 2>&1; then
  echo "tf-gnn-load is not installed; installing the project in editable mode..."
  python -m pip install -e .
fi

ARTIFACT_DIR="$(
  python - <<'PY'
from tf_gnn_loader.postgres.settings import Settings
print(Settings().export_dir)
PY
)"

mkdir -p "$ARTIFACT_DIR/logs"

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="${ARTIFACT_DIR}/logs/pipeline_${MODE}_${TIMESTAMP}.log"

exec > >(tee -a "$LOG_FILE") 2>&1

step() {
  local title=$1
  shift

  echo
  echo "================================================================"
  echo "$title"
  echo "================================================================"

  "$@"
}

echo "TransactionFraud_GNN pipeline"
echo "Repository: $ROOT_DIR"
echo "Mode:       $MODE"
echo "Log:        $LOG_FILE"

step "Validating configuration" python - <<'PY'
from tf_gnn_loader.postgres.settings import Settings as PostgresSettings
from tf_gnn_loader.tigergraph.settings import Settings as TigerGraphSettings

postgres = PostgresSettings()
tigergraph = TigerGraphSettings()

if tigergraph.graphname != "TransactionFraud_GNN":
    raise SystemExit(
        "GRAPHNAME must be 'TransactionFraud_GNN', got "
        f"{tigergraph.graphname!r}"
    )

# Settings validation already refuses an unset or short salt. Reaching
# this line means one is configured; printing only that fact keeps it out
# of the pipeline log, which is written to disk and tailed.
print(f"PostgreSQL export directory: {postgres.export_dir}")
print(f"TigerGraph host:             {tigergraph.host}")
print(f"TigerGraph graph:            {tigergraph.graphname}")
print("TigerGraph secret:           configured")
print("PII tokenisation salt:       configured")
PY

if [[ "$MODE" == "from-export" || "$MODE" == "load" ]]; then
  echo
  echo "This mode starts at the first operation that writes data to TigerGraph."
  echo "The export itself remains local until tf-gnn-load load runs below."
fi

if [[ "$MODE" == "jobs" ]]; then
  step "Reinstalling TigerGraph loading jobs" \
    python -m tf_gnn_loader.tigergraph.admin

  echo
  echo "Loading-job installation completed."
  exit 0
fi

if [[ "$MODE" == "full" ]]; then
  step "1/6 Inspecting PostgreSQL source" \
    tf-gnn-load inspect

  step "2/6 Preparing PostgreSQL tables and views" \
    tf-gnn-load prepare

  step "3/6 Auditing prepared PostgreSQL data" \
    tf-gnn-load audit

  step "4/6 Exporting immutable PSV shards" \
    tf-gnn-load export
fi

if [[ "$MODE" == "full" || "$MODE" == "from-export" || "$MODE" == "load" ]]; then
  MANIFEST_PATH="${ARTIFACT_DIR}/export_manifest.json"

  if [[ ! -f "$MANIFEST_PATH" ]]; then
    echo "Required export manifest does not exist: $MANIFEST_PATH" >&2
    echo "Run: $0 full" >&2
    exit 1
  fi

  step "Validating the export manifest" python - "$MANIFEST_PATH" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
manifest = json.loads(path.read_text(encoding="utf-8"))

datasets = manifest.get("datasets")
if not isinstance(datasets, dict):
    raise SystemExit("Manifest has no valid datasets object")

if manifest.get("graphname") != "TransactionFraud_GNN":
    raise SystemExit(
        f"Manifest graphname is {manifest.get('graphname')!r}, "
        "not 'TransactionFraud_GNN'"
    )

if len(datasets) != 24:
    raise SystemExit(f"Expected 24 datasets, found {len(datasets)}")

shards = 0
rows = 0
size = 0

for dataset_name, dataset in datasets.items():
    if not isinstance(dataset, dict):
        raise SystemExit(f"Invalid dataset record: {dataset_name}")

    dataset_shards = dataset.get("shards")
    if not isinstance(dataset_shards, list):
        raise SystemExit(f"Invalid shard list: {dataset_name}")

    shards += len(dataset_shards)
    rows += int(dataset.get("rows", 0))
    size += int(dataset.get("bytes", 0))

print(f"Datasets: {len(datasets)}")
print(f"Shards:   {shards}")
print(f"Rows:     {rows:,}")
print(f"Bytes:    {size:,}")
PY

  step "5/6 Uploading PSV shards and executing TigerGraph loading jobs" \
    tf-gnn-load load
fi

step "6/6 Verifying the TigerGraph graph" \
  tf-gnn-load verify

step "Checking critical transaction counts" \
  python - "$ARTIFACT_DIR" <<'PY'
import json
from pathlib import Path
import sys

artifact_dir = Path(sys.argv[1])
manifest_path = artifact_dir / "export_manifest.json"
verification_path = artifact_dir / "tigergraph_verification.json"

if not manifest_path.is_file():
    raise SystemExit(f"Missing manifest: {manifest_path}")

if not verification_path.is_file():
    raise SystemExit(f"Missing verification report: {verification_path}")

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
verification = json.loads(
    verification_path.read_text(encoding="utf-8")
)

datasets = manifest["datasets"]
transactions = datasets.get("22_transactions")

if not isinstance(transactions, dict):
    raise SystemExit("Manifest is missing 22_transactions")

expected = int(transactions["rows"])

vertices = verification.get("vertices", {})
edges = verification.get("edges", {})

# One PSV row creates the vertex and three mandatory participation edges,
# so all four must equal the manifest row count exactly.
#
# Transaction_At_Location comes off the same row but only when the
# merchant has an outlet, so it is the physical-merchant subset and is
# checked as a bound rather than an equality. Transaction_Used_Device and
# Transaction_From_IP are optional endpoints with their own datasets.
actual = {
    "Payment_Transaction": int(vertices.get("Payment_Transaction", -1)),
    "Transaction_From_Account": int(
        edges.get("Transaction_From_Account", -1)
    ),
    "Transaction_Used_Card": int(edges.get("Transaction_Used_Card", -1)),
    "Transaction_At_Merchant": int(
        edges.get("Transaction_At_Merchant", -1)
    ),
}

failures = {
    name: count
    for name, count in actual.items()
    if count != expected
}

location_edges = int(edges.get("Transaction_At_Location", -1))

if location_edges < 0 or location_edges > expected:
    failures["Transaction_At_Location"] = location_edges

print(f"Expected transaction rows:       {expected:,}")
for name, count in actual.items():
    print(f"{name + ':':34} {count:,}")
print(f"{'Transaction_At_Location:':34} {location_edges:,} (<= expected)")

if failures:
    details = ", ".join(
        f"{name}={count:,}" for name, count in failures.items()
    )
    raise SystemExit(
        "Critical TigerGraph count mismatch: " + details
    )

print("Critical transaction counts match the export manifest.")
PY

echo
echo "================================================================"
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "================================================================"
echo "Artifacts: $ARTIFACT_DIR"
echo "Log:       $LOG_FILE"
