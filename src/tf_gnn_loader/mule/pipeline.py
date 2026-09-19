"""CLI dispatch for the additional mule-temporal use case."""

from typing import Any

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.tigergraph.settings import Settings
from . import postgres, tigergraph
from .contract import DATASETS, GRAPH, GSQL
from .settings import postgres_settings


def run(command: str) -> dict[str, Any]:
    if command == "schema-path":
        return {
            "schema_path": str(GSQL / "schema.gsql"),
            "graphname": GRAPH,
            "instruction": "Fresh graph only. Existing graphs must match the temporal contract; no schema is automatically replaced.",
        }
    pg = postgres_settings()
    if command == "inspect":
        with connect(pg, autocommit=False) as conn:
            postgres.prepare_snapshot(conn)
            return {
                "database": postgres.current_database(conn),
                "source_schema": "mule_temporal",
                "tables": [d.table for d in DATASETS],
                "contract_matches": True,
            }
    if command in ("prepare", "audit"):
        # Canonical staging data already contains causal clocks and tokenized
        # IDs. Preparing this use case means validating it, never re-keying it.
        return postgres.audit(pg)
    if command == "export":
        with tigergraph.directory_lock(pg.export_dir):
            return postgres.export(pg)
    settings = Settings()
    if settings.graphname != GRAPH:
        raise RuntimeError(f"mule-temporal requires GRAPHNAME={GRAPH}")
    if command == "install-jobs":
        with tigergraph.directory_lock(pg.export_dir):
            return tigergraph.install(settings)
    if command == "load":
        return tigergraph.load(settings, pg.export_dir)
    if command == "verify":
        return tigergraph.verify(settings, pg.export_dir)
    if command == "push":
        # Fail before an expensive export if the target is incompatible.
        tigergraph.checked_client(settings)
        with tigergraph.directory_lock(pg.export_dir):
            if (pg.export_dir / "export_manifest.json").exists():
                manifest = tigergraph.read_manifest(pg.export_dir)
                with connect(pg) as conn:
                    database = postgres.current_database(conn)
                if database != manifest["source_database"]:
                    raise RuntimeError(
                        "The existing export belongs to a different PostgreSQL database"
                    )
                print("Resuming the existing immutable temporal export.", flush=True)
            else:
                postgres.export(pg)
        with tigergraph.directory_lock(pg.export_dir):
            installed = tigergraph.install(settings)
        loaded = tigergraph.load(settings, pg.export_dir)
        verified = tigergraph.verify(settings, pg.export_dir)
        return {
            "pushed": True,
            "graphname": GRAPH,
            "installation": installed,
            "load": loaded,
            "verification": verified,
        }
    raise ValueError(f"Unsupported temporal command: {command}")
