"""Command-line interface for the PhantomLedger to TransactionFraud_GNN loader."""

import argparse
from collections.abc import Sequence
import json
from typing import cast

from tf_gnn_loader.postgres.export import export_all
from tf_gnn_loader.postgres.inspect import inspect
from tf_gnn_loader.postgres.prepare import prepare
from tf_gnn_loader.postgres.settings import (
    Settings as PostgresSettings,
)
from tf_gnn_loader.postgres.verify import (
    verify as verify_postgres,
)
from tf_gnn_loader.tigergraph.admin import (
    install_loading_jobs,
    install_verify_query,
)
from tf_gnn_loader.tigergraph.gsql_paths import gsql_path
from tf_gnn_loader.tigergraph.loading import load
from tf_gnn_loader.tigergraph.settings import (
    Settings as TigerGraphSettings,
)
from tf_gnn_loader.tigergraph.verify import (
    verify as verify_tigergraph,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=("Prepare PhantomLedger data and load it into TransactionFraud_GNN")
    )

    commands = result.add_subparsers(
        dest="command",
        required=True,
    )

    inspect_parser = commands.add_parser(
        "inspect",
        help="Inspect the PhantomLedger PostgreSQL source",
    )

    inspect_parser.add_argument(
        "--exact",
        action="store_true",
        help=("Count all source tables exactly; this can be slow"),
    )

    commands.add_parser(
        "prepare",
        help=(
            "Run the versioned PostgreSQL validation, split, view, and audit SQL files"
        ),
    )

    commands.add_parser(
        "audit",
        help="Verify the prepared PostgreSQL split and load views",
    )

    commands.add_parser(
        "export",
        help="Export the prepared load views into PSV shards",
    )

    commands.add_parser(
        "schema-path",
        help=("Print the schema file that must be run once in the Savanna GSQL editor"),
    )

    commands.add_parser(
        "install-jobs",
        help="Install the TigerGraph loading jobs and tfgnn_validate_graph",
    )

    commands.add_parser(
        "load",
        help="Resumably upload all exported shards",
    )

    commands.add_parser(
        "verify",
        help="Verify the resulting TransactionFraud_GNN graph",
    )

    commands.add_parser(
        "push",
        help=(
            "Run the whole pipeline: prepare, audit, export, install "
            "jobs and validation query, load, verify. The "
            "TransactionFraud_GNN schema must already exist "
            "(see schema-path)."
        ),
    )

    return result


def _push() -> dict[str, object]:
    """
    Run every pipeline phase in order, failing fast.

    Each phase is the same code the individual subcommands run, so
    `push` is exactly equivalent to running them by hand:

        1. prepare        PostgreSQL validation, views, audits
        2. audit          refuse to continue on any audit failure
        3. export         PSV shards + export_manifest.json
        4. install-jobs   loading jobs + tfgnn_validate_graph query
        5. load           resumable shard upload (re-runs skip
                          completed shards via the state file)
        6. verify         manifest counts + loading validation

    The one step push cannot do is create the TransactionFraud_GNN
    schema: the REST++ secret used for authentication is minted per
    graph in Savanna, so a graph that does not exist yet cannot be
    reached with this repo's credentials. Run the schema file once in
    the Savanna GSQL editor first (`tf-gnn-load schema-path`).
    """

    postgres_settings = PostgresSettings()
    tigergraph_settings = TigerGraphSettings()

    print("[1/6] prepare: running PostgreSQL preparation SQL...")
    prepare(postgres_settings)

    print("[2/6] audit: verifying prepared PostgreSQL state...")
    postgres_report = verify_postgres(postgres_settings)

    print("[3/6] export: writing PSV shards...")
    manifest = export_all(postgres_settings)

    print("[4/6] install-jobs: installing loading jobs and validation query...")
    jobs = install_loading_jobs(tigergraph_settings)

    print(
        "  installed "
        + str(jobs["loading_job_count"])
        + " loading jobs into "
        + jobs["graphname"]
    )

    query = install_verify_query(tigergraph_settings)

    print("  installed query " + query["query_name"])

    print("[5/6] load: uploading shards into TransactionFraud_GNN...")
    load_report = load(
        tigergraph_settings,
        postgres_settings.export_dir,
    )

    print("[6/6] verify: validating the loaded graph...")
    verification = verify_tigergraph(
        tigergraph_settings,
        postgres_settings.export_dir,
    )

    return {
        "pushed": True,
        "graphname": tigergraph_settings.graphname,
        "postgres_audit_passed": postgres_report["passed"],
        "exported_datasets": len(manifest["datasets"]),
        "loading_jobs_installed": jobs["loading_job_count"],
        "load": load_report,
        "tigergraph_verification_passed": verification["passed"],
    }


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = parser().parse_args(argv)

    command = cast(
        str,
        args.command,
    )

    result: object

    if command == "inspect":
        settings = PostgresSettings()

        exact = cast(
            bool,
            getattr(args, "exact", False),
        )

        result = inspect(
            settings,
            exact=exact,
        )

    elif command == "prepare":
        settings = PostgresSettings()

        prepare(settings)

        result = {
            "prepared": True,
            "database": settings.pg_dsn,
        }

    elif command == "audit":
        settings = PostgresSettings()

        result = verify_postgres(settings)

    elif command == "export":
        settings = PostgresSettings()

        result = export_all(settings)

    elif command == "schema-path":
        path = gsql_path("schema")

        result = {
            "schema_path": str(path),
            "instruction": (
                "Run this file once in the Savanna GSQL editor "
                "using a read-write workspace"
            ),
        }

    elif command == "install-jobs":
        settings = TigerGraphSettings()

        jobs = install_loading_jobs(settings)

        query = install_verify_query(settings)

        result = {
            "loading_jobs_installed": jobs["loading_job_count"],
            "verify_query_installed": query["query_name"],
            "graphname": settings.graphname,
        }

    elif command == "load":
        postgres_settings = PostgresSettings()
        tigergraph_settings = TigerGraphSettings()

        load(
            tigergraph_settings,
            postgres_settings.export_dir,
        )

        result = {
            "load_complete": True,
            "graphname": tigergraph_settings.graphname,
        }

    elif command == "verify":
        postgres_settings = PostgresSettings()
        tigergraph_settings = TigerGraphSettings()

        result = verify_tigergraph(
            tigergraph_settings,
            postgres_settings.export_dir,
        )

    elif command == "push":
        result = _push()

    else:
        raise AssertionError(f"Unhandled command: {command}")

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
