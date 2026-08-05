"""GSQL script registry."""

from pathlib import Path

_GSQL_RELPATHS: dict[str, str] = {
    # The loader's copy of the deployed TransactionFraud_GNN DDL. It is
    # byte-identical to the modelling repo's gsql/schema/schema.gsql, and
    # it is here so the PSV column contract in gsql/loading_jobs.gsql and
    # sql/postgres/080_create_load_views.sql can be diffed against
    # something in-repo rather than against a screenshot.
    #
    # `tf-gnn-load schema-path` prints it. The loader cannot run it: the
    # REST++ secret is minted per graph in Savanna, so a graph that does
    # not exist yet cannot be reached with these credentials.
    "schema": "schema/schema.gsql",
    "loading_jobs": "loading_jobs.gsql",
    # tfgnn_validate_graph, the query the schema header names as the
    # thing to run before exporting or training.
    "verify_load": "verify_load.gsql",
}


class GsqlPathError(KeyError):
    """Raised when a requested GSQL script is not registered."""


def project_root() -> Path:
    """
    Resolve:

        <project>/src/tf_gnn_loader/tigergraph/gsql_paths.py

    parents[0] -> tigergraph
    parents[1] -> tf_gnn_loader
    parents[2] -> src
    parents[3] -> project root
    """

    return Path(__file__).resolve().parents[3]


def gsql_root() -> Path:
    return project_root() / "gsql"


def gsql_path(script_name: str) -> Path:
    relpath = _GSQL_RELPATHS.get(script_name)

    if relpath is None:
        known = ", ".join(sorted(_GSQL_RELPATHS))

        raise GsqlPathError(
            f"unknown GSQL script {script_name!r}; known scripts: {known}"
        )

    path = gsql_root() / relpath

    if not path.is_file():
        raise FileNotFoundError(f"GSQL source does not exist: {path}")

    return path


def script_names() -> list[str]:
    return sorted(_GSQL_RELPATHS)
