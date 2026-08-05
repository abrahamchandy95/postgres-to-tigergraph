"""Install the TransactionFraud_GNN loading jobs and validation query.

BOTH INSTALLS ARE IDEMPOTENT, and only one of them is so for free.

`CREATE OR REPLACE QUERY` handles the validation query. There is no such
form for a loading job: `CREATE LOADING JOB x` against an existing x
fails with

    Semantic Check Fails: The job name x already exists in other objects!

and it fails on the FIRST job, so the remaining 23 are never attempted.
That turns every re-run of `tf-gnn-load push` into a failure at step 4 ---
and a re-run is exactly what you do after a load that died halfway, which
is the moment you least want a spurious error. So the jobs that already
exist are dropped first; see _drop_existing_jobs.
"""

from pathlib import Path
import re
from typing import TypedDict

from tf_gnn_loader.tigergraph.client import Client
from tf_gnn_loader.tigergraph.gsql_paths import gsql_path
from tf_gnn_loader.tigergraph.settings import Settings


GRAPHNAME = "TransactionFraud_GNN"

VERIFY_QUERY_NAME = "tfgnn_validate_graph"

# Phrases the GSQL shell emits when a CREATE, DROP or INSTALL fails.
# Successful runs print "Successfully created queries" and
# "installed successfully" style messages instead.
_GSQL_FAILURE_MARKERS: tuple[str, ...] = (
    "syntax check error",
    "semantic check error",
    "semantic check fails",
    "could not be installed",
    "cannot be installed",
    "failed to install",
    "failed to create",
    "failed to drop",
)


_LOADING_JOB_NAME_PATTERN = re.compile(
    r"CREATE\s+LOADING\s+JOB\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


class InstallResult(TypedDict):
    graphname: str
    script_path: str
    loading_job_count: int
    replaced_job_count: int
    response: str


class InstallQueryResult(TypedDict):
    graphname: str
    script_path: str
    query_name: str
    response: str


def _read_gsql(path: Path) -> str:
    source = path.read_text(
        encoding="utf-8",
    )

    if not source.strip():
        raise RuntimeError(f"GSQL file is empty: {path}")

    return source


def _assert_graphname(settings: Settings) -> None:
    if settings.graphname != GRAPHNAME:
        raise RuntimeError(
            f"GRAPHNAME must be {GRAPHNAME!r}, " + f"got {settings.graphname!r}"
        )


def _assert_gsql_succeeded(
    response: str,
    context: str,
) -> None:
    lowered = response.lower()

    for marker in _GSQL_FAILURE_MARKERS:
        if marker in lowered:
            raise RuntimeError(f"{context} failed:\n" + response)


def _validate_loading_jobs(
    source: str,
    path: Path,
) -> int:
    """Validate that every loading job targets TransactionFraud_GNN."""

    if f"USE GRAPH {GRAPHNAME}" not in source:
        raise RuntimeError(f"{path} must contain 'USE GRAPH {GRAPHNAME}'")

    loading_job_count = source.upper().count("CREATE LOADING JOB")

    if loading_job_count == 0:
        raise RuntimeError(f"{path} contains no CREATE LOADING JOB statements")

    for line_number, line in enumerate(
        source.splitlines(),
        start=1,
    ):
        if "CREATE LOADING JOB" not in line.upper():
            continue

        if f"FOR GRAPH {GRAPHNAME}" not in line:
            raise RuntimeError(
                f"{path}:{line_number} does not target {GRAPHNAME}: " + line.strip()
            )

    return loading_job_count


def _installed_job_names(client: Client) -> set[str]:
    """Names of the loading jobs currently defined on the graph.

    Loading jobs are GRAPH-scoped, so a bare `SHOW JOB *` at global scope
    reports nothing at all --- which would read as "no jobs installed" and
    silently skip the drop. The USE GRAPH is what makes the listing real.
    """

    listing = client.gsql(f"USE GRAPH {GRAPHNAME}\nSHOW JOB *")

    return set(_LOADING_JOB_NAME_PATTERN.findall(listing))


def _drop_existing_jobs(
    client: Client,
    declared: list[str],
) -> list[str]:
    """Drop the declared jobs that already exist, and only those.

    Deliberately NOT `DROP JOB ALL`: that would also take out schema-change
    jobs and any loading job another pipeline installed on this graph. The
    intersection with what this file declares is the only set this function
    has any business touching.
    """

    installed = _installed_job_names(client)

    stale = [name for name in declared if name in installed]

    if not stale:
        return []

    response = client.gsql(f"USE GRAPH {GRAPHNAME}\nDROP JOB " + ", ".join(stale))

    _assert_gsql_succeeded(
        response,
        "Dropping the previously installed loading jobs",
    )

    return stale


def install_loading_jobs(
    settings: Settings,
    client: Client | None = None,
) -> InstallResult:
    """
    Install every loading job declared in gsql/loading_jobs.gsql.

    Re-runnable: jobs of the same name that already exist are dropped
    first, because GSQL has no CREATE OR REPLACE for a loading job and a
    plain CREATE against an existing name aborts the whole batch on the
    first collision.

    TransactionFraud_GNN and its REST++ secret must already exist: the
    secret is minted per graph in Savanna, so a graph that does not exist
    yet cannot be reached with this repo's credentials.
    """

    _assert_graphname(settings)

    path = gsql_path("loading_jobs")

    source = _read_gsql(path)

    loading_job_count = _validate_loading_jobs(
        source,
        path,
    )

    declared = _LOADING_JOB_NAME_PATTERN.findall(source)

    if client is None:
        client = Client(settings)

    replaced = _drop_existing_jobs(
        client,
        declared,
    )

    response = client.gsql(source)

    _assert_gsql_succeeded(
        response,
        f"Installing the {GRAPHNAME} loading jobs",
    )

    return {
        "graphname": GRAPHNAME,
        "script_path": str(path),
        "loading_job_count": loading_job_count,
        "replaced_job_count": len(replaced),
        "response": response,
    }


def install_verify_query(
    settings: Settings,
    client: Client | None = None,
) -> InstallQueryResult:
    """
    Install gsql/verify_load.gsql through the GSQL endpoint.

    The file uses CREATE OR REPLACE QUERY, so reinstalling after a
    schema or query change is safe. INSTALL QUERY compiles on the
    server and can take a few minutes; the client has no local
    timeout, so the call waits for TigerGraph.
    """

    _assert_graphname(settings)

    path = gsql_path("verify_load")

    source = _read_gsql(path)

    if f"USE GRAPH {GRAPHNAME}" not in source:
        raise RuntimeError(f"{path} must contain 'USE GRAPH {GRAPHNAME}'")

    if f"CREATE OR REPLACE QUERY {VERIFY_QUERY_NAME}" not in source:
        raise RuntimeError(
            f"{path} must declare " + f"CREATE OR REPLACE QUERY {VERIFY_QUERY_NAME}"
        )

    if f"INSTALL QUERY {VERIFY_QUERY_NAME}" not in source:
        raise RuntimeError(f"{path} must contain INSTALL QUERY {VERIFY_QUERY_NAME}")

    if client is None:
        client = Client(settings)

    response = client.gsql(source)

    _assert_gsql_succeeded(
        response,
        f"Installing {VERIFY_QUERY_NAME}",
    )

    return {
        "graphname": GRAPHNAME,
        "script_path": str(path),
        "query_name": VERIFY_QUERY_NAME,
        "response": response,
    }


def main() -> int:
    """Install loading jobs and the verify query without requiring cli.py."""

    settings = Settings()

    client = Client(settings)

    jobs = install_loading_jobs(
        settings,
        client=client,
    )

    print(
        "Installed "
        + str(jobs["loading_job_count"])
        + " loading jobs into "
        + jobs["graphname"]
        + " (replaced "
        + str(jobs["replaced_job_count"])
        + " existing)"
    )

    print(jobs["response"])

    query = install_verify_query(
        settings,
        client=client,
    )

    print("Installed query " + query["query_name"])

    print(query["response"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
