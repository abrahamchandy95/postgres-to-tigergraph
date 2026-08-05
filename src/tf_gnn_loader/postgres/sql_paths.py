from pathlib import Path


# EXECUTION ORDER IS A DEPENDENCY ORDER, not a naming convention:
#
#   001  source contract               reads card_fraud only
#   010  source indices                reads card_fraud only
#   020  policies, helpers, tokens     defines pii_token, funding_account_id
#   030  event_seq and first-seen      defines the clock every stamp uses
#   040  geography                     needs first-seen (location stamps)
#   050  party / account / card /      needs 020's derivations and 030's
#        merchant and their relations  stamps
#   060  tokenised PII                 needs 030's stamps and 040's home
#                                      areas
#   070  transaction manifest          needs 040's locations and 060's
#                                      device / IP tokens
#   080  load views                    needs everything above
#   090  audits                        reads the load views
_SQL_FILENAMES: tuple[str, ...] = (
    "001_validate_sources.sql",
    "010_create_source_indices.sql",
    "020_create_policies.sql",
    "030_create_event_sequence.sql",
    "040_create_geography_views.sql",
    "050_create_entity_views.sql",
    "060_create_pii_views.sql",
    "070_create_transaction_views.sql",
    "080_create_load_views.sql",
    "090_create_audit_views.sql",
)


def project_root() -> Path:
    """
    Current file:

        <project>/src/tf_gnn_loader/postgres/sql_paths.py

    parents[3] is <project>.
    """

    return Path(__file__).resolve().parents[3]


def postgres_sql_root() -> Path:
    return project_root() / "sql" / "postgres"


def sql_paths() -> tuple[Path, ...]:
    root = postgres_sql_root()

    paths = tuple(root / filename for filename in _SQL_FILENAMES)

    missing = [str(path) for path in paths if not path.is_file()]

    if missing:
        raise FileNotFoundError("Missing PostgreSQL SQL files: " + ", ".join(missing))

    return paths
