from __future__ import annotations

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.settings import Settings
from tf_gnn_loader.postgres.sql_paths import sql_paths


def _assert_pii_salt(settings: Settings) -> None:
    """Demand the salt here, where it is actually used.

    `prepare` is the only step that needs it: 060_create_pii_views.sql
    materialises the Device, IP_Address, Email, Phone, Address and
    Identity_Document token tables, and every one of those tokens is
    HMAC-SHA256 keyed by this salt. `inspect`, `audit`, `export` and
    `verify` all read the materialised tables and never call
    tf_gnn_prep.pii_token, so none of them requires it.

    Checked before the first statement rather than left to
    020_create_policies.sql, because 001 and 010 run first and index
    creation on a full-size corpus is not something to discover halfway
    through.
    """

    if settings.pii_salt.get_secret_value():
        return

    raise RuntimeError(
        "TFGNN_PII_SALT is not set, and `prepare` is the step that "
        "needs it.\n\n"
        "The TransactionFraud_GNN schema requires email, phone, "
        "address, identity-document, device and IP primary ids to be "
        "salted and tokenized before loading, so raw PII never reaches "
        "the graph. This salt is the HMAC key for those tokens.\n\n"
        "Generate one and put it in .env as TFGNN_PII_SALT:\n"
        '    python -c "import secrets; print(secrets.token_urlsafe(32))"\n\n'
        "KEEP IT. Re-running prepare with a different salt changes every "
        "one of those primary ids and re-keys the graph; a load already "
        "in place would be orphaned. If this graph has never been "
        "loaded, any fresh value is fine."
    )


def prepare(settings: Settings) -> None:
    """
    Execute every versioned PostgreSQL preparation file in order.

    The SQL files are trusted files stored inside this repository.
    Each file is sent as UTF-8 bytes because Psycopg accepts bytes as
    an executable query and basedpyright can verify that type directly.
    """

    _assert_pii_salt(settings)

    paths = sql_paths()

    with connect(
        settings,
        autocommit=True,
    ) as conn:
        for path in paths:
            print(f"Running {path.name}...")

            statement = path.read_text(
                encoding="utf-8",
            )

            if not statement.strip():
                raise RuntimeError(f"SQL file is empty: {path}")

            query = statement.encode("utf-8")

            with conn.cursor() as cursor:
                _ = cursor.execute(query)

            print(f"Completed {path.name}")

    print("PostgreSQL preparation complete.")
