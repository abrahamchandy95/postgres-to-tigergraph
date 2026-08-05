from __future__ import annotations

from typing import Any

import psycopg
from psycopg import Connection

from tf_gnn_loader.postgres.settings import Settings


def connect(
    settings: Settings,
    *,
    autocommit: bool = True,
) -> Connection[Any]:
    """
    Open a source connection with the PII salt bound to the session.

    THE SALT IS A SESSION GUC, NOT A LITERAL IN THE SQL. The
    TransactionFraud_GNN schema requires email, phone, address,
    identity-document, device and IP primary ids to be tokenized before
    loading, and sql/postgres/020_create_policies.sql derives the HMAC key
    from `tfgnn.pii_salt`. Binding it here means the secret reaches
    PostgreSQL through a parameterised statement and never through a file
    anyone can read.

    Settings validation already refuses an unset or short salt, so by the
    time this runs the value is present; 020 checks it again on the
    PostgreSQL side, because a session that reached the database some
    other way must fail just as loudly.
    """

    conn = psycopg.connect(
        settings.pg_dsn,
        autocommit=autocommit,
    )

    with conn.cursor() as cursor:
        cursor.execute("SET application_name = 'tf_gnn_loader'")
        cursor.execute("SET statement_timeout = 0")
        cursor.execute("SET lock_timeout = 0")

        # set_config, not SET: the value is a secret, and a bound
        # parameter travels outside the statement text, so it does not
        # appear in pg_stat_activity or the server log the way an
        # interpolated `SET tfgnn.pii_salt = '<secret>'` would.
        # is_local = false so it holds for the whole session.
        cursor.execute(
            "SELECT set_config('tfgnn.pii_salt', %s, false)",
            (settings.pii_salt.get_secret_value(),),
        )

    if not autocommit:
        # COMMIT, and both halves of that matter.
        #
        # set_config above is a SELECT, so it takes a snapshot and opens
        # the implicit transaction. `SET TRANSACTION ISOLATION LEVEL`,
        # which the exporter issues to get one consistent view across all
        # 24 datasets, must be the first command in its transaction ---
        # PostgreSQL rejects it outright otherwise. Ending the
        # transaction here means the caller always starts a clean one.
        #
        # COMMIT rather than ROLLBACK because a session-level SET made
        # inside a transaction is REVERTED when that transaction is
        # rolled back. A rollback would silently discard the salt and
        # every token would then fail on a missing GUC.
        conn.commit()

    return conn
