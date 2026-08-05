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

        # Bound only when configured. `inspect`, `audit`, `export` and
        # `verify` all work without a salt --- the load views read the
        # materialised token tables and never call tf_gnn_prep.pii_token
        # --- so an absent salt is not this function's problem to refuse.
        # `prepare` is where it is demanded, and 020_create_policies.sql
        # raises on the PostgreSQL side if the GUC is missing there.
        #
        # set_config, not SET: the value is a secret, and a bound
        # parameter travels outside the statement text, so it does not
        # appear in pg_stat_activity or the server log the way an
        # interpolated `SET tfgnn.pii_salt = '<secret>'` would.
        # is_local = false so it holds for the whole session.
        salt = settings.pii_salt.get_secret_value()

        if salt:
            cursor.execute(
                "SELECT set_config('tfgnn.pii_salt', %s, false)",
                (salt,),
            )

    if not autocommit:
        # COMMIT, and both halves of that matter.
        #
        # When a salt is configured, set_config above is a SELECT, so it
        # takes a snapshot and opens the implicit transaction.
        # `SET TRANSACTION ISOLATION LEVEL`, which the exporter issues to
        # get one consistent view across all 24 datasets, must be the
        # first command in its transaction --- PostgreSQL rejects it
        # outright otherwise. Ending the transaction here means the caller
        # always starts a clean one.
        #
        # Unconditional, even though a salt-less connection runs only
        # plain SETs and takes no snapshot: a connection whose
        # transaction semantics depend on whether an unrelated secret
        # happens to be configured is a trap for whoever debugs it next.
        #
        # COMMIT rather than ROLLBACK because a session-level SET made
        # inside a transaction is REVERTED when that transaction is
        # rolled back. A rollback would silently discard the salt and
        # every token would then fail on a missing GUC.
        conn.commit()

    return conn
