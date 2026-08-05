from __future__ import annotations

from tf_gnn_loader.postgres.connection import connect
from tf_gnn_loader.postgres.settings import Settings
from tf_gnn_loader.postgres.sql_paths import sql_paths


def prepare(settings: Settings) -> None:
    """
    Execute every versioned PostgreSQL preparation file in order.

    The SQL files are trusted files stored inside this repository.
    Each file is sent as UTF-8 bytes because Psycopg accepts bytes as
    an executable query and basedpyright can verify that type directly.
    """

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
