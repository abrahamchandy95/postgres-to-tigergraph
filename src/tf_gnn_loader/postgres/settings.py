from pathlib import Path
from typing import ClassVar

from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    pg_dsn: str = Field(
        default="dbname=phantomledger",
        description="PostgreSQL connection string",
    )

    export_dir: Path = Field(
        default=Path("./artifacts/tfgnn_load"),
        description="Directory for manifests and load shards",
    )

    shard_bytes: int = Field(
        default=90_000_000,
        description="Approximate maximum shard size",
    )

    pii_salt: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("TFGNN_PII_SALT", "PII_SALT"),
        description=(
            "Salt for the HMAC-SHA256 tokenisation of email, phone, "
            "address, identity-document, device and IP primary ids"
        ),
    )

    @field_validator("pg_dsn")
    @classmethod
    def validate_pg_dsn(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("PG_DSN must not be empty")

        return value

    @field_validator("shard_bytes")
    @classmethod
    def validate_shard_bytes(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("SHARD_BYTES must be positive")

        if value >= 128_000_000:
            raise ValueError("SHARD_BYTES must be below 128,000,000")

        return value

    @field_validator("pii_salt")
    @classmethod
    def validate_pii_salt(cls, value: SecretStr) -> SecretStr:
        """
        Validate the salt if one is given, but do not require one here.

        REQUIRING IT AT CONSTRUCTION WAS WRONG. Settings is built by every
        PostgreSQL subcommand, so a hard requirement here made
        `tf-gnn-load inspect` --- read-only, and it never reads a PII
        VALUE, only table metadata --- fail on a missing salt. That is a
        confusing error at a step that does not need the thing it is
        asking for.

        The salt is needed by exactly one step: `prepare`, which
        materialises the token tables in
        sql/postgres/060_create_pii_views.sql. tf_gnn_loader.postgres.prepare
        demands it there, and 020_create_policies.sql refuses on the
        PostgreSQL side as the backstop, so a session that reached the
        database some other way still cannot build unsalted tokens.

        `export` does NOT need it: the load views read the materialised
        token tables and never call tf_gnn_prep.pii_token.

        A LENGTH FLOOR STILL APPLIES WHEN A SALT IS GIVEN. A phone number
        or an email address carries little enough entropy that an
        eight-character salt is searchable, and a salt that is present but
        too weak is the failure nobody notices.
        """

        raw = value.get_secret_value().strip()

        if raw and len(raw) < 16:
            raise ValueError(
                "TFGNN_PII_SALT must be at least 16 characters; a short "
                "salt is dictionary-searchable."
            )

        return SecretStr(raw)
