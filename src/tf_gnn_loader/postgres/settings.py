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
        Refuse to run without a usable salt.

        The TransactionFraud_GNN schema requires tokenized PII primary
        ids, and the salt is the whole defence: a phone number or an email
        address carries little enough entropy that an unsalted digest is
        recoverable by dictionary search. A default would be worse than
        nothing, because it would be in this file.

        THE SALT MUST BE STABLE ACROSS RUNS. Changing it changes every
        Device, IP_Address, Email, Phone, Address and Identity_Document
        primary id, which silently re-keys the graph. 020 records its
        digest and refuses a mismatch, but the place to keep it safe is
        wherever secrets are kept.
        """

        raw = value.get_secret_value().strip()

        if not raw:
            raise ValueError(
                "TFGNN_PII_SALT must be set. The schema requires email, "
                "phone, address, identity-document, device and IP "
                "primary ids to be salted before loading. Generate one "
                'with `python -c "import secrets; '
                'print(secrets.token_urlsafe(32))"` and store it with '
                "your other secrets: re-keying orphans an existing load."
            )

        if len(raw) < 16:
            raise ValueError(
                "TFGNN_PII_SALT must be at least 16 characters; a short "
                "salt is dictionary-searchable."
            )

        return SecretStr(raw)
