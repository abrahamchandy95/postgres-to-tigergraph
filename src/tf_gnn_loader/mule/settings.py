from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tf_gnn_loader.postgres.settings import Settings as PostgresSettings


class MuleSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", case_sensitive=False
    )
    mule_pg_dsn: str = ""
    mule_export_dir: Path = Field(default=Path("artifacts/mule_temporal"))
    mule_upload_bytes: int = Field(default=10_000_000, gt=0, lt=128_000_000)
    mule_upload_workers: int = Field(default=1, ge=1, le=8)


def postgres_settings() -> PostgresSettings:
    """Dedicated overrides preserve the existing card-fraud configuration."""
    mule = MuleSettings()
    postgres = PostgresSettings()
    if mule.mule_pg_dsn.strip():
        postgres.pg_dsn = mule.mule_pg_dsn.strip()
    postgres.export_dir = mule.mule_export_dir
    return postgres
