from typing import ClassVar
from pydantic import Field, SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    host: str = Field(
        default="",
        description="TigerGraph host URL",
    )

    graphname: str = Field(
        default="",
        description="Name of the graph",
    )

    secret: SecretStr = Field(
        default=SecretStr(""),
        description="REST++ secret used to mint auth tokens",
    )

    @field_validator("host", "graphname")
    @classmethod
    def _str_required(
        cls,
        value: str,
        info: ValidationInfo,
    ) -> str:
        value = value.strip()

        if not value:
            raise ValueError(f"{info.field_name or 'field'} must be set in .env")

        if info.field_name == "host":
            return value.rstrip("/")

        return value

    @field_validator("secret")
    @classmethod
    def _secret_required(
        cls,
        value: SecretStr,
    ) -> SecretStr:
        if not value.get_secret_value():
            raise ValueError("secret must be set in .env")

        return value
