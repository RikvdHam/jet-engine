from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )

    environment: str = "development"
    session_secret: str
    storage_tmp_dir: str
    storage_raw_dir: str
    storage_validated_dir: str
    storage_views_dir: str

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


settings = Settings()
