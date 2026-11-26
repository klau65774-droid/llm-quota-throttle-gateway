"""Configuration and model pricing settings for the LLM Quota Throttle Gateway."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from llm_gateway.api_models import SupportedModel
from llm_gateway.redaction import RedactionMode


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """Pricing and routing metadata for a supported LLM model."""

    model: str
    prompt_usd_per_1k: float
    completion_usd_per_1k: float
    primary_provider: str
    fallback_provider: str


MODEL_SPECS: dict[str, ModelSpec] = {
    "gpt-4o-mini": ModelSpec(
        model="gpt-4o-mini",
        prompt_usd_per_1k=0.00015,
        completion_usd_per_1k=0.0006,
        primary_provider="openai-primary",
        fallback_provider="azure-backup",
    ),
    "gpt-4o": ModelSpec(
        model="gpt-4o",
        prompt_usd_per_1k=0.0025,
        completion_usd_per_1k=0.01,
        primary_provider="openai-primary",
        fallback_provider="azure-backup",
    ),
    "claude-3-5-sonnet": ModelSpec(
        model="claude-3-5-sonnet",
        prompt_usd_per_1k=0.003,
        completion_usd_per_1k=0.015,
        primary_provider="anthropic-primary",
        fallback_provider="bedrock-backup",
    ),
    "llama-3-70b": ModelSpec(
        model="llama-3-70b",
        prompt_usd_per_1k=0.00059,
        completion_usd_per_1k=0.00079,
        primary_provider="together-primary",
        fallback_provider="fireworks-backup",
    ),
}


class GatewaySettings(BaseSettings):
    """Runtime gateway configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="GATEWAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65_535)
    db_path: str = "data/gateway_state.sqlite3"
    cors_origins: str = "http://127.0.0.1:5173,http://localhost:5173"
    sealed_benchmark_dir: str = "../_sealed_benchmarks/llm-quota-throttle-gateway"
    store_max_keys: int = Field(default=2048, ge=16, le=200_000)
    default_model: SupportedModel = "gpt-4o-mini"
    redaction_mode: RedactionMode = "mask"
    otel_service_name: str = "llm-quota-throttle-gateway"

    @field_validator("cors_origins")
    @classmethod
    def _validate_origins(cls, value: str) -> str:
        for origin in (item.strip() for item in value.split(",")):
            if not origin:
                continue
            parsed = urlsplit(origin)
            port = parsed.port
            if (
                parsed.scheme not in ("http", "https")
                or not parsed.hostname
                or parsed.username is not None
                or "*" in origin
                or any(char.isspace() for char in origin)
                or "\\" in origin
                or origin != f"{parsed.scheme}://{parsed.netloc}"
                or port == 0
            ):
                raise ValueError("CORS origins must be explicit HTTP(S) origins")
        return value

    @field_validator("db_path")
    @classmethod
    def _validate_db_path(cls, value: str) -> str:
        if not value.strip() or "\x00" in value or value.startswith(("file:", ":")):
            raise ValueError("Database path must identify a local SQLite file")
        return value

    @property
    def cors_origin_list(self) -> list[str]:
        """Return sanitized CORS origin list."""
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def repo_root(self) -> Path:
        """Use the source checkout or the operator's working directory for a wheel."""
        root = Path(__file__).resolve().parents[2]
        return root if (root / "backend" / "pyproject.toml").is_file() else Path.cwd().resolve()

    def resolved_db_path(self) -> Path:
        """Return safe resolved SQLite database file path."""
        raw = Path(self.db_path)
        if raw.is_absolute():
            return raw.resolve()
        resolved = (self.repo_root / raw).resolve()
        if not resolved.is_relative_to(self.repo_root) or resolved == self.repo_root:
            raise ValueError("Relative database path must stay inside the project directory")
        return resolved

    def resolved_metrics_path(self) -> Path:
        """Return path to docs/test_metrics.json."""
        return (self.repo_root / "docs" / "test_metrics.json").resolve()


@lru_cache(maxsize=1)
def get_settings() -> GatewaySettings:
    """Return cached runtime settings instance."""
    return GatewaySettings()
