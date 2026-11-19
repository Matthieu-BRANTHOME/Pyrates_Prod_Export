"""Configuration loaded from the environment (.env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_EXPORT_DIR = Path("data/traces")
DEFAULT_ANALYSIS_DIR = Path("data/analysis")
DEFAULT_CHUNK_SIZE = 50_000


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    ssl: bool
    read_timeout: int


def load_env() -> None:
    """Load .env from the current directory (existing variables win)."""
    load_dotenv(Path.cwd() / ".env")


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(
            f"Missing setting {name}. Copy .env.example to .env and fill it in."
        )
    return value


def _flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def load_db_config() -> DbConfig:
    return DbConfig(
        host=_required("MYSQL_HOST"),
        port=int(os.environ.get("MYSQL_PORT") or 3306),
        user=_required("MYSQL_USER"),
        password=_required("MYSQL_PASSWORD"),
        database=_required("MYSQL_DATABASE"),
        ssl=_flag("MYSQL_SSL"),
        read_timeout=int(os.environ.get("MYSQL_READ_TIMEOUT") or 3600),
    )


def env_date(name: str) -> date | None:
    value = os.environ.get(name, "").strip()
    return date.fromisoformat(value) if value else None


def env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name, "").strip()
    return Path(value) if value else default


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default
