"""Helper for retrieving configuration settings from environment variables."""


import os
from typing import Optional
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

ENV_PATH = Path(__file__).resolve().parent / '.env'
if load_dotenv and ENV_PATH.exists():
    load_dotenv(str(ENV_PATH))


def get_config(name: str, default: Optional[str] = None) -> str:
    """Retrieve configuration from environment variables."""
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing configuration for {name}")
    return value
