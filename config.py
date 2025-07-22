"""Helper for retrieving configuration settings from environment variables."""

import os
import warnings
from typing import Optional
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

ENV_PATH = Path(__file__).resolve().parent / '.env'
if load_dotenv and ENV_PATH.exists():
    load_dotenv(str(ENV_PATH))


PLACEHOLDERS = [
    "xxxxxxxxxx",
    "your-project-id",
    "your-quicknode-endpoint",
]


def _is_placeholder(value: str) -> bool:
    """Return True if the value looks like a placeholder."""
    if value is None:
        return False
    lowered = value.lower()
    if "x" * 6 in lowered and lowered.strip("x") == "":
        return True
    for ph in PLACEHOLDERS:
        if ph in lowered:
            return True
    return False


def get_config(name: str, default: Optional[str] = None) -> str:
    """Retrieve configuration from environment variables."""
    env_value = os.getenv(name)
    value = env_value if env_value is not None else default
    if value is None:
        raise RuntimeError(f"Missing configuration for {name}")

    if env_value is None and _is_placeholder(value):
        warnings.warn(
            f"Configuration {name} is using a placeholder value: {value}",
            RuntimeWarning,
        )

    return value
