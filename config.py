"""Configuration helper utilities.

The module loads environment variables from a ``.env`` file when
``python-dotenv`` is available and exposes :func:`get_config` to retrieve
configuration values with optional defaults.
"""

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
