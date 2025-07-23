"""Data archival helpers."""

from __future__ import annotations

import zlib
import json
from datetime import datetime
from typing import List, Dict


def compress_record(record: Dict) -> bytes:
    return zlib.compress(json.dumps(record).encode())


def decompress_record(data: bytes) -> Dict:
    return json.loads(zlib.decompress(data).decode())
