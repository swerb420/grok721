"""Tiny distributed processing mock using asyncio."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List


async def process_item(item: Dict[str, Any]) -> Dict[str, Any]:
    await asyncio.sleep(0)
    return {**item, "processed": True}


async def process_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tasks = [asyncio.create_task(process_item(it)) for it in items]
    return await asyncio.gather(*tasks)
