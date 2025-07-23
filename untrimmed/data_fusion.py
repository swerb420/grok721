"""Simple multi-source data fusion pipeline placeholder."""

from __future__ import annotations

import asyncio
from typing import Dict, List
import logging


class DataFusionPipeline:
    """Combine async collectors from multiple sources."""

    def __init__(self, sources: Dict[str, callable]):
        self.sources = sources

    async def collect_all_sources(self) -> Dict[str, List[dict]]:
        tasks = [asyncio.create_task(src()) for src in self.sources.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        data: Dict[str, List[dict]] = {}
        for key, res in zip(self.sources.keys(), results):
            if isinstance(res, Exception):
                logging.error("Error collecting %s: %s", key, res)
                continue
            data[key] = res
        return data
