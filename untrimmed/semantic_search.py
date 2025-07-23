"""Lightweight semantic search placeholder used in untrimmed examples."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
import numpy as np


class SemanticSearchEngine:
    def __init__(self) -> None:
        self.vectors: List[np.ndarray] = []
        self.metadata: List[Dict[str, Any]] = []

    def index(self, documents: List[Tuple[str, str]]) -> None:
        for doc_id, text in documents:
            vec = self._embed(text)
            self.vectors.append(vec)
            self.metadata.append({"id": doc_id, "text": text})

    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        if not self.vectors:
            return []
        q = self._embed(query)
        sims = [self._sim(q, v) for v in self.vectors]
        best = np.argsort(sims)[::-1][:k]
        results = []
        for idx in best:
            meta = self.metadata[idx]
            results.append({"id": meta["id"], "text": meta["text"], "score": sims[idx]})
        return results

    def _embed(self, text: str) -> np.ndarray:
        # Tiny hashing based embedding
        tokens = text.lower().split()
        vec = np.zeros(64)
        for t in tokens:
            vec[hash(t) % 64] += 1
        norm = np.linalg.norm(vec)
        return vec / norm if norm > 0 else vec

    @staticmethod
    def _sim(a: np.ndarray, b: np.ndarray) -> float:
        if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
            return 0.0
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
