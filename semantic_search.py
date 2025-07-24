"""Lightweight semantic search stub."""

from typing import Dict, List


class SemanticSearchEngine:
    def __init__(self):
        self.documents: List[Dict[str, str]] = []

    def batch_index(self, docs: List[Dict[str, str]]):
        self.documents.extend(docs)

    def search(self, query: str, k: int = 5) -> List[Dict[str, str]]:
        results = [doc for doc in self.documents if query.lower() in doc['text'].lower()]
        return results[:k]
