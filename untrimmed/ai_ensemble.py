"""Simplified AI ensemble for sentiment classification."""

from __future__ import annotations

from typing import Any, Dict, List


class AIEnsemble:
    def __init__(self) -> None:
        self.models: List[callable] = [self._simple]

    def predict(self, texts: List[str]) -> List[Dict[str, Any]]:
        results = []
        for text in texts:
            scores = [m(text) for m in self.models]
            avg = sum(s["score"] for s in scores) / len(scores)
            label = "positive" if avg > 0 else "negative" if avg < 0 else "neutral"
            results.append({"label": label, "score": avg})
        return results

    @staticmethod
    def _simple(text: str) -> Dict[str, Any]:
        pos = sum(1 for w in ["good", "great", "bull"] if w in text.lower())
        neg = sum(1 for w in ["bad", "bear", "crash"] if w in text.lower())
        score = pos - neg
        return {"score": float(score)}
