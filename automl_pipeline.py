"""Minimal AutoML utilities used for tests.
Original module performed complex feature engineering and
hyper-parameter optimisation. This lightweight version only
implements a basic model search using scikit-learn."""

from dataclasses import dataclass
from typing import Any, Dict, List
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score


@dataclass
class ModelResult:
    name: str
    score: float
    model: Any


class AutoMLPipeline:
    def __init__(self):
        self.candidates = {
            "rf": RandomForestRegressor
        }

    def find_best_models(self, X: pd.DataFrame, y: pd.Series) -> List[ModelResult]:
        results: List[ModelResult] = []
        for name, cls in self.candidates.items():
            model = cls()
            scores = cross_val_score(model, X, y, cv=3, scoring="neg_mean_squared_error")
            model.fit(X, y)
            results.append(ModelResult(name=name, score=-scores.mean(), model=model))
        results.sort(key=lambda r: r.score)
        return results
