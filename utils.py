"""Utility functions used across pipeline scripts."""

from typing import Tuple, Optional, Callable, Any, List, Iterable
import time
import logging

try:  # pragma: no cover - optional dependency for tests
    import requests
except Exception:  # ModuleNotFoundError during tests
    requests = None  # type: ignore


def compute_vibe(
    sentiment_label: str,
    sentiment_score: float,
    likes: Optional[int] = None,
    retweets: Optional[int] = None,
    replies: Optional[int] = None,
) -> Tuple[float, str]:
    """Compute a simplified vibe score from sentiment and engagement."""
    # Normalize engagement counts so that missing or negative values don't
    # artificially lower the vibe score.
    # Preserve negative counts to penalize posts receiving backlash or bot spam
    likes = likes or 0
    retweets = retweets or 0
    replies = replies or 0
    has_negative = any(x < 0 for x in (likes, retweets, replies))

    engagement = (likes + retweets * 2 + replies) / 1000.0
    if sentiment_label == "POSITIVE":
        base_score = sentiment_score
    elif sentiment_label == "NEGATIVE":
        base_score = -sentiment_score
    else:
        base_score = 0.0
    vibe_score = (base_score + engagement) * 5
    vibe_score = min(max(vibe_score, 0), 10)
    if vibe_score > 7:
        vibe_label = "Hype/Positive Impact"
    elif vibe_score > 5:
        vibe_label = "Engaging/Neutral"
    elif vibe_score > 3:
        vibe_label = "Controversial/Mixed"
    else:
        vibe_label = "Negative/Low Engagement"
    if has_negative:
        vibe_label = "Negative/Low Engagement"
    return vibe_score, vibe_label


def fetch_with_fallback(
    fetch_func: Callable[..., Any],
    *,
    param_name: str = "interval",
    intervals: Optional[List[Any]] = None,
    pause: float = 1,
    **kwargs: Any,
) -> Any:
    """Fetch data trying increasingly coarse intervals.

    Parameters
    ----------
    fetch_func:
        Function performing the HTTP request.
    param_name:
        Name of the parameter controlling resolution (default ``"interval"``).
    intervals:
        Sequence of interval values to try in order of preference.
    pause:
        Seconds to sleep between retries for non-rate limit errors.
    kwargs:
        Additional arguments passed to ``fetch_func``.
    """
    if intervals is None:
        intervals = ["1min", "5min", "1h"]

    last_exc: Optional[Exception] = None
    for interval in intervals:
        params = dict(kwargs)
        params[param_name] = interval
        try:
            return fetch_func(**params)
        except Exception as exc:  # pragma: no cover - depends on runtime errors
            last_exc = exc
            resp = getattr(exc, "response", None)
            status = getattr(resp, "status_code", None)
            if status == 429:
                raw_wait = getattr(resp, "headers", {}).get("Retry-After", "60")
                try:
                    wait = int(raw_wait)
                except (TypeError, ValueError):
                    wait = 60
                logging.warning("Rate limited, waiting %s seconds", wait)
                time.sleep(wait)
            elif status is not None and status >= 500:
                logging.warning(
                    "Server error %s for %s=%s, backing off",
                    status,
                    param_name,
                    interval,
                )
                time.sleep(pause)
            else:
                raise
    if last_exc:
        raise last_exc


def intervals_for_source(source: str, valuable_sources: Iterable[str]) -> List[str]:
    """Return resolution intervals for a data source."""
    if source in valuable_sources:
        return ["1min", "5min", "1h"]
    return ["1h", "1d"]


def execute_dune_query(query_id: str, api_key: str, **kwargs: Any) -> List[dict]:
    """Compatibility wrapper around :mod:`pipelines.dune`."""
    from pipelines.dune import execute_dune_query as _exec

    return _exec(query_id, api_key, **kwargs)
