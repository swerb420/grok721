"""Utility functions used across pipeline scripts."""
from typing import Tuple, Optional, Callable, Any, List, Iterable
import time
import logging


def compute_vibe(
    sentiment_label: str,
    sentiment_score: float,
    likes: Optional[int] = None,
    retweets: Optional[int] = None,
    replies: Optional[int] = None,
) -> Tuple[float, str]:
    """Compute a simplified vibe score from sentiment and engagement."""
    # Normalize engagement counts. ``None`` values are treated as zero, but
    # negative values are allowed to reduce the vibe score to reflect
    # potentially adverse reactions.
    has_negative = any(
        x is not None and x < 0 for x in (likes, retweets, replies)
    )
    likes = likes if likes is not None else 0
    retweets = retweets if retweets is not None else 0
    replies = replies if replies is not None else 0
    engagement = (likes + retweets * 2 + replies) / 1000.0
    base_score = sentiment_score if sentiment_label == "POSITIVE" else -sentiment_score
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
                wait = int(getattr(resp, "headers", {}).get("Retry-After", 60))
                logging.warning("Rate limited, waiting %s seconds", wait)
                time.sleep(wait)
            elif status is not None and status >= 500:
                logging.warning(
                    "Server error %s for %s=%s, backing off", status, param_name, interval
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

