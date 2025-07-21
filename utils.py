"""Utility functions used across pipeline scripts."""
from typing import Tuple


def compute_vibe(sentiment_label: str, sentiment_score: float,
                 likes: int, retweets: int, replies: int) -> Tuple[float, str]:
    """Compute a simplified vibe score from sentiment and engagement."""
    engagement = (likes + retweets * 2 + replies) / 1000.0 if likes is not None else 0
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
    return vibe_score, vibe_label
