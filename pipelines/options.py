"""Ingest stock options data using yfinance."""
from __future__ import annotations

import datetime
import logging
from typing import Iterable

from config import get_config

try:  # pragma: no cover - optional dependency
    import yfinance as yf  # type: ignore
except Exception:  # ModuleNotFoundError during tests
    yf = None  # type: ignore

YFINANCE_TICKERS = get_config("YFINANCE_TICKERS", "AAPL,TSLA").split(",")


def ingest_yfinance_options(conn, tickers: Iterable[str] | None = None) -> None:
    """Fetch options chains from yfinance and store them in the database."""
    if yf is None:
        raise RuntimeError("yfinance package not available")
    cur = conn.cursor()
    for ticker in tickers or YFINANCE_TICKERS:
        try:
            tk = yf.Ticker(ticker)
            expirations = getattr(tk, "options", [])
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Failed retrieving options list for %s: %s", ticker, exc)
            continue
        for exp in expirations:
            try:
                chain = tk.option_chain(exp)
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.warning("Failed option chain for %s %s: %s", ticker, exp, exc)
                continue
            for opt_type, df in (("call", chain.calls), ("put", chain.puts)):
                for _, row in df.iterrows():
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO stock_options (
                            ticker, option_type, expiration, strike,
                            last_price, implied_vol, open_interest, volume,
                            fetch_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            ticker,
                            opt_type,
                            exp,
                            float(row.get("strike", 0)),
                            float(row.get("lastPrice", 0)),
                            float(row.get("impliedVolatility", 0)),
                            int(row.get("openInterest", 0)),
                            int(row.get("volume", 0)),
                            datetime.datetime.utcnow().isoformat(),
                        ),
                    )
        conn.commit()
        logging.info("Ingested options for %s", ticker)
