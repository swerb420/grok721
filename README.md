# grok721

This repository contains several example data pipeline scripts. `main.py` provides a minimal workflow that ingests gas prices and tweets. The other modules showcase progressively more complex pipelines and are meant as reference implementations.

## Pipeline overview
- `main.py` – basic ingestion example.
- `async_pipeline.py` – asynchronous variant using `aiohttp`.
- `extra_pipeline.py` – adds additional ingestion functions.
- `advanced_pipeline.py` – full pipeline with database setup and concurrency.
- `mega_pipeline.py` – expanded version with more sources.
- `ultimate_pipeline.py` – maximal demonstration combining all features.
- `untrimmed_pipeline.py` – raw variant containing the full import stack.

Additional resources describing the Dune Analytics dashboards referenced by
the pipelines can be found in [docs/dune_dashboards.md](docs/dune_dashboards.md).

## Dependencies

The `requirements.txt` file now contains only the minimal packages needed to
run `main.py` and `async_pipeline.py`.
Install these core dependencies first:

```bash
pip install -r requirements.txt
```

Heavier analytics and machine learning libraries have been moved to
`requirements-extra.txt`.  Only install this file when you plan to run one of
the advanced pipelines (`extra_pipeline.py`, `advanced_pipeline.py`,
`mega_pipeline.py`, `ultimate_pipeline.py`, `untrimmed_pipeline.py`):

```bash
pip install -r requirements.txt -r requirements-extra.txt
```


## Configuration

Create a copy of `.env.example` named `.env` and fill in your API
credentials. Environment variables such as `APIFY_TOKEN` and
`TELEGRAM_BOT_TOKEN` will be loaded automatically when the scripts run.
The pipelines also expose individual query IDs for the Dune dashboards
listed in `docs/dune_dashboards.md`:

```
DUNE_GAS_PRICES_QUERY_ID
DUNE_HYPERLIQUID_STATS_QUERY_ID
DUNE_HYPERLIQUID_QUERY_ID
DUNE_GMX_ANALYTICS_QUERY_ID
DUNE_HYPERLIQUID_FLOWS_QUERY_ID
DUNE_PERPS_HYPERLIQUID_QUERY_ID
DUNE_GMX_IO_QUERY_ID
DUNE_AIRDROPS_WALLETS_QUERY_ID
DUNE_SMART_WALLET_FINDER_QUERY_ID
DUNE_WALLET_BALANCES_QUERY_ID
```
Refer to `.env.example` for the complete list of supported variables.

## Running the async pipeline

For a lightweight example that works well on machines with limited memory,
install the dependencies and run `async_pipeline.py`:

```bash
pip install -r requirements.txt
python async_pipeline.py
```

The async version uses `aiohttp` for non-blocking requests and stores
gas price data in a local SQLite database. On low-memory systems it is
often more stable than the synchronous pipelines.

## Testing

Run the unit tests with [`pytest`](https://docs.pytest.org/) from the repository
root:

```bash
pytest -q
```

## License

This project is licensed under the [MIT License](LICENSE).
