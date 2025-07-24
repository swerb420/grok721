# grok721

This repository contains several example data pipeline scripts. `main.py` provides a minimal workflow that ingests gas prices, tweets and basic stock option chains. The other modules showcase progressively more complex pipelines and are meant as reference implementations.

## Pipeline overview
- `main.py` – basic ingestion example.
- `async_pipeline.py` – asynchronous variant using `aiohttp`.
- `extra_pipeline.py` – **experimental** expanded ingestion example.
- `advanced_pipeline.py` – **experimental** pipeline with many placeholders.
- `mega_pipeline.py` – **experimental** build with numerous sources.
- `ultimate_pipeline.py` – **experimental** maximal demonstration.
- `untrimmed_pipeline.py` – **experimental** raw variant with the full import stack.
- `sentiment_pipeline.py` – experimental sentiment-driven trading pipeline.
- `ingest_yfinance_options` – helper for storing basic stock option chains.

Additional resources describing the Dune Analytics dashboards referenced by
the pipelines can be found in [docs/dune_dashboards.md](docs/dune_dashboards.md).

## Dependencies

Create and activate a local virtual environment before installing
the requirements:

```bash
python3 -m venv venv
source venv/bin/activate
```

Using a virtual environment helps keep dependencies isolated on macOS and
prevents conflicts with system packages.

The `requirements.txt` file now contains only the minimal packages needed to
run `main.py` and `async_pipeline.py`. You can install them either with
`pip` as shown below or via the provided Conda environment.
Install these core dependencies first:

```bash
pip install -r requirements.txt
```

If you prefer Conda, create the environment defined in
`environment.yml` instead:

```bash
conda env create -f environment.yml
conda activate grok721
```

The packages in `requirements-extra.txt` are optional and are only needed for
the experimental pipelines (`extra_pipeline.py`, `advanced_pipeline.py`,
`mega_pipeline.py`, `ultimate_pipeline.py`, `untrimmed_pipeline.py`).
Install them in addition to the base requirements if you want to explore these
scripts:

```bash
pip install -r requirements.txt -r requirements-extra.txt
```

When using Conda, activate the `grok721` environment first and then run the
same `pip` command to add these extras.


### Conda

A minimal Conda environment is also provided for running `main.py` and `async_pipeline.py`:

```bash
conda env create -f environment.yml
conda activate grok721
```

Install any optional packages from `requirements-extra.txt` with `pip` if you want to try the experimental pipelines.


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
The `HISTORICAL_START` variable controls how far back tweets are fetched and
defaults to `2017-01-01`.
Refer to `.env.example` for the complete list of supported variables.

The `MAX_TWEETS_PER_USER` setting controls how many tweets are fetched for each
username. Smaller values reduce API usage and memory footprint.

## Running the async pipeline

For a lightweight example that works well on machines with limited memory,
install the dependencies and run `async_pipeline.py`:

```bash
pip install -r requirements.txt
python async_pipeline.py
```

For additional performance, you can install [`uvloop`](https://github.com/MagicStack/uvloop)
to replace the default event loop on Unix-like systems:

```bash
pip install uvloop  # optional
```

The async version uses `aiohttp` for non-blocking requests and stores
gas price data in a local SQLite database. On low-memory systems it is
often more stable than the synchronous pipelines.

## Testing

Run the unit tests with [`pytest`](https://docs.pytest.org/) from the repository
root:

```bash
pip install -r requirements.txt -r requirements-extra.txt
pytest -q
```

## Docker

Build a container image that runs the async pipeline:

```bash
docker build -t grok721 .
docker run --rm --env-file .env grok721
```

## License

This project is licensed under the [MIT License](LICENSE).
