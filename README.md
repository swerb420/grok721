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

## Dependencies

Install the base requirements for the lightweight scripts:

```bash
pip install -r requirements.txt
```

To run the advanced examples (`extra_pipeline.py`, `advanced_pipeline.py`, `mega_pipeline.py`, `ultimate_pipeline.py`, `untrimmed_pipeline.py`) install the extra dependencies as well:

```bash
pip install -r requirements.txt -r requirements-extra.txt
```


## Configuration

Create a copy of `.env.example` named `.env` and fill in your API
credentials. Environment variables such as `APIFY_TOKEN` and
`TELEGRAM_BOT_TOKEN` will be loaded automatically when the scripts run.
Refer to `.env.example` for the full list of supported variables.

## Running the async pipeline

For a lightweight example that works well on modest hardware, install
the dependencies and run `async_pipeline.py`:

```bash
pip install -r requirements.txt
python async_pipeline.py
```

The async version uses `aiohttp` for non-blocking requests and stores
gas price data in a local SQLite database.

## License

This project is licensed under the [MIT License](LICENSE).
