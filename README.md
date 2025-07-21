# grok721

This repository contains example data pipeline scripts. The `main.py` script
provides a simplified workflow that ingests gas prices and tweets. The
`extra_pipeline.py` module expands this with additional ingestion functions.

The `advanced_pipeline.py` and `mega_pipeline.py` files provide more
comprehensive examples. They demonstrate how to initialise a
comprehensive SQLite database and perform concurrent ingestion from many
data sources. These scripts are intended as reference implementations
rather than runnable programs out of the box.

## Configuration

Create a copy of `.env.example` named `.env` and fill in your API
credentials. Environment variables will be loaded automatically when the
scripts run.

## Running the async pipeline

For a lightweight example that works well on modest hardware, install
the dependencies and run `async_pipeline.py`:

```bash
pip install -r requirements.txt
python async_pipeline.py
```

The async version uses `aiohttp` for non-blocking requests and stores
gas price data in a local SQLite database.


## Running the tests

Install the requirements and execute `pytest`:

```bash
pip install -r requirements.txt
pytest
```

The test suite uses stub modules so the optional heavy dependencies are not required.
