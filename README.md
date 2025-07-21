# grok721

This repository contains example data pipeline scripts. The `main.py` script
provides a simplified workflow that ingests gas prices and tweets. The
`extra_pipeline.py` module expands this with additional ingestion functions.

The newly added `advanced_pipeline.py` file contains the full untrimmed
pipeline example that demonstrates how to initialise a comprehensive
SQLite database and perform concurrent ingestion from many data sources.
It is intended as a reference implementation rather than a runnable
script out of the box.

