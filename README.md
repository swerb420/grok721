# grok721

This repository hosts a minimal Python script demonstrating a basic data ingestion pipeline.
The script uses Apify to fetch tweets and Etherscan/Dune to collect gas price data.

## Usage
1. Install the required Python packages:
   ```bash
   pip install apify-client transformers telegram apscheduler requests
   ```
2. Fill in the API key placeholders at the top of `main.py`.
3. Run the script:
   ```bash
   python3 main.py
   ```

The script will fetch data once at startup and then schedule periodic updates.
