# Dune Analytics Dashboards

The following community dashboards are useful for tracking profitable wallets,
perpetual trading activity, and on-chain sentiment. They can be executed with
the Dune API (replace the query IDs with your own). Each entry includes the
original author and a short description of the data available.

1. **Gas Prices** – [@alex_kroeger](https://dune.com/kroeger0x/gas-prices) (`DUNE_GAS_PRICES_QUERY_ID`)
   - Tracks historical gas prices on Ethereum and major L2 chains.
   - Useful for correlating wallet activity with spikes in transaction costs.
2. **Hyperliquid Stats [+ User Analytics]** – [@x3research](https://dune.com/x3research/hyperliquid) (`DUNE_HYPERLIQUID_STATS_QUERY_ID`)
   - Provides user profit and volume analytics for the Hyperliquid exchange.
   - Helps identify top traders and profitable wallets.
3. **Hyperliquid** – [@kambenbrik](https://dune.com/kambenbrik/hyperliquid) (`DUNE_HYPERLIQUID_QUERY_ID`)
   - Shows liquidations and trading volumes across assets on Hyperliquid.
   - Useful for spotting unusual activity or liquidation spikes.
4. **GMX Analytics** – [@gmx-io](https://dune.com/gmx-io/gmx-analytics) (`DUNE_GMX_ANALYTICS_QUERY_ID`)
   - Visualises GMX perpetual trading volumes, fees and wallet P&L.
   - Valuable for comparing GMX and Hyperliquid wallet performance.
5. **Hyperliquid Flows** – [@mogie](https://dune.com/mogie/hyperliquid-flows) (`DUNE_HYPERLIQUID_FLOWS_QUERY_ID`)
   - Tracks deposits and withdrawals on Hyperliquid.
   - Can surface large inflows or outflows from specific wallets.
6. **Perps & Hyperliquid** – [@uwusanauwu](https://dune.com/uwusanauwu/perps) (`DUNE_PERPS_HYPERLIQUID_QUERY_ID`)
   - Contains cross‑platform funding rates and open interest data.
7. **GMX.io** – [@lako](https://dune.com/lako/lako-labs-gmx) (`DUNE_GMX_IO_QUERY_ID`)
   - Breaks down fees, liquidations and wallet interactions on GMX.
8. **Airdrops and Wallets v2** – [@cypherpepe](https://dune.com/cypherpepe/airdrops-and-wallets-v2) (`DUNE_AIRDROPS_WALLETS_QUERY_ID`)
   - Ranks wallets by airdrop profits and token volumes.
9. **Smart Wallet Finder** – [@chimpy](https://dune.com/chimpy/smart-wallet-finder) (`DUNE_SMART_WALLET_FINDER_QUERY_ID`)
   - Highlights profitable or high‑activity wallets across multiple chains.
10. **Wallet Balances** – [@0xRob](https://dune.com/0xRob/wallet-balances) (`DUNE_WALLET_BALANCES_QUERY_ID`)
    - Shows historical wallet balances and asset diversity.

These dashboards can be queried programmatically using the Dune API. Store the
results in your database to correlate wallet profitability with gas prices,
sentiment indicators and other market data.

## Using the Dune free tier

Dune's Community plan (as of 2025) permits unlimited query executions with a
40 requests/min rate limit and returns up to 1,000 rows per execution. This is
enough to ingest historical data from all ten dashboards by batching the
requests:

- The largest dataset, **Gas Prices**, spans about 3&nbsp;million hourly rows.
  Chunking by month requires roughly 100–200 executions.
- Other dashboards contain fewer than 100&nbsp;k rows each, so the total volume
  across all dashboards is around 500&nbsp;k–600&nbsp;k rows (approximately
  1–2&nbsp;GB of storage).
- A full initial ingest completes in roughly two to four hours on a machine with
  8&nbsp;GB of RAM and a 100&nbsp;GB SSD.
- Daily refreshes typically take only 10–20&nbsp;minutes.

Paid tiers are optional and mainly provide higher request rates or larger result
limits. The free tier is otherwise sufficient for the workflows demonstrated in
this repository.
