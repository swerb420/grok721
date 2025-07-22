# Dune Analytics Dashboards

The following community dashboards are useful for tracking profitable wallets,
perpetual trading activity, and on-chain sentiment. They can be executed with
the Dune API (replace the query IDs with your own). Each entry includes the
original author and a short description of the data available.

1. **Gas Prices** – [@alex_kroeger](https://dune.com/kroeger0x/gas-prices)
   - Tracks historical gas prices on Ethereum and major L2 chains.
   - Useful for correlating wallet activity with spikes in transaction costs.
2. **Hyperliquid Stats [+ User Analytics]** – [@x3research](https://dune.com/x3research/hyperliquid)
   - Provides user profit and volume analytics for the Hyperliquid exchange.
   - Helps identify top traders and profitable wallets.
3. **Hyperliquid** – [@kambenbrik](https://dune.com/kambenbrik/hyperliquid)
   - Shows liquidations and trading volumes across assets on Hyperliquid.
   - Useful for spotting unusual activity or liquidation spikes.
4. **GMX Analytics** – [@gmx-io](https://dune.com/gmx-io/gmx-analytics)
   - Visualises GMX perpetual trading volumes, fees and wallet P&L.
   - Valuable for comparing GMX and Hyperliquid wallet performance.
5. **Hyperliquid Flows** – [@mogie](https://dune.com/mogie/hyperliquid-flows)
   - Tracks deposits and withdrawals on Hyperliquid.
   - Can surface large inflows or outflows from specific wallets.
6. **Perps & Hyperliquid** – [@uwusanauwu](https://dune.com/uwusanauwu/perps)
   - Contains cross‑platform funding rates and open interest data.
7. **GMX.io** – [@lako](https://dune.com/lako/lako-labs-gmx)
   - Breaks down fees, liquidations and wallet interactions on GMX.
8. **Airdrops and Wallets v2** – [@cypherpepe](https://dune.com/cypherpepe/airdrops-and-wallets-v2)
   - Ranks wallets by airdrop profits and token volumes.
9. **Smart Wallet Finder** – [@chimpy](https://dune.com/chimpy/smart-wallet-finder)
   - Highlights profitable or high‑activity wallets across multiple chains.
10. **Wallet Balances** – [@0xRob](https://dune.com/0xRob/wallet-balances)
    - Shows historical wallet balances and asset diversity.

These dashboards can be queried programmatically using the Dune API. Store the
results in your database to correlate wallet profitability with gas prices,
sentiment indicators and other market data.
