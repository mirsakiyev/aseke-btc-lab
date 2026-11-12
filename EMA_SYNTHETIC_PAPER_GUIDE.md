# Kraken-reference EMA paper study

**PAPER ONLY · MANUAL_OUTSIDE_LAB · No validated model.**

The three original EMA books retain their logical account identities and independent 10,000-unit starting balances. The original four Breakout books remain separate. Historical warmup updates indicators only. Decisions require completed Kraken BTC/USD candles with causal receipt evidence and a frozen future study boundary.

| Profile | Decision timeframe | Primary model | Maximum holding period |
| --- | --- | --- | --- |
| Day | 15 minutes | 3× synthetic linear perpetual, long or short | 480 minutes |
| Swing | 4 hours | 3× synthetic linear perpetual, long or short | Seven days |
| Position | UTC daily | 1× cash-style, long only | 30 days |

The original EMA50/200, Wilder RSI14 and ATR14, native volume SMA20, previous-bar pullback, and five-bar structural extrema use completed Kraken bars. The stop includes the 0.5 ATR buffer and the 2.5 ATR target is anchored to the confirmation close. The market reference for hypothetical fills is a causally received closed one-minute Kraken candle.

## Frozen simulation assumptions

- Each fill uses a synthetic taker fee of 10 basis points and an adverse execution adjustment of five basis points. The cost stress doubles both.
- Leveraged positions incur a modeled financing debit of one basis point every eight hours on both sides when already open. The adverse-financing scenario uses three basis points. The cash-style Position baseline uses zero financing.
- Initial isolated margin is position notional divided by leverage. Maintenance is 1% of notional and liquidation carries a modeled 50-basis-point charge. The modeled liquidation trigger reserves closing costs, and the protective stop requires at least 0.5 ATR of clearance. Liquidation uses the Kraken reference proxy.
- Opening gaps use the observed reference open. Competing intraminute triggers resolve in the order liquidation, stop, target. Intraminute liquidation uses the adverse reference extreme. One-minute OHLC cannot identify tick ordering.
- Sizing reserves 0.25% of account equity for the modeled stop, including estimated costs, and respects the notional and collateral limits. Gap losses, accrued financing, and liquidation can exceed the planned stop risk. Size is rounded down in BTC.
- Kraken reference prices are USD. Ledger units use frozen nominal USD/USDT parity for simulation, with separate ±3% currency sensitivity. The modeled conversion does not represent an observed exchange rate or deposit.
- The 3×/5× comparison uses isolated counterfactual calculations. It has no effect on primary journals, positions, notifications or study clocks. At the same BTC size, leverage changes margin and liquidation distance. Price P&L depends on BTC quantity and price movement.

Kraken Spot supplies price and volume reference data. Actual futures mark, settlements, futures basis, executable spread and market impact remain outside this model. Each primary study starts at the first qualified future decision and retains its 90-day window and 25 economically complete trade evidence floor.

## Inspect the local deployment

The chart recovery amendment keeps the original strategy, execution model, eligibility and protective-minute path. The existing Kraken adapter can fetch one authorized missing chart candle per cycle within the venue's rolling history. A late receipt can repair historical indicators. The missed decision stays closed, and the next decision requires a timely chart, completed processing and a readiness checkpoint from before its UTC boundary. Missing protective minutes leave the position and accounting unresolved and surface an operational warning. Source conflicts or a gap beyond available history can still require operator review.

```powershell
.\ema-paper.ps1 synthetic-status
.\ema-paper.ps1 status --book day
.\ema-paper.ps1 status --book swing
.\ema-paper.ps1 status --book position
.\ema-paper.ps1 compare
.\health.ps1 status
```

The Prospective Paper dashboard shows the current status, signal and economic evidence for the original books. Where the reviewed permission allows it, the existing private Telegram outbox delivers allowlisted derived lifecycle summaries to the authorized recipient. Read the local permission and latest watchdog receipt for the current deployment state. Keep private source correspondence, receipt and checkpoint identities, indicator values, trade journals, and delivery state out of public version control. [Public release checklist](PUBLIC_REPOSITORY.md)

After confirming permission for private derived summaries and configuring the existing user-scoped bot, `.\health.ps1 enable-ema-private-telegram` verifies the configured private destination and records future-only cursors. The command sends no message. The scheduled watchdog sends summaries of newly committed experimental ideas, simulated entries, expiries or invalidations, simulated exits, and debounced operational events. Existing routes establish a separate future-only cursor for newly added idea categories. The allowlist includes paper event and ledger timestamps and excludes individual prices, stop and target levels, quantities, notional, source receipts and raw market data. Transport failures and ambiguous acceptance remain visible in local delivery state.
