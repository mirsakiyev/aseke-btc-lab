# EMA Pullback Trend Continuation

Family `ema_pullback_trend_continuation`. The reviewed signal version is `source_faithful_v1`. The original [Pine v6 script](source_faithful_v1.pine) and [user description](user_description.txt) are preserved verbatim from the supplied sections, with their original UTF-8 bytes and SHA-256 hashes. The registry also stores both artifacts by content hash.

| Artifact | SHA-256 |
| --- | --- |
| Pine v6 | `31ea034ab2ebf4b722cfc56b5b6b22fbf0b074b015942ee3dce7a6e41818684f` |
| Description | `eec7fff419455e74b62afce409111c9a48841fe60d0b84b566ec8cbfff4d0a53` |

## Signal

At a completed, causally available decision close, a flat account can enter long when EMA50 exceeds EMA200, the previous low is at or below previous EMA50 or previous close is below it, and current close exceeds current EMA50. Current volume must strictly exceed its 20-bar SMA and RSI(14) must exceed 40. Short requires EMA50 below EMA200, the previous high at or above previous EMA50 or previous close above it, current close below EMA50, above-average volume, and RSI(14) below 60. Both EMAs use the decision timeframe. The bullish and bearish gates use the same default periods on 4h and 1D bars.

The rolling five-bar low and high include the confirmation bar, as does the volume mean. Long freezes stop at the five-bar low minus 0.5 ATR(14) and target at confirmation close plus 2.5 ATR(14). Short freezes stop at the five-bar high plus 0.5 ATR(14) and target at confirmation close minus 2.5 ATR(14). The signal uses first-close EMA initialization, Wilder smoothing, and Pine's nine-decimal float comparisons. A flat series with both Wilder gain and loss averages at zero leaves RSI undefined. The local evaluator requires 200 contiguous, complete decision bars before admitting a signal and resets after gaps or invalid bars. It records gate outcomes and the indicator values available at the decision. [TradingView RSI](https://www.tradingview.com/support/solutions/43000502338-relative-strength-index-rsi/) · [Pine built-ins](https://www.tradingview.com/pine-script-docs/language/built-ins/) · [Pine float comparisons](https://www.tradingview.com/pine-script-docs/language/type-system/)

## Source and description

| | Executable rule |
| --- | --- |
| A | Trend depends on EMA50 versus EMA200. A separate price-versus-EMA200 filter is absent. |
| B | Both EMAs use the same timeframe. |
| C | The previous-bar condition checks high, low or close against EMA50. It cannot establish direction of approach. |
| D | Multiple pullbacks in one trend can qualify when flat. |
| E | EMA slope and separation do not enter the rule. |
| F | The confirmation bar enters the five-bar stop extreme. |
| G | The confirmation bar enters the volume SMA. |
| H | RSI must be strictly above 40 for longs or strictly below 60 for shorts. |
| I | Target uses confirmation close and stays fixed through entry. |
| J | Target distance is 2.5 ATR. Reward in units of risk depends on the structural stop and actual entry. |
| K | The Pine source specifies no session, maximum hold or daily flat. |
| L | The source allocates 10% of equity to position notional and charges 0.05% per fill side. Risk depends on stop distance and fill. |
| M | P&L labels, arrows, plots, and backgrounds are visual. The reviewed port treats alert text as source material. |

Possible later versions may require a genuine approach from the intended side, EMA slope or separation, ADX trend strength, first-pullback eligibility, or alternate ATR settings. The hypothesis that trending markets favor this strategy still requires research.

## Versions

| Registry ID suffix | Implementation version | Research configuration |
| --- | --- | --- |
| `/source` | `source_faithful_v1` | Chart-timeframe reference on Swing 4h. Both sides, next chart opening price, 10% equity notional before rounding and venue constraints, and 0.05% commission per side. No session or maximum hold. |
| `/position` | `aseke_position_v1` | KuCoin BTC-USDT Spot, 1D, long only. Uses the frozen ASEKE Position risk and costs with 0.25% stop-risk budget, 1× leverage, 25% notional cap, 10 bp fee plus 5 bp adverse proxy per side, and 30-day maximum hold. |
| `/swing` | `aseke_swing_v1` | KuCoin XBTUSDTM futures, 4h, both sides. Uses the frozen ASEKE Swing policy with 0.25% stop-risk budget, 3× leverage, 100% notional cap, 6 bp fee plus 5 bp adverse proxy per side, and seven-day maximum hold. Requires paired mark/trade data, settled funding, and dated contract and margin rules. |

The ASEKE variants use the lab's eligible native-bar IOC timing and risk sizing, with the existing matched rules-only comparison baseline. The source reference can use the existing Phase 15 control under that control's own timing and sizing as a comparison diagnostic. The source reference is ineligible for shortlisting. Each EMA variant preserves the confirmation-price bracket, subject to declared tick rounding and invalid-gap rejection. Day remains a possible separately reviewed 15m variant with an explicit session policy. [Admission requirements](../../DATA_ADMISSION_PLAN.md)

## Timing and parity

Under the Pine defaults used here, the strategy evaluates at bar close and market orders first fill on the next available tick, ordinarily the next chart bar's open. Adjacent chart bars share a time boundary, so the next open can have the signal close's UTC timestamp while using the next bar's opening price. TradingView's historical broker emulator assumes an OHLC path ordered by the opening price's proximity to the high or low and fills price-based orders crossed between bars at the next open. Lower-timeframe magnification and calculation overrides can change its simulated results. [TradingView strategy execution and broker emulator](https://www.tradingview.com/pine-script-docs/concepts/strategies/)

The lab reuses its Spot and futures simulation engines, with futures triggers on mark and fills on trade price. It records uncertain same-bar bracket collisions and rejects entries with invalid rounded brackets at the modeled fill. The source benchmark leaves open exposure unresolved at the replay boundary without a synthetic time exit. The shared account replay conservatively skips an entry when signal availability ties the interval start or a prior exit. Native candle granularity, funding, costs, contract terms, margin, and handling of unavailable inputs can also differ from a TradingView export. The current evidence supports synthetic fixture checks only. Trade-by-trade parity requires TradingView results for the same symbol, candles, inputs, and simulator properties.

**BLOCKED_DATA.** Real economic replay requires independently permitted, admitted development data. The Spot candidate still requires confirmed reuse permission, dated venue terms, and approval of availability and cost assumptions. For this 200-daily-bar family, start the export on 2022-06-01 or earlier if retaining the 2023-01-01 development start. That gives at least 214 calendar days before development, subject to complete valid data. The required KuCoin futures bundle is missing. [Strategy Lab data admission plan](../../DATA_ADMISSION_PLAN.md)

Each EMA replay case retains the decision grid and a losslessly compressed `decision_audit`, including unavailable decisions, indicator values, market gates, profile signal direction, and account replay outcome where evaluated. Decode `data` with Base64, gzip and JSON in that order. Each decoded row follows `columns`, and its UTC time is `start + row_index × step`. Verify the decompressed bytes with the included SHA-256. The single-decision `evaluate` CLI prints those values directly for an admitted dataset and accepts `--position-open` to exercise the account gate.

## Inspect and test

From the repository root, import the three frozen variants and run the deterministic signal contract. The import uses the existing Strategy Lab Phase 15 baseline snapshots for policy settings. Replace `$version` with a hash returned by `list`. `test` runs the synthetic signal contract and records its code hash and results in that version's lifecycle.

```powershell
.\strategy-lab.ps1 import-ema-pullback
.\strategy-lab.ps1 list
.\strategy-lab.ps1 show $version
.\strategy-lab.ps1 test $version
.\strategy-lab.ps1 export-source 31ea034ab2ebf4b722cfc56b5b6b22fbf0b074b015942ee3dce7a6e41818684f
.\strategy-lab.ps1 export-source eec7fff419455e74b62afce409111c9a48841fe60d0b84b566ec8cbfff4d0a53
```

In Streamlit, open **Workspace → Strategy Library / Research Lab → Strategy Library** and select the EMA Pullback Trend Continuation family.

## Delivery files

- Added `lab/strategy_lab/ema_pullback.py`, `ema_execution.py`, and `ema_registration.py` for indicators, execution adaptation, and registration.
- Updated `lab/strategy_lab/__main__.py`, `dashboard.py`, `evaluator.py`, `jobs.py`, `replay.py`, `specification.py`, and `testing.py` for Strategy Lab integration.
- Added `tests/test_ema_pullback_execution.py`, `test_ema_pullback_indicators.py`, `test_ema_pullback_registration.py`, and `test_ema_pullback_strategy.py`.
- Added this README, the original Pine v6 and description artifacts, and `.gitattributes` to preserve the artifact bytes. Updated `STRATEGY_LAB_RUNBOOK.md`.
