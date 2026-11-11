# Phase 15 first completed Day paper trade

The first completed Day/fixed paper trade was a KuCoin `XBTUSDTM` short. The modeled entry was 74,381.2 USDT at 03:46 UTC. The trade closed on its stop at 74,788.9 USDT at 04:28 UTC for a net loss of 20.88248652 USDT, or 0.8459068R. The 42-minute modeled hold complied with the Day UTC session.

Unless indicated otherwise, event times below are UTC on November 10, 2026. The trade took place on November 9 from 22:46 to 23:28 in Toronto (EST). Source timestamps identify the modeled market event. Receipt and recording timestamps identify when the evidence was actually available and processed.

## Frozen publication and sizing

| Field | Recorded value |
| --- | --- |
| Idea ID | `864444796912d21d3b72065ce2576cadf939cb371039194b4bee881ee5868617` |
| Book | Day/fixed, `cb9af59101023f1522ed68ac18d64fdcf00e9b19bad98a652174d5ec3b2b3804` |
| Market and direction | KuCoin `XBTUSDTM` USDT-margined linear perpetual, SHORT |
| Strategy | `kucoin-manual-breakout-v1`, `breakout_v1` entry and `fixed_r_v1` exit, rules only |
| Decision boundary and publication | 03:45:00 and 03:45:52.144758 |
| Entry window | 03:46:00 inclusive through 03:55:52.144758 exclusive. The first modeled open followed publication by 7.855242 seconds. |
| Breakout threshold | 74,600.1 USDT. The completed decision candle closed at 74,398.5 and both higher-timeframe regimes were bearish. |
| Frozen adverse-adjusted entry range | 74,355.6 to 74,441.43571428572 USDT, with a short limit of 74,355.6. Publication bid was 74,425.9. |
| Stop and take-profit | Mark stop 74,742.0, from 2 × ATR. Published illustrative 2R target 73,582.8 at the order bound. The original paper engine derived a target of 73,659.5 from the actual modeled fill. The exact tick result is 73,659.6, as explained below. |
| Size and risk | 42 contracts × 0.001 BTC = 0.042 BTC at 3× selected leverage. Risk budget 25 USDT, planned risk 24.68650968 USDT. |
| Actual modeled entry exposure | 3,124.0104 USDT notional, 31.240104% of starting equity. Reserved margin 1,041.3368 USDT. |

## Event and mark-to-market timeline

| Market/source time | Receipt or recording time | Evidence and book treatment |
| --- | --- | --- |
| 03:45:00 | Decision candle received 03:45:46.072553 | Phase 10 saved its conditional decision at 03:45:47.253175. Phase 15 used that decision's quote receipts and the same closed-candle inputs. |
| 03:45:52.144758 | Published at the same time | The frozen idea became eligible at the next minute open. |
| 03:46:00–03:47:00 | Trade received 04:00:54.439988, mark received 04:00:55.002098 | Raw trade open 74,418.5 crossed the short threshold and mark open 74,420.62 remained below the stop. The modeled sell at 74,381.2 was inside the frozen range. |
| 03:46:00 | `ENTRY_OBSERVED` and `HYPOTHETICAL_FILL` recorded at 04:00:55.756658 | First receipt-backed accounting. Entry commission was 1.87440624 USDT. Wallet cash became 9,998.12559376 USDT, with 8,956.78879376 USDT free after margin. |
| 03:47:00 | In the 04:00 batch | First completed minute marked at 74,504.46, giving 9,992.94867376 USDT marked equity. |
| 04:00:00 | In the 04:00 batch | Mark close 74,531.23, equity 9,991.82433376 USDT. |
| 04:15:00 | Trade received 04:15:57.408346, mark received 04:15:57.955479 | Mark close 74,603.40, equity 9,988.79319376 USDT. |
| 04:27:00 | In the 04:30 batch | Last completed open-position mark 74,720.09, equity 9,983.89221376 USDT. |
| 04:27:00–04:28:00 | Trade received 04:31:00.524942, mark received 04:31:01.084219 | Mark opened at 74,720.06, then reached 74,751.50. It crossed the 74,742.0 stop intraminute. The paired trade high of 74,751.5 was the conservative short-cover proxy. The 5 bps adverse adjustment and upward 0.1 tick rounding gave 74,788.9. |
| 04:28:00 | `CLOSED` recorded at 04:31:01.290849 | Reason `stop`. The close used the trigger candle's ending timestamp because the exact within-minute trigger time is unknown. Margin was released, the book went flat, and the active cursor was cleared. |

All 42 paired trade and mark minutes were present from the modeled entry through the exit candle. The in-memory replay reconstructed 41 open-position minute-close marks. Mark lows across the path never fell below 74,418.5, so neither the original nor corrected 2R target was reached. There were no expected funding settlements during the hold and no liquidation. The trade result is a modeled outcome from later candle receipts. The entry evidence was accounted nearly 15 minutes after the modeled entry, and the close was accounted just over 3 minutes after the exit candle ended.

### Session, expiry and the 04:28 field

The publication expired exactly ten minutes after issue at 03:55:52.144758. That TTL governs entry eligibility. The 03:46 open was eligible and before the November 10 entry cutoff of 22:00 UTC. The effective open-position deadline was the earlier of 11:46 UTC (entry plus 480 minutes) and 23:55 UTC (the mandatory flat deadline for the same UTC date). The stop triggered well before both. The evening in Toronto crossed neither deadline because the Day session rolls on the UTC date.

`last_source_close` reports the close time of the last native candle processed by the book. The final 04:27–04:28 candle also generated the stop, so its close equals this trade's modeled exit time. The separate active `cursor` is null, with no pending idea, position or exposure. The ledger records no session breach.

## Accounting reconciliation

Amounts are USDT except quantity and R. The frozen cost model charged 6 bps taker commission and applied 5 bps adverse price adjustment per side, with the applicable tick rounding.

| Component | Calculation | Value |
| --- | --- | ---: |
| Raw gross before adverse | `0.042 × (74,418.5 − 74,751.5)` | −13.98600000 |
| Entry adverse cost | `0.042 × (74,418.5 − 74,381.2)` | 1.56660000 |
| Exit adverse cost | `0.042 × (74,788.9 − 74,751.5)` | 1.57080000 |
| Gross at modeled fills | Raw gross less the two adverse costs | −17.12340000 |
| Entry commission | `3,124.0104 × 0.0006` | 1.87440624 |
| Exit commission | `3,141.1338 × 0.0006` | 1.88468028 |
| Total commission | Entry plus exit commission | 3.75908652 |
| Funding, liquidation cost and unfunded deficit | No events in the holding window | 0.00000000 |
| Realized net P&L | Modeled gross less commissions | −20.88248652 |
| Final realized equity and cash | Starting equity 10,000.00 plus realized net | 9,979.11751348 |
| Net R | `−20.88248652 / 24.68650968` | −0.8459068046R |
| Turnover | `3,124.0104 + 3,141.1338` | 6,265.14420000 |

The 3.1374 USDT adverse cost is already embedded in modeled gross. Subtracting it again would double-count it. The book's residual cash reconciliation difference is approximately 0.0000000000005 USDT in absolute value.

## Unfilled publications

| Idea ID | Publication and entry window | Result and exact evidence |
| --- | --- | --- |
| `16e99c292f3988f8bd3997efabd3e6e28707438d37980b6e642a66c8cf8c7d08` | Published 02:46:56.268938. Eligible 02:47:00 through 02:56:56.268938 exclusive. | `EXPIRED` at the first disallowed minute open, 02:57:00, recorded 03:00:54.440521. The ten eligible opens from 02:47 through 02:56 were all above the short threshold of 74,805.6. The lowest was 74,820.7. The mark high stayed below the 75,103.7 stop. Native request IDs 85 and 86. |
| `31716e440f0b5f7d31e6c490dea294f053b4475afb63dacbc81cc1ac28519b44` | Published 03:31:04.379654. Eligible 03:32:00 through 03:41:04.379654 exclusive. | `INVALIDATED` at 03:32:00, recorded 03:45:52.121039. The 74,615.6 trade open crossed the 74,714.8 short threshold and the 74,612.74 mark open remained below the 74,946.1 stop. The adverse-adjusted sell price of 74,578.2 fell 10.5 USDT below the 74,588.7 minimum and short limit, so size was zero. Native request IDs 92 and 93. |

## Point-in-time publication evidence

The Phase 9 bootstrap and Phase 10 cycle receipts reconstruct all 880 decision candles for each publication. Every checksum-verified payload was filtered by its original receipt timestamp, and each reconstructed frame matches both the saved Phase 10 input digest and the immutable Phase 15 publication digest. All latest input candles and both venue quotes had arrived by the original Phase 10 decision and the later Phase 15 publication. All three pairs of publication quotes passed the recorded freshness and clock-order rules.

| Decision boundary | Latest input candle receipt | Bid/ask market time → receipt | Mark market time → receipt | Phase 10 decision → Phase 15 publication |
| --- | --- | --- | --- | --- |
| 02:45:00 | 02:45:46.648064 | 02:46:20.041000 → 02:46:19.578832 | 02:46:20 → 02:46:20.115038 | 02:46:20.124656 → 02:46:56.268938 |
| 03:30:00 | 03:30:41.213941 | 03:30:42.288000 → 03:30:41.807243 | 03:30:42 → 03:30:42.354960 | 03:30:42.370637 → 03:31:04.379654 |
| 03:45:00 | 03:45:46.072553 | 03:45:47.189000 → 03:45:46.699087 | 03:45:47 → 03:45:47.237464 | 03:45:47.253175 → 03:45:52.144758 |

The bid/ask payload timestamps slightly lead their local receipt timestamps and still precede the actual decisions. The offsets are inside the configured one-second clock-skew allowance. The completed trade's entry minutes arrived 55.002098 seconds after their 04:00 capture boundary, inside the two-minute entry receipt allowance. The modeled 03:46 execution retains the 04:00 receipt and recording timestamps.

## Corrections and verification

- **Phase 15 target precision.** The original engine calculated the short target as a binary float before rounding down to the 0.1 tick. For this fill, the exact 2R target is `74,381.2 − 2 × (74,742.0 − 74,381.2) = 73,659.6`. The intermediate float fell just below that tick and the original engine used 73,659.5. Phase 15 now computes the price difference in decimal before applying the same registered directional rounding in both the publication preview and the modeled fill. A focused short fixture failed on the original target and passes with an exact 73,659.6 touch. The historical trade stopped under either value. The corrected engine reproduced all eight original event rows byte for byte and the exact final book in an isolated database.
- **Telegram target wording and deduplication.** The old alert described the published bound-based price as a fixed target. The alert now says the published 2R price is illustrative and is recomputed from the modeled fill, matching the dashboard. The three real publications predate watchdog commissioning at 15:39:35 UTC on November 10. The live idea outbox contains zero rows. The actual scanner, pointed at the original paper rows and an isolated outbox, queued zero with the live commissioning cutoff. With a fixture cutoff and transport, it delivered each of the three unique publications once across repeated scans and database reopen. Tests cover fixed and trailing messages and exclude watch-only decisions.
- **Phase 17.1 scheduled follow-up.** The existing managed collector dispatched the Position pilot retry at 02:45:59.054481 UTC on November 11. It completed at 02:46:05.978921 with exit code 0, 24 of 24 Spot trade hours and zero mature labels. Its state is `WAITING_FOR_MATURE_SAMPLES` and recurring eligibility remains on. The watchdog cleared the incident at 02:50:28. Telegram accepted its recovery on attempt 2 at 02:53:29, message ID 32.

The full suite passed 149 tests. The original paper database retained SHA-256 `485742bb2f5451de43f6d59bc1cc10c4697d3fc18bf3f25aa5899326aba5aed1` across the audit and replays. Execution is **MANUAL_OUTSIDE_LAB** and approval is **No validated model**.

## Evidence references

- `runs/phase15/paper.sqlite3`, table `event`. Completed trade event keys are `1c4ad38d33d262b70891cdd58b5c101551175a00043ecce46d78ad30bc9a5792` (publication), `917999bfbffd42443bc87982c5f61cf84e68adff6d448d06cfb1c9eff3427c55` (entry observed), `cfed250ddea70433fcf95ed6e228327cdadb53057f7c75f12fc80375b908f693` (hypothetical fill), and `342eba63d5a5441243a324f88b9a5dbf541ce4f757cccf6f2078479313fa00a7` (closed).
- `runs/phase15/registrations/00850dad14451b34635ece9adba09b1d29263ff63cb01eeab68d5bb4e4d16ae4.json` and `runs/phase15/REPORT.md`.
- `runs/phase13/observations/9e8ac6a44015ee11bb4c2b81d614c64a17475b932777223c53f5973f33056aed/observations.sqlite3` and its request-linked raw files. Completed trade price requests 94–95, 97–98, and 99–100 span the entry, marks and exit.
- `runs/phase9/acquisition/4ec15336506eeba3a46890c5789dd27252cb684d02694a04c81880d9df9ca208` for bootstrap receipts. `runs/phase10/cycles`, `runs/phase10/raw` and `runs/phase10/decisions/day` for live inputs. The completed trade's decision file is `3dd2f6bcb6e8cf4727ac7f8cf182570163c4c0fda1fbc7b5835631bd9e772ba4.json`.
- `runs/phase17/pilot/worker.json`, `runs/phase17/pilot/profiles/position/readiness.json`, `runs/phase17/pilot/pilot-check/position/20261111T000000/{result,resources}.json`, and `runs/health/watchdog.sqlite3`.
