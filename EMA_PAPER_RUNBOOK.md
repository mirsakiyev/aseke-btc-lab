# EMA prospective paper books

The current original EMA books use the [Kraken-reference synthetic protocol](EMA_SYNTHETIC_PAPER_GUIDE.md). Run `.\ema-paper.ps1 synthetic-status` for local readiness. The KuCoin and cross-venue procedures below document earlier frozen registrations.

**MANUAL_OUTSIDE_LAB · No validated model.** These are three independent, explicitly authorized hypothetical accounts. Their starting equity is 10,000 USDT each. The source-reference strategy has no paper account. The original four Phase 15 accounts and their study rules are independent.

The earlier feature-only Kraken amendment required independently authorized native KuCoin data, economic evidence and commissioning of the same-book dispatcher. The frozen original protocol and its historical intake commands are described below for audit.

| Book | Instrument | UTC decisions | First eligible entry grid after confirmed publication | Protection observations | Hold cap |
| --- | --- | --- | --- | --- | --- |
| Day | KuCoin XBTUSDTM futures, long and short, 3× nominal | 15 minutes | Next 15-minute open | 1-minute trade and mark | 480 minutes from modeled fill |
| Swing | KuCoin XBTUSDTM futures, long and short, 3× nominal | 4 hours | Next 4-hour open | 1-minute trade and mark | 10,080 minutes |
| Position | KuCoin BTC-USDT Spot, long only, 1× | UTC daily, aggregated from hourly OHLCV | Next hourly open | 1-minute trade | 43,200 minutes |

All three have a 24/7 UTC session. The Day hold cap runs across midnight, including on weekends. Its 480-minute cap and continuous session are deliberate ASEKE rules. The original Phase 15 Day book retains its 22:00 entry cutoff and 23:55 flat rule.

## Frozen readiness and study clock

Each registration pins its strategy version, source and execution code, risk, fees, adverse price proxy, entry clock, and Phase 15 comparison books. The EMA stop and target remain tied to the confirmation candle. Orders use the frozen price band, tick/lot and capacity checks, and one committed opening instant after actual publication. Futures triggers use mark prices and modeled exits use trade prices. Missing settlement, changed dated terms, or ambiguous intraminute sequencing can leave an exposure unresolved. There is no synthetic fill or forced valuation across a missing interval.

Warmup requires 200 consecutive valid and causally available chart candles, with finite indicators and dated execution evidence. This is at least 50 hours for Day, 33 days and 8 hours for Swing, and 200 UTC days for Position. It uses no historical import or protected Phase 14/15 or Phase 17.1 data. A source grant and receipt handoff must be bound to the book. Provider receipt, local handoff, confirmation boundary, ledger record and notification acceptance are separate timestamps.

After readiness, the next strictly future decision boundary is frozen as the start, independent of signals. The primary horizon lasts 90 calendar days. Modeled entry opens must precede its exclusive end. Follow-through is the book's hold cap plus two hours for execution evidence. An unresolved outcome or fewer than 25 complete closed trades at closeout yields `INCONCLUSIVE`. Larger samples still require operator review and comparable evidence.

The final source observation in a decision bucket must be available within 120 seconds. The existing 30-second processing allowance starts after the latest causal evidence. Publication expires six minutes after the decision boundary. Only the next frozen entry grid can open a position. A missing entry minute stays unresolved when price evidence times out. A late confirmed handoff cannot reopen an expired entry. The runtime applies a two-hour grace for an expected funding settlement.

## Independent receipt handoff

The existing Phase 13/14 futures receipts are coverage-only under the later shared-source policy. Phase 17's grant excludes prospective evaluation and Phase 15 controls its own Spot-minute receipts. Those stores, the holdout, and old development/warmup archives are outside this source policy. **The workstation has no approved independent EMA source producer today.** The watchdog polls the handoff and will advance readiness when qualifying receipts arrive. Existing market collectors continue in their own scopes.

A qualified producer may reuse the existing KuCoin public request and response formats. Authorize a separate book-bound source only when its permission and provenance have been verified. The grant JSON uses these keys.

| Key | Value |
| --- | --- |
| `book_id`, `market`, `instrument` | Values from the exact paper registration |
| `scope`, `venue` | `EMA_PAPER_PROSPECTIVE_ONLY`, `KuCoin` |
| `operator`, `producer`, `rights_reference` | Verified identity and source-use reference |
| `independent_forward_receipts`, `protected_evidence_excluded` | `true`, as separately verified by the operator |

Ingest a receipt JSON containing the returned `source_id`, `producer`, exact existing Phase 9 public `request` spec, `http_status: 200`, original UTF-8 `raw_response`, its `raw_sha256`, and timezone-aware provider `received_at`. Ingestion records its own actual handoff clock. It verifies route, venue instrument, checksum, time bounds, and first observations. Corrections are retained separately and can block an affected minute. Use the first receipts from after the grant. Day and Swing can share one independent receipt stream when both book grants name the same producer and permission. No HTTP client or additional scheduled collector is started by the handoff. [Existing public request definitions](lab/phase9/public.py) · [Handoff validator](lab/ema_paper/source.py)

```powershell
.\ema-paper.ps1 list
.\ema-paper.ps1 status --book day
.\ema-paper.ps1 status --book swing
.\ema-paper.ps1 status --book position
.\ema-paper.ps1 events --book day --limit 25
.\ema-paper.ps1 compare
```

When the independent source grant and producer are ready:

```powershell
.\ema-paper.ps1 authorize-source --book day --descriptor C:\intake\ema-day-source.json
.\ema-paper.ps1 ingest --book day --file C:\intake\public-receipt.json
```

`register --operator "Name"` is idempotent and is needed only to initialize these three registrations. `run-once` performs one bounded operator evaluation with the same lock as the managed cycle. It does not deliver Telegram messages directly.

## Watchdog, notifications, and dashboard

The existing **ASEKE BTC Lab - Health Watchdog** task loads the EMA cycle in its three-minute scheduled check. It evaluates the three books serially and records `ema_paper` and `ema_paper_notifications` in `runs/health/last.json`. The task definition, the running collectors, and the original account ledger stay independent. The worker uses one compute thread, a 45-second EMA cycle allowance, 60 execution minutes per book per check, a 640 MiB process RSS gate, at least 1,250 MiB available host RAM and 2 GiB free disk. The new paper ledger cap is 128 MiB, the independent receipt store cap is 512 MiB, and each raw receipt is limited to 2 MiB.

For the Kraken-reference study, the existing durable Telegram outbox sends allowlisted private summaries of newly committed ideas, modeled fills, expiries or invalidations, exits and debounced operational events. The private destination is verified at delivery. Paper event and ledger timestamps can appear in the summary. Individual prices, SL/TP, quantities, source receipts and raw data stay local. The separate cursors exclude events committed before enablement, and warmup sends no notifications. The delivery receipt records API acceptance. [Private summary procedure](EMA_SYNTHETIC_PAPER_GUIDE.md#inspect-the-local-deployment) · [Watchdog and Telegram semantics](HEALTH_MONITORING_RUNBOOK.md)

Open **Workspace → Prospective paper** for all seven accounts, metrics, evidence gaps, chart clocks, and delivery history. Comparisons use the overlap of registered eligibility windows when it exists. The Day comparison also distinguishes EMA entries inside and outside the breakout session. Small samples remain descriptive. These books supply no training labels.

## Adding another family

Register and test the exact strategy version in Strategy Lab. Supply a separate paper registration with a reviewed source permission, signal-to-entry clock, frozen bracket or exit adapter, contract/risk assumptions, and horizon. Reuse the book, evidence and event interfaces, adding a profile route and comparison benchmark under an explicit new authorization. Check for interactions with the existing sources and alert categories before scheduling. The current registration command deliberately creates only the three authorized EMA books. [Strategy Lab runbook](STRATEGY_LAB_RUNBOOK.md)
