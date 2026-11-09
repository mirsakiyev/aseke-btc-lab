# Data dictionary

Market, execution and decision times are UTC in storage. Toronto time is a dashboard display conversion. Half-open intervals include the opening timestamp and exclude the closing boundary.

## Market snapshots

| Field | Meaning |
| --- | --- |
| `open_at`, `close_at` | Native candle interval in UTC |
| `open`, `high`, `low`, `close` | Observed USD per BTC |
| `volume` | BTC traded during the candle |
| `resolution_seconds` | Native 300 or 3,600-second interval |
| `source_id` | Venue, market type and instrument |
| `raw_sha256` | Exact raw response body containing the normalized candle |
| `ingested_at` | UTC time the lab received the response |

Request checkpoints store bounds, requested resolution, URL, raw digest, fragment location, accepted row count, and duplicate count. Snapshots are identified by file digest and checkpoint-set digest. The coverage report includes earliest/latest observed data, internal gaps, uncovered edges, and source reconciliation.

## Research labels and features

| Field | Meaning |
| --- | --- |
| `signal_at` | Decision close at which the baseline setup was detected |
| `available_at` | Decision close plus processing allowance |
| `entry_at`, `resolved_at`, `deadline` | Modeled entry open, observed outcome resolution, and maximum allowed exit time |
| `entry_raw`, `exit_raw` | Candle prices before adverse execution adjustments |
| `status` | Exit rule or reason the setup could not resolve |
| `target` | Unit-size net outcome divided by adverse entry notional. Null when unresolved or ineligible |
| `predicted_net_return` | Output of the saved reference Ridge model on a complete feature row |
| `momentum`, `atr_fraction` | Trailing close return and ATR divided by close |
| `breakout_distance`, `relative_volume` | Distance from prior high and relative volume, both fractional |
| `context_<frame>_spread` | Completed context comparison divided by reference minus one |
| `session_remaining_hours` | UTC hours remaining at the Day decision close |

The label file includes setups the simulated account could not take while holding BTC. Its target is a regression outcome. Trade counts and realized account P&L come from the journal.

## Executed research ledger

| Field | Meaning |
| --- | --- |
| `profile`, `profile_version`, `profile_hash` | Trade’s originating strategy profile and effective configuration |
| `strategy_version`, `model_version` | Originating strategy and candidate artifact. Baseline model is null |
| `stop`, `exit_range`, `deadline`, `max_minutes`, `session_flat_utc` | Frozen protective, signal, and elapsed/session exit policy |
| `entry_at`, `exit_at`, `exit_bar_open` | Entry, known exit or conservative intrabar resolution, and the underlying exit candle |
| `units_btc`, `entry_price`, `exit_price` | Fixed fractional size and adverse executable price estimates |
| `entry_notional`, `exit_notional`, `fees_usd`, `net_usd` | Filled notionals, paid fees, and realized net USD outcome |
| `planned_risk_usd`, `risk_budget_usd` | Decision-close loss estimate and budget from pre-submission equity |
| `fill_to_stop_risk_usd`, `fill_risk_overrun_usd`, `realized_loss_overrun_usd` | Stop risk at actual entry and amounts beyond the risk budget |
| `allocation_overrun_usd` | Actual entry notional in excess of the planned exposure cap |
| `reason`, `deadline_status`, `holding_minutes` | Exit or unresolved reason, deadline result and elapsed time through resolution or the last observable mark |

Equity files hold candle-close cash, BTC, USD equity, BTC weight, drawdown and the aligned benchmark marks. Monthly CSV values are fractional returns from the previous available month-end mark or starting equity. A final partial month uses the last observable mark. A gap with an open position ends the result and leaves the last marked unrealized value in the manifest.

## Registry and reports

Model records bind market identity, effective profile hash, feature/target/strategy versions, training cutoff, fitted-sample hash, execution hash, code and dependency versions, source digest, artifact digest, and the report ID. The run manifest captures split counts and overlap, chronological prediction error, reference and alternate scenario results, output hashes, effective dates, and explicit holdout and paper approval flags.
