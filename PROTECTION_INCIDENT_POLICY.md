# Protection incident lifecycle and prospective recovery v1

The implementation uses `PROTECTION_INCIDENT_POLICY_V1`. Activation requires a matching adopted build and an authorization event in the current journal. Preserve prior uncertainty and review each incident using its original evidence.

## Lifecycle and evidence

The immutable paper event journal stores the lifecycle. SQLite triggers reject updates and deletes of the new incident, transport, activation and resume events. The projection is rebuilt from events and survives process restarts.

| Event | Effect |
|---|---|
| `PROTECTION_POLICY_ACTIVATED` | Records the reviewed policy, generation, managed build and operator authorization |
| `PROTECTION_TRANSPORT_WITNESS` | Preserves bounded transport evidence after the consumer commits it |
| `PROTECTION_INCIDENT_OPENED` | Opens a structured incident for a position |
| `PROTECTION_INCIDENT_EVIDENCE_ADDED` | Adds another cause or witness to the existing active incident |
| `PROTECTION_INCIDENT_RECOVERED` | Records the first accepted recovery reference and ends the current feed outage |
| `PROTECTION_INCIDENT_IMPORTED` | Preserves a legacy gap with explicit unknown historical fields |
| `PROTECTION_INCIDENT_ADJUDICATED` | Appends the operator's evidence-supported historical disposition |
| `BOOK_PROSPECTIVE_RESUME` | Authorizes later prospective publication and records capital dependency |

The missing-reference path and transport-discontinuity path use the same incident recorder when the policy is active. A repeated witness or continuing missing-reference condition updates the existing incident. A valid price is required to record recovery. Subscription acknowledgement by itself does not establish price recovery. Recovered prices must follow the transport fence and, after a disconnect, belong to a later subscription epoch. Fresh prices preserve historical uncertainty.

Each incident retains generation, book and position IDs, policy version, a frozen position model, the last accepted reference, detection time, source and transport evidence, epoch, queue counters, interval boundaries and their precision, first accepted recovery reference, and historical coverage and exit-impact classifications. Transport witnesses retain sanitized exception details, available sent and received close codes and reasons, monotonic timing, anchors and the raw hash of the first transport-valid recovery point. Witness overflow is explicit. The consumer acknowledges witness IDs only after durable journal persistence.

The current feed-health projection is distinct from historical coverage. A successful recovery changes `current_feed_health` from `UNAVAILABLE` to `RECOVERED`. It leaves `historical_coverage=UNRESOLVED` until supported evidence is reviewed. Legacy import retains `UNKNOWN_HISTORICAL` feed health and unknown first-recovery fields.

The existing 45-second missing-reference threshold remains in force. A transport interruption can open an incident before that threshold. Its existence records uncertainty and does not assert that a stop or target crossed. An empty consumer queue can produce `MISSING_REFERENCE` without establishing a loss of market coverage.

## Read-only review

Run from the verified, adopted runtime root.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run protection report --slot ema/day
```

The report returns `generation`, `book_id`, `position_id`, `policy_active`, projected incidents and a `review_token`. It also exposes nominal cash, the historical book problem and any existing prospective-admission authorization. Legacy gaps are projected for review without changing their original events.

Obtain a new report after an applied operation or material account/incident change. Every mutation rechecks the token under the existing writer lease and one SQLite transaction. A stale token fails. The operation plus request content is bound to the request ID. Repeating an already applied identical request is idempotent. Reusing its ID with different arguments fails.

## Request and approval procedure

Every operation uses a JSON request file with these common fields.

```json
{
  "generation": "COPY_CURRENT_GENERATION",
  "slot": "ema/day",
  "review_token": "COPY_FROM_THE_LATEST_REPORT",
  "request_id": "UNIQUE_OPERATOR_REQUEST_ID",
  "reason": "The specific reviewed reason for this operation"
}
```

The executing Windows user is recorded as the authorizing operator. The evidence provenance field identifies the acquisition/review source and is retained separately.

Commands default to preview.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run protection activate --request .\activate-request.json
& .\.venv\Scripts\python.exe -m lab.paper_run protection adjudicate --request .\adjudicate-request.json
& .\.venv\Scripts\python.exe -m lab.paper_run protection resume --request .\resume-request.json
```

A preview can reject unmet safety prerequisites. State changes require both `--apply` and `--approve` after the concrete request has been reviewed and explicitly authorized.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run protection activate --request .\activate-request.json --apply --approve
& .\.venv\Scripts\python.exe -m lab.paper_run protection adjudicate --request .\adjudicate-request.json --apply --approve
& .\.venv\Scripts\python.exe -m lab.paper_run protection resume --request .\resume-request.json --apply --approve
```

Application requires the exact reviewed operation and scope.

### Activate

An activation request needs only the common fields. Activation requires all seven books flat, no pending ideas, finite reconciled nominal accounting, and a fresh managed-runner heartbeat whose build matches the reviewed source. Runtime incident recording remains disabled until the activation event is committed.

Adopt the code using the existing reviewed managed-handoff procedure. A handoff while a position is open needs an explicitly reviewed protection-preserving procedure. A source edit does not itself authorize a restart.

### Adjudicate

Add `incident_id` from the report and a nested `evidence` manifest to the common request. Each incident requires its own disposition. `review_interval_end` is optional. If supplied, it must cover the journal-derived recovery bound. It is a conservative endpoint for evidence review and does not claim an exact first recovery timestamp.

For legacy incidents, the command derives a recovery upper bound from accepted protection evidence, repeated rejection transitions, or a committed exit reference. An unknown upper bound permits only an explicit insufficiency disposition. A caller cannot shorten the interval to avoid missing or adverse evidence.

A no-crossing request can use the preserved native chart audit.

```json
{
  "generation": "COPY_CURRENT_GENERATION",
  "slot": "ema/day",
  "review_token": "COPY_FROM_THE_LATEST_REPORT",
  "request_id": "UNIQUE_NO_CROSSING_REVIEW_ID",
  "reason": "Review same-market complete candle bounds while preserving unknown trade-stream coverage",
  "incident_id": "COPY_THE_SELECTED_INCIDENT_ID",
  "evidence": {
    "outcome": "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED",
    "path": "ABSOLUTE_PATH_TO_REVIEWED_NATIVE_CHART_AUDIT",
    "sha256": "SHA256_OF_EXACT_CHART_AUDIT_BYTES",
    "provenance": {
      "origin": "PRESERVED_NATIVE_KRAKEN_OHLC_AUDIT",
      "operator": "IDENTITY_OF_THE_EVIDENCE_REVIEWER",
      "acquired_at": "EXPLICIT_OFFSET_TIMESTAMP",
      "source": "KRAKEN_BTC_USD_LAST_TRADE",
      "symbol": "BTC/USD",
      "trigger_basis": "KRAKEN_BTC_USD_LAST_TRADE",
      "stream_id": "COPY_APPROVED_STREAM_ID"
    }
  }
}
```

The validator checks the artifact SHA256 and every referenced raw payload, complete minute coverage, same market and authorized source stream, exact identities, receipt ordering and the approved read-only source SQLite store. Candle values must reconcile to unique final native observations. It compares the frozen stop, target, liquidation model, time deadline and tier against the reviewed interval. A touched threshold supports possible exit impact. A missing candle, funding/stop change effective before the end of the reviewed interval, unsupported instrument or damaged provenance fails the static no-crossing claim. Financing that occurs only after the interval does not invalidate its earlier static model. An explicit insufficiency disposition remains available without asserting a favorable static model.

An explicit insufficiency request has the same common fields and manifest structure.

```json
{
  "generation": "COPY_CURRENT_GENERATION",
  "slot": "ema/day",
  "review_token": "COPY_FROM_THE_LATEST_REPORT",
  "request_id": "UNIQUE_INSUFFICIENT_EVIDENCE_REVIEW_ID",
  "reason": "Acknowledge missing historical recovery evidence without validating the result",
  "incident_id": "COPY_THE_SELECTED_INCIDENT_ID",
  "review_interval_end": "AN_EXPLICIT_OFFSET_TIMESTAMP_COVERING_THE_AVAILABLE_REVIEW_BOUND",
  "evidence": {
    "outcome": "INSUFFICIENT_EVIDENCE",
    "path": "ABSOLUTE_PATH_TO_THE_REVIEW_NOTE_JSON",
    "sha256": "SHA256_OF_THE_EXACT_NOTE_BYTES",
    "provenance": {
      "origin": "PRESERVED_INCIDENT_REVIEW",
      "operator": "IDENTITY_OF_THE_EVIDENCE_REVIEWER",
      "acquired_at": "EXPLICIT_OFFSET_TIMESTAMP",
      "source": "KRAKEN_BTC_USD_LAST_TRADE",
      "symbol": "BTC/USD",
      "trigger_basis": "KRAKEN_BTC_USD_LAST_TRADE",
      "stream_id": "COPY_APPROVED_STREAM_ID"
    }
  }
}
```

The insufficiency note is an immutable JSON artifact with the exact `generation`, `book_id`, `position_id`, a `recorded_at` timestamp no earlier than the reviewed interval end, and a reason. A schema name such as `OPERATOR_EVIDENCE_NOTE_V1` distinguishes the note from a native chart audit. Its bytes and hash become the evidence reference.

| Outcome | Implemented effect |
|---|---|
| `NO_EXIT_THRESHOLD_CROSSING_SUPPORTED` | Records supported absence of a threshold crossing, with historical trade coverage still unresolved |
| `EXIT_IMPACT_POSSIBLE` | Records a threshold or time deadline inside the reviewed interval |
| `INSUFFICIENT_EVIDENCE` | Records an explicit unresolved disposition |
| `COVERAGE_VERIFIED` | Rejected by the current candle evidence adapter |

A complete trade-coverage claim requires an additional reviewed evidence adapter and source contract. Fresh subscriptions, ordered trade IDs and native candle bounds do not supply that proof.

### Resume

A resume request needs the common fields. Resume requires a flat book, no pending idea, no permanent block, the historical `UNRESOLVED_PROTECTION_INTERVAL` problem, matching fill and close counts, a disposition for every incident, no active unavailable-protection incident, and finite reconciled nominal accounting. An unmodeled instrument tier requires a separate supported resolution. Current clock qualification, source qualification, streaming Kraken transport and a fresh valid reference are rechecked before commitment.

The command retains `book.problem` and the original provisional close. It records `BOOK_PROSPECTIVE_RESUME` and an independently verified `prospective_admission` projection. The effective boundary is the next strictly future strategy timeframe. Later decisions remain subject to their existing entry conditions and study dates. A new unresolved incident removes the current admission projection and requires another review.

The authorization includes incident and position ancestry and a research segment ID. Dependent fills and closes carry that provenance. Resetting the balance or changing the book ID is outside this procedure.

## Protection and research reporting

Fresh accepted forward observations continue protecting open positions regardless of historical uncertainty. The existing source identity, sequence, clock, two-second freshness and final commit checks remain active. A legitimate stop, target, liquidation or time exit can close an unresolved position. Cost reconciliation proceeds, and its close remains provisional.

The reporting projection exposes nominal accounting separately from validated closed outcomes. It excludes provisional closes, incident-affected outcomes, incomplete outcome accounting and results dependent on unresolved capital. Operational resume does not promote those results. Analytical promotion is not implemented in this version, including after a no-threshold-crossing disposition.

[Incident projection](lab/paper_run/protection_incidents.py) · [Operator commands](lab/paper_run/protection_commands.py) · [Evidence validation](lab/paper_run/protection_evidence.py) · [Research classification](lab/paper_run/protection_reporting.py)

## Registered capabilities and source-correct review

Ongoing recovery now uses registered membership, including explicitly admitted future books. The original sealed seven-book registration and migration checks remain unchanged. A strategy name does not select a favorable evidence adapter: the admitted capability contract identifies the actual source, instrument, product, price basis, transport/subscription and source version. `protection report --slot SLOT` exposes that contract and the known historical stream IDs required to prepare evidence.

| Actual protection reference | Supported evidence path | Important limit |
|---|---|---|
| Kraken BTC/USD last trade | Native same-market bounds, static or journal-derived effective segments; explicit insufficiency | Candles never establish complete continuous trade coverage |
| KuCoin futures mark | Source-correct explicit insufficiency and operator review | Kraken trade candles cannot establish absence of a KuCoin mark-threshold crossing |
| KuCoin Spot ticker last | Source-correct explicit insufficiency and operator review | Sampled reference points do not prove continuous coverage; Spot does not acquire a futures funding/liquidation obligation |

A favorable finding requires the matching approved adapter. An unsupported adapter, missing model or missing interval produces an actionable review requirement, not a favorable fallback. A truthful `INSUFFICIENT_EVIDENCE` note may preserve unknown original interval endpoints and missing historical model fields. Its generation, book, position, source identity, recording time, hash and limitations must still match the review context. It does not invent an original detection time or recovery observation.

Historical bare flags and provisional closes without a corresponding incident are projected as `UNATTRIBUTED_HISTORICAL_PROTECTION_FLAG`. The original detection and interval start remain null. A known terminal timestamp is labelled as a terminal receipt, not the beginning of the outage. The existing explicit import/adjudicate/resume path can preserve this uncertainty without creating an unexplained permanent dead end.

Stop-only models retain `target=null`. Cash Spot has no recomputed futures liquidation threshold. `TRAIL_UPDATED`, `STOP_UPDATED`, `TRAILING_STOP_UPDATED` and funding cash flows are recognized at their actual effective journal times. A single latest stop is never applied retrospectively. Changing terms require `ASEKE_PROTECTION_SEGMENTED_CHART_AUDIT_V1`: each supplied interval, model and effective-event list must match the journal-derived segmentation. Native minute bounds that straddle a change are conservatively checked under both applicable models. This can require an `EXIT_IMPACT_POSSIBLE` disposition even when finer timing is unavailable. Unmodeled tiers or incomplete effective changes require explicit insufficiency or a separately reviewed terms adapter.

## Per-book financial and permission checks

Prospective resume remains scoped to the selected flat book. Other registered accounts may remain exposed and protected. The selected account must have unique matching `HYPOTHETICAL_FILL`, `EXIT_OBSERVED` and `CLOSED` evidence for every terminal position, correct insert ordering, finite cash flows, matching funding journal totals and reconciled nominal aggregates. Historical provisional/research exclusions do not themselves fail nominal reconciliation.

Recovery work uses that same detailed terminal check whenever terminal or incident history exists. Every known incident position requires a matching genuine terminal `CLOSED` receipt; an empty financial journal and unchanged balance cannot establish that a previously exposed position reconciled. `WAIT_FINANCIAL_RECONCILIATION` records the exact missing or inconsistent evidence. Historical `accounting_complete=false` is not a financial input to this check, so a properly reconciled provisional result can proceed to explicit historical review without becoming validated research.

`UNMODELED_TIER` has a distinct `UNSUPPORTED_FINANCIAL_MODEL_REVIEW_REQUIRED` path once the affected book is flat. While exposed, accepted-price protection and legitimate exits remain the priority. The restriction requires a separately reviewed instrument/tier/cost model and genuine financial receipts; a fresh quote, technical rearm, favorable chart finding or ordinary incident resume cannot resolve it. This release provides no model-substitution or financial-write approval for that path. Preserve the restriction and evidence until a separate supported resolution is reviewed and implemented; never insert a fictitious cost, reset capital or classify the limitation as a favorable absence of exit impact.

Every permission is checked against its immutable authorization. A later historical protection cause, newly imported incident, changed incident disposition, unmodeled tier, historical operational fault or new provisional close revokes an old projection. An ordinary current clock/source restriction suspends actual entry through current gates; it does not erase a valid historical permission. A clean capital-dependent close also does not erase that permission. A permanent block still refuses admission.

A missing legacy `problem` projection does not establish clean history. Read-only status and admission restoration conservatively derive the historical restriction from retained incident events, unexplained provisional closes or an unresolved position flag, without altering the original journal or balances. Current operational availability, nominal finances, historical coverage and research validation remain separate fields.

## Durable bounded recovery work

`recovery_work` records the current assessment and `recovery_work_event` retains its append-only audit. Work identity binds generation, registered book, capability contract, incident IDs and the operational episode when supplied. A new incident creates a new scope and supersedes unfinished older work. Evidence acquisition, evidence assessment, disposition and permission remain distinct stages. Staging an evidence manifest or receiving fresh prices grants no financial permission.

The runner may reassess transient technical guards and request source evidence through existing authorized acquisition and resource limits. Acquisition is bounded to twenty attempts per work item, with exponential delay from fifteen seconds to five minutes. Evidence manifests and assessments are limited to thirty-two each. An exhausted item requires explicit technical review and rearm. Rearm preserves all source permissions and freshness checks and never adjudicates an incident, clears a position flag or grants a prospective resume.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run.recovery policy
& .\.venv\Scripts\python.exe -m lab.paper_run.recovery status --book $bookId
& .\.venv\Scripts\python.exe -m lab.paper_run.recovery rearm --request $rearmRequest
& .\.venv\Scripts\python.exe -m lab.paper_run.recovery rearm --request $rearmRequest --apply --approve
```

A rearm request contains exactly `generation`, `book_id`, `work_id`, `request_id` and `reason`. Its immutable request identity is idempotent; changed arguments under the same ID are refused. It operates under the existing writer lease and transaction. Read-only work status does not create missing tables.

`AUTO_RESUME_POLICY_V1_DRAFT` is a disabled review proposal in this release. No blanket automatic historical approval or policy activation occurs. Its proposed permitted outcome is source-admitted `NO_EXIT_THRESHOLD_CROSSING_SUPPORTED`, together with flat/no-pending exposure, genuine terminal reconciliation, every relevant incident disposed, qualified current source/clock/reference/runner checks, no intentional pause or independent risk restriction, and a strictly future boundary. Insufficient evidence, ambiguous exit impact, unsupported capabilities, active protection loss and unreconciled finances retain their operator-review path. Commissioning any automatic policy requires a separate explicit versioned review and implementation/activation action; this release has no automatic-policy activation command.
