# Protection incident lifecycle and prospective recovery v1

The implementation uses `PROTECTION_INCIDENT_POLICY_V1` for generation `caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9`. The framework is installed in source and requires explicit policy activation in the current paper journal. Implementation and a read-only report do not activate it.

The existing EMA Day position `95d7d033bdfbc27d29c4347d0949d24acb5fa5f5ea3f90f00cb69a245e365c3e` retains its protection uncertainty. Applying this framework to that position requires separate approval. The policy activation command requires all seven accounts flat and a managed runner that has adopted the reviewed implementation.

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

Run from the active repository.

```powershell
Set-Location 'C:/Users/16477/Projects/aseke-btc-lab'
& .\.venv\Scripts\python.exe -m lab.paper_run protection report --slot ema/day
```

The report returns `generation`, `book_id`, `position_id`, `policy_active`, projected incidents and a `review_token`. It also exposes nominal cash, the historical book problem and any existing prospective-admission authorization. Legacy gaps are projected for review without changing their original events.

Obtain a new report after an applied operation or material account/incident change. Every mutation rechecks the token under the existing writer lease and one SQLite transaction. A stale token fails. The operation plus request content is bound to the request ID. Repeating an already applied identical request is idempotent. Reusing its ID with different arguments fails.

## Request and approval procedure

Every operation uses a JSON request file with these common fields.

```json
{
  "generation": "caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9",
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

These application examples describe the supported procedure. They do not authorize activation, adjudication or resume for the current open EMA Day position.

### Activate

An activation request needs only the common fields. Activation requires all seven books flat, no pending ideas, finite reconciled nominal accounting, and a fresh managed-runner heartbeat whose build matches the reviewed source. Runtime incident recording remains disabled until the activation event is committed.

Adopt the code using the existing reviewed managed-handoff procedure. A handoff while a position is open needs an explicitly reviewed protection-preserving procedure. A source edit does not itself authorize a restart.

### Adjudicate

Add `incident_id` from the report and a nested `evidence` manifest to the common request. Each incident requires its own disposition. `review_interval_end` is optional. If supplied, it must cover the journal-derived recovery bound. It is a conservative endpoint for evidence review and does not claim an exact first recovery timestamp.

For legacy incidents, the command derives a recovery upper bound from accepted protection evidence, repeated rejection transitions, or a committed exit reference. An unknown upper bound permits only an explicit insufficiency disposition. A caller cannot shorten the interval to avoid missing or adverse evidence.

A no-crossing request can use the preserved native chart audit.

```json
{
  "generation": "caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9",
  "slot": "ema/day",
  "review_token": "COPY_FROM_THE_LATEST_REPORT",
  "request_id": "UNIQUE_NO_CROSSING_REVIEW_ID",
  "reason": "Review same-market complete candle bounds while preserving unknown trade-stream coverage",
  "incident_id": "COPY_THE_SELECTED_INCIDENT_ID",
  "evidence": {
    "outcome": "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED",
    "path": "C:/Users/16477/AppData/Local/ASEKE-BTC-Lab/protection-audit/ema-day-chart-audit-20261117T172713840614Z.json",
    "sha256": "84ad5610a2c6733e7720c42b8e79bd5aefe62d28d2df1fa45c23e6b59663af6f",
    "provenance": {
      "origin": "PRESERVED_NATIVE_KRAKEN_OHLC_AUDIT",
      "operator": "IDENTITY_OF_THE_EVIDENCE_REVIEWER",
      "acquired_at": "2026-11-17T17:27:13.840614+00:00",
      "source": "KRAKEN_BTC_USD_LAST_TRADE",
      "symbol": "BTC/USD",
      "trigger_basis": "KRAKEN_BTC_USD_LAST_TRADE",
      "stream_id": "b55f401582a344422ee7295df4b1a31d737094dd62a507a280e65b27e5faf5b7"
    }
  }
}
```

The validator checks the artifact SHA256 and every referenced raw payload, complete minute coverage, same market and authorized source stream, exact identities, receipt ordering and the approved read-only source SQLite store. Candle values must reconcile to unique final native observations. It compares the frozen stop, target, liquidation model, time deadline and tier against the reviewed interval. A touched threshold supports possible exit impact. A missing candle, funding/stop change effective before the end of the reviewed interval, unsupported instrument or damaged provenance fails the static no-crossing claim. Financing that occurs only after the interval does not invalidate its earlier static model. An explicit insufficiency disposition remains available without asserting a favorable static model.

An explicit insufficiency request has the same common fields and manifest structure.

```json
{
  "generation": "caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9",
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
      "stream_id": "b55f401582a344422ee7295df4b1a31d737094dd62a507a280e65b27e5faf5b7"
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

## Current historical incident

The two originally audited November 17 stream discontinuities were recorded at 16:23:58.045 UTC and 17:02:10.432 UTC. The preserved 85-minute audit supports a limited no-crossing review through 17:25 UTC. The live predecessor runner recorded an additional discontinuity at 17:51:41.337 UTC during implementation, then resumed accepted observations. That incident lies outside the preserved audit and requires its own disposition. Missing first-recovery observations remain unknown. The current position's `unresolved_protection` flag is preserved.

The framework, operator requests and reporting must be reviewed before activation against production. Testing occurs in isolated databases. Application commands take the existing writer lease and a single transaction, repeat exposure and evidence checks, and bind idempotency to both the operation and request. A crash rolls back the incomplete journal/state transition.

[Incident projection](C:/Users/16477/Projects/aseke-btc-lab/lab/paper_run/protection_incidents.py) · [Operator commands](C:/Users/16477/Projects/aseke-btc-lab/lab/paper_run/protection_commands.py) · [Evidence validation](C:/Users/16477/Projects/aseke-btc-lab/lab/paper_run/protection_evidence.py) · [Research classification](C:/Users/16477/Projects/aseke-btc-lab/lab/paper_run/protection_reporting.py)
