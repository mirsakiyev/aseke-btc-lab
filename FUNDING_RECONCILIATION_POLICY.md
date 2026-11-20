# Funding settlement and reconciliation V1

`FUNDING_SETTLEMENT_RECONCILIATION_V1` governs funding for the active PAPER generation. Each decision uses the registered model, original due schedule and evidence from the active account's journal. Generation retirement preserves any outstanding obligations in the retired record.

## Independent states

Funding does not replace the exposure or accepted-price protection state. An open position remains entry-occupied. A normally pending rate is `FUNDING_SETTLEMENT_PENDING`, with `financial_complete=false` and `actionable=false`; this does not manufacture a protection gap. A missed forward window or demonstrably missing/inaccessible rate is `FUNDING_EVIDENCE_MISSING`. Inconsistent schedules or native revisions are `FUNDING_CONFLICT`. A ledger/projection mismatch is `FINANCIAL_RECONCILIATION_BLOCKED`. A financially settled outcome may remain research-provisional.

Every obligation binds generation, book, position, nominal settlement and full source scope. Its projection exposes the exact due time, original 45-second valuation deadline, rate/valuation evidence states, first eligible existing collection, source query evidence, applied receipt, required action and whether it is actionable. A schedule fault has its own stable schedule-scope ID; it does not create a union of speculative settlements.

## Authorized evidence and bounded assessment

The original futures model remains `KUCOIN_SETTLED_RATE_FORWARD_MARK_VALUATION`: KuCoin XBTUSDTM native settled funding history and an actually observed fresh KuCoin futures mark snapshot. The source reader checks admitted original and correction records, the frozen acquisition identity and request bounds, native response hashes, actual settlement timestamp, symbol, receipt time and correction consistency. All admitted revisions must agree; newest-wins is forbidden. A zero rate requires an observed zero. Public settled-history publication is not a guarantee that a source can never correct it.

Existing collection remains hourly with an exclusive upper bound. A request ending exactly at 00:00 excludes the 00:00 settlement. The first ordinary covering collection is the subsequent hourly boundary plus the frozen collection grace. The separately sealed publication grace and actual covering query evidence distinguish normal pending, confirmed missing, failed collection and unconfirmed finality. This release performs no additional HTTP requests or subscriptions.

The lifecycle bounds one exposure assessment to 512 obligations, checking capacity before constructing the grid. Source reads use batches no larger than the source adapter's 64-slot limit and preserve its revision, query and raw-byte bounds. Exceeding a bound requires scoped review; it never supplies a default rate or drops an obligation. Read-only assessment creates no tables or financial records.

Frozen and current dated schedules must have the same interval, phase and due grid across the actual exposure. A rolled next-settlement anchor on the same grid is compatible. A changed interval or phase requires review of effective regime evidence. The lifecycle neither unions incompatible schedules nor silently deletes the original obligations. A source actual timestamp outside the position's actual exposure is explicitly excluded with its evidence; no fictitious zero payment is inserted.

Current metadata must supply a valid explicitly zoned `as_of` no later than the decision. Missing, invalid or future timestamps block financial application with their exact cause while retaining the original frozen due slots in the projection. No new arbitrary metadata age limit is introduced here. A genuinely fresh mark may still be preserved for a frozen due slot during that metadata-only fault; preserving source evidence does not authorize payment, and prevents losing a valid forward observation merely because contract evidence needs repair.

## Forward valuation and honest recovery

A forward valuation must have source time in `[settlement, settlement + 45 seconds]`. It must preserve its original local receipt time, pass the unchanged two-second age/latency checks and match the admitted source, instrument and transport. New captures retain the complete accepted observation, original clocks, raw-response hash and capture time. Legacy position references remain labelled as legacy projections and disclose their missing provenance; no raw response is claimed to have been retained when only a recorded hash exists.

After the window elapses, a current mark cannot repair it. The reader may expose an existing accepted observation from the authoritative observation table as a review candidate. Displaying that candidate does not restore the reference or apply money. The separate preview-first operator procedure may restore a missing mutable reference only from exact preserved evidence, binding its observation identity/body hash, generation, book, position, obligation and reviewed state. That later operation is labelled reconciliation of preserved accepted forward evidence. It is never represented as the original capture, a fresh entry reference or new historical market data.

A native historical mark candle is a different valuation model and is not admitted as a fallback by this release. If no valid preserved forward snapshot exists, the current model remains explicitly unresolved. Any alternative historical model requires separate versioned review and implementation, with honest research exclusions. Missing prices, rates, source finality or cost values are never invented.

## Writer ownership, application and corrections

`settle` operates only within the authoritative caller's existing writer transaction and never commits, starts another collector, resets an account, forces an exit or clears a protection flag. Settlement checks immutable receipts before applying funding. A lost mutable `settled_funding` list can be reconstructed from genuine receipts without charging again. Missing or inconsistent account/position funding projections block application. Each funding event retains rate source/store/revision/hash, actual settlement and rate receipt time, full valuation provenance, original schedule evidence, application time and obligation ID.

A later rate correction never rewrites an applied `FUNDING` event or silently changes its amount. Conflicts and applied changes require explicit financial review. This initial version has no approved additive economic-delta mechanism for an already applied settlement or closed outcome. Keep the restriction and original evidence until a separately reviewed additive accounting/reporting procedure exists. Ordinary technical rearm or prospective incident resume does not authorize such a change.

Late rate application affects collateral and liquidation calculations only when the financial event is actually applied. The projection labels the interval from effective settlement to application as not retrospectively verified; no historical liquidation or exit is invented. Applications after the original 45-second forward window, and approved preserved-reference reconstruction, set `funding_provisional`. This is separate from `unresolved_protection`. Genuine exits can be observed while funding is unresolved; final `CLOSED` accounting waits for adequate cost evidence and retains the financial research exclusion and dependent capital ancestry.

## Durable transitions and operator review

`FUNDING_STATUS_CHANGED` records meaningful obligation state/evidence revisions, prior state, exact cause, financial completeness and source scope. Unchanged scans and restarts do not duplicate transitions; a real recurrence gets a new sequence. Its journal is append-only. Existing operational notification routing consumes these facts; normal pending is not falsely announced as a completed financial recovery. An open account remains entry-occupied after funding resolves.

Run the read-only inspection from the adopted runtime for the exact affected slot. It returns obligation states, the review token and any preserved forward evidence candidate.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run.funding_commands inspect --slot breakout/swing/fixed
& .\.venv\Scripts\python.exe -m lab.paper_run.funding_commands restore-forward-valuation --request $request
```

A request contains `protocol` (`PRESERVED_FORWARD_FUNDING_RECONCILIATION_V1`), `generation`, `slot`, `book_id`, `position_id`, `settlement_at`, `observation_event_id`, `observation_sha256`, `observation_body_sha256`, `review_token`, `request_id` and `reason`. Copy the current identities and both observation hashes from the inspected obligation. Add `--apply --approve` only for the exact reviewed request. Application rechecks the active generation, adopted writer, clock, financial token and original evidence under the existing writer lease. Retry the identical request after an interruption and verify its receipt before preparing another.

The existing private health outbox consumes versioned funding state changes. A normally pending acquisition remains distinct from an actionable incident. A completed settlement on an occupied position clears only the applicable funding restriction. See the [PAPER operating runbook](PAPER_OPERATIONS_RUNBOOK.md) for the current startup and reset procedure.
