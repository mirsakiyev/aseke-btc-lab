# Paper strategy capability and membership contract v1

The common operational framework applies to registered books. A strategy name,
family, profile label or source-witness anchor does not grant a capability.
The original sealed seven-book registration and historical migration checks
remain unchanged. Future membership is a separate, explicitly approved,
append-only `PAPER_MEMBERSHIP_AMENDMENT_V1` journal projection.

This is paper-only, `MANUAL_OUTSIDE_LAB`, with approval status `No validated
model`. Capability metadata does not authorize exchange orders, new endpoints,
new subscriptions, historical-price fills or automatic incident approval.

## Supported contract

`lab.paper_run.capabilities.contract(lane, generation=..., slot=...)` resolves
`PAPER_STRATEGY_CAPABILITIES_V1`. Current sealed EMA and Breakout lanes inherit
compatible declarations. A new strategy must explicitly declare its supported
adapters in `lane.capabilities`.

The declaration contains generation, slot, book and strategy/version identity;
actual reference venue, instrument and product; signal and protection data;
entry, exit and optional liquidation trigger bases; full transport/subscription
and source version; cadence and continuity semantics; the two-second freshness,
250 ms clock and 100 ms monotonic requirements; exit policy and target
optionality; funding/margin requirements; and evidence, recovery,
reconciliation and notification adapters.

| Existing book | Signal / entry adapter | Actual reference | Modeled product | Funding | Exit capability |
|---|---|---|---|---|---|
| breakout/day/fixed | breakout_v1 / breakout_reference_v1 | KuCoin XBTUSDTM mark snapshot | Linear perpetual | KuCoin settled rate | Fixed stop and target |
| breakout/swing/fixed | breakout_v1 / breakout_reference_v1 | KuCoin XBTUSDTM mark snapshot | Linear perpetual | KuCoin settled rate | Fixed stop and target |
| breakout/swing/trail | breakout_v1 / breakout_reference_v1 | KuCoin XBTUSDTM mark snapshot | Linear perpetual | KuCoin settled rate | ATR trail, target may be absent |
| breakout/position/fixed | breakout_v1 / breakout_reference_v1 | KuCoin BTC-USDT ticker last reference | Cash Spot | None | Fixed stop and target |
| ema/day | ema_chart_v1 / ema_reference_v1 | Kraken BTC/USD last-trade stream | Synthetic leveraged reference paper | Synthetic debit | Signal ATR bracket |
| ema/swing | ema_chart_v1 / ema_reference_v1 | Kraken BTC/USD last-trade stream | Synthetic leveraged reference paper | Synthetic debit | Signal ATR bracket |
| ema/position | ema_chart_v1 / ema_reference_v1 | Kraken BTC/USD last-trade stream | Cash Spot | None | Signal ATR bracket |

Cash Spot requires long-only admission and does not acquire futures funding or
liquidation requirements. Target absence is a declared trailing-exit capability,
not a missing target to invent. Unsupported adapters, instruments, transports,
execution models or inconsistent declarations fail registration with a stable
specific reason code.

`source_scope_id(lane)` hashes the complete actual-reference identity. Kraken's
actual source product is Spot even when a strategy models synthetic leverage.
EMA Day and EMA Position therefore share source health but retain different
execution capabilities. A WebSocket subscription epoch is incident evidence,
not a new source identity.

KuCoin HTTP references preserve the existing minimum 15-second request spacing
and 45-second protection-gap model. They are sampled points, not a continuous
trade record. Kraken WebSocket trade evidence has its own event-stream and
heartbeat semantics. Neither source grants complete historical coverage merely
because current observations are healthy.

## Adding a supported future strategy

1. Implement or select a reviewed signal/entry adapter. A strategy with a new
   family can explicitly select an existing supported adapter without adding
   its name to monitoring, protection, incident or recovery code.
2. Prepare the full future lane, including a unique book ID, strategy/version,
   frozen study windows and risk settings, source qualification and reference
   model. Existing books must never be renamed or replaced.
3. Build its declaration with `capabilities.declare(...)`, supplying the exact
   generation, slot and approved adapter IDs. A new reference scope requires a
   separate source-permission review; existing source admission is not permission
   to call another route or open another subscription.
4. Prepare a request containing exactly `generation`, `slot`, `lane`,
   `request_id` and `reason`. Keep the reviewed request bytes for idempotent retry.
5. Preview and, only after explicit authorization, apply the supported command:

```powershell
.\.venv\Scripts\python.exe -m lab.paper_run.membership status
.\.venv\Scripts\python.exe -m lab.paper_run.membership register --request .\future-member.json
.\.venv\Scripts\python.exe -m lab.paper_run.membership register --request .\future-member.json --apply --approve
```

Registration uses the existing writer lease and one transaction. The amendment,
one explicitly approved opening endowment and initial book state commit together.
Repeated identical requests do not create another account or endowment. A changed
request cannot reuse the old request ID. The original registration remains the
authority for its seven books; amendments only add approved members. Operational
account checks compare actual IDs against registered expected IDs, rather than
assuming a fixed account count.

The new member immediately inherits a committed outage restriction for its
source scope. A flat book does not receive a fabricated historical exposure
incident. Membership remains visible if an admitted adapter later becomes
unsupported, so status and health can identify the affected book without
suppressing unrelated books. Registry hash/identity corruption remains a
separate fail-closed integrity error.

No production eighth book is part of this release. The non-EMA/non-Breakout
eighth-strategy scenario exists only in isolated fixtures.

## Per-book observation delivery

`PAPER_REFERENCE_DELIVERY_V1` separates source ingestion from per-book work.
An ingestion watermark never proves that every dependent account processed a
point. Reserved work retains its source scope, original position or pending
idea identity, exact observation and limited frozen exposure model.

The queue is bounded to 16 pending points per book/source. Financial application
and its delivery acknowledgment use the same existing writer transaction. A
failed transaction leaves the reserved point pending after restart; another book
can progress independently. An older stored point must pass the unchanged live
freshness checks before any economic transition. Late catch-up evidence cannot
be used to backdate a paper entry or exit.

Overflow and expired work retain structured, conservative loss ranges. An open
exposure's loss cannot be cleared until a matching durable protection cause
contains that exact loss identity. Review does not erase uncertainty. Flat or
pending-only consumers do not acquire fictitious protection incidents. Latest
checkpoints are compact metadata, not an unbounded per-tick market archive.

The managed process's separate bounded retry buffer also has an atomic
`consumer-losses.json` spool. `PAPER_CONSUMER_LOSS_SPOOL_V1` markers identify a
local consumer-capacity loss, explicitly not an exchange disconnect. They retain
actual first/last event, exchange and local receipt bounds, a conservative count
lower bound, a stable episode correlation and a content-addressed revision.
Repeated loss recording is idempotent; a late acknowledgment of an older revision
cannot erase a later coalesced range. A marker is cleared only after the actual
immutable transport-witness journal proves that exact generation/scope/revision
committed. No market price or raw payload is kept in this metadata spool.

The spool is bounded to eight source scopes and 64 KiB. Missing state can start a
new empty spool; malformed, NUL-filled, checksum-invalid or wrong-generation
existing state fails closed with the original path, size and SHA256. Preserve
that file before an explicitly reviewed repair. Do not replace damaged evidence
with a default or fabricate a successful checkpoint.

## Recovery and analytical state

Current source health, exposure, current protection, nominal reconciliation,
historical coverage, incident dispositions, research validity and prospective
entry availability are separate fields. An occupied account is not available
for overlapping entry. A source recovery does not settle historical evidence.
An approved future resume does not validate provisional or capital-dependent
performance.

Operator recovery remains preview-first and per book. Evidence acquisition,
assessment, disposition and permission are distinct durable stages. KuCoin can
receive an honest source-correct insufficiency disposition without presenting
Kraken candles as mark evidence. Favorable findings require the matching approved
adapter and effective stop/funding intervals, including actual `TRAIL_UPDATED`
events. Missing history remains explicitly unknown.

The automatic-resume policy is staged for review and remains disabled by default.
Unresolved impact, unsupported capabilities, unreconciled finances, active
protection loss, intentional pauses and independent risk restrictions retain
their review paths. An old permit cannot authorize a later incident.

## Deployment and interruption

Health-only notifications can be commissioned independently through the existing
health outbox and lock. Financial-runtime changes require the separately
reviewed managed handoff. Active positions remain under their current owner;
they must not be force-closed to create a deployment window. Preserve generation,
book IDs, journal prefixes, legitimate new marks/funding/trailing/exits and all
uncertainty exclusions. Never restore a stale database over later economics.

The ongoing flat-handoff guard reads the sealed registration and retirement
from its explicitly supplied ledger root, adds immutable registered membership,
and compares exact expected book identities to actual ledger/state identities.
An explicitly registered flat eighth member is supported. An unregistered extra
account or an equal-count set containing the wrong ID is refused. Process
ownership, ancestry, executable, command line, creation times and writer-lease
checks are retained. The original seven-book initialization and historical
migration assertions are not removed.

See the [PAPER operating runbook](PAPER_OPERATIONS_RUNBOOK.md), [protection incident policy](PROTECTION_INCIDENT_POLICY.md)
and [health runbook](HEALTH_MONITORING_RUNBOOK.md) for current inspection,
evidence review, delivery and generation controls.
