# Strategy Lab data admission plan

Reviewed on 2026-11-11 Toronto time against commit `1e9fa72`. All market-data intervals below are UTC, include the start, and exclude the end. Bare dates mean 00:00 UTC. The registry had seven versions, zero admitted datasets, zero jobs, and zero proposals. **MANUAL_OUTSIDE_LAB · No validated model.**

## Admission decision

| Profile | Recommended source | Status |
| --- | --- | --- |
| Position | Gap-free late Phase 9 KuCoin BTC-USDT hourly Spot shard | Best local candidate. Date coverage, OHLCV, and hashes verified. Admission awaits permission to reuse the archive, dated instrument terms, and approval of modeled availability and costs. |
| Day and Swing | Independently permitted pre-holdout KuCoin XBTUSDTM one-minute trade and mark archive with complete funding and dated contract evidence | No qualifying local archive. Keep both profiles blocked. Proposed coverage below is a specification for an archive the operator may already have or separately authorize. |

## Local inventory

Counts refer to stored candle opens on the native grid. Gaps are missing expected opens inside the stated interval. Live stores are sampled at the explicit end below. Sources that share receipts or derive from the same market series overlap.

| Source and instrument | UTC interval and cadence | Rows and gaps | Receipt provenance and availability |
| --- | --- | --- | --- |
| Coinbase Exchange BTC-USD Spot, published hourly snapshot | 2018-01-01 → 2026-11-08 · 1h | 77,551, 41 gaps. Pre-holdout 68,796 with 36 gaps. Sealed 8,755 with 5 gaps. | Raw-response hashes and archived `ingested_at` on 2026-11-08, 20:31–20:36 UTC. Retrieval evidence only for historical availability. [Pointer](data/coinbase_exchange/spot/BTC-USD/3600/latest.json) |
| Coinbase Exchange BTC-USD Spot, published five-minute snapshot | 2023-11-08 → 2026-11-08 · 5m | 315,475, 173 gaps. Pre-holdout 210,434 with 94 gaps. Sealed 105,041 with 79 gaps. | Raw-response hashes and archived `ingested_at` on 2026-11-08, 20:34–20:43 UTC. Retrieval evidence only. [Pointer](data/coinbase_exchange/spot/BTC-USD/300/latest.json) |
| Superseded Coinbase BTC-USD Spot snapshots | 1h from 2018-01-01 to 2018-01-25 22:00. 5m from 2023-11-08 to 00:30 that day, and to 2025-11-14 02:50 in the intermediate snapshot. | 598 and 6 rows respectively with zero gaps. Intermediate 5m has 212,196 rows and 94 gaps, including 1,762 observed rows inside the seal. | Earlier publications of the backfill, with the same receipt limitations. [Backfill conventions](README.md) |
| Coinbase Exchange BTC-USD live-forward Spot | 2026-11-08 → 2026-11-11 23:00 at 1h, and to 23:25 at 5m | 95 hourly and 1,145 five-minute candles, zero gaps in both. | Native HTTP response receipts joined to the observations. Entirely after the Strategy Lab cutoff. [Observation store](data/forward/observation.sqlite3) |
| KuCoin BTC-USDT Spot, Phase 9 early import | 2018-01-01 → 2021-11-01 · 1h | 33,599, one gap at 2018-01-04 17:00. | 23 verified archived public GET pages received 2026-11-09 15:58:51–15:59:04. Historical close-time proxy only. [Import manifest](runs/phase9/imports/fdc334a5a387c7e5af5736695204c6bcc851f4fe988d36360cd3109c42a9f15c/manifest.json) |
| KuCoin BTC-USDT Spot, Phase 9 late import | 2021-11-01 → 2025-11-08 · 1h | 35,232, zero gaps. | 24 verified archived public GET pages received 2026-11-09 15:59:06–15:59:19. Historical close-time proxy only. [Import manifest](runs/phase9/imports/13ffe8ff298e4cc8b37b1c8a2720789756007010f55831cc61f8eafe24f59843/manifest.json) |
| KuCoin Spot discovery probes | 2022-06-30, 2025-11-07, 2026-11-08 · 1h, each one UTC day | 24 per day, zero gaps. First two overlap the late Phase 9 import, including the four-hour Phase 9 June 2022 probe. | Retrospective retrieval with raw receipts. Phase 17 scope is `PUBLIC_CAPABILITY_PROBE_ONLY`. [Discovery result](runs/phase17/discovery/result.json) |
| KuCoin Phase 10 decision caches | BTC-USDT 1d from 2026-05-25 to 2026-11-11. XBTUSDTM 15m from 2026-11-02 17:15 to 2026-11-11 23:15 and 4h from 2026-04-14 12:00 to 2026-11-11 20:00. | 170 Spot daily, 888 futures 15m, 1,268 futures 4h, all contiguous. Respectively 167, 507, and 1,245 opens lie inside the sealed calendar. | Actual bootstrap and later monitoring receipts. Decision/feature context at coarse cadence. [Cache directory](data/phase10/candles) |
| KuCoin XBTUSDTM, Phase 9 mark probe | 2025-01-02 00:00 → 00:15 · 1m | 15 marks, zero gaps. | Archived GET received 2026-11-09 15:56:57. `PRE_HOLDOUT_RESEARCH` probe lacks a qualified economic bundle. [Import manifest](runs/phase9/imports/43486837217a69de38e6591eab1a1e5e7f525fc26fd12d4d33dc6d2f57b62b21/manifest.json) |
| KuCoin XBTUSDTM, Phase 17 discovery | 30-minute windows starting 2024-12-31 23:30, 2025-01-02 00:00, 2025-11-07 00:00, and 2026-11-08 00:00 · 1m. Daily context 2024-05-01 → 2024-11-01. | Each pre-cutoff window has 30 trade and 30 mark, zero gaps. Post-seal probe has 28 trade and 30 mark, two trade gaps. Daily context has 184 trade candles, zero gaps. | Archived public responses from 2026-11-09 around 22:47 UTC for the minute probes. Probe-only, with no historical arrival evidence. [Discovery result](runs/phase17/discovery/result.json) |
| KuCoin XBTUSDTM, Phase 14 seed | 2026-11-01 04:00 → 2026-11-09 19:30 at 15m. 2026-04-22 → 2026-11-09 at 1d. | 830 and 201 trade candles, zero gaps. 656 and 200 opens respectively overlap the sealed calendar. | Saved raw hashes and receipt lineage under the frozen feature-initialization exception. [Seed manifest](runs/phase14/seed/862055d9f8abc1eb18628efa43270847586a40351eb1d50ba1fcc1bab1ca545f.json) |
| KuCoin XBTUSDTM, shared Phase 13/14/15 native observations | 2026-11-09 18:56 → 2026-11-11 23:15 · 1m | 3,131 trade, 3,139 mark, 3,131 pairs, eight trade gaps and zero mark gaps. Six actual settlements. Phase 14 slice from 20:00 has 3,067 trade and 3,075 mark. Phase 15 overlap from 21:45 has 2,963 trade and 2,970 mark. | Actual raw hashes and first receipts. Price receipt delays 46.6–1,242.8 seconds after each minute close in this slice. [Native store](runs/phase13/observations/9e8ac6a44015ee11bb4c2b81d614c64a17475b932777223c53f5973f33056aed/observations.sqlite3) |
| KuCoin BTC-USDT Spot, Phase 15 minutes | 2026-11-09 21:45 → 22:00 · 1m | 15 candles, zero gaps. | First raw receipt 22:01:07 UTC. Protected prospective store. [Phase 15 runbook](PHASE15_RUNBOOK.md) |
| KuCoin BTC-USDT Spot, independent Phase 17 | 2026-11-09 23:00 → 2026-11-11 23:00 · 1h | 48 candles, zero gaps. | Native receipts from 2026-11-10 00:01:01 through 2026-11-11 23:00:55. Separate native allocation outside the cutoff. [Native store](runs/phase17/spot/observations/8fe5c90e0c43348ca25261b941063c6b6fe132c90517961f92d7c71b8ad70219/observations.sqlite3) |
| Binance USD-M BTCUSDT futures, Phase 11 archive | 2020-01-01 → 2025-11-08 · 1m | 3,078,720 trade with zero gaps, 3,078,667 mark with 53 gaps, 6,414 actual settlements. | Archive receipts and checksums. Full interval failed the paired mark gate. Historical margin-tier coverage was unavailable. [Coverage record](runs/phase11/datasets/5580fe90fab410bf9ef9402d08bac444b60d61a4b4f966ea62667594fe0c1ae0/coverage.json) |
| Binance USD-M BTCUSDT futures, Phase 12 qualified view | 2020-12-18 → 2025-11-08 · 1m | 2,571,840 per trade and mark basis, zero gaps, 5,358 actual settlements. | Checksum-linked archive provenance and hypothetical replay terms. Consumed research only. Wrong venue/product and above the Strategy Lab row cap in full. [Qualified view](runs/phase12/views/293c978133ac1a53a0de4e649fab82f324cdb707c865558898ace479e7aa5460.json) · [Phase 12 runbook](PHASE12_RUNBOOK.md) |

## Inherited permissions and exclusions

- **Sealed holdout.** `[2025-11-08, 2026-11-08)` remains sealed for performance evaluation. Strategy Lab uses the earliest of 2025-11-08, the locked holdout start, and current UTC. Its data and test-window end may equal the cutoff because the end is exclusive. Coinbase BTC-USD is outside every current KuCoin admission capability even on pre-cutoff dates. [Holdout lock](data/holdout.lock.json) · [Admission checks](lab/strategy_lab/datasets.py)
- **Phase 9, 12 and 16.** The Phase 9 assets carry `PRE_HOLDOUT_RESEARCH`. The original KuCoin Spot replay and initial Phase 16 comparisons consumed these dates. Phase 16 used KuCoin Spot and Binance Day/Swing under `RESEARCH_ONLY`, with Phase 14 and 15 excluded. The stored Binance Vision terms cite CC BY-NC-SA 4.0 except where specifically designated and limit historical backtesting to personal, non-production research. Phase 12 also restricts the view to research. Reusing an independently authorized export is exploratory historical work on consumed data. Phase 16’s later native allocation has its own permissions and post-protection chronology. [Phase 9](PHASE9_KUCOIN_RUNBOOK.md) · [Stored Binance terms](runs/phase11/datasets/5580fe90fab410bf9ef9402d08bac444b60d61a4b4f966ea62667594fe0c1ae0/terms.md) · [Initial Phase 16 manifest](runs/phase16/campaigns/4f047a162aa4910f46f9eb7ffd750b51f214d4ea3a38931b5bdc5f6ae4c969cc/manifest.json) · [Phase 16](PHASE16_RUNBOOK.md)
- **Phase 14.** The amended cohort is `[2026-11-09 20:00, 2027-02-07 20:00)`. The seed’s `FEATURE_CONTEXT_ONLY` exception covers frozen initialization, including its explicitly permitted calendar overlap. It grants no fitting, labels, selection, ranking, historical replay, or P&L rights. Exclude the seed, bridge receipts, and protected cohort from this intake. [Amendment](runs/phase14/amendment/7b39be6c4968dced9e873040fb750667c20b2275bd87902c0bd90276aec95414.json) · [Phase 14](PHASE14_RUNBOOK.md)
- **Phase 15.** Publications run `[2026-11-09 21:45, 2026-12-09 21:45)` with follow-through to 2027-01-09 21:45 exclusive. Exclude the four paper books, protected observations, quote-proxy outcomes, and user execution reports. The Phase 15 challenger’s separately reviewed outcome policy grants no Strategy Lab permission. [Registration](runs/phase15/registrations/00850dad14451b34635ece9adba09b1d29263ff63cb01eeab68d5bb4e4d16ae4.json) · [Phase 15](PHASE15_RUNBOOK.md)
- **Phase 17 and 17.1.** The native registration permits scoped local public-data research and excludes prospective evaluation, live use, and redistribution. Through Phase 14 closeout it uses shared futures receipts for coverage counts only. The independent native futures boundary is 2027-02-09 00:00. Phase 17.1 allows its independent Phase 9/10 receipt context for feature initialization and excludes the Phase 14 seed/outcomes, holdout, and Phase 15 paper/user outcomes. These separate permissions leave the Strategy Lab cutoff in force. [Pilot registration](runs/phase17/pilot/registrations/ad4e33079c93458179d8e1f4ff615d1a41eb508cc629627bba1934534bc566a9.json) · [Phase 17 and 17.1](PHASE17_RUNBOOK.md)

## Position intake and first replay

Use the Phase 9 late shard’s `normalized.parquet` as the source. Verified SHA-256 is `1d5056fa992e96e17b24d9ec5c69faac3a18c29e2ff579b766ca702fb1698aa1` for the entire original file, which is 2,624,933 bytes. The proposed export trims it to **2022-07-01 → 2025-11-08**, with 29,424 verified hourly opens and zero gaps. Hash the new export independently and preserve the original hash and raw-page receipt references. Use profile `position`, venue `KuCoin`, market `SPOT`, instrument `BTC-USDT`, `native_seconds: 3600`, and `use: DEVELOPMENT_ONLY`.

| Purpose | UTC interval | Verified hourly opens |
| --- | --- | --- |
| Warmup | 2022-07-01 → 2023-01-01 | 4,416 |
| Development | 2023-01-01 → 2025-01-01 | 17,544 |
| Validation | 2025-01-01 → 2025-11-08 | 7,464 |

The warmup allows more than 21 complete weekly closes for the seeded 20-week SMA and its previous value. Recheck warmup for the imported strategy. Both test windows are consumed historical evidence. Freeze the strategy, costs, and dates before requesting the replay.

**Approval and evidence still required.** Confirm permission to reuse and retain this export for local research. The Phase 9 manifest establishes official public GET access, and the existing campaign pins `OBSERVED_CURRENT_AS_HYPOTHETICAL`. Its 2026-11-09 Spot rules snapshot, including 0.1 USDT tick and 0.00000001 BTC increment, provides no effective historical bounds. Obtain dated BTC-USDT terms covering the entire proposed export or reselect a fully evidenced interval and recheck warmup. Record the owner, source terms, exclusions, and explicit acceptance of `HISTORICAL_CLOSE_PROXY` and scenario costs. The seeded 10 bp taker fee and 5 bp adverse price proxy per side are published-reference/model assumptions. Historical or account-specific charges remain unverified. Keep real download receipts in provenance. In the replay, the proxy treats a complete candle as available at close and applies the strategy processing delay. [Phase 9 scope and replay terms](PHASE9_KUCOIN_RUNBOOK.md) · [Sandbox intake schema](STRATEGY_LAB_RUNBOOK.md)

## Futures intake specification

Require a separate profile manifest for Day and Swing. Each names KuCoin `XBTUSDTM` USDT linear perpetual futures and binds independent UTC one-minute trade OHLCV and mark OHLC files. Require a continuous paired grid, actual settled funding and dated schedule segments, dated contract multiplier, ticks, lot sizes, and fixed margin/liquidation tier terms covering the whole export. Freeze cost assumptions separately. Record receipt provenance and independently documented publication timing, or explicitly approve a close-time proxy for exploratory replay. The protected native stream is neither a qualifying source nor a substitute for historical availability.

| Economic input | Local KuCoin evidence and gap |
| --- | --- |
| Trade and mark | Only scattered paired pre-cutoff minute probes. Daily and 15-minute context cannot establish minute execution, mark stops, or liquidation. The post-cutoff shared stream contains eight missing trade minutes in the measured slice. |
| Funding | Individual pre-cutoff samples show 04/12/20 UTC on 2025-01-02 and 00/08/16 UTC on 2025-11-06 and 2025-11-07. The current metadata gives a 480-minute cycle and a 2025-06-17 08:00 effective-cycle field. These points cannot establish contiguous historical regimes or complete settlements. Missing rates cannot be filled with zero. |
| Contract and costs | The 2026-11-09 instrument and margin snapshots show a 0.001 BTC contract multiplier, 0.1 USDT price tick and first-tier 250,000 USDT notional / 0.4% maintenance. Historical effective dates and tier changes remain unverified. The seeded 6 bp taker fee and 5 bp adverse proxy per side are scenario costs. The 30 bp liquidation fee is modeled. |
| Causal availability | The probes were fetched retrospectively. The shared native stream shows real collection lag, with the earliest measured minute receipt 46.6 seconds after close, versus the seeded 30-second processing allowance. A download timestamp cannot verify original publication or a fill. |

The earlier acquisition sizing for the 2025 range called for about 4,480 price pages, 45 funding pages and 62 MiB of raw price responses. Its frozen bulk decision used zero requests. The Binance archive, disjoint KuCoin probes, and protected stores cannot fill the missing KuCoin economic evidence. [Discovery and acquisition decision](PHASE17_RUNBOOK.md#discovery) · [Current metadata](runs/phase17/metadata/a8a9ce1206c4c90c9b21a25ad4cc4e7fb40e49dbcb56d7bbffb342e491acd20b.json)

These are conditional **first-replay** targets if a qualified archive and separately approved costs become available. All times are aligned to both decision grids.

| Profile | Export and expected rows per basis | Warmup | Development | Validation |
| --- | --- | --- | --- | --- |
| Day | 2025-08-25 → 2025-11-08 · 108,000 | 2025-08-25 → 2025-09-19 | 2025-09-19 → 2025-10-14 | 2025-10-14 → 2025-11-08 |
| Swing | 2025-01-01 → 2025-11-08 · 447,840 | 2025-01-01 → 2025-07-28 | 2025-07-28 → 2025-09-15 | 2025-09-15 → 2025-11-08 |

Day has 2,400 scheduled decisions per 25-day window, below the 2,500 proposal cap. Swing has 208 calendar days before development for its 200-day SMA and prior value. The export is close to the 450,000-row cap. Verify that one dated contract and margin regime covers it. Neither validation window can meet the current bootstrap minimum of 12 trade weeks or eight trade months. Use these windows to check coverage and economics. An uncertainty-based screen would need a separately reviewed longer design.

## One-job resource estimate

Measured on the Ryzen 5 2600X host with six cores, 12 logical CPUs, 7.95 GiB RAM, about 2.29 GiB available and 139.7 GiB free disk at the sample. A read-only import of the worker modules took 3.09 seconds at 181 MiB RSS. Reading the full late Spot Parquet’s seven required columns took 0.012 seconds and brought RSS to 194 MiB. No replay was started for these measurements.

| First job | Planning estimate | Disk planning |
| --- | --- | --- |
| Position, eight replay cases | 10–90 seconds, 200–400 MiB worker RSS | Original is 2.50 MiB. Allow roughly 25–40 MiB for external and sandbox copies, database/checkpoints, and one report export. |
| Day, ten replay cases | 30–180 seconds, 250–450 MiB | Unmeasured compressed size. Allow up to 96 MiB for the two sandbox assets plus up to 22 MiB checkpoint payload and space for intake copies and export. |
| Swing, ten replay cases | Several minutes, 350–640 MiB; near-cap run could hit the RSS or time guard | Same file ceilings as Day. Decoded frames and price arrays alone are roughly 85 MiB before copies, feature state, marks, and imports. |

The foreground worker uses one compute thread and shared research locks. Guards are 640 MiB worker RSS, 600 CPU seconds per attempt, 900 elapsed seconds from first execution, 1,250 MiB host available RAM, and 2 GiB free disk. There are three attempts within the original deadline, 12 active requests, 48 MiB per Parquet asset, 2 MiB per checkpoint, and 512 MiB total sandbox storage. Measure the first approved run before scheduling any additional work. [Runner limits](lab/strategy_lab/jobs.py) · [Asset limits](lab/strategy_lab/datasets.py)

## Commands after explicit approval

Prepare the external intake JSON and relative Parquet paths under `C:\intake` only after resolving permission and dated terms. Record provenance, the new SHA-256, and the availability decision. The reviewed strategy version needs a current-code unit test, a matching profile, and the seeded v1 rules-only baseline. The following is the Position sequence for the proposed dates. `run-next` takes the oldest queued request, so check the queue first.

```powershell
$operator = "Your operator name"
$version = "Reviewed Position strategy version hash"
.\strategy-lab.ps1 show $version
.\strategy-lab.ps1 test $version
$admission = .\strategy-lab.ps1 admit-data C:\intake\position-development.json --approve $operator | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $admission.hash) { throw "Admission failed" }
$request = .\strategy-lab.ps1 request $version $admission.hash `
  --development 2023-01-01T00:00:00Z 2025-01-01T00:00:00Z `
  --validation 2025-01-01T00:00:00Z 2025-11-08T00:00:00Z --approve $operator | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $request.hash) { throw "Request failed" }
.\strategy-lab.ps1 jobs
.\strategy-lab.ps1 run-next
.\strategy-lab.ps1 status $request.hash
.\strategy-lab.ps1 logs $request.hash
# After status is COMPLETE
.\strategy-lab.ps1 export $request.hash --save
```

Dataset reuse, the close-time proxy and scenario costs need the operator’s intake decision. The historical request needs its own explicit `--approve`. New source acquisition and prospective activation require separate approval. This plan excludes protected-data use. [Command reference](STRATEGY_LAB_RUNBOOK.md)

## Dashboard verification

The loopback root and health endpoint returned HTTP 200. A live Streamlit session selected **Workspace → Strategy Library / Research Lab** and rendered seven variants, one family, zero queued jobs, and zero running jobs. Windows reported the actual TCP listener as `python.exe` PID **7628**. Its parent **7580** matches the scheduled dashboard supervisor’s recorded child, and ancestor **6468** matches the supervisor PID and creation time. Windows withheld executable paths and command lines for those processes from this session. The scheduled task action identifies the project virtual environment and `lab.health.dashboard_service --scheduled`. [Local dashboard](http://127.0.0.1:8501) · [Supervisor record](runs/health/dashboard-supervisor.json)
