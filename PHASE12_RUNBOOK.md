# Phase 12 complete-history futures amendment

The Phase 12 run uses the Phase 11 partitioned replay and supervisor. It operates in `MANUAL_OUTSIDE_LAB` with all continuous training gates off. Binance historical inputs and derivative models remain `RESEARCH_ONLY`. Approval remains **No validated model**.

## Operation

```powershell
.\phase12.ps1 freeze
.\phase12.ps1 status
.\phase12.ps1 report
.\phase12.ps1 run-once
.\phase12.ps1 resume '<campaign-id>'
```

Use the actual campaign ID returned by status or run-once. The admitted process uses the original single-job lock, two threads, 640 MiB child-tree RSS, 1,250 MiB available RAM reserve, 256 MiB output, and two-hour wall deadline. Admission starts that deadline once. A blocked admission leaves it unset. Resume keeps consumed trial and fit reservations and completed records. A completed campaign reuses its result. Keep Phase 8, Phase 9, and Phase 10 continuous research switches off while the one-shot command runs.

The workstation and child RSS guards stopped the initial worker before it prepared any labels. A continuation used compact trailing arrays and streamed the frozen trade-label records. A second continuation stored equity and exposure in compact native-grid arrays after an account replay reached the RSS cap. The continuation receipts record the code hashes and parity checks. The manifest, deadline, frozen eligibility, and completed work are linked across the resumes.

## Preregistered coverage rule

The policy selects the first complete UTC day after the last documented mark-price gap. The cached minute audit reproduced the 29 missing January 19 and 24 missing December 17 observations. The selected start is **December 18, 2020 at 00:00 UTC**. The end is **November 8, 2025 at 00:00 UTC**, exclusive. Each price basis contains **2,571,840** aligned original observations. The view retains **5,358** original funding settlements. The archived inputs, overlays, and exact settlement times are checksum-linked. Only the intersecting December partition is sliced. There are zero source requests in Phase 12, with the same Phase 11 ledger at 462 of 463 GETs.

The excluded cached prefix is January 1 through December 18, 2020 exclusive. It includes 506,880 trade rows, 506,827 observed marks and 1,056 settlements, and encompasses the March sell-off, the ensuing recovery and the autumn rally. Its gaps, raw evidence and rejected Phase 11 admission remain in the original records. The earlier historical calendar without available Binance observations stays outside this view.

Trade OHLCV supplies complete decision buckets and causal feature contexts. Mark supplies the stop, target, and liquidation trigger basis. Trade prices supply executable-price proxies, and the original settled funding events feed exposure accounting. The view begins with fresh feature state and zero carried positions or intents. The existing feature implementation determines the first ready observation. Chronological maturity must precede the next boundary strictly. The original inner and outer dates, session and holding rules, fees, risk, and sample floors remain in effect.

The cached warm-up audit through the first inner split found the first feature-ready Day decision at 2020-12-26 12:00 UTC and the first Swing decision at 2021-07-07 00:00 UTC. It counted 52,813 Day and 2,140 Swing feature-ready decisions before July 1, 2022. This read-only audit measures feature readiness. The separately frozen campaign eligibility table supplies mature outcome counts.

## Evidence

The original rejection, the new view, and the execution manifest have separate identities. The campaign freezes all variant/fold eligibility evidence before estimator fitting. The eligibility table records observed label states, sample counts, purges, required execution issues and coverage per fold. An ineligible variant or fold receives its own reason. Account replay retains the original account-continuity policy. Forecast MAE and account results are reported separately.

Campaign artifacts are in `runs/phase12/campaigns`. The report is `runs/phase12/REPORT.md`. Synthetic integration exercises real admission logic, label simulation, estimator fitting, checksum-verified model loading and replay. Synthetic fits do not enter the historical ledger.

The completed campaign recorded 136 estimator fits, 68 estimator and 34 mean-control forecast evaluations, and 138 account-fold replays. Six Swing range-reversion forecast variants have partial coverage because the 2023 inner validation had zero mature outcomes against a floor of ten. The two passive spot controls are inapplicable. The final continuation peaked at 616 MiB RSS and wrote about 51 MiB of outputs. A repeat reused the completed result and original deadline.
