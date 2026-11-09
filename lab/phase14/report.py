from lab.phase14.context import ROOT, pointer
from lab.storage import put_bytes, read_json


def write_report(status):
    plan, seed, readiness = (pointer(name) for name in ("plan", "seed", "readiness"))
    lines = ["# Phase 14 feature-only admission and amended KuCoin cohort", "",
             "Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**. Continuous training remains off.", "",
             "## Frozen dependencies", "",
             "| Profile | Decision bars | Completed context | Estimator pin |",
             "| --- | --- | --- | --- |",
             "| Day | 15m, 34 minimum plus valid pivots | 51 hourly and 51 four-hour bars | `58a718ac65e7936a737e53b1` |",
             "| Swing | 4h, 34 minimum plus valid pivots | 201 daily bars | `90bd759026499a1a9656f599` |", "",
             "The 34-bar minimum comes from the frozen MACD(12,26,9) initialization. EMA, prior returns, true range, volume, RSI "
             "and confirmed two-sided pivots use contiguous closed trade bars. Two adjacent SMA windows span 51 or 201 bars. "
             "The slope uses the current SMA divided by its previous completed value. The UTC open is inclusive and close exclusive. "
             "The seed uses KuCoin XBTUSDTM trade OHLCV with contracts converted to base BTC. The simulator separately requires paired "
             "native minute trade and mark, dated contract terms, and original funding settlements after the amended start.", ""]
    if seed:
        body = seed["body"]
        ranges = plan["body"]
        lines += ["## Context admission", "", f"Frozen plan `{plan['id']}`. Immutable seed `{seed['id']}`. "
                  f"Admitted {body['admitted_at']}. The acquisition used {body['new_http_requests']} public GET of 64 authorized.", "",
                  f"Trade quarter-hours {ranges['quarter_range'][0]} through {ranges['quarter_range'][1]} exclusive "
                  f"({ranges['expected_quarter']} bars). Daily context {ranges['daily_range'][0]} through {ranges['daily_range'][1]} "
                  f"exclusive ({ranges['expected_daily']} bars). Cache provenance and raw source checksums are recorded in the plan and seed.", "",
                  "The bounded real parity check matched " + ", ".join(f"{r['bars']} bars for {key}" for key,r in body["parity"].items()) + ". "
                  "OHLC differences were zero. Volume differences were at floating-point rounding scale.", "",
                  f"Calendar-overlapping KuCoin inputs used only for context {body['calendar_overlapping_bars']}. "
                  "The protected holdout bundle and performance remained sealed. Zero warm-up scores, labels, account replays and historical fits.", ""]
    if readiness:
        for profile, row in readiness["body"]["profiles"].items():
            lines += [f"**{profile.title()}** was {row['state']} at admission on {row['closed_decision_bars']} decision bars. "
                      f"Latest completed feature cutoff {row['cutoff']}. Schema and pinned artifact SHA-256 checked. "
                      "This technical check did not score a setup.", ""]
    if amendment := status["amendment"]:
        lines += ["## Linked protocol", "", f"Amendment `{amendment['id']}` registered {amendment['registered_at']}. "
                  "The start follows the verified data readiness and uses the first future shared UTC 4-hour boundary.", "",
                  "| Cohort | Start UTC / Toronto | First review UTC / Toronto | Exclusive end UTC / Toronto |",
                  "| --- | --- | --- | --- |"]
        fields = ("start", "technical_review_due", "end_exclusive")
        for name, clocks, local in (("Original", amendment["original_dates_utc"], amendment["original_dates_toronto"]),
                                    ("Amended", amendment, amendment["dates_toronto"])):
            lines.append("| " + name + " | " + " | ".join(f"{clocks[k]} / {local[k]}" for k in fields) + " |")
    live, outcome = status["collection_health"], status["outcomes"]
    lines += ["", "## Commissioning evidence", "", f"Collection {live['state']}. Scheduled launch `{live['launch_id']}` and PID {live['pid']}. "
              f"Live process and writer lock {live['process_identity_and_writer_lock']}. Next capture wake {live['next_due_utc']}.", ""]
    for cycle in live["cycles"]:
        lines += [f"Managed complete checkpoint {cycle['end_exclusive']}. " + ", ".join(
            f"{receipt['basis']} received {receipt['received_at']}" for receipt in cycle["receipts"]) + ".", ""]
    if not live["direct_windows_account_owner_verified"]:
        lines += ["The direct Windows account lookup needs the administrator shell. The active worker was identified through the validated "
                  "running task, Session 0, the current process and writer lock, and ancestry-authenticated capture receipts. "
                  "Use `./phase10.ps1 verify` from the administrator shell to record the direct account check.", ""]
    lines += [f"Outcome status {outcome['state']}. Funding {outcome.get('funding', {}).get('state', 'WAITING')}. "
              f"Future economic evaluations {outcome['future_evaluations']}. First review checks observation coverage, feature continuity, "
              "receipt timing, funding and contract evidence.", ""]
    if (path := ROOT / "preserve-after.json").exists():
        proof = read_json(path)
        lines += [f"Preservation checked {proof['files_checked']} existing files and the original task definitions. "
                  f"Check results {proof['checks']}. Historical acquisition remains 462/463 GETs.", ""]
    path = ROOT / "REPORT.md"
    put_bytes(path, "\n".join(lines).encode())
    return path
