import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lab.config import Profile, load_profiles
from lab.data import aggregate, coverage, snapshot
from lab.model import check_model, windows
from lab.phase2 import store as forward_store
from lab.phase2.checkpoint import frozen_runs
from lab.phase2.collector import status as collector_status
from lab.phase2.shadow_ledger import account_view
from lab.phase3.observation import session_entry_allowed
from lab.research import clip, latest_run
from lab.storage import utc

DISPLAY_ZONE = "America/Toronto"


def local(value) -> str:
    return "—" if value is None or pd.isna(value) else utc(value).tz_convert(DISPLAY_ZONE).strftime("%Y-%m-%d %H:%M %Z")


def toronto_axis(chart: go.Figure, index: pd.DatetimeIndex, title: str) -> None:
    ticks = index[::max(1, len(index) // 7)]
    chart.update_xaxes(tickmode="array", tickvals=ticks, ticktext=[local(ts) for ts in ticks],
                       title=f"{title} · America/Toronto")


def market_chart(bars: pd.DataFrame, label: str) -> go.Figure:
    x = bars.index
    chart = go.Figure(go.Candlestick(
        x=x, open=bars.open, high=bars.high, low=bars.low, close=bars.close,
        text=[local(ts) for ts in x],
        increasing_line_color="#0D7D71", decreasing_line_color="#C0574C",
        name=label,
    ))
    chart.update_layout(height=430, xaxis_rangeslider_visible=False, yaxis_title="USD per BTC",
                        margin=dict(l=0, r=0, t=10, b=0))
    toronto_axis(chart, x, "Candle open")
    return chart


def metric_table(accounts: dict) -> pd.DataFrame:
    rows = {}
    for name, metrics in accounts.items():
        if not metrics:
            continue
        rows[name] = {
            "Net return": f"{metrics['net_return']:.2%}",
            "End equity": f"${metrics['ending_equity_usd']:,.2f}",
            "Max drawdown": f"{metrics['max_drawdown']:.2%}",
            "Mean BTC weight": f"{metrics['mean_btc_weight']:.2%}",
            "Time in BTC": f"{metrics['time_exposure_observed']:.2%}",
            "Closed trades": metrics["trade_count"],
            "Fees": f"${metrics['fees_usd']:,.2f}",
            "Turnover": f"{metrics['turnover']:.2f}×",
            "Net expectancy": usd(metrics["net_expectancy_usd"]),
            "Average win": usd(metrics["average_win_usd"]),
            "Average loss": usd(metrics["average_loss_usd"]),
            "Mean closed hold (minutes)": metrics["mean_closed_holding_minutes"],
            "Unrealized P&L": usd(metrics["unrealized_usd"]),
            "Effective start": local(metrics["evaluation_start"]),
            "Last observed mark": local(metrics["evaluation_end"]),
            "Simulation status": metrics["status"],
        }
    return pd.DataFrame.from_dict(rows, orient="index")


def usd(value) -> str:
    return "—" if value is None else f"${value:,.2f}"


def results(profile, manifest: dict, directory) -> None:
    state = manifest["readiness"]
    if profile.identity != manifest["profile_hash"]:
        state = "STALE_MODEL" if manifest["model"] else "PROFILE_CHANGED"
    elif manifest["model"]:
        try:
            check_model(manifest["model"], profile, profile.costs)
        except ValueError:
            state = "STALE_MODEL"
    st.write(f"**{state}** · {manifest['economic_evidence']}")
    model = manifest["model"]
    st.caption(f"Model {model['model_version'] if model else 'unfitted'} · Run {manifest['run_id']}")
    if model:
        st.caption(f"Training cutoff {local(model['training_cutoff'])} · "
                   f"Frozen research model · Threshold {model['entry_threshold']:.2%} predicted net return")
    if manifest.get("blocker"):
        st.info(manifest["blocker"])
    st.caption("Each profile has its own hypothetical account. All results below use development data.")
    period_name = st.radio("Development window", ["evaluation", "validation"], horizontal=True,
                           format_func=str.title)
    period = manifest["periods"].get(period_name, {})
    if not period or not period.get("baseline"):
        st.info("This snapshot does not have a baseline result for the selected window.")
        return
    accounts = {"Baseline": period["baseline"], "Candidate · research only": period.get("candidate")}
    st.dataframe(metric_table(accounts), width="stretch")
    st.caption("Benchmarks use each account’s effective dates and cost scenario.")
    benchmark = pd.DataFrame(period["baseline"]["benchmarks"]).T[
        ["net_return", "max_drawdown", "mean_btc_weight", "fees_usd"]
    ]
    st.dataframe(benchmark, width="stretch")
    st.subheader("Equity")
    equity = pd.read_parquet(directory / f"{period_name}-baseline-equity.parquet")
    curves = equity[["equity", "cash_benchmark", "full_btc", "passive_25pct"]].rename(columns={"equity": "Baseline"})
    if period.get("candidate"):
        candidate = pd.read_parquet(directory / f"{period_name}-candidate-equity.parquet")
        curves = curves.join(candidate.equity.rename("Candidate · research only"), how="outer")
    chart = go.Figure()
    for name in curves:
        curve = curves[name].dropna()
        days = curve.index.tz_convert(DISPLAY_ZONE).normalize()
        curve = curve[~days.duplicated(keep="last")]
        chart.add_scatter(x=curve.index, y=curve, mode="lines", name=name,
                          text=[local(ts) for ts in curve.index], hovertemplate="%{text}<br>$%{y:,.2f}")
    toronto_axis(chart, curves.index, "Observed equity mark")
    st.plotly_chart(chart, width="stretch")
    st.subheader("Trade journal")
    account = st.radio("Journal account", [name for name, metrics in accounts.items() if metrics],
                       horizontal=True)
    key = "baseline" if account == "Baseline" else "candidate"
    journal = pd.read_parquet(directory / f"{period_name}-{key}-trades.parquet")
    if journal.empty:
        st.caption("No simulated fills in this account and window.")
    else:
        for column in ("signal_at", "available_at", "entry_at", "exit_at", "exit_bar_open", "deadline"):
            journal[column] = journal[column].map(local)
        st.dataframe(journal, width="stretch")
    with st.expander("Monthly account and benchmark returns"):
        monthly = pd.read_csv(directory / f"{period_name}-{key}-monthly.csv")
        if "marked_through_utc" in monthly:
            monthly["Last mark · Toronto"] = monthly.pop("marked_through_utc").map(local)
        st.dataframe(monthly, width="stretch")
    st.subheader("Unapproved model research")
    st.caption("Scores estimate net return on entry notional. The frozen filter accepts scores above zero.")
    if period.get("prediction_error"):
        st.json(period["prediction_error"])
    counts = {"training": dict(manifest.get("training_samples", {})), period_name: dict(period["samples"])}
    for record in counts.values():
        for field in ("first_signal", "last_signal"):
            if field in record:
                record[field] = local(record[field])
    st.json(counts)
    if period_name == "evaluation" and manifest["sensitivity"]:
        st.subheader("Cost and delay sensitivity")
        rows = []
        for scenario in manifest["sensitivity"]:
            rows.append({
                **scenario["evaluation_execution"], "baseline_return": scenario["baseline"]["net_return"],
                "baseline_status": scenario["baseline"]["status"],
                "candidate_return": scenario.get("candidate", {}).get("net_return"),
                "candidate_status": scenario.get("candidate", {}).get("status"),
            })
        st.dataframe(pd.DataFrame(rows), width="stretch")


@st.fragment(run_every="30s")
def forward_panel(profile) -> None:
    with st.expander("Forward research", expanded=True):
        current = collector_status()
        feed = current.get("feeds", {}).get(str(profile.seconds), {})
        process = current.get("pid")
        online = current["process_alive"] and not current["runner_stale"]
        collector = current.get("state", "not_started") if online else (
            "status stale" if current["process_alive"] else "offline"
        )
        st.markdown("**Public market data collection**")
        st.write(f"Collector **{collector}**" + (f" · PID {process}" if online else ""))
        heartbeat = current.get("heartbeat", {})
        st.caption(f"Last heartbeat {local(heartbeat.get('received_at'))} · "
                   f"Latest observed {profile.execution} close {local(feed.get('last_observed_close'))} · "
                   f"Last REST receipt {local(feed.get('last_response_at'))}")
        if current.get("last_error") or heartbeat.get("error"):
            st.warning(current.get("last_error") or heartbeat["error"])
        st.markdown("**Experimental shadow · hypothetical only**")
        if not forward_store.DATABASE.is_file():
            st.caption("No forward experiment has been activated.")
            return
        with forward_store.connect(forward_store.DATABASE, readonly=True) as db:
            db.execute("BEGIN")
            experiment = forward_store.get(db, "experiment")
            state = forward_store.get_account(db, profile.id)
            observations = forward_store.events(db, profile.id, limit=25)
        if not experiment or not state:
            st.caption("No forward experiment has been activated.")
            return
        shadow = current.get("shadow", {})
        status = shadow.get("state", "not enabled") if online else "worker offline; saved state"
        st.write(f"Shadow **{status.replace('_', ' ')}** · "
                 f"Observation began {local(experiment['observation_start_utc'])}")
        if shadow.get("problem") or shadow.get("model_issues", {}).get(profile.id):
            st.warning(shadow.get("model_issues", {}).get(profile.id) or shadow["problem"])
        if state["unresolved"]:
            unresolved = state["unresolved"]
            st.warning(f"Exposure uncertain since {local(unresolved['missing_open'])}. "
                       f"Deadline status {unresolved['deadline_status'].replace('_', ' ')}.")
        view = account_view(state)
        columns = st.columns(4)
        columns[0].metric("Prospective shadow decisions", state.get("prospective_decision_count", state["decision_count"]))
        columns[1].metric("Entry fills / closed trades", f"{state['entry_fills']} / {state['closed_trades']}")
        columns[2].metric("Realized P&L", usd(state["realized_usd"]))
        columns[3].metric("Last marked equity", usd(view["equity_usd_at_last_mark"]))
        st.caption(f"Unrealized {usd(view['unrealized_usd_at_last_mark'])} · "
                   f"Last mark {local(state.get('mark_at'))} · "
                   f"Late catch-up decisions {state.get('late_catchup_count', 0)} · "
                   f"Reconstructed or uncertain closed trades {state['reconstructed_or_uncertain_trades']}")
        if state.get("position") or state.get("pending_entry"):
            position = state.get("position") or {}
            policy = position.get("intent") or state["pending_entry"]
            st.caption(f"{'Open position' if position else 'Pending entry'} · "
                       f"{policy['units_btc']:.8f} BTC · Stop {usd(policy['stop'])} · "
                       f"Deadline {local(policy['deadline'])}")
        if observations:
            policy = profile
            pinned = experiment.get("profile_hashes", {}).get(profile.id)
            if profile.session and pinned is not None and pinned != profile.identity:
                policy = Profile(**frozen_runs()[profile.id][1]["profile"])
            rows = [{
                "Recorded (Toronto)": local(row["recorded_at"]),
                "Decision close (Toronto)": local(row.get("decision_close")),
                "Event": row["kind"].replace("_", " "),
                "Reason": (row.get("decision_outcome") or row.get("reason") or "").replace("_", " "),
                "Observation": row.get("observation_quality", "").replace("_", " "),
                "Baseline signal": row.get("baseline_signal"),
                "Next execution opening (Toronto)": local(row.get("modeled_fill_at")) if row["kind"] == "decision" else "—",
                "Session permits entry at opening": session_entry_allowed(policy, row["decision_close"], row["modeled_fill_at"])
                if policy.session and row["kind"] == "decision" else None,
                "Hypothetical fill (Toronto)": local(row.get("modeled_fill_at")) if row["kind"] in ("entry_fill", "exit_fill") else "—",
                "Predicted net return": (f"{row['score_decimal']:.3%}" if row.get("score_decimal") is not None else "—"),
            } for row in observations]
            st.dataframe(pd.DataFrame(rows), width="stretch")
        st.caption("Model scores and delayed bar fills are research observations. Holdout performance is sealed.")


def main() -> None:
    st.set_page_config(page_title="ASEKE BTC Lab", page_icon="₿", layout="wide")
    profiles = load_profiles()
    with st.sidebar:
        view = st.radio("Workspace", ("Observer", "Research / Learning", "Trade ideas", "Challenger learning",
                                     "Native learning readiness", "Operational health"), key="workspace")
        if view == "Research / Learning":
            from lab.learning.dashboard import panel
        elif view == "Trade ideas":
            from lab.phase15.dashboard import panel
        elif view == "Challenger learning":
            from lab.phase16.dashboard import panel
        elif view == "Native learning readiness":
            from lab.phase17.dashboard import panel
        elif view == "Operational health":
            from lab.health.panel import panel
        else:
            panel = None
        st.header("Research profile")
        key = st.radio("Strategy profile", list(profiles), format_func=lambda item: profiles[item].label,
                       label_visibility="collapsed", key="profile")
        st.button("Scalping · Coming later", disabled=True)
        st.caption("Charts and reports use America/Toronto display time. Session and data boundaries use UTC.")
    if panel is not None:
        panel()
        return
    profile = profiles[key]
    st.title("ASEKE BTC Lab")
    st.caption("Local development research · Coinbase Exchange BTC-USD · Unlevered spot")
    st.header(profile.label)
    cols = st.columns(4)
    for col, title, value in zip(cols, ("Decisions", "Context", "Execution candles", "Intended hold"),
                                 (profile.decision, ", ".join(c.frame for c in profile.context),
                                  profile.execution, profile.holding_label)):
        col.metric(title, value)
    if profile.session:
        cutoff = profile.session.cutoff_minute_utc
        flat = profile.session.flat_minute_utc
        st.caption(f"UTC session · no new fills at or after {cutoff // 60:02d}:{cutoff % 60:02d} · "
                   f"flat by {flat // 60:02d}:{flat % 60:02d}")
    st.caption(f"Reference cost per side · {profile.costs.fee_bps} bps fee + "
               f"{profile.costs.proxy_bps} bps adverse price proxy · {profile.costs.processing_seconds}s processing")
    st.info("No validated model")
    forward_panel(profile)
    rules = windows()
    frame, record = snapshot(profile.seconds)
    observed = coverage(frame, profile.seconds, end=utc(rules.data_cutoff))
    st.subheader("Data coverage")
    st.write(f"**{observed['bars']:,}** observed bars · {local(observed['first_open'])} to "
             f"{local(observed['last_close'])}")
    st.caption(f"Frozen backfill target · {observed['missing_target_bars']:,} missing bars, including "
               f"{observed['internal_gap_bars']:,} internal gaps.")
    if record:
        age = observed["freshness_seconds"]
        freshness = f" · Last close {age / 3600:,.1f} hours ago" if age is not None else ""
        st.caption(f"Snapshot published {local(record['published_at'])} · {record['sha256'][:16]}{freshness}")
    if not frame.empty:
        st.subheader("Development price history")
        intervals = ["5min", "15min"] if profile.seconds == 300 else []
        intervals += ["1h", "4h", "1D", "7D"]
        interval = st.selectbox("Chart interval", intervals, index=intervals.index(profile.decision), key=f"chart-{key}")
        bars = aggregate(clip(frame, utc(rules.holdout_start)), interval, profile.step).tail(200)
        if not bars.empty:
            st.plotly_chart(market_chart(bars, profile.instrument), width="stretch")
    item = latest_run(key)
    st.subheader("Development results")
    if item:
        directory, manifest = item
        if manifest["source_snapshot"] != record:
            st.caption("This run used a different saved candle snapshot. See its manifest for dates and hashes.")
        results(profile, manifest, directory)
    else:
        st.info("DATA_MISSING" if frame.empty else "No saved development run for this profile.")


main()
