import streamlit as st

from lab.phase16.control import change
from lab.phase16.report import fmt, status


def panel():
    st.title("Challenger learning")
    data = status()
    st.info(data["approval"])
    st.caption("MANUAL_OUTSIDE_LAB · Candle collection, model fitting, and economic evaluation are separate stages. Candidates remain research-only.")
    st.write(f"Recurring checks {'on' if data['controls']['recurring'] else 'off'} · Legacy training "
             f"{'off' if data['legacy_training_off'] else 'active outside this route'} · Next collector check "
             f"{data['next_managed_check'] or 'pending'}")
    if checked := data["last_managed_check"]:
        st.caption(f"Last managed check {checked['at']} · {checked['state']}")
    for column, action, label in zip(st.columns(3), ("enable", "pause", "stop"), ("Enable recurring checks", "Pause challenger", "Stop challenger")):
        if column.button(label, key=f"phase16-{action}"):
            change(action)
            st.rerun()
    st.code(".\\phase16.ps1 run-once\n.\\phase16.ps1 resume\n.\\phase16.ps1 report", language="powershell")
    if cohort := data["phase15"]:
        st.caption(f"Phase 15 publishes through {cohort['publication_end_exclusive']} exclusive. Open-book follow-through "
                   f"ends {cohort['follow_end_exclusive']} exclusive. Its recipes and accounts are separate.")
    if data["campaign"]:
        st.caption(f"Initial campaign {data['campaign'][:16]} · {data['counts'].get('actual_fits',0)} fits · "
                   f"{data['counts'].get('completed_accounts',0)} continuous account cases · One research job, two threads, 640 MiB.")
    profile = st.radio("Profile", ("position", "swing", "day"), format_func=str.title, horizontal=True, key="phase16-profile")
    row = data["profiles"][profile]
    st.subheader(profile.title())
    st.write(f"**{row['state']}** · {row['reason']}")
    st.caption("Operational recipes · " + (", ".join(row["live_recipes"]) or "Awaiting a live registration"))
    if trial := row["recipe"]:
        st.write(f"Challenger · `{trial['entry']}` / `{trial['exit_policy']}` · Fixed Ridge feature hypothesis")
        st.caption(f"Data {row['data_manifest']} · {row['source']} · Input end {row['development_cutoff']}")
        with st.expander("Target, features and comparisons"):
            st.write(row["target"])
            st.write("Feature schema · " + ", ".join(row["features"]))
            st.write("Comparators · " + ", ".join(row["comparators"]))
            budget = row["budget"]
            st.caption(f"One job · {budget['max_threads']} threads · {budget['child_rss_mib']} MiB child RSS · "
                       f"{budget['wall_seconds']/3600:g} hours · {budget['output_cap_mib']} MiB output")
    if queued := row["queued_source"]:
        st.caption(f"Queued allocation {queued['campaign']} · Data {queued['data_manifest']} · {queued['scope']} · "
                   f"Input end {queued['end_exclusive']}")
    st.write(f"Last experiment {row['last_state'] or 'pending'} · {row['conclusion'] or 'comparison pending'}")
    if row["conclusion_reason"]:
        st.caption(row["conclusion_reason"])
    st.write(f"Mature admitted setups {row['mature_labels'] if row['mature_labels'] is not None else 'pending'} · "
             f"New mature setups {row['new_mature_labels']} / {row['required_new_mature'] or '—'} · Interval {row['interval_days'] or '—'} days")
    st.caption(f"Latest training cutoff {row['training_cutoff'] or 'pending'} · Last completed {row['last_completed_at'] or 'pending'} · "
               f"Next eligible {row['next_eligible_check'] or 'pending new admitted data'}")
    if row["models"]:
        st.dataframe([{"Outer fold": model["fold"], "Model version": model["version"], "Training cutoff": model["training_cutoff"],
                       "Mature training labels": model["mature_labels"], "Use": model["scope"]} for model in row["models"]], hide_index=True)
    if baseline := row["last_model_baseline"]:
        st.caption("Last completed model comparison · Primary assumptions")
        st.dataframe([{"Lane": lane.replace("_", " "), "Net return": fmt(metrics["return"], percentage=True),
                       "Drawdown": fmt(metrics["drawdown_observed_marks"], percentage=True), "Closed trades": metrics["closed_trades"]}
                      for lane, metrics in baseline.items()], hide_index=True)
    if compared := row["primary_comparison"]:
        with st.expander("Candidate versus matching rule under stress"):
            st.dataframe([{"Assumptions": case.replace("_", " "), "Lane": lane.replace("_", " "),
                           "Net return": fmt(m["return"], percentage=True), "Closed trades": m["closed_trades"],
                           "Drawdown": fmt(m["drawdown_observed_marks"], percentage=True),
                           "Exposure": fmt(m["exposure_fraction_of_observed_marks"], percentage=True)}
                          for case, lanes in compared.items() for lane, m in lanes.items()], hide_index=True)
    st.warning(row["permission"])
