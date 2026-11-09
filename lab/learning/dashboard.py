import pandas as pd
import streamlit as st

from lab.learning import state
from lab.learning.control import command, latest_report
from lab.learning.design import PROFILE_IDS


def dispatch(action, **options):
    try:
        command(action, wait=False, **options)
    except (OSError, ValueError, RuntimeError) as exc:
        st.session_state.learning_error = str(exc)


def set_profile(profile, widget):
    dispatch("profile", profile=profile, enabled=st.session_state[widget])


def panel():
    st.title("Research / Learning")
    st.caption("Isolated historical research accounts. Approved lane · No validated model")
    current = state.status()
    st.metric("Learner", current["state"])
    st.write(current["reason"])
    if current.get("active_trial"):
        st.code(current["active_trial"], language=None)
    if error := st.session_state.pop("learning_error", None):
        st.error(error)
    busy = current["state"] in ("QUEUED", "TRAINING", "EVALUATING", "PAUSING")
    for column, (label, action, disabled) in zip(st.columns(5), (
        ("Run once", "run-once", busy or current["mode"] != "off"),
        ("Enable", "start", busy or current["mode"] == "continuous"),
        ("Pause", "pause", current["mode"] not in ("once", "continuous")),
        ("Resume", "resume", current["mode"] != "paused" or busy),
        ("Stop", "stop", current["mode"] == "off"),
    )):
        column.button(label, on_click=dispatch, args=(action,), disabled=disabled, key=f"learning-{action}")
    for key in PROFILE_IDS:
        widget = f"learning-profile-{key}"
        st.session_state[widget] = current["profiles"][key]
        st.checkbox(key, key=widget, disabled=busy, on_change=set_profile, args=(key, widget))
    if st.button("Refresh", key="learning-refresh"):
        st.rerun()
    st.caption(f"{current['campaigns']} campaigns · {current['cumulative_candidate_attempts']} candidate attempts · "
               f"{current['cumulative_fit_attempts']} model fit attempts")
    if current["latest_job"]:
        st.caption(f"Campaign deadline {current['latest_job']['deadline']} · "
                   f"{len(current['latest_job']['trial_ids'])} registered rows including controls")
    result = latest_report()
    report = result["report"] if result else None
    if report:
        st.write(f"Development cutoff {report['cutoff']} · {report['candidate_count']} primary configurations")
        st.write(report["profiles"])
        st.caption(result["directory"])
    rows = []
    for item in current["latest_results"].values():
        trial, metrics = item["trial"], item.get("metrics", {})
        rows.append({"profile": trial["profile"], "entry": trial.get("entry"), "exit": trial.get("exit_policy"),
                     "variant": trial["model"], "cost": trial["cost"], "status": item["status"],
                     "reason": item.get("reason"), "mature labels": item.get("labels", {}).get("complete_mature_outcomes"),
                     "new mature": item.get("new_mature_outcomes"),
                     "return": metrics.get("return"), "drawdown": metrics.get("drawdown_observed_marks"),
                     "funding": metrics.get("funding"), "liquidation costs": metrics.get("liquidation_cost"),
                     "uncertainty": str(metrics.get("uncertainty", {}).get("mean_net_r_95_interval") or
                                        metrics.get("uncertainty", {}).get("status", ""))})
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
