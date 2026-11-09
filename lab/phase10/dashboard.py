import streamlit as st

from lab.phase9.ideas import recent
from lab.phase9.reference import APPROVAL, MODE, NAMES
from lab.phase10.worker import set_control, status
from lab.storage import utc


LABELS = {"position": "Spot / Position", "swing": "Swing perpetual", "day": "Day perpetual"}


def change(key, widget, profile=None):
    set_control(key, st.session_state[widget], profile)


def panel():
    st.title("Trade ideas")
    snapshot = status()
    flags, identity = snapshot["controls"], snapshot["identity"]
    st.caption(f"KuCoin public reference | {MODE}")
    st.write(f"Worker {snapshot['runtime']} | launch {identity['launcher'] or 'none'} | PID {identity['pid'] or 'none'}")
    chosen = st.radio("Idea profile", NAMES, format_func=LABELS.get, horizontal=True, key="phase10-profile")
    specs = (("monitor", "Public monitoring enabled", None),
             ("ideas", "Ideas enabled for this profile", chosen),
             ("training", "Bounded training enabled for this profile", chosen))
    for column, (key, label, profile) in zip(st.columns(3), specs):
        widget = f"phase10-{key}-{profile or 'all'}"
        st.session_state[widget] = flags[key][profile] if profile else flags[key]
        column.toggle(label, key=widget, on_change=change, args=(key, widget, profile),
                      disabled=key == "training" and chosen == "position")
    current = snapshot["profiles"][chosen]
    st.caption(f"Next decision {current['next_due_local']} | {current.get('waiting_reason', 'Waiting for worker')}")
    st.caption(f"Research {current['training_readiness']} | {current['model_readiness']}")
    st.button("Refresh worker status")
    with st.expander("Worker, collection and research details"):
        st.json(snapshot)
    st.subheader("Validated signals")
    st.info(APPROVAL)
    st.subheader("Experimental ideas")
    for index, row in enumerate(recent(chosen)):
        local = utc(row["decision_at"]).tz_convert(flags["timezone"]).strftime("%Y-%m-%d %H:%M:%S %Z")
        with st.expander(f"{row['market_state']} | {local} | {row['instrument']}", expanded=index == 0):
            st.write(row["reason"])
            st.caption(f"{row['evidence_basis']} | {row['strategy_version']} | {row['idea_id'][:16]}")
            if row.get("structure"):
                st.json(row["structure"], expanded=False)
            if row["status"] == "CONDITIONAL":
                st.json({key: row[key] for key in ("entry", "stop", "take_profit", "time_exit", "paper", "cost")},
                        expanded=False)
            st.caption(f"Market events {len(row['events'])-len(row['simulation_events'])-len(row['user_reported_events'])}"
                       f" | Simulated {len(row['simulation_events'])} | User-reported {len(row['user_reported_events'])}")
