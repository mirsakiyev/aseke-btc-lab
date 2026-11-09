import streamlit as st

from lab.phase9.ideas import decide, observe, recent
from lab.phase9.public import collect
from lab.phase9.reference import APPROVAL, MODE, NAMES, controls, set_control
from lab.storage import utc


LABELS = {"position": "Spot / Position", "swing": "Swing perpetual", "day": "Day perpetual"}


def local(value):
    return utc(value).tz_convert("America/Toronto").strftime("%Y-%m-%d %H:%M:%S %Z")


def save_toggle(name, widget, profile=None):
    set_control(name, st.session_state[widget], profile)


def panel():
    st.title("Trade ideas")
    st.caption(f"KuCoin public reference · {MODE}")
    selected = st.radio("Idea profile", NAMES, format_func=LABELS.get, horizontal=True, key="idea-profile")
    flags = controls()
    for column, (name, label, profile) in zip(st.columns(3), (
        ("monitor", "Public monitoring", None), ("training", "Allow KuCoin research jobs", None),
        ("ideas", "Ideas for this profile", selected),
    )):
        widget = f"phase9-{name}-{profile or 'all'}"
        st.session_state[widget] = flags[name][profile] if profile else flags[name]
        column.toggle(label, key=widget, on_change=save_toggle, args=(name, widget, profile))
    st.caption("KuCoin acquisition, research runs, and idea publication are operator-requested. Coinbase scheduled monitoring is separate.")
    check, observe_quote = st.columns(2)
    try:
        if check.button("Check current setup", disabled=not (flags["monitor"] and flags["ideas"][selected])):
            with st.spinner("Reading a bounded public market snapshot"):
                decide([selected], collect("current"))
        if observe_quote.button("Observe entry conditions", disabled=not flags["monitor"]):
            with st.spinner("Reading a fresh public quote"):
                observe(collect("quotes"))
    except (ValueError, OSError, RuntimeError) as exc:
        st.error(str(exc))
    st.subheader("Validated signals")
    st.info(APPROVAL)
    st.subheader("Experimental ideas")
    history = recent(selected)
    if not history:
        st.write("No local decision record for this profile.")
        return
    for index, item in enumerate(history):
        state = item["market_state"]
        with st.expander(f"{state} · {local(item['decision_at'])} · {item['instrument']}", expanded=index == 0):
            st.write(item["reason"])
            st.caption(f"{item['evidence_basis']} · {item['strategy_version']} · {item['idea_id'][:16]}")
            if item.get("source_last_close"):
                st.caption(f"Source close {local(item['source_last_close'])} · "
                           f"{item['candle_age_seconds']:.0f}s old at decision")
            if item.get("structure"):
                st.json(item["structure"], expanded=False)
            if item["status"] == "CONDITIONAL":
                if state in ("CONDITIONAL", "ENTRY_CONDITION_OBSERVED"):
                    st.write(f"{item['direction']} conditional entry · expires {local(item['expires_at'])}")
                elif state == "STALE_QUOTE":
                    st.write("Original plan · a fresh quote is required")
                else:
                    st.write("Original plan · entry window closed")
                st.json({key: item[key] for key in ("entry", "stop", "take_profit", "time_exit", "paper", "cost")}, expanded=False)
            st.caption(f"Market events {len(item['events']) - len(item['simulation_events']) - len(item['user_reported_events'])} · "
                       f"Simulated {len(item['simulation_events'])} · User-reported {len(item['user_reported_events'])}")
