from contextlib import suppress

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lab.phase9.ideas import recent
from lab.phase10.worker import set_control as set_worker_control
from lab.phase15.catalog import set_control
from lab.phase15.worker import status
from lab.storage import utc


LABELS = {"position": "Position · BTC-USDT", "swing": "Swing · XBTUSDTM", "day": "Day · XBTUSDTM"}


def clock(value):
    if not value:
        return "—"
    at = utc(value)
    return f"{at:%b %d %H:%M UTC} / {at.tz_convert('America/Toronto'):%H:%M %Z}"


def number(value, digits=2):
    return f"{value:,.{digits}f}" if value is not None else "—"


def draw(row, opened):
    if not row["chart"]:
        return
    frame = pd.DataFrame(row["chart"])
    series = pd.to_datetime(frame.close_at, utc=True).dt.tz_convert("America/Toronto")
    fig = go.Figure(go.Candlestick(x=series, open=frame.open, high=frame.high, low=frame.low, close=frame.close,
                                   name="Completed KuCoin trade candles"))
    for value, label, color in ((row.get("confirmation_level"), "Confirmation close", "#3772d6"),
                                 ((row.get("plan") or {}).get("stop"), "Proposed initial stop", "#bb4242"),
                                 ((row.get("plan") or {}).get("fixed_target"), "Illustrative fixed target", "#318359"),
                                 ((opened or {}).get("stop"), "Current paper stop", "#9164c4")):
        if value is not None:
            fig.add_hline(y=value, line_dash="dot", line_color=color, annotation_text=f"{label} {value:,.2f}")
    if plan := row.get("plan"):
        fig.add_hrect(y0=plan["entry"]["minimum"], y1=plan["entry"]["maximum"], fillcolor="#3772d6", opacity=.14,
                      line_width=0, annotation_text="Acceptable entry range")
    fig.update_layout(height=360, xaxis_rangeslider_visible=False, xaxis_title="Completed bar close · Toronto time",
                      yaxis_title="USDT per BTC", margin=dict(l=10,r=10,t=24,b=0))
    st.plotly_chart(fig, width="stretch", key=f"phase15-chart-{row['profile']}-{row['exit_policy']}")


def trade_card(plan, published=None):
    entry, size = plan["entry"], plan["sizing"]
    st.write(f"Entry requires {entry['quote_basis']} confirmation for the conditional idea. "
             f"The paper book uses an adverse-adjusted trade-minute open in **{number(entry['minimum'])}–{number(entry['maximum'])} USDT**.")
    exit_rule = (f"Illustrative {plan['target_r']:g}R target {number(plan['fixed_target'])} USDT. Recomputed from the modeled fill."
                 if plan["target_r"] else f"Trail {plan['trailing_atr']:g} × ATR behind completed 4-hour closes. The stop only tightens.")
    st.write(f"Initial stop **{number(plan['stop'])} USDT** on {plan['trigger_basis']} uses {plan.get('initial_stop_rule', 'the frozen stop rule')}. "
             f"Exit execution uses {plan['exit_execution_basis']} prices. {exit_rule}")
    st.caption(f"{'Published '+clock(published)+' · ' if published else ''}Eligible from {clock(plan['eligible_from'])} · "
               f"Entry expiry {clock(plan['entry_expiry'])} · Maximum hold {plan['max_hold_minutes']/60:g} h · "
               f"Session flat {clock(plan['session_flat'])}")
    st.write(f"Illustrative {number(size['base_quantity'], 6)} BTC · {number(size['notional'])} USDT notional · "
             f"{number(size['margin'])} USDT margin · {size['effective_exposure']:.1%} account exposure.")
    st.write(f"Planned risk **{number(plan['order']['planned_risk'])} USDT** · Initial stop scenario "
             f"{number(size['initial_stop_loss'])} USDT · Same-size double-cost stop stress {number(size['double_cost_stop_loss'])} USDT.")
    st.caption(f"Reference taker {plan['cost']['taker']:.3%} and adverse price {plan['cost']['adverse_proxy']:.3%} per side. "
               + ("Published public funding is accounted separately." if plan["trigger_basis"] == "MARK" else "Spot funding does not apply."))


def book_panel(book):
    if not book:
        st.caption("Paper book awaits registration.")
        return
    if "counts" not in book:
        st.caption("Paper book registered. Its first cycle is pending.")
        return
    st.markdown("**Independent paper comparison account**")
    columns = st.columns(4)
    for column, label, value in zip(columns, ("Marked equity", "Available cash", "Reserved risk", "Max drawdown"),
                                    (number(book["equity"]), number(book["available_cash"]), number(book["reserved_risk"]),
                                     f"{book['drawdown']:.2%}")):
        column.metric(label, value)
    counts = book["counts"]
    st.caption(f"{book['book_status']} · Published {counts.get('PUBLISHED',0)} · Entry observed {counts.get('ENTRY_OBSERVED',0)} · "
               f"Paper fills {counts.get('HYPOTHETICAL_FILL',0)} · Closed {counts.get('CLOSED',0)} · User reported {counts.get('USER_REPORTED_EXECUTION',0)}")
    if book["problem"]:
        st.warning(book["problem"])
    st.write(f"Gross price P&L {number(book['gross'])} · Fees {number(book['fees'])} · Funding {number(book['funding'])} · "
             f"Closed net {number(book['closed_net'])} · Unrealized {number(book['unrealized'])} USDT")
    st.caption(f"Closed-trade fees {number(book['closed_fees'])} · Closed-trade funding {number(book['closed_funding'])} · "
               f"Cash reconciliation difference {number(book['cash_reconciliation_difference'], 6)} USDT")
    st.caption(f"Exposure {number(book['exposure'])} · Margin {number(book['reserved_margin'])} · Turnover {number(book['turnover'])} USDT · "
               f"Net expectancy {number(book['expectancy'])} USDT / {number(book['expectancy_r'])}R · "
               f"Last accounted close {clock(book['last_source_close'])}")
    if not counts.get("CLOSED", 0):
        st.caption("No closed outcomes yet. Expectancy is pending.")
    else:
        st.caption(f"Average hold {number(book['average_hold_minutes'])} min · Top three share of positive gains "
                   f"{number(book['top_three_share_of_positive_gains'])} · Adverse cost embedded in gross "
                   f"{number(book['adverse_cost_in_realized_gross'])} USDT")
    if book["position"]:
        pos = book["position"]
        st.write(f"Open since {clock(pos['entry_at'])} · Time exit {clock(pos['deadline'])} · Current stop {number(pos['stop'])} USDT")
    if book["reasons"]:
        st.dataframe([{"Reason": key, "Count": value} for key,value in book["reasons"].items()], hide_index=True)
    if book["last_ideas"]:
        with st.expander("Published trade cards"):
            for item in book["last_ideas"]:
                st.markdown(f"**{item['direction']} · {item['strategy_version']} · {item['id'][:12]}**")
                trade_card(item["plan"], item["published_at"])
    if book["recent_closed"]:
        st.dataframe([{"Entry": clock(item["entry_at"]), "Exit": clock(item["exit_at"]), "Reason": item["reason"],
                       "Net USDT": item["net"], "Net R": item["net_r"], "Planned risk": item["planned_risk"],
                       "Realized loss": item["realized_loss"], "Hold min": item["holding_minutes"]} for item in book["recent_closed"]], hide_index=True)
    if book["last_events"]:
        with st.expander("Paper and user-reported event ledger"):
            st.dataframe([{"Event": item["type"], "Idea": item["idea_id"][:12], "Source time": clock(item["source_at"]),
                           "Recorded": clock(item["recorded_at"]), "Reason": item.get("reason", "—")}
                          for item in book["last_events"]], hide_index=True)


def panel():
    st.title("Trade opportunities")
    data = status()
    profile = st.radio("Profile", tuple(LABELS), format_func=LABELS.get, horizontal=True, key="phase15-profile")
    st.info(data["approval"])
    st.caption("KuCoin public data · EXPERIMENTAL / RULES_ONLY · MANUAL_OUTSIDE_LAB")
    status_tab, current_tab = st.tabs(["Operations", "Setups & books"])
    with status_tab:
        st.write(f"Public monitor {'on' if data['collection']['monitor_enabled'] else 'off'} · Shared minute feed "
                 f"{'on' if data['collection']['shared_enabled'] else 'off'} · Managed Phase 15 "
                 f"{'verified' if data['worker']['managed_phase15_verified'] else 'awaiting same-launch evidence'}")
        st.caption(f"Next shared capture {clock(data['collection']['next_due'])} · Spot minute requests {data['collection']['spot_requests']}")
        st.write(f"Legacy ideas for this profile {'on' if data['worker_controls']['ideas'][profile] else 'off'} · "
                 f"Continuous training {'off' if data['continuous_training_off'] else 'enabled outside Phase 15'}")
        queue = data["training_queues"][profile]
        st.caption(f"{queue['source']} · {len(queue['waiting'])} waiting · profile training gate {'on' if queue['gate_enabled'] else 'off'}")
        for col, key, name, label in ((st, "monitor", None, "Existing public market monitor"),
                                     (st, "ideas", profile, "Existing profile idea permission")):
            widget = f"phase15-worker-{key}-{name or 'all'}"
            st.session_state[widget] = data["worker_controls"][key][name] if name else data["worker_controls"][key]
            col.toggle(label, key=widget, on_change=lambda k=key, n=name, w=widget: set_worker_control(k, st.session_state[w], n))
        for col, (key, label) in zip(st.columns(4), (("analysis","Analysis"),("publication","New ideas"),
                                                   ("paper","Paper tracking"),("spot_minutes","Spot minute capture"))):
            widget = f"phase15-{key}"
            st.session_state[widget] = data["controls"][key]
            col.toggle(label, key=widget, on_change=lambda name=key, state=widget: set_control(name, st.session_state[state]))
        if cohort := data["phase14"]:
            st.write(f"Frozen Phase 14 · {cohort['evaluation_state']} · review {clock(cohort['review'])} · final evaluation manual")
        st.button("Refresh status", key="phase15-refresh")
    with current_tab:
        if not data["opportunities"]:
            st.warning("Current opportunity analysis is disabled.")
        for key, row in data["opportunities"].items():
            if row["profile"] != profile:
                continue
            st.subheader(f"{row['strategy']} / {row['exit_policy']}")
            st.caption(f"{row['instrument']} · {row['timeframe']} · {row['strategy_version']} · no model")
            st.caption("Analysis uses Phase 10 public candles and quote receipts. Paper accounting uses "
                       + ("the separate public Spot minute feed." if profile == "position" else "shared paired futures minute and funding observations."))
            st.write(f"**{row['state']}** · {row['direction'] or 'No eligible direction'} · {row['reason']}")
            st.caption(f"Completed bar {clock(row['latest_completed'])} · candle age {number(row.get('candle_age_seconds'),0)} s · "
                       f"quote age {number(row['quote_age_seconds'],0)} s · next decision {clock(row['next_decision_at'])}")
            if row["session"]["entry_cutoff"]:
                st.caption(f"Day entry cutoff {clock(row['session']['entry_cutoff'])} · flat {clock(row['session']['flat'])}")
            if "confirmation_level" in row:
                st.write(f"{row['confirmation']} **{number(row['confirmation_level'])}**. "
                         f"Distance from last close {number(row['distance_to_confirmation'])} USDT. {row['watch_invalidation']}.")
            if structure := row.get("structure"):
                st.caption(f"Close {number(structure['close'])} · Prior range {number(structure['prior_low'])}–{number(structure['prior_high'])} · "
                           f"ATR {number(structure['atr'])} · Confirmed pivots {number(structure['pivot_low'])}/{number(structure['pivot_high'])}")
                for name, context in structure["context"].items():
                    distance = context["close_over_sma_minus_one"]
                    st.caption(f"{name} close vs SMA({context['sma_bars']}) · {f'{distance:+.2%}' if distance is not None else 'waiting'}")
            if row["gates"]:
                st.dataframe([{"Gate": name, "State": value} for name,value in row["gates"].items()], hide_index=True)
            book = data["books"].get(key, {})
            draw(row, book.get("position"))
            if plan := row.get("plan"):
                st.caption("Current setup preview. Actual publication times appear in the paper book's published cards.")
                if book.get("pending") or book.get("position") or book.get("problem"):
                    st.warning("The book is occupied or unresolved. This preview cannot open a second position.")
                trade_card(plan)
            else:
                st.caption("Watchlist entries cannot receive paper fills.")
            book_panel(book)
        with st.expander("Earlier Phase 9/10 ideas and quote-proxy records"):
            with suppress(RuntimeError):
                for item in recent(profile):
                    st.caption(f"{clock(item['decision_at'])} · {item['market_state']} · {item['strategy_version']} · {item['reason']}")
                    st.caption(f"Quote-proxy events {len(item['simulation_events'])} · User-reported executions {len(item['user_reported_events'])}")
