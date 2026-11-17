"""Read the active seven-slot ledger without opening retired account databases."""

from contextlib import closing
import json
import sqlite3

import streamlit as st

from lab.storage import utc


APPROVAL = "No validated model"
MODE = "MANUAL_OUTSIDE_LAB"
PRICE_BASES = {
    "KUCOIN_XBTUSDTM_MARK": "KuCoin XBTUSDTM mark",
    "KUCOIN_SPOT_LAST_PRICE_AT_TICKER_SNAPSHOT": "KuCoin BTC-USDT Spot last price at ticker snapshot",
    "KRAKEN_BTC_USD_LAST_TRADE": "Kraken BTC/USD Spot last trade",
}


def current_view(*, family=None):
    """Return None for the legacy UI until the verified retirement is permanent."""
    from lab.paper_run.gate import retired

    marker = retired()
    if not marker:
        return None
    from lab.paper_run import runtime

    view = runtime.status()
    books = {slot: dict(book) for slot, book in view.get("books", {}).items()
             if family is None or slot.startswith(f"{family}/")}
    if books and view.get("generation_id"):
        with closing(runtime.connect(readonly=True)) as db:
            db.execute("BEGIN")
            for book in books.values():
                count, wins = db.execute(
                    "SELECT count(*),count(CASE WHEN json_extract(body,'$.net')>0 THEN 1 END) FROM event "
                    "WHERE book=? AND kind='CLOSED' AND json_extract(body,'$.net') IS NOT NULL",
                    (book["book_id"],),
                ).fetchone()
                book["win_rate"] = wins / count if count and book.get("accounting_complete", True) else None
    return {**view, "books": books, "archive": view.get("archive") or marker["body"]["path"],
            "cutoff": view.get("cutoff") or marker["body"]["cutoff"], "retirement_id": marker["id"],
            "mode": MODE, "approval": APPROVAL, "comparisons": []}


def events(slot, limit):
    from lab.paper_run import runtime

    if not (book := runtime.status().get("books", {}).get(slot)):
        return []
    with closing(runtime.connect(readonly=True)) as db:
        return [{"id": key, "type": kind, "idea_id": idea, "source_at": occurred, "recorded_at": recorded,
                 "details": json.loads(raw)} for key, kind, idea, occurred, recorded, raw in db.execute(
            "SELECT key,kind,idea,source_at,recorded_at,body FROM event WHERE book=? ORDER BY rowid DESC LIMIT ?",
            (book["book_id"], limit),
        )]


def _money(value):
    return f"{value:,.2f}" if value is not None else "—"


def _stamp(value):
    if isinstance(value, dict):
        value = next((value[key] for key in ("at", "recorded_at", "received_at", "source_at", "decision_at")
                      if value.get(key)), None)
    if not value:
        return "—"
    return f"{utc(value).tz_convert('America/Toronto'):%b %d %H:%M:%S %Z}"


def _summary(book):
    okay = book.get("accounting_complete", True)
    observation = book.get("last_observation")
    reference = (observation.get("price_text", observation.get("reference_price_text", observation.get("price", observation.get("mark"))))
                 if isinstance(observation, dict) else None)
    return {
        "Paper book": book["label"], "State": book["state"],
        "Price basis": PRICE_BASES.get(book["price_basis"], book["price_basis"]),
        "Freshness budget (s)": book["freshness_seconds"], "Balance": _money(book.get("cash")),
        "Equity": _money(book.get("equity") if okay else None), "Exposure": _money(book.get("exposure")),
        "Realized net": _money(book.get("closed_net") if okay else None),
        "Closed trades": (book.get("counts") or {}).get("CLOSED", 0),
        "Win rate": f"{book['win_rate']:.1%}" if book.get("win_rate") is not None else "—",
        "Last evaluation": _stamp(book.get("last_evaluation")), "Next evaluation": _stamp(book.get("next_evaluation")),
        "Latest accepted price": str(reference) if reference is not None else None,
        "Price observed": _stamp(observation.get("source_at")) if isinstance(observation, dict) else "—",
        "Run ID": book["run_id"],
    }


def _detail(book):
    st.subheader(book["label"])
    basis = PRICE_BASES.get(book["price_basis"], book["price_basis"])
    st.write(f"**{book['state']}** · {basis} · {book['freshness_seconds']:g}s freshness budget")
    st.caption(f"{book['entry_model']} entry · {book['exit_model']} exit")
    if reason := book.get("reason") or book.get("problem"):
        st.warning(str(reason))
    if not book.get("accounting_complete", True):
        st.warning("Accounting is unresolved. Final performance is unavailable.")
    st.json({
        "run_id": book["run_id"], "book_id": book["book_id"], "retired_book_id": book.get("retired_book_id"),
        "account_unit": book.get("ledger_unit", "USDT").replace("NOMINAL_USDT", "nominal USDT"),
        "starting_balance": book.get("starting_equity"), "balance": book.get("cash"), "open_exposure": book.get("exposure"),
        "net_closed": book.get("closed_net") if book.get("accounting_complete", True) else None,
        "fees": book.get("fees"), "funding": book.get("funding"), "financing": book.get("financing"),
        "liquidation_cost": book.get("liquidation_cost"), "unfunded_deficit": book.get("unfunded_deficit"),
        "last_evaluation": book.get("last_evaluation"), "last_completed_evaluation": book.get("last_completed_evaluation"),
        "next_evaluation": book.get("next_evaluation"),
        "last_accepted_price_observation": book.get("last_observation"), "position": book.get("position"),
    }, expanded=False)
    history = book.get("recent_closed") or []
    st.subheader("Current run trade history")
    if history:
        complete = book.get("accounting_complete", True)
        st.dataframe([{"Entry": _stamp(row.get("entry_at")), "Exit": _stamp(row.get("exit_at")),
                       "Reason": row.get("reason"), "Entry price": row.get("entry_price"), "Exit price": row.get("exit_price"),
                       "Net": row.get("net") if complete and not row.get("provisional") else None}
                      for row in history], hide_index=True, width="stretch")
    else:
        st.caption("No completed paper trades.")
    from lab.ema_paper.paper_view import _delivery

    if deliveries := _delivery(book["book_id"]):
        st.subheader("Telegram delivery")
        st.dataframe(deliveries, hide_index=True, width="stretch")


@st.fragment(run_every="30s")
def panel(*, family=None):
    st.title("Prospective paper")
    st.caption(f"{MODE} · {APPROVAL} · Independent hypothetical balances")
    try:
        view = current_view(family=family)
    except (OSError, KeyError, ValueError, RuntimeError, sqlite3.Error) as exc:
        st.warning(f"Fresh paper evidence unavailable ({type(exc).__name__}).")
        return
    if not view:
        st.info("The verified cutover is pending.")
        return
    books = view.get("books", {})
    if books:
        st.dataframe([_summary(book) for book in books.values()], hide_index=True, width="stretch")
        slot = st.selectbox("Paper book", list(books), format_func=lambda key: books[key]["label"])
        _detail(books[slot])
    else:
        st.info("The legacy runs are retired. Successor account initialization is pending.")
    if archive := view.get("archive"):
        with st.expander("Archived paper history"):
            st.write("Legacy delayed-price simulations are excluded from current performance.")
            st.code(str(archive), language=None)
            st.caption(f"Cutoff {view['cutoff']} · Retirement audit {view['retirement_id']}")
