"""Present active paper books and preserve Kraken retirement audit access."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3

import streamlit as st

from lab.ema_paper import paper_view
from lab.ema_paper.ledger import report as account_report


LABELS = {"day": "Kraken Spot EMA Day 15m · 24/7", "swing": "Kraken Spot EMA Swing 4H",
          "position": "Kraken Spot EMA Position Daily"}


def _body(db, table, book):
    row = db.execute(f"SELECT body FROM {table} WHERE book=?", (book,)).fetchone()
    return json.loads(row[0]) if row else {}


def report(*, root=None):
    """Read registrations, readiness and account evidence from one database snapshot."""
    from lab.kraken_paper import catalog, retirement

    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        return {}
    result = {}
    with closing(catalog.db_open(readonly=True, root=root)) as db:
        db.execute("BEGIN")
        if retirement.audit(db):
            return result
        for profile, registration in catalog.registrations(db).items():
            lane = registration["body"]
            book = lane["book_id"]
            readiness = _body(db, "readiness", book)
            eligibility = _body(db, "eligibility", book)
            events = paper_view._journal(db, book)
            result["kraken/" + profile] = {
                "state": "WAITING_FOR_DATA", **readiness, **eligibility, "profile": profile,
                "instrument": "BTC/USD", "currency": "USD", "venue": "Kraken Spot",
                "label": LABELS[profile], "registration_id": registration["id"], "registration": registration,
                "strategy_version": lane["strategy_version"], "strategy_hash": lane["strategy_hash"],
                "account": account_report(db, lane), "stats": paper_view._stats(events),
                "trades": [row for row in events if row["event"] == "CLOSED"][-40:][::-1],
            }
    return result


def detail(book):
    coverage = book.get("coverage") or {}
    signal = book.get("last_signal") or {}
    values = signal.get("values") or book.get("warmup_values") or {}
    lane = book["registration"]["body"]
    permission = book.get("source_permission") or (
        "BLOCKED_PERMISSION" if book["state"] == "BLOCKED_PERMISSION" else
        "Recorded" if lane.get("authorization") or lane.get("source", {}).get("source_id") or coverage.get("source_id")
        else "Pending verification"
    )
    st.subheader("Kraken Spot source and indicator evidence")
    st.json({
        "source_permission": permission,
        "warmup_snapshot_id": book.get("warmup_snapshot_id"),
        "seed_content_id": book.get("seed_content_id"),
        "latest_complete_candle": coverage.get("latest_close_at") or book.get("seed_end"),
        "latest_provider_receipt": coverage.get("latest_provider_received_at") or book.get("warmup_last_receipt_at"),
        "latest_local_handoff": coverage.get("latest_handoff_at"),
        "next_decision": book.get("next_evaluation_at"),
        "ema50": values.get("ema_fast"), "ema200": values.get("ema_slow"),
        "rsi14": values.get("rsi"), "atr14": values.get("atr"),
        "last_signal": signal.get("decision"),
        "signal_gates": signal.get("gates"), "cost": lane.get("cost"),
        "telegram_delivery": ("Enabled in registration" if lane.get("permissions", {}).get("telegram_lifecycle")
                              else "Local only under registered data-use scope"),
    }, expanded=False)
    paper_view._detail(book, currency="USD")
    st.caption("Kraken Spot results use USD cash, trade-price execution, and a separate observation window. "
               "Matched Kraken Spot comparisons require an eligible Kraken Spot benchmark.")


def _safe_report():
    try:
        return report()
    except (OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as exc:
        st.warning(f"Kraken Spot paper evidence is unavailable ({type(exc).__name__}).")
        return {}


def panel():
    paper_view.panel(additional_report=_safe_report, additional_detail=detail)
    from lab.kraken_paper import retirement

    try:
        retired = retirement.history()
    except (OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as exc:
        st.warning(f"Kraken paper retirement audit is unavailable ({type(exc).__name__}).")
        return
    if retired:
        with st.expander("Retired Kraken Spot paper accounts · Audit history"):
            st.table([{"Profile": row["profile"], "Retired at (UTC)": row["retired_at"],
                       "Book ID": row["book_id"], "Registration ID": row["registration_id"],
                       "Ledger events": row["event_count"]} for row in retired.values()])
