"""Read-only inventory and comparisons for prospective BTC paper books."""

from contextlib import closing
import json
from statistics import mean
import sqlite3

import pandas as pd
import streamlit as st

from lab.config import fingerprint
from lab.health.dashboard_service import ROOT as WATCHDOG
from lab.health.state import local
from lab.phase15 import catalog as breakout
from lab.phase15.paper import book_report
from lab.storage import RUNS, utc, utc_now


PROFILES = ("position", "swing", "day")
LABELS = {
    "position/fixed": "Breakout Position Fixed", "swing/fixed": "Breakout Swing Fixed",
    "swing/trail": "Breakout Swing Trailing", "day/fixed": "Breakout Day Fixed",
    "ema/position": "EMA Pullback Position Daily", "ema/swing": "EMA Pullback Swing 4H",
    "ema/day": "EMA Pullback Day 15m · 24/7",
}
TRADE_EVENTS = ("PUBLISHED", "HYPOTHETICAL_FILL", "EXPIRED", "INVALIDATED", "CLOSED")
MIN_CLOSED = 25
KRAKEN_REFERENCE = "KRAKEN_SPOT_REFERENCE"


def _journal(db, book):
    rows = [
        {"event": kind, "idea_id": idea, "source_at": occurred, "recorded_at": recorded, **json.loads(body)}
        for kind, idea, occurred, recorded, body in db.execute(
            "SELECT kind,idea,source_at,recorded_at,body FROM event WHERE book=? "
            "AND kind IN (?,?,?,?,?) ORDER BY rowid", (book, *TRADE_EVENTS))
    ]
    modes = {
        row["idea_id"]: row.get("plan", {}).get("evidence", {}).get("source_mode")
        for row in rows if row["event"] == "PUBLISHED"
    }
    for row in rows:
        if modes.get(row["idea_id"]) in {"KRAKEN_PROXY_SEEDED", "KUCOIN_NATIVE_READY", KRAKEN_REFERENCE}:
            row["source_mode"] = modes[row["idea_id"]]
    return rows


def _stats(events):
    completed = [row for row in events if row["event"] == "CLOSED"]
    winners = [row["net"] for row in completed if row["net"] > 0]
    losers = [row["net"] for row in completed if row["net"] < 0]
    counts = {kind: sum(row["event"] == kind for row in events) for kind in TRADE_EVENTS}
    return {
        "published": counts["PUBLISHED"], "entries": counts["HYPOTHETICAL_FILL"],
        "no_entry": counts["EXPIRED"] + counts["INVALIDATED"], "closed": len(completed),
        "wins": len(winners), "losses": len(losers),
        "win_rate": len(winners) / len(completed) if completed else None,
        "average_winner": mean(winners) if winners else None, "average_loser": mean(losers) if losers else None,
        "net": sum(row["net"] for row in completed),
        "expectancy": mean(row["net"] for row in completed) if completed else None,
        "expectancy_r": mean(row["net_r"] for row in completed) if completed else None,
        "profit_factor": sum(winners) / -sum(losers) if losers else None,
        "fees": sum(row.get("fees", 0.) for row in completed),
        "funding": sum(row.get("funding", 0.) for row in completed),
        "adverse_cost": sum(row.get("adverse_cost_in_gross", 0.) for row in completed),
        "average_hold_minutes": mean(row["holding_minutes"] for row in completed) if completed else None,
        "evidence": "INSUFFICIENT_EVIDENCE" if len(completed) < MIN_CLOSED else "DESCRIPTIVE_ONLY",
    }


def _overlap(left, right, now):
    if not all(book.get("first_eligible_at") and book.get("primary_end_exclusive") for book in (left, right)):
        return None
    start = max(utc(left["first_eligible_at"]), utc(right["first_eligible_at"]))
    end = min(utc(left["primary_end_exclusive"]), utc(right["primary_end_exclusive"]), now)
    return (start, end) if start < end else None


def _cohort(events, start, end, now):
    ideas = {
        row["idea_id"] for row in events
        if row["event"] == "PUBLISHED" and start <= utc(row.get("boundary") or row["source_at"]) < end
    }
    return [row for row in events if row["idea_id"] in ideas and utc(row["recorded_at"]) <= now]


def _minute(at):
    stamp = utc(at)
    return 60 * stamp.hour + stamp.minute


def _day_slices(events, cutoff, flat):
    outside_ideas = {
        row["idea_id"] for row in events
        if row["event"] == "PUBLISHED" and _minute(row.get("plan", {}).get("eligible_from") or row["source_at"]) >= cutoff
    }
    outside_ideas.update(
        row["idea_id"] for row in events
        if row["event"] == "HYPOTHETICAL_FILL" and _minute(row["source_at"]) >= cutoff
    )
    beyond_flat = sum(
        utc(row["exit_at"]) > utc(row["entry_at"]).normalize() + pd.Timedelta(minutes=flat)
        for row in events if row["event"] == "CLOSED"
    )
    return {
        "ideas_with_entry_clock_at_or_after_cutoff": _stats([row for row in events if row["idea_id"] in outside_ideas]),
        "ideas_with_entry_clock_before_cutoff": _stats([row for row in events if row["idea_id"] not in outside_ideas]),
        "ema_closed_after_breakout_flat": beyond_flat,
        "breakout_entry_cutoff_utc": f"{cutoff // 60:02}:{cutoff % 60:02}",
        "breakout_flat_utc": f"{flat // 60:02}:{flat % 60:02}",
    }


def matched_comparisons(books, journals, now):
    rows = []
    for profile in PROFILES:
        candidate = "ema/" + profile
        for control, old in books.items():
            if control.startswith("ema/") or old["profile"] != profile or candidate not in books:
                continue
            item = {"profile": profile, "control": control, "candidate": candidate}
            pinned = books[candidate].get("registration", {}).get("body", {})
            if pinned.get("benchmark_registration") and (
                    old.get("registration_id") != pinned["benchmark_registration"]
                    or old.get("account", {}).get("book_id") != pinned["benchmark_books"].get(control)):
                rows.append({**item, "state": "BENCHMARK_IDENTITY_MISMATCH"})
                continue
            window = _overlap(old, books[candidate], now)
            if window is None:
                rows.append({**item, "state": "NO_MATCHED_WINDOW"})
                continue
            start, end = window
            controls = _cohort(journals[control], start, end, now)
            candidates = _cohort(journals[candidate], start, end, now)
            stats = {control: _stats(controls), candidate: _stats(candidates)}
            enough = all(values["closed"] >= MIN_CLOSED for values in stats.values())
            item.update(start=start.isoformat(), end_exclusive=end.isoformat(), books=stats,
                        state="DESCRIPTIVE_ONLY" if enough else "INSUFFICIENT_EVIDENCE")
            if profile == "day" and old.get("cutoff_minute") is not None and old.get("flat_minute") is not None:
                item["session_slices"] = _day_slices(candidates, old["cutoff_minute"], old["flat_minute"])
            rows.append(item)
    return rows


def _legacy_books(now):
    record = breakout.registered()
    if record is None:
        return {}, {}
    body = record["body"]
    settings = breakout.controls()
    books, journals = {}, {}
    with closing(breakout.db_open(readonly=True)) as db:
        db.execute("BEGIN")
        for key, lane in body["lanes"].items():
            account = book_report(db, lane)
            state = (
                "UNRESOLVED" if account["problem"] else
                "PAUSED" if not settings["paper"] or not settings["publication"] else
                "WAITING_FOR_START" if now < utc(body["start"]) else
                "ACTIVE" if now < utc(body["publication_end_exclusive"]) else
                "FOLLOW_THROUGH" if now < utc(body["follow_end_exclusive"]) else "PRIMARY_COMPLETE"
            )
            session = lane["profile_rules"]
            journals[key] = _journal(db, lane["book_id"])
            books[key] = {
                "profile": lane["profile"], "instrument": lane["mode"]["symbol"], "registration_id": record["id"],
                "strategy_version": lane["strategy_version"], "state": state, "account": account,
                "first_eligible_at": body["start"], "primary_end_exclusive": body["publication_end_exclusive"],
                "follow_end_exclusive": body["follow_end_exclusive"],
                "cutoff_minute": session.get("entry_cutoff_minute_utc"), "flat_minute": session.get("flat_minute_utc"),
            }
    return books, journals


def _ema_books():
    from lab.ema_paper import position_seam, synthetic_protocol
    from lab.ema_paper.ledger import report as account_report
    from lab.ema_paper.dispatcher import protocol_status

    books, journals = {}, {}
    path = RUNS / "ema_paper" / "paper.sqlite3"
    if not path.is_file():
        return books, journals
    # The dashboard may outlive an evaluator deployment. Read the sealed book
    # and readiness records directly without importing the execution runtime.
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        db.execute("BEGIN")
        for profile, identifier, payload in db.execute("SELECT profile,id,body FROM registration ORDER BY profile"):
            lane = json.loads(payload)
            if fingerprint(lane) != identifier or lane["profile"] != profile:
                raise ValueError("EMA prospective registration checksum mismatch")
            readiness = db.execute("SELECT body FROM readiness WHERE book=?", (lane["book_id"],)).fetchone()
            view = json.loads(readiness[0]) if readiness else {}
            key = "ema/" + profile
            marks = db.execute(
                "SELECT source_at,recorded_at,equity,exposure FROM equity_sample WHERE book=? "
                "ORDER BY source_at DESC LIMIT 3000", (lane["book_id"],)
            ).fetchall()
            account = account_report(db, lane)
            if marks:
                points = {utc(point["source_at"]): point for point in account.get("equity_curve", [])}
                points.update({
                    utc(at): {"source_at": at, "recorded_at": saved, "equity": equity, "exposure": exposure}
                    for at, saved, equity, exposure in marks
                })
                account["equity_curve"] = [points[at] for at in sorted(points)[-3000:]]
            books[key] = {
                **view, "profile": profile, "instrument": lane["instrument"], "registration_id": identifier,
                "registration": {"id": identifier, "body": lane},
                "strategy_version": lane["spec"]["implementation_version"], "strategy_hash": lane["strategy_hash"],
                "account": account,
            }
            journals[key] = _journal(db, lane["book_id"])
    try:
        registered = {book["profile"]: book["registration"] for book in books.values()}
        runtime = {book["profile"]: book for book in books.values()}
        read_status = position_seam.protocol_status if synthetic_protocol.active() else protocol_status
        source_protocols = read_status(registrations=registered, runtime_books=runtime)
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        source_protocols = {}
        for book in books.values():
            book["source_protocol_error"] = str(exc)
    for key, book in books.items():
        protocol = source_protocols.get(book["profile"], {})
        synthetic = protocol.get("synthetic") is True
        if synthetic or protocol.get("amendment_id"):
            book["source_protocol"] = protocol
            book["state"], book["reason"] = protocol["state"], protocol.get("reason") or protocol.get("source_permission")
            book["first_eligible_at"] = protocol.get("first_eligible_at")
            book["next_evaluation_at"] = protocol.get("next_evaluation_at")
            if synthetic:
                book["instrument"] = "BTC/USD · Kraken Spot reference"
                book["currency"] = "nominal USDT"
            modes = (KRAKEN_REFERENCE,) if synthetic else ("KRAKEN_PROXY_SEEDED", "KUCOIN_NATIVE_READY")
            book["source_mode_outcomes"] = {
                mode: _stats([row for row in journals[key] if row.get("source_mode") == mode])
                for mode in modes
            }
    return books, journals


def report(*, now=None):
    """Read the seven-account inventory without updating any ledger."""
    now = utc_now() if now is None else utc(now)
    books, journals = _legacy_books(now)
    current, new_journals = _ema_books()
    books.update(current)
    journals.update(new_journals)
    comparisons = matched_comparisons(books, journals, now)
    for key, book in books.items():
        book["stats"] = _stats(journals[key])
        book["label"] = LABELS[key]
        book["trades"] = [row for row in journals[key] if row["event"] == "CLOSED"][-40:][::-1]
    return {"at": now.isoformat(), "books": books, "comparisons": comparisons,
            "minimum_closed": MIN_CLOSED, "mode": "MANUAL_OUTSIDE_LAB", "approval": "No validated model"}


def _table_row(book):
    account, stats = book["account"], book["stats"]
    return {
        "Paper book": book["label"], "Instrument": book["instrument"], "State": book["state"],
        "Starting equity": account["starting_equity"], "Equity": account["equity"],
        "Published": stats["published"], "Entries": stats["entries"], "Closed": stats["closed"],
        "Realized net": stats["net"], "Expectancy R": stats["expectancy_r"], "Evidence": stats["evidence"],
    }


def _synthetic_detail(protocol):
    model = protocol["execution_model"]
    assumptions = protocol.get("execution_assumptions")
    if isinstance(model, dict):
        assumptions = assumptions or model
        model = model["version"]
    st.caption(f"SYNTHETIC PAPER · {model} · {protocol['source_mode']}")
    st.write("Price reference · Kraken BTC/USD Spot")
    leverage = protocol.get("primary_leverage")
    if leverage is not None:
        st.write(f"Primary leverage · {leverage:g}×")
    if counterfactual := protocol.get("counterfactual_leverage"):
        st.write("Isolated leverage sensitivity", counterfactual)
    st.write("Account unit · nominal USDT")
    st.caption("Modeled fees, financing and liquidation use the frozen simulation assumptions. "
               "Actual futures mark and funding are unavailable. USD/USDT parity is a simulation convention.")
    st.write(f"Kraken warmup · {protocol.get('warmup_count') or 0} completed candles")
    st.json({key: protocol[key] for key in (
        "amendment_id", "execution_model_id", "currency", "warmup_seed_id", "latest_close_at",
        "last_native_receipt_at", "last_execution_receipt_at", "readiness", "current_indicators",
        "native_volume", "native_swing", "first_eligible_at", "next_evaluation_at",
    ) if key in protocol}, expanded=False)
    if not protocol.get("current_indicators") and protocol.get("prior_archive_indicators"):
        st.write(f"Prior archived features · {protocol['prior_archive_count']} candles")
        st.json(protocol["prior_archive_indicators"], expanded=False)
    if assumptions:
        st.write("Frozen execution assumptions")
        st.json(assumptions, expanded=False)
    if risk := protocol.get("counterfactual_risk"):
        st.write("Counterfactual risk on the latest signal")
        st.json(risk, expanded=False)
    if stress := protocol.get("stress_risk"):
        st.write("Fee, financing and currency stress on the latest signal")
        st.json(stress, expanded=False)


def _delivery(book_id):
    path = WATCHDOG / "watchdog.sqlite3"
    if not path.is_file():
        return []
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
        rows = db.execute(
            "SELECT o.category,o.status,o.created,a.request_started,a.api_accepted_at,a.telegram_date,o.detail "
            "FROM outbox o LEFT JOIN outbox_attempt a ON a.id=o.id AND a.number=o.attempts "
            "WHERE json_extract(o.event_json,'$.book_id')=? ORDER BY o.rowid DESC LIMIT 30", (book_id,)
        )
        def clock(value):
            return local(float(value)) if value is not None else "—"
        return [{"Event": category, "State": state, "Queued": clock(created), "Request": clock(request),
                 "API accepted": clock(accepted), "Telegram time": clock(telegram), "Detail": detail}
                for category, state, created, request, accepted, telegram, detail in rows]


def _private_summary_status():
    path = WATCHDOG / "last.json"
    if not path.is_file():
        return None
    try:
        health = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return health.get("ema_private_route") or health.get("ema_private_notifications", {}).get("state")


def _detail(book, *, currency="USDT"):
    account = book["account"]
    currency = book.get("currency", currency)
    st.subheader(book["label"])
    st.caption(f"{book['strategy_version']} · {book['state']} · {book['registration_id']}")
    if book.get("strategy_hash"):
        st.code(book["strategy_hash"], language=None)
    if book.get("reason"):
        st.write(book["reason"])
    protocol = book.get("source_protocol") or {}
    synthetic = protocol.get("synthetic") is True
    if synthetic:
        _synthetic_detail(protocol)
    elif protocol:
        st.caption(protocol["strategy_id"])
        st.caption(f"{protocol['protocol']} · {protocol['feature_mode']} · {protocol['source_mode']}")
        st.write(f"Warmup · {protocol['warmup_provider']} {protocol['warmup_instrument']} · {protocol['warmup_count']} candles")
        st.write(f"Forward · {protocol['forward_provider']} {protocol['forward_instrument']}")
        st.write(protocol["transition_warning"])
        st.write(protocol["source_permission"])
        st.write(protocol["execution_permission"])
        st.json({key: protocol[key] for key in (
            "amendment_id", "warmup_seed_id", "archive_snapshot_id", "last_warmup_receipt_at", "last_native_receipt_at",
            "native_volume", "native_swing", "price_basis", "transition_at", "first_eligible_at", "readiness",
        )}, expanded=False)
        if indicators := protocol.get("current_indicators") or protocol.get("seed_indicators"):
            st.json(indicators, expanded=False)
        st.json(book["source_mode_outcomes"], expanded=False)
    elif book.get("source_protocol_error"):
        st.warning(book["source_protocol_error"])
    elif book.get("warmup") is not None:
        st.write("Warmup", book["warmup"])
    st.json({key: book.get(key) for key in (
        "source_ready_at", "first_eligible_at", "primary_end_exclusive", "follow_end_exclusive",
        "last_evaluated_at", "next_evaluation_at", "coverage", "last_signal")}, expanded=False)
    marked = account.get("equity_curve")
    if marked and len(marked) > 1:
        frame = pd.DataFrame(marked)
        if {"source_at", "equity"} <= set(frame):
            frame["source_at"] = pd.to_datetime(frame["source_at"], utc=True).dt.tz_convert("America/Toronto")
            st.line_chart(frame.set_index("source_at")["equity"], y_label=f"Paper equity ({currency})")
    if account.get("problem"):
        st.warning(account["problem"])
    details = {**book["stats"], "cash": account["cash"], "available_cash": account.get("available_cash"),
               "margin": account.get("reserved_margin"), "reserved_risk": account.get("reserved_risk"),
               "unrealized": account.get("unrealized"), "equity": account["equity"],
               "return": account["equity"] / account["starting_equity"] - 1, "drawdown": account["drawdown"],
               "turnover": account["turnover"], "exposure": account.get("exposure"),
               "open_positions": int(bool(account.get("position"))),
               "maximum_adverse_excursion": account.get("maximum_adverse_excursion"),
               "maximum_favorable_excursion": account.get("maximum_favorable_excursion"),
               "occupied_complete_minutes": account.get("occupied_complete_minutes"),
               "total_fees": account["fees"], "total_funding": account["funding"],
               "problem": account.get("problem"), "reasons": account.get("reasons", {}),
               "pending": account.get("pending"), "position": account.get("position")}
    columns = ("idea_id", "entry_at", "exit_at", "exit_kind", "net", "net_r", "fees", "funding", "holding_minutes")
    if book.get("source_protocol"):
        columns += ("source_mode",)
    if synthetic:
        details["account_unit"] = currency
        details["margin_usage"] = details.pop("margin")
        details["modeled_closed_fees"] = details.pop("fees")
        details["modeled_closed_funding"] = details.pop("funding")
        details["modeled_fees"] = details.pop("total_fees")
        details["modeled_funding"] = details.pop("total_funding")
        details["modeled_liquidation_cost"] = account.get("liquidation_cost")
        if margin := (account.get("position") or {}).get("synthetic_margin"):
            details["current_modeled_margin"] = margin
        columns += ("liquidation_cost",)
    if book.get("venue") == "Kraken Spot":
        for field in ("margin", "funding", "total_funding"):
            del details[field]
        columns = tuple(field for field in columns if field != "funding")
    st.json(details, expanded=False)
    if book["trades"]:
        labels = {"fees": "Modeled fees", "funding": "Modeled funding", "liquidation_cost": "Modeled liquidation cost"}
        st.dataframe([{labels.get(key, key) if synthetic else key: row.get(key) for key in columns}
                      for row in book["trades"]], hide_index=True, width="stretch")
    else:
        st.caption("No closed trades.")
    if synthetic:
        st.subheader("Local synthetic lifecycle")
        events = [row for row in account.get("last_events", ()) if row["type"] in TRADE_EVENTS]
        if events:
            st.dataframe([{"Event": row["type"], "Idea": row["idea_id"], "Modeled at": row["source_at"],
                           "Recorded at": row["recorded_at"], "Reason": row.get("reason"), "Net": row.get("net")}
                          for row in events], hide_index=True, width="stretch")
        else:
            st.caption("No committed lifecycle events.")
    st.subheader("Telegram delivery history")
    if synthetic:
        route_status = _private_summary_status()
        if route_status == "PRIVATE_SUMMARIES_ENABLED":
            st.caption("Private simulated entry and exit summaries enabled. Individual trade prices and raw Kraken observations stay local.")
        elif route_status:
            st.caption(f"Private summary status · {route_status}")
        else:
            st.caption("Synthetic lifecycle events are available in the local journal. Private summaries have no watchdog receipt yet.")
    if alerts := _delivery(account["book_id"]):
        st.dataframe(alerts, hide_index=True, width="stretch")
    else:
        st.caption("No delivery records for this book.")


@st.fragment(run_every="30s")
def panel(*, additional_report=None, additional_detail=None):
    data = report()
    extra = additional_report() if additional_report is not None else {}
    books = {**data["books"], **extra}
    st.title("Prospective paper")
    st.caption(f"{data['mode']} · {data['approval']} · Independent hypothetical balances")
    if not books:
        st.info("No prospective paper registrations.")
        return
    rows = []
    show_currency = bool(extra or any(book.get("currency") for book in books.values()))
    for profile in PROFILES:
        for book in books.values():
            if book["profile"] != profile:
                continue
            row = _table_row(book)
            if show_currency:
                row["Currency"] = book.get("currency", "USDT")
            if (protocol := book.get("source_protocol")) and protocol.get("synthetic"):
                row["Execution"] = "Synthetic paper"
                row["Leverage"] = protocol.get("primary_leverage")
            rows.append(row)
    st.dataframe(rows, hide_index=True, width="stretch")
    selected = st.selectbox("Paper book", list(books), format_func=lambda key: books[key]["label"])
    render = additional_detail if selected in extra and additional_detail is not None else _detail
    render(books[selected])
    st.subheader("Matched observation windows")
    st.caption("Cohorts use decisions in the overlapping registered windows and include completed follow-through. "
               "Signal, source availability, risk, and session policies may differ. The assessment floor is 25 closed outcomes per book.")
    for row in data["comparisons"]:
        title = f"{LABELS[row['candidate']]} / {LABELS[row['control']]} · {row['state']}"
        with st.expander(title):
            if books[row["candidate"]].get("source_protocol", {}).get("synthetic"):
                st.caption("The EMA outcome uses a synthetic Kraken-reference execution model. "
                           "The Breakout outcome retains its separate registered execution model.")
            if row["state"] == "NO_MATCHED_WINDOW":
                st.info("No overlapping eligible observation window is recorded.")
                continue
            st.write(f"UTC {row['start']} to {row['end_exclusive']} (exclusive)")
            st.dataframe([{"Paper book": LABELS[key], **values} for key, values in row["books"].items()],
                         hide_index=True, width="stretch")
            if slices := row.get("session_slices"):
                st.caption("Day EMA trades 24/7. Breakout limits entry and flattens daily. "
                           "These session slices are descriptive and do not isolate a causal session effect.")
                st.json(slices, expanded=False)
