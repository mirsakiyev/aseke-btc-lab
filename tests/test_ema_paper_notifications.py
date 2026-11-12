from contextlib import closing
import json

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog, notifications, paper_view as dashboard
from lab.health import delivery, state
from lab.phase15 import paper
from lab.storage import utc


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "watchdog")
    root = tmp_path / "ema"
    now = utc("2026-11-20T12:00Z")
    lane = {"book_id": "ema-day-fixture", "profile": "day", "instrument": "XBTUSDTM", "market": "FUTURES",
            "registered_at": now.isoformat(), "risk": {"starting_equity": 10000., "leverage": 3.}}
    registration = {"day": {"id": "ema-test-registration", "body": lane}}
    with closing(catalog.db_open(root=root)) as db:
        paper.save(db, paper.initial(lane))
        db.commit()
    return root, registration, lane, now


def publish(db, lane, identifier, when, *, boundary=None, source_mode=None):
    idea = {
        "id": identifier, "direction": "LONG", "exit_policy": "source_bracket_v1",
        "strategy_version": "aseke_day_v1", "published_at": when.isoformat(),
        "boundary": (boundary or when).isoformat(),
        "plan": {"eligible_from": (when + pd.Timedelta(minutes=15)).isoformat(),
                 "entry_expiry": (when + pd.Timedelta(minutes=16)).isoformat(),
                 "max_hold_minutes": 480, "stop": 96., "target": 108.,
                 "entry": {"minimum": 96., "maximum": 101.},
                 "sizing": {"planned_risk": 25.}, "evidence": {"source_received_at": when.isoformat()}},
    }
    if source_mode:
        idea["plan"]["evidence"]["source_mode"] = source_mode
    paper.event(db, lane, "PUBLISHED", identifier, when, when, idea)
    return idea


def test_ema_lifecycle_delivery_deduplicates_on_restart(stores):
    root, registration, lane, registered_at = stores
    publication = registered_at + pd.Timedelta(minutes=1)
    modeled = publication + pd.Timedelta(minutes=15)
    received = modeled + pd.Timedelta(minutes=15)
    ended = modeled + pd.Timedelta(minutes=22)
    with closing(catalog.db_open(root=root)) as source:
        publish(source, lane, "legacy", registered_at - pd.Timedelta(minutes=1))
        publish(source, lane, "stale", publication, boundary=registered_at - pd.Timedelta(minutes=1))
        publish(source, lane, "trade", publication, source_mode="KRAKEN_PROXY_SEEDED")
        paper.event(source, lane, "HYPOTHETICAL_FILL", "trade", modeled, received,
                    {"quantity": .01, "contracts": 10., "price": 100., "stop": 96., "target": 108.,
                     "planned_risk": 25., "source_received_at": received.isoformat()})
        paper.event(source, lane, "CLOSED", "trade", ended, ended,
                    {"reason": "target", "entry_at": modeled.isoformat(), "exit_price": 108.,
                     "holding_minutes": 22, "gross_price_pnl": 8., "fees": .2, "funding": .1,
                     "liquidation_cost": 0., "unfunded_deficit": 0., "net": 7.9, "net_r": .316,
                     "equity_before": 10000., "equity_after": 10007.9, "adverse_cost_in_gross": .5,
                     "source_received_at": ended.isoformat()})
        publish(source, lane, "expired", publication)
        paper.event(source, lane, "EXPIRED", "expired", modeled, received, {"reason": "Entry minute never arrived"})
        source.commit()

    sent = []

    def send(message):
        sent.append(message)
        return delivery.Delivery("DELIVERED", "accepted by fixture", message_id=len(sent))

    for offset in (0, 3):
        check = ended + pd.Timedelta(minutes=offset)
        with closing(catalog.db_open(readonly=True, root=root)) as source, state.connect() as db:
            result = notifications.collect(db, check, state.policy(), registration, source)
            assert result["day"]["latest_rowid"] == result["day"]["cursor_rowid"]
            state.flush(db, check.timestamp(), state.policy(), send)
            assert db.execute("SELECT count(*) FROM outbox_attempt").fetchone()[0] == 5
            records = db.execute("SELECT idea_id,category,event_json FROM outbox ORDER BY rowid").fetchall()
            assert [(row["idea_id"], row["category"]) for row in records] == [
                ("trade", "ideas"), ("trade", "paper_entry"), ("trade", "paper_exit"),
                ("expired", "ideas"), ("expired", "paper_no_entry")]
            context = json.loads(records[1]["event_json"])
            assert context["source_at"] == modeled.isoformat()
            assert context["source_received_at"] == context["recorded_at"] == received.isoformat()
            assert context["source_mode"] == "KRAKEN_PROXY_SEEDED"
    assert len(sent) == 5
    assert "EMA Pullback Day" in sent[0] and "Frozen target 108.0" in sent[0]
    assert "DELAYED CONFIRMATION" in sent[1] and "10 contracts" in sent[1]
    assert "Funding +0.1000" in sent[2] and "Updated paper equity 10,007.9000" in sent[2]
    assert "Entry minute never arrived" in sent[4] and "No hypothetical position was opened." in sent[4]
    assert all("Feature mode KRAKEN_PROXY_SEEDED" in message for message in sent[:3])
    assert all("No real exchange order was placed." in message for message in sent)


def test_open_updates_are_opt_in_and_scoped(stores):
    root, registration, lane, registered_at = stores
    entered = registered_at + pd.Timedelta(minutes=15)
    check = entered + pd.Timedelta(minutes=31)
    with closing(catalog.db_open(root=root)) as source:
        book = paper.initial(lane)
        book["position"] = {
            "id": "active-idea", "direction": "LONG", "entry_at": entered.isoformat(),
            "deadline": (entered + pd.Timedelta(minutes=480)).isoformat(), "last_mark": 102., "stop": 96.,
            "mark_source_at": check.isoformat(), "mark_received_at": check.isoformat(),
            "position": {"entry": 100., "remaining": .1},
        }
        paper.save(source, book)
        cfg = state.policy()
        with state.connect() as db:
            state.enqueue(db, "paper-update:phase15", "open_position", "original", check.timestamp())
            notifications.collect(db, check, cfg, registration, source)
            assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1
            cfg["categories"]["open_position"] = True
            for _ in range(2):
                notifications.collect(db, check, cfg, registration, source)
            assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 2
            book["position"] = None
            paper.save(source, book)
            notifications.collect(db, check, cfg, registration, source)
            outcomes = {row[0]: row[1] for row in db.execute("SELECT id,status FROM outbox")}
            assert outcomes["paper-update:phase15"] == "PENDING"
            assert outcomes[next(key for key in outcomes if key.startswith(notifications.UPDATE_PREFIX))] == "SUPERSEDED"


def test_kraken_synthetic_events_advance_cursor_without_external_output(stores):
    root, registrations, lane, registered_at = stores
    publication = registered_at + pd.Timedelta(minutes=1)
    modeled = publication + pd.Timedelta(minutes=15)
    checked = modeled + pd.Timedelta(minutes=31)
    with closing(catalog.db_open(root=root)) as source:
        idea = publish(source, lane, "synthetic", publication, source_mode="KRAKEN_SPOT_REFERENCE")
        paper.event(source, lane, "HYPOTHETICAL_FILL", "synthetic", modeled, modeled,
                    {"price": 100., "stop": 96., "target": 108., "quantity": .01, "planned_risk": 25.})
        paper.event(source, lane, "CLOSED", "synthetic", modeled, modeled, {"reason": "liquidation"})
        publish(source, lane, "no-entry", publication, source_mode="KRAKEN_SPOT_REFERENCE")
        paper.event(source, lane, "EXPIRED", "no-entry", modeled, modeled, {"reason": "Entry did not qualify"})
        book = paper.initial(lane)
        book["position"] = {"id": "open-idea", "entry_at": modeled.isoformat(), "plan": idea["plan"]}
        paper.save(source, book)
        source.commit()

    cfg = state.policy()
    cfg["categories"]["open_position"] = True
    for offset in (0, 1):
        now = checked + pd.Timedelta(minutes=offset)
        with closing(catalog.db_open(readonly=True, root=root)) as source, state.connect() as db:
            if not offset:
                state.enqueue(db, "paper:phase15", "ideas", "original Phase 15", now.timestamp())
            result = notifications.collect(db, now, cfg, registrations, source)
            assert result["day"]["cursor_rowid"] == result["day"]["latest_rowid"] == 5
            assert [tuple(row) for row in db.execute("SELECT id,text,status FROM outbox")] == [
                ("paper:phase15", "original Phase 15", "PENDING")
            ]
            db.commit()


def test_matched_window_uses_eligible_idea_cohorts_and_day_session_slices():
    now = utc("2026-11-24T12:00Z")
    control, candidate = "day/fixed", "ema/day"
    books = {
        control: {"profile": "day", "first_eligible_at": "2026-11-20T00:00Z",
                  "primary_end_exclusive": "2026-11-23T00:00Z", "cutoff_minute": 1320, "flat_minute": 1435},
        candidate: {"profile": "day", "first_eligible_at": None, "primary_end_exclusive": "2026-11-24T00:00Z"},
    }
    assert dashboard.matched_comparisons(books, {control: [], candidate: []}, now)[0]["state"] == "NO_MATCHED_WINDOW"
    books[candidate]["first_eligible_at"] = "2026-11-21T00:00Z"

    def journal(idea, when, eligible):
        return [
            {"event": "PUBLISHED", "idea_id": idea, "source_at": when, "recorded_at": when,
             "plan": {"eligible_from": eligible}},
            {"event": "HYPOTHETICAL_FILL", "idea_id": idea, "source_at": eligible, "recorded_at": eligible},
            {"event": "CLOSED", "idea_id": idea, "source_at": "2026-11-23T03:00Z", "recorded_at": "2026-11-23T03:00Z",
             "entry_at": eligible, "exit_at": "2026-11-23T03:00Z", "net": 2., "net_r": 1., "holding_minutes": 120},
        ]

    past = journal("before-match", "2026-11-20T20:00Z", "2026-11-20T20:15Z")
    shared = journal("before-cutoff", "2026-11-22T20:00Z", "2026-11-22T20:15Z")
    overnight = journal("after-cutoff", "2026-11-22T22:00Z", "2026-11-22T22:15Z")
    report, = dashboard.matched_comparisons(books, {control: shared, candidate: past + shared + overnight}, now)
    assert report["state"] == "INSUFFICIENT_EVIDENCE"
    assert report["start"] == "2026-11-21T00:00:00+00:00"
    assert report["books"][candidate]["closed"] == 2
    assert report["session_slices"]["ideas_with_entry_clock_at_or_after_cutoff"]["closed"] == 1
    assert report["session_slices"]["ema_closed_after_breakout_flat"] == 2


def test_dashboard_shows_seven_independent_books(monkeypatch):
    from streamlit.testing.v1 import AppTest

    books = {}
    for key, label in dashboard.LABELS.items():
        profile = key.split("/")[1] if key.startswith("ema/") else key.split("/")[0]
        instrument = "BTC-USDT" if profile == "position" else "XBTUSDTM"
        account = paper.initial({"book_id": key, "risk": {"starting_equity": 10000.}})
        books[key] = {"profile": profile, "instrument": instrument, "registration_id": key,
                      "strategy_version": "fixture", "state": "BLOCKED_PERMISSION" if key.startswith("ema/") else "ACTIVE",
                      "account": account, "stats": dashboard._stats([]), "trades": [], "label": label}
    monkeypatch.setattr(dashboard, "report", lambda: {
        "mode": "MANUAL_OUTSIDE_LAB", "approval": "No validated model", "books": books, "comparisons": [],
    })
    monkeypatch.setattr(dashboard, "_delivery", lambda _book: [])
    app = AppTest.from_string("from lab.ema_paper.paper_view import panel\npanel()").run()
    assert not app.exception
    table = app.dataframe[0].value
    assert len(table) == 7 and table["Starting equity"].eq(10000.).all()
    assert set(table["Paper book"]) == set(dashboard.LABELS.values())
    assert table["State"].eq("BLOCKED_PERMISSION").sum() == 3


def test_dashboard_distinguishes_kraken_synthetic_economics(monkeypatch):
    from streamlit.testing.v1 import AppTest

    account = paper.initial({"book_id": "ema/day", "risk": {"starting_equity": 10000.}})
    account.update(reserved_margin=25., liquidation_cost=.2)
    protocol = {"synthetic": True, "source_mode": "KRAKEN_SPOT_REFERENCE",
                "execution_model": "SYNTHETIC_BTC_LINEAR_PERPETUAL_V1", "primary_leverage": 3,
                "counterfactual_leverage": [5], "currency": "nominal USDT",
                "warmup_count": 720, "current_indicators": {"ema50": 101, "ema200": 99, "rsi14": 52, "atr14": 2.1},
                "latest_close_at": "2026-11-24T12:00Z", "last_native_receipt_at": "2026-11-24T12:01Z",
                "execution_assumptions": {"taker_fee_bps": 10, "funding_interval_hours": 8},
                "telegram_state": "LOCAL_ONLY_KRAKEN_SCOPE"}
    day = {"profile": "day", "instrument": "BTC/USD · Kraken Spot reference", "registration_id": "ema/day",
           "currency": "nominal USDT", "strategy_version": "fixture", "state": "ACTIVE", "account": account,
           "stats": dashboard._stats([]), "trades": [], "label": dashboard.LABELS["ema/day"],
           "source_protocol": protocol}
    monkeypatch.setattr(dashboard, "report", lambda: {
        "mode": "MANUAL_OUTSIDE_LAB", "approval": "No validated model", "books": {"ema/day": day}, "comparisons": [],
    })
    monkeypatch.setattr(dashboard, "_delivery", lambda _book: [])
    monkeypatch.setattr(dashboard, "_private_summary_status", lambda: "PRIVATE_SUMMARIES_ENABLED")
    app = AppTest.from_string("from lab.ema_paper.paper_view import panel\npanel()").run()
    assert not app.exception
    assert app.dataframe[0].value.loc[0, "Instrument"] == "BTC/USD · Kraken Spot reference"
    assert app.dataframe[0].value.loc[0, "Currency"] == "nominal USDT"
    assert any("SYNTHETIC PAPER" in element.value for element in app.caption)
    assert any("Actual futures mark and funding are unavailable" in element.value for element in app.caption)
    assert any("Private simulated entry and exit summaries enabled" in element.value for element in app.caption)
    assert any("Primary leverage · 3×" in element.value for element in app.markdown)
    assert any("modeled_liquidation_cost" in element.value for element in app.json)


def test_dashboard_reads_current_synthetic_revision_for_the_original_book(stores, monkeypatch):
    from lab.ema_paper import synthetic_protocol, synthetic_runtime

    root, _, lane, registered_at = stores
    root = root.parent / "ema_paper"
    lane.update(spec={"implementation_version": "fixture-ema"}, strategy_hash="fixture-original-hash")
    identifier = fingerprint(lane)
    when = registered_at + pd.Timedelta(minutes=1)
    with closing(catalog.db_open(root=root)) as db:
        paper.save(db, paper.initial(lane))
        db.execute("INSERT INTO registration VALUES (?, ?, ?)", (lane["profile"], identifier, json.dumps(lane)))
        publish(db, lane, "committed-synthetic", when, source_mode="KRAKEN_SPOT_REFERENCE")
        db.commit()

    def reviewed_status(*, registrations, runtime_books):
        assert registrations["day"]["id"] == identifier
        assert runtime_books["day"]["instrument"] == "XBTUSDTM"
        return {"day": {"synthetic": True, "state": "WAITING_FOR_DATA", "source_mode": "KRAKEN_SPOT_REFERENCE"}}

    monkeypatch.setattr(dashboard, "RUNS", root.parent)
    monkeypatch.setattr(synthetic_protocol, "active", lambda: True)
    monkeypatch.setattr(synthetic_runtime, "protocol_status", reviewed_status)
    books, journals = dashboard._ema_books()
    book = books["ema/day"]
    assert book["account"]["book_id"] == lane["book_id"] and book["registration_id"] == identifier
    assert book["instrument"] == "BTC/USD · Kraken Spot reference" and book["currency"] == "nominal USDT"
    assert book["source_mode_outcomes"]["KRAKEN_SPOT_REFERENCE"]["published"] == 1
    assert journals["ema/day"][0]["source_mode"] == "KRAKEN_SPOT_REFERENCE"
