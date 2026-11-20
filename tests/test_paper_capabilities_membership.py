from contextlib import closing
from copy import deepcopy
import json
import sqlite3

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.paper_run import capabilities as caps, membership, source_amendment
from lab.phase15 import paper
from lab.storage import utc


AT = utc("2026-11-18T23:00:00Z")
GENERATION = "registered-generation"


def lane(slot, source, *, trailing=False, cash=False):
    ema = slot.startswith("ema/")
    model = ("REALTIME_CASH_SPOT_REFERENCE_PAPER" if cash else "REALTIME_SYNTHETIC_SPOT_REFERENCE_PAPER") if ema else (
        "REALTIME_SPOT_TICKER_REFERENCE_PAPER" if cash else "REALTIME_MARK_REFERENCE_PAPER")
    return {"book_id": fingerprint(slot), "label": slot, "family": "EMA" if ema else "BREAKOUT",
            "strategy_version": "frozen-v1", "source": source,
            "symbol": "BTC/USD" if ema else "BTC-USDT" if cash else "XBTUSDTM",
            "price_basis": source, "observation_kind": "WEBSOCKET_TRADE" if ema else "HTTP_SNAPSHOT",
            "entry_model": model, "exit_model": model, "market": "SPOT" if cash else "FUTURES",
            "timeframe": "15min", "risk": {"starting_equity": 10000.}, "qualification": {"state": "QUALIFIED"},
            "exit": "atr_trail_v1" if trailing else "signal_atr_bracket_v1" if ema else "fixed_r_v1",
            "publication_end_exclusive": (AT + pd.Timedelta(days=30)).isoformat(),
            "follow_end_exclusive": (AT + pd.Timedelta(days=60)).isoformat()}


def seven():
    return {
        "breakout/day/fixed": lane("breakout/day/fixed", caps.MARK),
        "breakout/swing/fixed": lane("breakout/swing/fixed", caps.MARK),
        "breakout/swing/trail": lane("breakout/swing/trail", caps.MARK, trailing=True),
        "breakout/position/fixed": lane("breakout/position/fixed", caps.SPOT, cash=True),
        "ema/day": lane("ema/day", caps.KRAKEN),
        "ema/swing": lane("ema/swing", caps.KRAKEN),
        "ema/position": lane("ema/position", caps.KRAKEN, cash=True),
    }


@pytest.fixture
def registered(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT);"
                     "CREATE TABLE book(id TEXT PRIMARY KEY,state TEXT);")
    record = {"id": GENERATION, "body": {"start": (AT-pd.Timedelta(days=1)).isoformat(), "lanes": seven()}}
    monkeypatch.setattr(source_amendment, "active", lambda _: None)
    monkeypatch.setattr(membership, "_inherit_source_restriction", lambda *_: None)
    with db:
        for item in record["body"]["lanes"].values():
            paper.save(db, paper.initial(item))
    yield db, record
    db.close()


def eighth(record):
    value = deepcopy(record["body"]["lanes"]["ema/day"])
    slot = "orbital/reversion"
    value.update(book_id=fingerprint(slot), family="ORBITAL_REVERSION", strategy="orbital-rule-v1", label="Orbital research")
    value["capabilities"] = caps.declare(value, signal_adapter="ema_chart_v1", entry_adapter="ema_reference_v1",
                                          generation=record["id"], slot=slot)
    return {"generation": record["id"], "slot": slot, "lane": value,
            "request_id": "isolated-eighth-only", "reason": "Explicit isolated future membership test"}


def apply(db, record, request):
    with db:
        db.execute("BEGIN IMMEDIATE")
        return membership.register(db, record, request, at=AT, operator="isolated-test", approved=True)


def test_all_seven_inherit_source_correct_complete_contracts_without_mutation():
    lanes = seven()
    original = deepcopy(lanes)
    contracts = {slot: caps.contract(value, generation=GENERATION, slot=slot) for slot, value in lanes.items()}
    assert lanes == original
    assert len(contracts) == 7
    assert len({caps.source_scope_id(value) for value in lanes.values()}) == 3
    for slot, contract in contracts.items():
        assert contract["book_id"] == lanes[slot]["book_id"]
        assert contract["generation"] == GENERATION
        assert contract["protection_data"] == {"source": lanes[slot]["source"], "freshness_seconds": 2,
                                               "clock_target_ms": 250, "monotonic_tolerance_ms": 100}
        assert contract["paper_only"] and contract["execution_mode"] == "MANUAL_OUTSIDE_LAB"
        assert contract["approval"] == "No validated model"
        assert not contract["recovery"]["automatic_resume_enabled"]
        assert all(k in contract["source"] for k in ("venue", "instrument", "product", "price_basis", "transport", "subscription", "version", "cadence", "continuity"))
    assert contracts["breakout/swing/trail"]["exit"]["target_optional"]
    assert contracts["breakout/swing/trail"]["adapters"]["trail"] == "breakout_atr_v1"
    for slot in ("breakout/position/fixed", "ema/position"):
        assert contracts[slot]["funding"] == "NONE"
        assert not contracts[slot]["margin_required"]
        assert not contracts[slot]["exit"]["liquidation_required"]
    assert contracts["breakout/day/fixed"]["funding"] == "KUCOIN_SETTLED"
    assert contracts["ema/day"]["funding"] == "SYNTHETIC_DEBIT"
    assert contracts["breakout/day/fixed"]["adapters"]["evidence"] == "explicit_insufficiency_v1"
    assert contracts["ema/day"]["adapters"]["evidence"] == "kraken_native_trade_bounds_v1"


def test_source_identity_uses_actual_reference_not_the_modeled_product():
    lanes = seven()
    assert caps.source_scope_id(lanes["ema/day"]) == caps.source_scope_id(lanes["ema/position"])
    assert caps.source_identity(lanes["ema/day"])["product"] == "SPOT"
    assert caps.contract(lanes["ema/day"])["model_product"] == "SYNTHETIC_LINEAR_PERP"
    assert caps.source_scope_id(lanes["breakout/day/fixed"]) != caps.source_scope_id(lanes["breakout/position/fixed"])
    assert caps.source_identity(lanes["breakout/day/fixed"])["cadence"]["minimum_request_interval_seconds"] == 15
    assert caps.source_identity(lanes["breakout/position/fixed"])["subscription"]["endpoint"].endswith('/api/ua/v2/market/ticker')


def test_non_ema_non_breakout_eighth_is_discovered_from_registered_events(registered):
    db, record = registered
    original = deepcopy(record)
    before = membership.lanes(record, db=db)
    request = eighth(record)
    result = apply(db, record, request)
    assert result["state"] == "APPLIED"
    after = membership.lanes(record, db=db)
    assert len(before) == 7 and len(after) == 8
    assert after[request["slot"]]["family"] not in ("EMA", "BREAKOUT")
    assert caps.adapter(after[request["slot"]], "signal") == "ema_chart_v1"
    assert caps.adapter(after[request["slot"]], "recovery") == "operator_incident_v1"
    assert caps.adapter(after[request["slot"]], "notification") == "health_outbox_v1"
    assert record == original and all(after[slot] == lane for slot, lane in before.items())
    assert membership.validate_accounts(db, record) == after
    assert len(membership.expected_book_ids(record, db=db)) == 8


@pytest.mark.parametrize("mutation,code", [
    (lambda x: x['lane']['capabilities']['adapters'].update(signal='unreviewed-alpha'), 'UNSUPPORTED_SIGNAL_ADAPTER'),
    (lambda x: x['lane']['capabilities']['adapters'].update(entry='unreviewed-sizing'), 'UNSUPPORTED_ENTRY_ADAPTER'),
    (lambda x: x['lane']['capabilities']['protection_data'].update(freshness_seconds=3), 'CAPABILITY_DECLARATION_MISMATCH'),
    (lambda x: x['lane']['capabilities']['recovery'].update(automatic_resume_enabled=True), 'CAPABILITY_DECLARATION_MISMATCH'),
    (lambda x: x['lane'].update(paper_only=False), 'PAPER_ONLY_REQUIRED'),
    (lambda x: x['lane'].update(execution_mode='LIVE'), 'MANUAL_OUTSIDE_LAB_REQUIRED'),
    (lambda x: x['lane'].update(approval='Validated model'), 'UNVALIDATED_MODEL_APPROVAL_REQUIRED'),
])
def test_registration_rejects_unsupported_capabilities_without_new_account(registered, mutation, code):
    db, record = registered
    request = eighth(record)
    mutation(request)
    with pytest.raises(ValueError, match=code):
        apply(db, record, request)
    assert len(membership.lanes(record, db=db)) == 7
    assert db.execute('SELECT count(*) FROM book').fetchone()[0] == 7
    assert db.execute('SELECT count(*) FROM event').fetchone()[0] == 0


def test_future_unknown_family_requires_explicit_declaration(registered):
    _, record = registered
    value = eighth(record)['lane']
    value.pop('capabilities')
    with pytest.raises(ValueError, match='EXPLICIT_CAPABILITIES_REQUIRED'):
        caps.contract(value)


def test_duplicate_requests_are_idempotent_and_changed_identity_is_rejected(registered):
    db, record = registered
    request = eighth(record)
    first = apply(db, record, request)
    second = apply(db, record, request)
    assert second['state'] == 'ALREADY_APPLIED' and second['event_id'] == first['event_id']
    assert db.execute('SELECT count(*) FROM event WHERE kind=?', (membership.KIND,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM event WHERE kind='OPENING_BALANCE'").fetchone()[0] == 1
    changed = deepcopy(request)
    changed['reason'] = 'Changed meaning under old request ID'
    with pytest.raises(ValueError, match='reused'):
        apply(db, record, changed)


def test_membership_evidence_is_append_only_and_account_identity_must_match(registered):
    db, record = registered
    apply(db, record, eighth(record))
    for sql in ("DELETE FROM event WHERE kind=?", "UPDATE event SET body='{}' WHERE kind=?"):
        with pytest.raises(sqlite3.IntegrityError, match='append-only'):
            db.execute(sql, (membership.KIND,))
    db.execute("DELETE FROM book WHERE id=?", (fingerprint('orbital/reversion'),))
    with pytest.raises(ValueError, match='REGISTERED_BOOK_MEMBERSHIP_MISMATCH'):
        membership.validate_accounts(db, record)


def test_registration_interruption_rolls_back_endowment_and_inherited_restriction(registered, monkeypatch):
    db, record = registered
    request = eighth(record)
    def fail(*_):
        raise OSError('interruption during source restriction inheritance')
    monkeypatch.setattr(membership, '_inherit_source_restriction', fail)
    with pytest.raises(OSError, match='interruption'):
        apply(db, record, request)
    assert db.execute('SELECT count(*) FROM book').fetchone()[0] == 7
    assert db.execute('SELECT count(*) FROM event').fetchone()[0] == 0
    monkeypatch.setattr(membership, '_inherit_source_restriction', lambda *_: None)
    assert apply(db, record, request)['state'] == 'APPLIED'


def test_new_flat_book_inherits_existing_source_restriction_callback(registered, monkeypatch):
    db, record = registered
    inherited = []
    def inherit(_, __, lane, book, at):
        inherited.append((caps.source_scope_id(lane), at))
        book['operational_restriction'] = {'code': 'SOURCE_UNAVAILABLE', 'scope': inherited[-1][0]}
    monkeypatch.setattr(membership, '_inherit_source_restriction', inherit)
    request = eighth(record)
    apply(db, record, request)
    book = paper.load(db, request['lane'])
    assert len(inherited) == 1 and book['operational_restriction']['code'] == 'SOURCE_UNAVAILABLE'
    assert book['position'] is None and not book.get('unresolved_protection')
    assert not db.execute("SELECT 1 FROM event WHERE kind LIKE 'PROTECTION_INCIDENT_%'").fetchone()


def test_registration_requires_approved_transaction_and_cannot_replace_legacy(registered):
    db, record = registered
    request = eighth(record)
    with pytest.raises(PermissionError, match='approval'):
        membership.register(db, record, request, at=AT)
    with pytest.raises(ValueError, match='WRITER_TRANSACTION'):
        membership.register(db, record, request, at=AT, approved=True)
    request['slot'] = 'ema/day'
    request['lane']['capabilities']['slot'] = request['slot']
    with pytest.raises(ValueError, match='REPLACES_AN_EXISTING_BOOK'):
        apply(db, record, request)


def test_admitted_membership_remains_visible_when_a_capability_becomes_unsupported(registered, monkeypatch):
    db, record = registered
    request = eighth(record)
    apply(db, record, request)
    monkeypatch.setattr(caps, 'SIGNALS', {'breakout_v1'})
    result = membership.describe(db, record)
    assert result['state'] == 'PARTIAL'
    assert len(result['books']) == 8
    assert result['books'][request['slot']]['state'] == 'UNSUPPORTED_CAPABILITIES'
    assert result['books']['breakout/day/fixed']['state'] == 'SUPPORTED'
    assert len(membership.expected_book_ids(record, db=db)) == 8


def test_registration_cannot_silently_add_another_approved_transport_scope(registered):
    db, record = registered
    request = eighth(record)
    request['lane']['observation_kind'] = 'HTTP_SNAPSHOT'
    request['lane']['capabilities'] = caps.declare(request['lane'], signal_adapter='ema_chart_v1',
        entry_adapter='ema_reference_v1', generation=record['id'], slot=request['slot'])
    with pytest.raises(ValueError, match='SOURCE_SCOPE_REQUIRES_SEPARATE_REVIEW'):
        apply(db, record, request)
    assert db.execute('SELECT count(*) FROM book').fetchone()[0] == 7


def test_membership_cli_previews_without_implicit_approval(registered, tmp_path, monkeypatch, capsys):
    _, record = registered
    path = tmp_path / 'request.json'
    request = eighth(record)
    path.write_text(json.dumps(request), encoding='utf-8')
    calls = []
    def execute(value, **options):
        calls.append((value, options))
        return {'state': 'PREVIEW'}
    monkeypatch.setattr(membership, 'execute', execute)
    membership.main(['register', '--request', str(path)])
    assert json.loads(capsys.readouterr().out)['state'] == 'PREVIEW'
    assert calls == [(request, {'apply': False, 'authorized': False})]
    membership.main(['register', '--request', str(path), '--apply', '--approve'])
    assert calls[-1][1] == {'apply': True, 'authorized': True}


def test_unknown_source_or_instrument_has_a_specific_registration_error(registered):
    _, record = registered
    value = eighth(record)['lane']
    value['symbol'] = 'ETH/USD'
    with pytest.raises(ValueError, match='UNSUPPORTED_REFERENCE_SOURCE'):
        caps.declare(value, signal_adapter='ema_chart_v1', entry_adapter='ema_reference_v1')
