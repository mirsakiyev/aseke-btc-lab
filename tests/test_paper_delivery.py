from contextlib import closing
from copy import deepcopy
import json
import sqlite3

import pandas as pd
import pytest

from lab.paper_run import capabilities, delivery, execution
from lab.phase15 import paper
from test_paper_run_execution import OBSERVED, EXPIRY, clock, lane, observation, planned, terms


@pytest.fixture
def journal():
    db = sqlite3.connect(':memory:')
    db.executescript('CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT);'
                     'CREATE TABLE book(id TEXT PRIMARY KEY,state TEXT);'
                     'CREATE TABLE source_state(id TEXT PRIMARY KEY,state TEXT);')
    yield db
    db.close()


def account(db, identifier='consumer-a', *, opened=True, expiry=EXPIRY):
    value = {**lane(), 'family': 'BREAKOUT', 'exit': 'fixed_r_v1', 'book_id': identifier}
    with db:
        book = planned(db, value, hold=90, expiry=expiry)
        if opened:
            execution.entry(db, book, value, observation(OBSERVED, event_id='entry'), terms, read_clock=clock(OBSERVED))
        paper.save(db, book)
    return value, book


def reserve(db, value, book, obs):
    with db:
        db.execute('BEGIN IMMEDIATE')
        return delivery.reserve(db, value, book, obs, at=pd.Timestamp(obs['received_at'])+pd.Timedelta(milliseconds=25))


def kinds(db, book_id, kind):
    return [json.loads(raw) for raw, in db.execute('SELECT body FROM event WHERE book=? AND kind=?', (book_id, kind))]


def test_failed_financial_transaction_cannot_advance_consumer_checkpoint(journal):
    value, book = account(journal)
    point_at = OBSERVED+pd.Timedelta(seconds=5)
    obs = observation(point_at, 94., event_id='stop')
    scope = capabilities.source_scope_id(value)
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        delivery.reserve(journal, value, book, obs, at=point_at+pd.Timedelta(milliseconds=150))
        journal.execute('INSERT INTO source_state VALUES(?,?)', (value['source'], json.dumps({'last_observation': obs})))
    with pytest.raises(OSError, match='commit interrupted'):
        with journal:
            journal.execute('BEGIN IMMEDIATE')
            current = paper.load(journal, value)
            execution.protect(journal, current, value, obs, read_clock=clock(point_at))
            paper.save(journal, current)
            delivery.acknowledge(journal, book['book_id'], scope, 'stop', point_at+pd.Timedelta(milliseconds=150), 'APPLIED')
            raise OSError('commit interrupted')
    assert not kinds(journal, book['book_id'], 'CLOSED')
    assert paper.load(journal, value)['position']
    assert delivery.state(journal, book['book_id'], scope)['checkpoint'] is None
    assert delivery.pending(journal, book['book_id'], scope)[0]['observation'] == obs
    assert json.loads(journal.execute('SELECT state FROM source_state').fetchone()[0])['last_observation']['event_id'] == 'stop'
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        current = paper.load(journal, value)
        execution.protect(journal, current, value, obs, read_clock=clock(point_at))
        paper.save(journal, current)
        delivery.acknowledge(journal, book['book_id'], scope, 'stop', point_at+pd.Timedelta(milliseconds=150), 'APPLIED')
    assert not paper.load(journal, value)['position']
    closed, = kinds(journal, book['book_id'], 'CLOSED')
    assert closed['price'] == 94.
    assert not delivery.pending(journal, book['book_id'], scope)
    assert reserve(journal, value, book, obs)['state'] == 'ALREADY_RESERVED'
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        assert delivery.acknowledge(journal, book['book_id'], scope, 'stop', point_at+pd.Timedelta(seconds=1), 'APPLIED')['state'] == 'ALREADY_ACKNOWLEDGED'
    assert len(kinds(journal, book['book_id'], 'CLOSED')) == 1


def test_failed_consumer_does_not_prevent_another_book_protection(journal):
    one, book_one = account(journal, 'one')
    two, book_two = account(journal, 'two')
    point_at = OBSERVED+pd.Timedelta(seconds=5)
    obs = observation(point_at, 94., event_id='shared-stop')
    reserve(journal, one, book_one, obs)
    reserve(journal, two, book_two, obs)
    with pytest.raises(RuntimeError, match='consumer one'):
        with journal:
            journal.execute('BEGIN IMMEDIATE')
            raise RuntimeError('consumer one unavailable')
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        execution.protect(journal, book_two, two, obs, read_clock=clock(point_at))
        paper.save(journal, book_two)
        delivery.acknowledge(journal, two['book_id'], capabilities.source_scope_id(two), 'shared-stop', point_at+pd.Timedelta(milliseconds=150), 'APPLIED')
    assert delivery.pending(journal, one['book_id'])
    assert not delivery.pending(journal, two['book_id'])
    assert paper.load(journal, one)['position'] and not paper.load(journal, two)['position']


@pytest.mark.parametrize('expires', [True, False])
def test_stored_catch_up_reference_cannot_create_a_delayed_live_entry(journal, expires):
    value, book = account(journal, opened=False, expiry=EXPIRY if expires else OBSERVED+pd.Timedelta(hours=1))
    obs = observation(OBSERVED, event_id='old-reference')
    reserve(journal, value, book, obs)
    delayed = OBSERVED+pd.Timedelta(minutes=15)
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        execution.entry(journal, book, value, obs, terms, read_clock=lambda:(delayed, 900_000_000_000))
        paper.save(journal, book)
        delivery.acknowledge(journal, value['book_id'], capabilities.source_scope_id(value), obs['event_id'], delayed, 'EXPIRED' if expires else 'STALE')
    assert not book['position'] and not kinds(journal, value['book_id'], 'HYPOTHETICAL_FILL')
    assert not kinds(journal, value['book_id'], 'PROTECTION_GAP')
    assert not journal.execute("SELECT 1 FROM event WHERE kind LIKE 'PROTECTION_INCIDENT_%'").fetchone()
    loss, = delivery.losses(journal, value['book_id'], capabilities.source_scope_id(value))
    assert not loss['requires_review'] and loss['target_kind'] == 'PENDING_ENTRY'


def test_flat_consumer_never_receives_fictitious_exposure_work(journal):
    value, _ = account(journal)
    book = paper.initial(value)
    result = reserve(journal, value, book, observation(OBSERVED, event_id='unused'))
    assert result['state'] == 'NO_EXPOSURE_OR_PENDING_ENTRY'
    assert delivery.pending_books(journal) == []
    assert not delivery.losses(journal, value['book_id'], capabilities.source_scope_id(value))


def test_overflow_retains_a_bounded_conservative_exposure_loss_until_journaled(journal):
    value, book = account(journal)
    scope = capabilities.source_scope_id(value)
    for i in range(delivery.MAX_PENDING+2):
        reserve(journal, value, book, observation(OBSERVED+pd.Timedelta(seconds=i+1), event_id=str(i)))
    state = delivery.state(journal, value['book_id'], scope)
    assert len(state['pending']) == delivery.MAX_PENDING
    loss, = state['losses']
    assert loss['first_event_id'] == '0' and loss['last_event_id'] == '1' and loss['lost_observations'] == 2
    assert loss['requires_review'] and loss['position_id'] == book['position']['id']
    assert loss['frozen_position']['stop'] == book['position']['stop']
    at = OBSERVED+pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match='REVIEW_EVIDENCE_MISSING'):
        with journal:
            journal.execute('BEGIN IMMEDIATE')
            delivery.acknowledge_loss(journal, value['book_id'], scope, loss['id'], at, {'incident_id':'invented'})
    assert delivery.losses(journal, value['book_id'], scope) == [loss]
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        paper.event(journal, book, 'PROTECTION_GAP', book['position']['id'], at, at,
                    {'reason':'DELIVERY_QUEUE_OVERFLOW','delivery_loss_id':loss['id'],'delivery_loss':loss})
        event_id = journal.execute("SELECT key FROM event WHERE kind='PROTECTION_GAP'").fetchone()[0]
        delivery.acknowledge_loss(journal, value['book_id'], scope, loss['id'], at, {'event_id':event_id})
    assert not delivery.losses(journal, value['book_id'], scope)
    assert delivery.state(journal, value['book_id'], scope)['loss_checkpoint']['recorded_evidence']['event_id'] == event_id
    assert len(delivery.pending(journal, value['book_id'], scope)) == delivery.MAX_PENDING


def test_pending_overflow_does_not_claim_unobserved_position_protection(journal):
    value, book = account(journal, opened=False)
    for i in range(delivery.MAX_PENDING+1):
        reserve(journal, value, book, observation(OBSERVED+pd.Timedelta(seconds=i+1), event_id=str(i)))
    loss, = delivery.losses(journal, value['book_id'], capabilities.source_scope_id(value))
    assert not loss['requires_review'] and loss['position_id'] is None
    with journal:
        journal.execute('BEGIN IMMEDIATE')
        delivery.acknowledge_loss(journal, value['book_id'], capabilities.source_scope_id(value), loss['id'], OBSERVED+pd.Timedelta(minutes=1),
                                  {'reason':'Unused pending observation; a new independently fresh reference remains required'})
    assert not journal.execute("SELECT 1 FROM event WHERE kind LIKE 'PROTECTION_%'").fetchone()


def test_work_and_checkpoint_survive_restart_without_replaying_a_close(journal, tmp_path):
    value, book = account(journal)
    obs = observation(OBSERVED+pd.Timedelta(seconds=1), event_id='one')
    reserve(journal, value, book, obs)
    target = tmp_path/'restart.sqlite3'
    with closing(sqlite3.connect(target)) as saved:
        journal.backup(saved)
    with sqlite3.connect(target) as db:
        work, = delivery.pending(db, value['book_id'])
        assert work['observation'] == obs and work['position_id'] == book['position']['id']
        db.execute('BEGIN IMMEDIATE')
        delivery.acknowledge(db, value['book_id'], work['source_scope_id'], obs['event_id'], OBSERVED+pd.Timedelta(seconds=2), 'NO_ECONOMIC_TRANSITION')
    with sqlite3.connect(target) as db:
        assert not delivery.pending(db, value['book_id'])
        db.execute('BEGIN IMMEDIATE')
        assert delivery.acknowledge(db, value['book_id'], capabilities.source_scope_id(value), 'one', OBSERVED+pd.Timedelta(seconds=3), 'NO_ECONOMIC_TRANSITION')['state'] == 'ALREADY_ACKNOWLEDGED'


def test_out_of_order_ack_and_changed_duplicate_payload_fail_closed(journal):
    value, book = account(journal)
    first = observation(OBSERVED+pd.Timedelta(seconds=1), event_id='one')
    second = observation(OBSERVED+pd.Timedelta(seconds=2), event_id='two')
    reserve(journal, value, book, first)
    reserve(journal, value, book, second)
    scope = capabilities.source_scope_id(value)
    with pytest.raises(ValueError, match='OUT_OF_ORDER'):
        with journal:
            journal.execute('BEGIN IMMEDIATE')
            delivery.acknowledge(journal, value['book_id'], scope, 'two', OBSERVED+pd.Timedelta(seconds=3), 'PROCESSED')
    changed = deepcopy(second)
    changed['price'] = 10.
    with pytest.raises(ValueError, match='EVIDENCE_CHANGED'):
        reserve(journal, value, book, changed)
    assert [item['event_id'] for item in delivery.pending(journal, value['book_id'])] == ['one','two']
    assert reserve(journal, value, book, first)['state'] == 'REPLAY_OR_REORDERED'


def test_deliveries_require_an_existing_transaction_and_keep_target_identity(journal):
    value, book = account(journal)
    obs = observation(OBSERVED+pd.Timedelta(seconds=1), event_id='one')
    with pytest.raises(ValueError, match='WRITER_TRANSACTION'):
        delivery.reserve(journal, value, book, obs)
    item = reserve(journal, value, book, obs)
    original = item['position_id']
    book['position']['id'] = 'new-position'
    work, = delivery.pending(journal, value['book_id'])
    assert work['position_id'] == original and work['position_id'] != book['position']['id']


def test_loss_capacity_never_discards_previously_unreviewed_ranges(journal):
    value, book = account(journal)
    for i in range(delivery.MAX_PENDING+delivery.MAX_LOSSES):
        book['position']['id'] = str(i)
        reserve(journal, value, book, observation(OBSERVED+pd.Timedelta(seconds=i+1), event_id=str(i)))
    scope = capabilities.source_scope_id(value)
    original = delivery.state(journal, value['book_id'], scope)
    assert len(original['losses']) == delivery.MAX_LOSSES
    book['position']['id'] = 'overflow-target'
    with pytest.raises(RuntimeError, match='LOSS_CAPACITY_REQUIRES_REVIEW'):
        reserve(journal, value, book, observation(OBSERVED+pd.Timedelta(minutes=1), event_id='capacity'))
    assert delivery.state(journal, value['book_id'], scope) == original
