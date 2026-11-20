from copy import deepcopy
import json
import sqlite3

import pandas as pd
import pytest

from lab.paper_run import capabilities, membership, operations, protection_incidents as incidents
from lab.phase15 import paper
from test_paper_capabilities_membership import AT, eighth, registered


HEALTHY = {'available': True, 'reason_code': None}


def events(db, kind):
    return [json.loads(raw) for raw, in db.execute('SELECT body FROM event WHERE kind=? ORDER BY rowid', (kind,))]


def book(db, lane):
    return paper.load(db, lane)


def health(db, record, lane, at=AT, *, available=True, reason=None, correlation=None):
    shared = operations.source_transition(db, record, lane, at, available=available, reason=reason, correlation_id=correlation)
    return {'available': available, 'reason_code': reason, 'source_scope_id': shared['source_scope_id'],
            'correlation_id': shared['correlation_id']}


def capture(db, record, lane, value, at=AT, *, source=None, clock=True, entries=True):
    return operations.capture(db, record, lane, value, at, source_health=source or HEALTHY,
                              clock_ok=clock, entries_enabled=entries)


def status(db, record, lane, value, at=AT, *, source=None, clock=True, entries=True, financial=True, research=None):
    return operations.status(db, record, lane, value, at, source_health=source or HEALTHY,
                             clock_ok=clock, entries_enabled=entries, financial_complete=financial, research=research)


def position(lane, identifier):
    return {'id': identifier, 'entry_at': (AT-pd.Timedelta(hours=1)).isoformat(),
            'stop': 95., 'target': None if lane.get('exit') == 'atr_trail_v1' else 110.,
            'deadline': (AT+pd.Timedelta(hours=1)).isoformat(),
            'mark_source_at': (AT-pd.Timedelta(seconds=1)).isoformat(),
            'mark_received_at': (AT-pd.Timedelta(milliseconds=500)).isoformat(), 'mark_event_id': '100', 'last_mark': 100.,
            'entry_evidence': {'source': lane['source'], 'symbol': lane['symbol'], 'observation_kind': lane['observation_kind']},
            'plan': {'trigger_basis': lane['price_basis']},
            'position': {'remaining': 1., 'reserved': 100., 'entry': 100., 'direction': 1}}


def activate(db, record):
    lane = record['body']['lanes']['breakout/position/fixed']
    value = book(db, lane)
    incidents.append(db, value, 'PROTECTION_POLICY_ACTIVATED', record['id'], AT-pd.Timedelta(hours=2),
                     {'generation': record['id'], 'policy': incidents.POLICY}, identity='isolated-policy')


def add_eighth(db, record, monkeypatch):
    request = eighth(record)
    monkeypatch.setattr(membership, '_inherit_source_restriction',
                        lambda *args: operations.inherit_source_restriction(*args))
    with db:
        db.execute('BEGIN IMMEDIATE')
        membership.register(db, record, request, at=AT, approved=True)
    return request['slot'], request['lane']


def test_all_seven_and_an_unrelated_eighth_name_inherit_the_operational_contract(registered, monkeypatch):
    db, record = registered
    slot, _ = add_eighth(db, record, monkeypatch)
    lanes = membership.lanes(record, db=db)
    assert len(lanes) == 8 and lanes[slot]['family'] not in ('EMA','BREAKOUT')
    for lane in lanes.values():
        with db:
            capture(db, record, lane, book(db, lane))
        value = status(db, record, lane, book(db, lane))
        assert value['entry_permission']['available']
        assert value['exposure']['state'] == 'FLAT'
        assert value['current_protection'] == 'NOT_REQUIRED'
        assert value['recovery']['automatic_resume'] == 'DISABLED_PENDING_VERSIONED_POLICY_APPROVAL'
        assert value['capabilities']['adapters']['notification'] == 'health_outbox_v1'
    assert not events(db, 'PROTECTION_INCIDENT_OPENED')


def test_new_book_in_an_existing_outage_inherits_the_actual_restriction_without_a_fake_gap(registered, monkeypatch):
    db, record = registered
    existing = record['body']['lanes']['ema/day']
    detected = AT-pd.Timedelta(minutes=2)
    with db:
        shared = operations.source_transition(db, record, existing, detected, available=False,
                                             reason='KRAKEN_CONSUMER_UNAVAILABLE', correlation_id='one-shared-outage')
    slot, future = add_eighth(db, record, monkeypatch)
    future_book = book(db, future)
    value = status(db, record, future, future_book, source={'available':False,'reason_code':'KRAKEN_CONSUMER_UNAVAILABLE'})
    assert not value['entry_permission']['available']
    assert value['exposure']['state'] == 'FLAT' and value['current_protection'] == 'NOT_REQUIRED'
    opened = [row for row in events(db, operations.OPENED) if row['book_id'] == future['book_id']]
    assert len(opened) == 1
    assert opened[0]['restriction_started_at'] == detected.isoformat()
    assert opened[0]['evidence']['correlation_id'] == shared['correlation_id']
    assert opened[0]['evidence']['inherited_source_outage']
    assert not incidents.project(db, future['book_id']) and not future_book.get('position')
    assert slot in membership.lanes(record, db=db)


def test_shared_source_has_one_correlation_and_separate_exposure_incidents(registered):
    db, record = registered
    selected = [record['body']['lanes'][slot] for slot in ('breakout/swing/fixed','breakout/swing/trail')]
    unrelated = record['body']['lanes']['ema/day']
    with db:
        activate(db, record)
        first = operations.source_transition(db, record, selected[0], AT, available=False, reason='SHARED_SOURCE_LOST')
        second = operations.source_transition(db, record, selected[1], AT+pd.Timedelta(seconds=1), available=False, reason='SHARED_SOURCE_LOST')
        assert first == second
        identifiers = []
        for n,lane in enumerate(selected):
            value = book(db, lane)
            value['position'] = position(lane, 'open-'+str(n))
            identifiers.append(incidents.open_incident(db, value, AT, 'SHARED_SOURCE_LOST', lane=lane,
                                                       correlation_id=first['correlation_id']))
            capture(db, record, lane, value, source={'available':False,'reason_code':'SHARED_SOURCE_LOST'})
            paper.save(db,value)
        capture(db, record, unrelated, book(db,unrelated))
    assert len(set(identifiers)) == 2
    assert len(events(db, operations.SOURCE)) == 1
    assert {i['source_correlation_id'] for lane in selected for i in incidents.project(db,lane['book_id'])} == {first['correlation_id']}
    assert status(db,record,unrelated,book(db,unrelated))['entry_permission']['available']
    assert not incidents.project(db,unrelated['book_id'])


@pytest.mark.parametrize('exposure,expected', [('open','POSITION_OPEN'),('pending','PENDING_ENTRY')])
def test_occupied_aggregate_active_account_never_claims_an_overlapping_entry(registered, exposure, expected):
    db,record=registered
    lane=record['body']['lanes']['ema/day'];value=book(db,lane)
    if exposure=='open':value['position']=position(lane,'existing-position')
    else:value['pending']={'id':'existing-intention'}
    state=status(db,record,lane,value)
    assert not state['entry_permission']['available']
    assert state['entry_permission']['reason_code']==expected
    assert not state['current_restrictions']
    assert not events(db,operations.OPENED)


@pytest.mark.parametrize('cause', ['clock','pause','qualification','financing','flat_finances','permanent'])
def test_restrictions_are_durable_deduplicated_and_truthfully_cleared(registered,cause,tmp_path):
    db,record=registered
    lane=deepcopy(record['body']['lanes']['ema/day']);value=book(db,lane)
    clock_ok,entries=True,True
    reason={'clock':'CLOCK_UNVERIFIED','pause':'INTENTIONAL_ENTRY_PAUSE','qualification':'SOURCE_QUALIFICATION_UNAVAILABLE',
            'financing':'FINANCIAL_RECONCILIATION_REQUIRED','flat_finances':'FINANCIAL_RECONCILIATION_REQUIRED','permanent':'PERMANENT_RISK_BLOCK'}[cause]
    if cause=='clock':clock_ok=False
    elif cause=='pause':entries=False
    elif cause=='qualification':lane['qualification']={'state':'BLOCKED','reason':'SOURCE_UNAPPROVED'}
    elif cause=='financing':value['position']=position(lane,'financing-position');value['position']['unresolved_costs']='MISSING_SETTLEMENT'
    elif cause=='flat_finances':value['cash']+=1.
    else:value['permanent_block']=True
    with db:
        capture(db,record,lane,value,clock=clock_ok,entries=entries)
        capture(db,record,lane,value,AT+pd.Timedelta(seconds=5),clock=clock_ok,entries=entries)
    opened=events(db,operations.OPENED)
    assert len(opened)==1 and opened[0]['reason_code']==reason
    assert opened[0]['detected_at']==AT.isoformat() and opened[0]['next_action']
    path=tmp_path/'restart.sqlite3'
    saved=sqlite3.connect(path);db.backup(saved);saved.close()
    with sqlite3.connect(path) as restarted:
        capture(restarted,record,lane,value,AT+pd.Timedelta(seconds=10),clock=clock_ok,entries=entries)
        assert len(events(restarted,operations.OPENED))==1
        clean=paper.initial(lane);lane['qualification']={'state':'QUALIFIED'}
        capture(restarted,record,lane,clean,AT+pd.Timedelta(seconds=15))
        assert len(events(restarted,operations.CLEARED))==1
        assert not status(restarted,record,lane,clean)['current_restrictions']
    assert not incidents.project(db,lane['book_id'])


def test_reason_change_does_not_move_the_start_of_a_single_source_outage(registered):
    db,record=registered;lane=record['body']['lanes']['ema/day']
    with db:
        first=operations.source_transition(db,record,lane,AT,available=False,reason='NETWORK_LOSS')
        changed=operations.source_transition(db,record,lane,AT+pd.Timedelta(seconds=5),available=False,reason='SOURCE_BACKOFF')
        assert first['started_at']==changed['started_at']==AT.isoformat()
        assert first['correlation_id']==changed['correlation_id']
        operations.source_transition(db,record,lane,AT+pd.Timedelta(seconds=10),available=True)
        later=operations.source_transition(db,record,lane,AT+pd.Timedelta(seconds=15),available=False,reason='NETWORK_LOSS')
    assert later['correlation_id']!=first['correlation_id']
    assert later['started_at']!=(first['started_at'])
    assert len(events(db,operations.SOURCE))==4


def test_historical_uncertainty_does_not_erase_a_valid_prospective_admission(registered):
    db,record=registered;lane=record['body']['lanes']['ema/day'];value=book(db,lane)
    at=AT+pd.Timedelta(minutes=15)
    with db:
        activate(db,record)
        incidents.append(db,value,'PROTECTION_INCIDENT_IMPORTED','old-position',AT-pd.Timedelta(minutes=10),
                         {'generation':record['id'],'incident_id':'old-incident','position_id':'old-position',
                          'current_feed_health':'UNKNOWN_HISTORICAL','historical_coverage':'UNRESOLVED',
                          'outcome':'INSUFFICIENT_EVIDENCE'},identity='old-incident')
        dependency={'incident_ids':['old-incident'],'position_ids':['old-position'],'segment_id':'new-prospective-segment'}
        key=incidents.append(db,value,'BOOK_PROSPECTIVE_RESUME','old-position',AT,
                             {'generation':record['id'],'effective_at':at.isoformat(),'capital_dependency':dependency},identity='new-explicit-resume')
        value.update(problem='UNRESOLVED_PROTECTION_INTERVAL',prospective_admission={
            'policy':incidents.POLICY,'event_id':key,'effective_at':at.isoformat(),'capital_dependency':dependency})
        paper.save(db,value)
    incidents.restore_admission(db,value)
    before=status(db,record,lane,value,at-pd.Timedelta(seconds=1),research={'research_state':'QUARANTINED'})
    after=status(db,record,lane,value,at+pd.Timedelta(seconds=1),research={'research_state':'QUARANTINED'})
    assert before['entry_permission']['reason_code']=='WAIT_FUTURE_ADMISSION'
    assert after['entry_permission']['available']
    assert after['historical_coverage']=='UNRESOLVED' and after['research_state']=='QUARANTINED'
    assert value['problem']=='UNRESOLVED_PROTECTION_INTERVAL' and value['cash']==10000.


def test_normal_no_trade_and_study_window_are_not_protection_incidents(registered):
    db,record=registered;lane=deepcopy(record['body']['lanes']['ema/day']);value=book(db,lane)
    value['last_evaluation']={'state':'EVALUATED','signal':{'decision':'NO_TRADE'},'reason':None}
    with db:capture(db,record,lane,value)
    assert status(db,record,lane,value)['entry_permission']['available']
    lane['publication_end_exclusive']=AT.isoformat()
    with db:capture(db,record,lane,value)
    assert status(db,record,lane,value)['entry_permission']['reason_code']=='STUDY_WINDOW_CLOSED'
    assert not events(db,operations.OPENED) and not incidents.project(db,lane['book_id'])


def test_trailing_failure_has_an_actionable_history_and_success_only_clears_current_fault(registered):
    db,record=registered;lane=record['body']['lanes']['breakout/swing/trail'];value=book(db,lane)
    value['position']=position(lane,'trailing-position')
    with db:
        activate(db,record)
        fault=operations.record_fault(db,record,lane,value,AT,'TRAIL_CALCULATION_FAILED',details={'exception_type':'ValueError'})
        paper.save(db,value)
        capture(db,record,lane,value)
    projected=incidents.project(db,lane['book_id'])
    assert len(projected)==1 and projected[0]['reason']=='TRAIL_CALCULATION_FAILED'
    assert projected[0]['historical_coverage']=='UNRESOLVED'
    assert value['position']['unresolved_protection']
    assert fault['next_action'] and fault['requires_incident_review']
    limited=status(db,record,lane,value)
    assert limited['current_protection']=='LIMITED_BY_OPERATIONAL_FAULT'
    assert not limited['entry_permission']['available']
    with db:
        operations.clear_fault(value,'TRAIL_CALCULATION_FAILED')
        capture(db,record,lane,value,AT+pd.Timedelta(seconds=5))
    assert value['position']['unresolved_protection']
    assert incidents.project(db,lane['book_id'])[0]['historical_coverage']=='UNRESOLVED'
    assert len(events(db,operations.CLEARED))==1
    assert events(db,operations.CLEARED)[0]['historical_coverage']=='UNRESOLVED'


def test_operational_transitions_are_append_only(registered):
    db,record=registered;lane=record['body']['lanes']['ema/day'];value=book(db,lane)
    with db:capture(db,record,lane,value,clock=False)
    for statement in ("UPDATE event SET body='{}' WHERE kind=?",'DELETE FROM event WHERE kind=?'):
        with pytest.raises(sqlite3.IntegrityError,match='append-only'):
            db.execute(statement,(operations.OPENED,))
