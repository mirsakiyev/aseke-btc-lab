"""Isolated proofs for a serialized operator exit alongside the managed writer."""

from contextlib import closing
from copy import deepcopy
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.paper_run import execution, gate, operator_close as operator, protection_reporting, runtime, source_amendment
from lab.phase15 import paper
from lab.storage import write_json, writer_lock
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, history, intent, live_point, saved_book,
)


@pytest.fixture
def operation(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(91, price="100.")], {})
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        book['position']['unresolved_protection'] = True
        paper.event(db, book, 'PROTECTION_GAP', book['position']['id'], now[0], now[0],
                    {'reason': 'original-incident', 'last_reference': book['position']['mark_received_at']})
        paper.save(db, book)
    position = saved_book(lane)['position']
    request = {'generation': record['id'], 'slot': DAY, 'book_id': lane['book_id'],
               'position_id': position['id'], 'request_id': 'operator-request-1',
               'reason': 'Explicit isolated administrative paper close'}
    now[0] = TICK + pd.Timedelta(milliseconds=300)
    write_json(gate.ROOT / 'entries-paused', {'generation': record['id'], 'paused_at': now[0].isoformat()})
    write_json(gate.ROOT / 'last-cycle.json', {'state': 'RUNNING', 'generation': record['id'],
               'source_amendment_id': amendment['id'], 'pid': 1234, 'runner_build_id': 'legacy-approved-runner',
               'at': now[0].isoformat()})
    write_json(gate.ROOT / 'ema-live-status.json', {'state': 'STREAMING', 'amendment_id': amendment['id'],
                                                  'at': now[0].isoformat()})
    monkeypatch.setattr(runtime.kraken_live, 'permission', lambda _: None)
    monkeypatch.setattr(operator, 'WAIT_SECONDS', 0.)
    return SimpleNamespace(record=record, now=now, amendment=amendment, lane=lane,
                           request=request, position=deepcopy(position))


def apply(context, **kwargs):
    return operator.execute(context.request, apply=True, authorized=True, **kwargs)


def begin(context):
    result = apply(context)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_NOT_EXECUTED'
    assert len(history(context.lane, operator.REQUEST_EVENT)) == 1
    return result


def emit(context, *, price='99.', event_id=92, accept=True, at=None, **changes):
    at = TICK + pd.Timedelta(seconds=1) if at is None else at
    point = live_point(event_id, price=price, at=at)
    point.update(changes)
    context.now[0] = at + pd.Timedelta(milliseconds=125)
    if accept:
        runtime.live_prices(context.record, context.amendment, [point], {})
    else:
        with closing(runtime.connect()) as db, db:
            state = runtime._source_state(db, context.lane['source'])
            state['last_observation'] = point
            runtime._save_source(db, context.lane['source'], state)
    return point


def refresh_heartbeats(context):
    for name in ('last-cycle.json', 'ema-live-status.json'):
        path = gate.ROOT / name
        body = json.loads(path.read_text())
        body['at'] = context.now[0].isoformat()
        write_json(path, body)


def financing_every_minute(context):
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, context.lane)
        book['position']['plan'].update(model_financing_interval_minutes=1, model_financing_rate=.001)
        paper.save(db, book)


def test_preview_is_read_only_and_uses_legacy_managed_collector(operation):
    before = (gate.ROOT / 'paper.sqlite3').read_bytes()
    result = operator.execute(operation.request)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_PREVIEW'
    assert result['runner']['runner_build_id'] == 'legacy-approved-runner'
    assert result['new_post_request_observation_required']
    assert (gate.ROOT / 'paper.sqlite3').read_bytes() == before
    assert not history(operation.lane, operator.REQUEST_EVENT)
    with pytest.raises(ValueError, match='approval'):
        operator.execute(operation.request, apply=True)


@pytest.mark.parametrize('field,value', [('generation', 'wrong-generation'), ('book_id', 'wrong-book'),
                                       ('position_id', 'wrong-position'), ('slot', 'ema/swing')])
def test_wrong_identity_is_refused_without_a_request_or_exit(operation, field, value):
    request = {**operation.request, field: value}
    with pytest.raises(ValueError):
        operator.execute(request, apply=True, authorized=True)
    assert not history(operation.lane, operator.REQUEST_EVENT)
    assert not history(operation.lane, 'EXIT_OBSERVED')
    assert saved_book(operation.lane)['position']['id'] == operation.position['id']


def test_exact_new_price_close_fees_nominal_cash_and_history_reconcile(operation):
    begin(operation)
    observed = emit(operation, price='99.1234')
    original_gaps = history(operation.lane, 'PROTECTION_GAP')
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_CLOSED'
    assert result['operator_request_applied'] and result['financially_closed']
    book = saved_book(operation.lane)
    observed_exit, = history(operation.lane, 'EXIT_OBSERVED')
    closed, = history(operation.lane, 'CLOSED')
    assert observed_exit['price'] == closed['exit_price'] == observed['price']
    assert closed['reason'] == operator.EXIT_REASON
    assert closed['operator_intervened'] and closed['provisional']
    assert closed['unresolved_protection']
    assert closed['original_deadline'] == operation.position['deadline']
    assert closed['operator_request'] == operation.request
    assert closed['exchange_source_at'] == observed['source_at']
    assert closed['source_received_at'] == observed['received_at']
    assert closed['quantity_closed'] == operation.position['position']['remaining']
    assert closed['exit_adverse_rate'] == 0. and not closed['liquidity_verified']
    assert not book['position'] and book['problem'] == 'UNRESOLVED_PROTECTION_INTERVAL'
    assert book['cash'] == pytest.approx(book['starting_equity'] + book['gross'] + book['funding'] - book['fees'])
    quantity = closed['quantity_closed']
    assert closed['fees'] == pytest.approx(quantity * (100. + observed['price']) * operation.position['plan']['cost']['taker'])
    assert history(operation.lane, 'PROTECTION_GAP') == original_gaps
    with closing(runtime.connect(readonly=True)) as db:
        research = protection_reporting.summarize(db, operation.lane, book)
    assert research['validated_closed_count'] == 0
    assert research['excluded_closed_count'] == 1


def test_duplicate_request_cannot_close_twice_and_changed_arguments_are_rejected(operation):
    begin(operation)
    emit(operation)
    first = apply(operation)
    second = apply(operation)
    assert first['events'] == second['events']
    assert second['state'] == 'TARGET_ALREADY_CLOSED'
    assert len(history(operation.lane, 'EXIT_OBSERVED')) == len(history(operation.lane, 'CLOSED')) == 1
    assert len(history(operation.lane, operator.REQUEST_EVENT)) == 1
    with pytest.raises(ValueError, match='reused'):
        operator.execute({**operation.request, 'reason': 'changed'}, apply=True, authorized=True)


@pytest.mark.parametrize('kind', ['stale', 'future', 'replay', 'old-receipt', 'step', 'wrong-source'])
def test_invalid_or_old_references_cannot_exit(operation, kind):
    begin(operation)
    point = emit(operation, accept=False)
    if kind == 'stale':
        operation.now[0] += pd.Timedelta(seconds=3)
    elif kind == 'future':
        point['source_at'] = (operation.now[0] + pd.Timedelta(seconds=1)).isoformat()
    elif kind == 'replay':
        point['event_id'] = '91'
    elif kind == 'old-receipt':
        point = live_point(92, at=TICK + pd.Timedelta(milliseconds=200))
    elif kind == 'step':
        point['received_mono_ns'] += 300_000_000
    else:
        point['source'] = 'WRONG_SOURCE'
    with closing(runtime.connect()) as db, db:
        state = runtime._source_state(db, operation.lane['source']);state['last_observation'] = point
        runtime._save_source(db, operation.lane['source'], state)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_NOT_EXECUTED'
    assert not history(operation.lane, 'EXIT_OBSERVED')
    assert saved_book(operation.lane)['position']['id'] == operation.position['id']


def test_clock_invalid_before_request_does_not_create_a_request(operation, monkeypatch):
    monkeypatch.setattr(runtime, 'synchronized', lambda _: False)
    with pytest.raises(operator.EvidenceUnavailable, match='CLOCK'):
        apply(operation)
    assert not history(operation.lane, operator.REQUEST_EVENT)


def test_final_commit_clock_failure_rolls_back_exit_and_all_finances(operation, monkeypatch):
    begin(operation)
    emit(operation)
    before = saved_book(operation.lane)
    calls = [0]

    def qualify(_):
        calls[0] += 1
        return calls[0] < 3

    monkeypatch.setattr(runtime, 'synchronized', qualify)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_NOT_EXECUTED'
    assert saved_book(operation.lane) == before
    assert not history(operation.lane, 'EXIT_OBSERVED')
    assert not history(operation.lane, 'CLOSED')


def test_final_commit_staleness_rolls_back_new_funding_and_exit(operation, monkeypatch):
    financing_every_minute(operation)
    begin(operation)
    emit(operation, accept=False, at=TICK.ceil('min') + pd.Timedelta(milliseconds=50))
    refresh_heartbeats(operation)
    before = saved_book(operation.lane)
    original = operator._healthy
    calls = [0]

    def qualify(*args):
        calls[0] += 1
        result = original(*args)
        if calls[0] == 3:
            operation.now[0] += pd.Timedelta(seconds=3)
        return result

    monkeypatch.setattr(operator, '_healthy', qualify)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_NOT_EXECUTED'
    assert result['reason'] == 'LEDGER_COMMIT_STALE_REFERENCE'
    assert saved_book(operation.lane) == before
    assert not history(operation.lane, 'FUNDING')
    assert not history(operation.lane, 'EXIT_OBSERVED')
    assert len(history(operation.lane, operator.REQUEST_EVENT)) == 1


def test_existing_modeled_financing_settles_once_and_cash_reconciles(operation):
    financing_every_minute(operation)
    begin(operation)
    observed = emit(operation, at=TICK.ceil('min') + pd.Timedelta(milliseconds=50))
    refresh_heartbeats(operation)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_CLOSED'
    funding, = history(operation.lane, 'FUNDING')
    closed, = history(operation.lane, 'CLOSED')
    book = saved_book(operation.lane)
    assert funding['cash_flow'] < 0.
    assert closed['funding'] == book['funding'] == funding['cash_flow']
    assert funding['valuation_reference'] == observed['price']
    assert book['cash'] == pytest.approx(book['starting_equity'] + book['gross'] + book['funding'] - book['fees'])
    apply(operation)
    assert len(history(operation.lane, 'FUNDING')) == len(history(operation.lane, 'CLOSED')) == 1


def test_exit_observed_does_not_invent_missing_financing(operation):
    financing_every_minute(operation)
    begin(operation)
    emit(operation, at=TICK.ceil('min') + pd.Timedelta(seconds=46))
    refresh_heartbeats(operation)
    before = saved_book(operation.lane)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_OBSERVED_AWAITING_ACCOUNTING'
    assert not result['financially_closed']
    book = saved_book(operation.lane)
    assert book['position']['exit_observed']['reason'] == operator.EXIT_REASON
    assert book['position']['unresolved_costs'] == 'WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION'
    assert book['position']['unresolved_protection']
    assert book['cash'] == before['cash']
    assert not history(operation.lane, 'CLOSED') and not history(operation.lane, 'FUNDING')
    assert apply(operation)['state'] == 'EXIT_ALREADY_OBSERVED'
    assert len(history(operation.lane, 'EXIT_OBSERVED')) == 1


def test_same_accepted_trade_id_with_changed_payload_is_refused(operation):
    begin(operation)
    point = emit(operation)
    point.update(price=98., price_text='98.')
    with closing(runtime.connect()) as db, db:
        state = runtime._source_state(db, operation.lane['source']);state['last_observation'] = point
        runtime._save_source(db, operation.lane['source'], state)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_NOT_EXECUTED'
    assert result['reason'] == 'CURRENT_PROTECTION_REFERENCE_MISMATCH'
    assert not history(operation.lane, 'EXIT_OBSERVED')


def test_unqualified_source_never_records_an_operator_request(operation, monkeypatch):
    original = source_amendment.lanes

    def unqualified(*args):
        lanes = deepcopy(original(*args))
        lanes[DAY]['qualification']['state'] = 'BLOCKED'
        return lanes

    monkeypatch.setattr(source_amendment, 'lanes', unqualified)
    with pytest.raises(ValueError, match='not qualified'):
        apply(operation)
    assert not history(operation.lane, operator.REQUEST_EVENT)


@pytest.mark.parametrize('price,reason', [('106.', 'stop'), ('94.', 'target')])
def test_newer_point_runs_normal_protection_and_natural_exit_wins(operation, price, reason):
    begin(operation)
    emit(operation, price=price, accept=False)
    result = apply(operation)
    closed, = history(operation.lane, 'CLOSED')
    assert result['state'] == 'NATURAL_EXIT_WON_RACE'
    assert closed['reason'] == reason and not closed.get('operator_intervened')
    assert len(history(operation.lane, 'EXIT_OBSERVED')) == 1


def test_natural_exit_before_operator_attempt_is_preserved(operation):
    begin(operation)
    emit(operation, price='106.')
    before = saved_book(operation.lane)
    result = apply(operation)
    assert result['state'] == 'TARGET_ALREADY_CLOSED' and not result['operator_request_applied']
    assert saved_book(operation.lane) == before
    assert history(operation.lane, 'CLOSED')[0]['reason'] == 'stop'


def test_a_new_position_is_never_closed_by_old_request(operation):
    begin(operation)
    emit(operation, price='106.')
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, operation.lane)
        book['position'] = deepcopy(operation.position)
        book['position']['id'] = 'unapproved-successor-position'
        paper.save(db, book)
    with pytest.raises(ValueError, match='different position'):
        apply(operation)
    assert saved_book(operation.lane)['position']['id'] == 'unapproved-successor-position'
    assert len(history(operation.lane, 'CLOSED')) == 1


def test_writer_contention_never_mutates_or_bypasses_ownership(operation):
    with writer_lock(gate.ROOT / '.writer.lock'), pytest.raises(RuntimeError, match='Another writer'):
        apply(operation)
    assert not history(operation.lane, operator.REQUEST_EVENT)
    assert not history(operation.lane, 'EXIT_OBSERVED')


def test_interruption_after_request_and_before_exit_is_safely_resumable(operation, monkeypatch):
    original = operator._attempt
    monkeypatch.setattr(operator, '_attempt', lambda _: (_ for _ in ()).throw(OSError('interruption')))
    with pytest.raises(OSError, match='interruption'):
        apply(operation)
    assert len(history(operation.lane, operator.REQUEST_EVENT)) == 1
    assert saved_book(operation.lane)['position']
    monkeypatch.setattr(operator, '_attempt', original)
    emit(operation)
    result = apply(operation)
    assert result['state'] == 'OPERATOR_PAPER_EXIT_CLOSED'
    assert len(history(operation.lane, operator.REQUEST_EVENT)) == len(history(operation.lane, 'CLOSED')) == 1


def test_candidate_exit_interruption_rolls_back_and_can_retry(operation, monkeypatch):
    begin(operation);emit(operation)
    original = paper.save

    def fail_after_save(db, book):
        original(db, book)
        raise OSError('interruption after save')

    monkeypatch.setattr(paper, 'save', fail_after_save)
    with pytest.raises(OSError, match='interruption'):
        apply(operation)
    assert saved_book(operation.lane)['position']
    assert not history(operation.lane, 'EXIT_OBSERVED')
    monkeypatch.setattr(paper, 'save', original)
    assert apply(operation)['state'] == 'OPERATOR_PAPER_EXIT_CLOSED'
    assert len(history(operation.lane, 'CLOSED')) == 1


def test_pause_and_source_qualification_are_required(operation, monkeypatch):
    (gate.ROOT / 'entries-paused').unlink()
    with pytest.raises(ValueError, match='pause'):
        apply(operation)
    assert not history(operation.lane, operator.REQUEST_EVENT)


def test_wait_releases_writer_so_managed_protection_can_deliver_new_point(operation, monkeypatch):
    monkeypatch.setattr(operator, 'WAIT_SECONDS', .5)
    deliveries = []

    def collect(_):
        emit(operation)
        deliveries.append(True)

    monkeypatch.setattr(operator.time, 'sleep', collect)
    assert apply(operation)['state'] == 'OPERATOR_PAPER_EXIT_CLOSED'
    assert deliveries == [True]


def test_command_loads_the_exact_request_file(operation, tmp_path):
    path = tmp_path / 'operator-request.json'
    write_json(path, operation.request)
    result = operator.command(SimpleNamespace(request=path, apply=False, approve=False))
    assert result['state'] == 'OPERATOR_PAPER_EXIT_PREVIEW'
