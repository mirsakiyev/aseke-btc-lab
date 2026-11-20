"""Funding failure and price protection share the actual writer transaction."""
from dataclasses import asdict
from types import SimpleNamespace

from lab.paper_run import runtime,execution
from lab.phase15 import paper
from lab.phase9.ideas import current_rules
from test_paper_run_execution import TERMS,CONFIG
from test_paper_funding_lifecycle import SLOT,finance


def prepare(f,monkeypatch):
    f.book['position']['plan'].update(contract=asdict(current_rules('FUTURES',TERMS,CONFIG)),cost={'taker':.0006})
    f.book['position'].update(mark_source_at=(SLOT-funding_half_second()).isoformat(),
                              mark_received_at=(SLOT-funding_half_second()).isoformat(),mark_event_id='previous')
    monkeypatch.setattr(runtime,'current',lambda:{'id':'generation'})
    monkeypatch.setattr(runtime,'utc_now',lambda:f.now[0])
    monkeypatch.setattr(execution,'utc_now',lambda:f.now[0])
    monkeypatch.setattr(execution,'time',SimpleNamespace(monotonic_ns=lambda:f.now[0].value))
    paper.save(f.db,f.book);f.db.commit()


def funding_half_second():
    import pandas as pd
    return pd.Timedelta(milliseconds=500)


def test_runtime_keeps_protecting_during_a_normal_pending_rate(finance,monkeypatch):
    f=finance;prepare(f,monkeypatch)
    runtime._apply_live_point(f.db,f.book,f.lane,{},f.point(),metadata=f.metadata)
    saved=paper.load(f.db,f.lane)
    assert saved['position']['funding_state']['state']=='FUNDING_SETTLEMENT_PENDING'
    assert saved['position']['mark_event_id']==f.point()['event_id']
    assert not saved['position'].get('unresolved_protection')
    assert saved['position']['stop']==110. and saved['position']['position']['remaining']==2.


def test_runtime_keeps_protecting_during_a_native_funding_conflict(finance,monkeypatch):
    f=finance;prepare(f,monkeypatch)
    f.state.clear();f.state.update(state='CONFLICT',reason_code='NATIVE_RATE_REVISIONS_CONFLICT')
    runtime._apply_live_point(f.db,f.book,f.lane,{},f.point(),metadata=f.metadata)
    saved=paper.load(f.db,f.lane)
    assert saved['position']['funding_state']['state']=='FUNDING_CONFLICT'
    assert saved['position']['mark_event_id']==f.point()['event_id']
    assert saved['funding']==0. and saved['cash']==10000.
    assert not saved['position'].get('unresolved_protection')
