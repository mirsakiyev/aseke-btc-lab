"""Only an approved exact preserved observation can repair a missing projection."""
from copy import deepcopy
import json

import pandas as pd
import pytest

from lab.paper_run import funding_commands as commands, funding_sources, source_amendment
from lab.phase15 import paper
from test_paper_funding_lifecycle import SLOT, finance


@pytest.fixture
def review(finance, monkeypatch):
    f = finance
    f.db.execute('CREATE TABLE observation(source TEXT,at TEXT,event_id TEXT,body TEXT,PRIMARY KEY(source,event_id))')
    f.point_value = f.point()
    f.db.execute('INSERT INTO observation VALUES (?,?,?,?)', (f.lane['source'],f.point_value['source_at'],
                 f.point_value['event_id'],json.dumps(f.point_value,sort_keys=True)))
    paper.save(f.db,f.book)
    f.db.commit()
    monkeypatch.setattr(source_amendment,'active',lambda _:None)
    f.slot='breakout/swing/fixed'
    f.record={'id':'generation','body':{'lanes':{f.slot:f.lane}}}
    f.now[0]=SLOT+pd.Timedelta(minutes=15)
    candidate,=funding_sources.archived_forward(f.db,f.lane,f.book['position'],SLOT,f.now[0])
    f.request={'protocol':commands.VERSION,'generation':f.record['id'],'slot':f.slot,'book_id':f.book['book_id'],
               'position_id':f.book['position']['id'],'settlement_at':SLOT.isoformat(),
               'observation_event_id':candidate['observation_identity']['event_id'],
               'observation_sha256':candidate['observation_sha256'],
               'observation_body_sha256':candidate['observation_body_sha256'],
               'review_token':commands._token(f.db,f.record,f.lane,f.book),
               'request_id':'approved-preserved-reference','reason':'Explicit isolated review of an actual archived forward observation'}
    return f


def perform(f,request=None,**kwargs):
    return commands.restore(f.db,f.record,request or f.request,f.now[0],f.metadata,**kwargs)


def test_preview_is_read_only_and_cannot_invent_or_substitute_current_evidence(review):
    before,changes=deepcopy(review.book),review.db.total_changes
    result=perform(review)
    assert result['state']=='FUNDING_RECONCILIATION_PREVIEW'
    assert result['would_restore']['evidence']['recovery_label']=='PRESERVED_ACCEPTED_FORWARD_EVIDENCE'
    assert result['would_restore']['historical_liquidation_path']=='NOT_RETROACTIVELY_VERIFIED'
    assert review.book==before and review.db.total_changes==changes
    bad={**review.request,'observation_sha256':'0'*64}
    with pytest.raises(ValueError,match='PRESERVED_FORWARD_EVIDENCE_NOT_VERIFIED'):
        perform(review,bad)


def test_exact_approved_recovery_and_native_settlement_are_atomic_and_idempotent(review,monkeypatch):
    monkeypatch.setattr(commands,'_adopted',lambda *_:None)
    review.available()
    with review.db:
        review.db.execute('BEGIN IMMEDIATE')
        result=perform(review,apply=True,approved=True)
    assert result['state']=='FUNDING_RECONCILIATION_APPLIED'
    book=paper.load(review.db,review.lane)
    assert book['position']['funding_provisional']
    ref=book['position']['funding_references'][SLOT.isoformat()]
    assert ref['price']==review.point_value['price'] and ref['source_at']==review.point_value['source_at']
    assert ref['received_at']==review.point_value['received_at'] and ref['restored_at']==review.now[0].isoformat()
    assert ref['historical_reconciliation'] and ref['evidence']['raw_hash_status']=='RECORDED_HASH_ONLY_RAW_BYTES_NOT_RETAINED'
    assert book['cash']==pytest.approx(10000.2)
    with review.db:
        review.db.execute('BEGIN IMMEDIATE')
        repeated=perform(review,apply=True,approved=True)
    assert repeated['state']=='ALREADY_APPLIED'
    assert paper.load(review.db,review.lane)['cash']==book['cash']
    assert review.db.execute("SELECT count(*) FROM event WHERE kind='FUNDING'").fetchone()[0]==1
    assert review.db.execute("SELECT count(*) FROM event WHERE kind=?",(commands.KIND,)).fetchone()[0]==1
    assert not review.db.execute("SELECT 1 FROM event WHERE kind IN ('HYPOTHETICAL_FILL','EXIT_OBSERVED')").fetchone()
    with pytest.raises(ValueError,match='REQUEST_REUSED'):
        perform(review,{**review.request,'reason':'Changed arguments reuse the same request ID'})


def test_interrupted_restoration_rolls_back_reference_money_and_approval_together(review,monkeypatch):
    monkeypatch.setattr(commands,'_adopted',lambda *_:None)
    before=paper.load(review.db,review.lane)
    review.available()
    with pytest.raises(OSError,match='interrupted'):
        with review.db:
            review.db.execute('BEGIN IMMEDIATE')
            perform(review,apply=True,approved=True)
            raise OSError('interrupted')
    assert paper.load(review.db,review.lane)==before
    assert not review.db.execute("SELECT 1 FROM event WHERE kind IN ('FUNDING',?)",(commands.KIND,)).fetchone()


@pytest.mark.parametrize('change,reason', [
    ({'position_id':'a-different-position'},'POSITION_CHANGED'),
    ({'review_token':'an-old-token'},'REVIEW_TOKEN_CHANGED'),
    ({'settlement_at':(SLOT+pd.Timedelta(minutes=1)).isoformat()},'NOT_AN_EXPECTED_OBLIGATION'),
    ({'generation':'another-generation'},'GENERATION_MISMATCH'),
])
def test_changed_target_or_scope_is_rejected(review,change,reason):
    with pytest.raises(ValueError,match=reason):
        perform(review,{**review.request,**change})


def test_unadopted_code_and_missing_approval_cannot_write_the_financial_book(review,monkeypatch):
    with pytest.raises(PermissionError,match='EXPLICIT_APPROVAL'):
        with review.db:
            review.db.execute('BEGIN IMMEDIATE')
            perform(review,apply=True)
    def unadopted(*_):
        raise ValueError('FUNDING_REVIEWED_WRITER_BUILD_NOT_ADOPTED')
    monkeypatch.setattr(commands,'_adopted',unadopted)
    with pytest.raises(ValueError,match='NOT_ADOPTED'):
        with review.db:
            review.db.execute('BEGIN IMMEDIATE')
            perform(review,apply=True,approved=True)
    assert not review.db.execute('SELECT 1 FROM event WHERE kind=?',(commands.KIND,)).fetchone()


def test_other_exposure_does_not_require_an_all_flat_recovery_or_modify_other_book(review,monkeypatch):
    other_lane={**review.lane,'book_id':'other-current-book'}
    other=deepcopy(review.book);other['book_id']=other_lane['book_id'];other['position']['id']='unrelated-open'
    with review.db:
        paper.save(review.db,other)
    review.record['body']['lanes']['other/supported']=other_lane
    monkeypatch.setattr(commands,'_adopted',lambda *_:None)
    with review.db:
        review.db.execute('BEGIN IMMEDIATE')
        perform(review,apply=True,approved=True)
    assert paper.load(review.db,other_lane)==other
