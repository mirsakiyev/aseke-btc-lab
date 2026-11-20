from copy import deepcopy
import json
import sqlite3

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.paper_run import consumer_spool as spool
from lab.storage import read_json,utc,write_json


AT=utc('2026-11-18T23:00:00Z')
GENERATION='a'*64
SCOPE='b'*64


@pytest.fixture
def root(tmp_path):
    write_json(tmp_path/'current.json',{'id':GENERATION})
    with sqlite3.connect(tmp_path/'paper.sqlite3') as db:
        db.execute('CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT)')
    return tmp_path


def point(identifier,at=AT):
    return {'source':'KRAKEN_BTC_USD_LAST_TRADE','symbol':'BTC/USD','observation_kind':'WEBSOCKET_TRADE',
            'event_id':str(identifier),'source_at':(at-pd.Timedelta(milliseconds=150)).isoformat(),
            'received_at':(at-pd.Timedelta(milliseconds=25)).isoformat(),
            'price':100.,'raw_sha256':'c'*64,'do_not_preserve_raw_payload':'secret test sentinel'}


def journal(root,witness,*,at=AT):
    identifier=fingerprint(['actual-witness-journal',witness['id']])
    with sqlite3.connect(root/'paper.sqlite3') as db:
        db.execute('INSERT INTO event VALUES(?,?,?,?,?,?,?)',(
            identifier,'SOURCE:'+SCOPE,'PROTECTION_TRANSPORT_WITNESS',witness['id'],at.isoformat(),at.isoformat(),
            json.dumps({'generation':GENERATION,'source_scope_id':SCOPE,'witness':witness})))
    return identifier


def test_actual_local_loss_survives_restart_without_prices_or_credentials(root):
    result=spool.record_loss(root,GENERATION,SCOPE,[point(1),point(2)],at=AT)
    saved,=spool.pending(root,GENERATION)
    assert saved==result['witness']
    assert saved['kind']=='CONSUMER_QUEUE_OVERFLOW'
    assert saved['cause']=='LOCAL_FINANCIAL_CONSUMER_BUFFER_CAPACITY'
    assert saved['exchange_disconnect'] is False
    assert saved['first_event_id']=='1' and saved['last_event_id']=='2'
    assert saved['dropped_count_lower_bound']==2
    raw=(root/spool.FILE).read_text(encoding='utf-8')
    assert 'secret test sentinel' not in raw and '"price"' not in raw
    assert len(raw.encode('utf-8'))<spool.MAX_BYTES
    envelope=read_json(root/spool.FILE)
    assert envelope['sha256']==fingerprint(envelope['body'])


def test_repeated_loss_is_idempotent_and_extensions_keep_the_episode(root):
    first=spool.record_loss(root,GENERATION,SCOPE,[point(1),point(2)],at=AT)['witness']
    original=(root/spool.FILE).read_bytes()
    assert spool.record_loss(root,GENERATION,SCOPE,[point(1),point(2)],at=AT+pd.Timedelta(seconds=1))['state']=='ALREADY_RECORDED'
    assert (root/spool.FILE).read_bytes()==original
    second=spool.record_loss(root,GENERATION,SCOPE,[point(2),point(3,AT+pd.Timedelta(seconds=1))],at=AT+pd.Timedelta(seconds=1))['witness']
    assert first['id']!=second['id'] and first['correlation_id']==second['correlation_id']
    assert second['first_event_id']=='1' and second['last_event_id']=='3'
    assert second['dropped_count_lower_bound']==3
    assert second['first_detected_at']==AT.isoformat()
    assert second['last_detected_at']==(AT+pd.Timedelta(seconds=1)).isoformat()


def test_uncommitted_or_ambiguous_journal_does_not_remove_the_loss(root):
    witness=spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)['witness']
    before=(root/spool.FILE).read_bytes()
    with pytest.raises(ValueError,match='NOT_COMMITTED'):
        spool.mark_committed(root,GENERATION,[witness['id']],at=AT)
    assert (root/spool.FILE).read_bytes()==before
    with sqlite3.connect(root/'paper.sqlite3') as db:
        db.execute('INSERT INTO event VALUES(?,?,?,?,?,?,?)',('uncommitted','source','PROTECTION_TRANSPORT_WITNESS',witness['id'],AT.isoformat(),AT.isoformat(),json.dumps({'generation':GENERATION,'witness':witness})))
        # Deliberately roll back the candidate journal transaction.
        db.rollback()
    with pytest.raises(ValueError,match='NOT_COMMITTED'):
        spool.mark_committed(root,GENERATION,[witness['id']],at=AT)
    assert spool.pending(root,GENERATION)==[witness]


def test_committed_revision_cannot_acknowledge_a_later_coalesced_revision(root):
    first=spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)['witness']
    journal(root,first)
    second=spool.record_loss(root,GENERATION,SCOPE,[point(2,AT+pd.Timedelta(seconds=1))],at=AT+pd.Timedelta(seconds=1))['witness']
    result=spool.mark_committed(root,GENERATION,[first['id']],at=AT+pd.Timedelta(seconds=2))
    assert not result['committed_ids'] and spool.pending(root,GENERATION)==[second]
    event_id=journal(root,second,at=AT+pd.Timedelta(seconds=2))
    result=spool.mark_committed(root,GENERATION,[second['id']],at=AT+pd.Timedelta(seconds=3))
    assert result['committed_ids']==[second['id']] and not spool.pending(root,GENERATION)
    body=read_json(root/spool.FILE)['body']
    assert body['checkpoints'][SCOPE]['journal_evidence']['event_id']==event_id
    original=(root/spool.FILE).read_bytes()
    spool.mark_committed(root,GENERATION,[second['id']],at=AT+pd.Timedelta(seconds=4))
    assert (root/spool.FILE).read_bytes()==original
    assert spool.record_loss(root,GENERATION,SCOPE,[point(1),point(2)],at=AT+pd.Timedelta(seconds=5))['state']=='ALREADY_COMMITTED'


def test_new_loss_after_commit_has_new_episode_and_preserves_previous_proof(root):
    old=spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)['witness']
    event_id=journal(root,old)
    spool.mark_committed(root,GENERATION,[old['id']],at=AT+pd.Timedelta(seconds=1))
    new=spool.record_loss(root,GENERATION,SCOPE,[point(2,AT+pd.Timedelta(seconds=2))],at=AT+pd.Timedelta(seconds=2))['witness']
    assert new['correlation_id']!=old['correlation_id']
    assert read_json(root/spool.FILE)['body']['checkpoints'][SCOPE]['journal_evidence']['event_id']==event_id


@pytest.mark.parametrize('damage',[b'\x00'*88,b'{}',b'{broken json'])
def test_corruption_is_reported_with_original_hash_and_never_replaced(root,damage):
    (root/spool.FILE).write_bytes(damage)
    with pytest.raises(spool.CorruptSpool) as exc:
        spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)
    from hashlib import sha256
    assert exc.value.sha256==sha256(damage).hexdigest() and exc.value.size==len(damage)
    assert (root/spool.FILE).read_bytes()==damage


def test_wrong_generation_or_source_scope_fails_without_touching_existing_metadata(root):
    spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)
    before=(root/spool.FILE).read_bytes()
    with pytest.raises(ValueError,match='GENERATION'):
        spool.pending(root,'d'*64)
    with pytest.raises(ValueError,match='SOURCE_SCOPE'):
        spool.record_loss(root,GENERATION,'not-a-scope',[point(2)],at=AT)
    assert (root/spool.FILE).read_bytes()==before


def test_failed_atomic_spool_write_keeps_prior_loss_for_retry(root,monkeypatch):
    first=spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)['witness']
    before=(root/spool.FILE).read_bytes()
    original=spool.write_json
    monkeypatch.setattr(spool,'write_json',lambda *_:(_ for _ in ()).throw(OSError('disk write failed')))
    with pytest.raises(OSError,match='disk write failed'):
        spool.record_loss(root,GENERATION,SCOPE,[point(2,AT+pd.Timedelta(seconds=1))],at=AT+pd.Timedelta(seconds=1))
    assert (root/spool.FILE).read_bytes()==before
    monkeypatch.setattr(spool,'write_json',original)
    result=spool.record_loss(root,GENERATION,SCOPE,[point(2,AT+pd.Timedelta(seconds=1))],at=AT+pd.Timedelta(seconds=1))
    assert result['witness']['correlation_id']==first['correlation_id']
    assert result['witness']['dropped_count_lower_bound']==2


def test_foreign_or_modified_witness_cannot_supply_commit_proof(root):
    witness=spool.record_loss(root,GENERATION,SCOPE,[point(1)],at=AT)['witness']
    changed=deepcopy(witness);changed['reason']='INVENTED'
    journal(root,changed)
    with pytest.raises(ValueError,match='NOT_COMMITTED'):
        spool.mark_committed(root,GENERATION,[witness['id']],at=AT)
    assert spool.pending(root,GENERATION)==[witness]
