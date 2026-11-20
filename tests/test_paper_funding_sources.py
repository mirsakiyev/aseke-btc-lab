from contextlib import closing
from copy import deepcopy
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.phase9.data import settled_rates
from lab.phase9.public import funding
from lab.phase13 import observation, protocol
from lab.paper_run import funding_sources as sources
from lab.storage import utc


SLOT = utc("2026-11-19T00:00:00Z")


@pytest.fixture
def native(tmp_path, monkeypatch):
    store = {"id": fingerprint("original-authorized-source"), "storage_root": tmp_path,
             "body": {"start": (SLOT-pd.Timedelta(days=2)).isoformat(),
                      "end_exclusive": (SLOT+pd.Timedelta(days=3)).isoformat(),
                      "acquisition": dict(protocol.OBSERVATION)}}
    amendment = {"id": fingerprint("sealed-amendment"), "body": {"original_protocol": store["id"],
                 "registration_version": "phase14-feature-context-amendment-v1",
                 "acquisition": dict(protocol.OBSERVATION),
                 "start": store["body"]["start"], "end_exclusive": store["body"]["end_exclusive"],
                 "instrument": {"venue": "KuCoin", "symbol": "XBTUSDTM", "trade_type": "FUTURES", "settlement": "USDT"},
                 "rules": {"funding_receipt_grace_seconds": 7200}}}
    body = {"source_protocol": store["id"], "phase14_amendment": amendment["id"]}
    monkeypatch.setattr(sources.feeds, "amendment_pointer", lambda _: amendment)
    monkeypatch.setattr(sources.outcomes.protocol, "active", lambda: store)
    with closing(observation.db_open(store)):
        pass
    return store, amendment, body


def query(native, *, end=None, received=None, rows=None, status="RECEIVED", record=True):
    store, _, _ = native
    end = SLOT+pd.Timedelta(hours=1) if end is None else end
    received = end+pd.Timedelta(seconds=50) if received is None else received
    spec = funding("funding", max(utc(store["body"]["start"]), end-pd.Timedelta(hours=24)), end)
    raw = json.dumps({"code": "200000", "data": {"symbol": "XBTUSDTM", "list": rows or []}}, separators=(",", ":")).encode()
    digest = sha256(raw).hexdigest()
    raw_path = observation.directory(store)/"raw"/(digest+".json")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(raw)
    receipt = {"status": status, "http_status": 200 if status == "RECEIVED" else 429,
               "received_at": received.isoformat(), "raw_sha256": digest}
    with closing(observation.db_open(store)) as db, db:
        if status == "RECEIVED":
            frame = settled_rates(json.loads(raw)["data"], spec)
            receipt["rows"] = len(frame)
            if record:
                observation.store_frame(db, "funding", frame, receipt, store["body"]["acquisition"])
        identifier = db.execute("INSERT INTO request(boundary,basis,spec,reserved,receipt) VALUES(?,?,?,?,?)",
                                (end.isoformat(), "funding", observation.encode(spec),
                                 (received-pd.Timedelta(milliseconds=200)).isoformat(), observation.encode(receipt))).lastrowid
    return identifier, raw_path, receipt


def point(rate=.000062, at=SLOT):
    return {"ts": int(at.timestamp()*1000), "fundingRate": str(rate)}


def report(native, now=None):
    return sources.rates(native[2], [SLOT], SLOT+pd.Timedelta(hours=3) if now is None else now)[SLOT.isoformat()]


def test_midnight_end_exclusive_query_is_normal_pending_without_false_missing_rate(native):
    query(native, end=SLOT, rows=[point(at=SLOT-pd.Timedelta(hours=8))])
    result = report(native, SLOT+pd.Timedelta(minutes=33))
    assert result["state"] == "PENDING" and result["reason_code"] == "FUNDING_FIRST_COVERING_COLLECTION_NOT_DUE"
    assert result["first_eligible_collection_at"] == (SLOT+pd.Timedelta(hours=1, seconds=45)).isoformat()
    assert result["collection_deadline_at"] == (SLOT+pd.Timedelta(hours=2, seconds=45)).isoformat()
    assert not result["queries"][0]["covers_settlement"] and result["selected"] is None


def test_verified_native_rate_is_available_before_publication_grace_and_retains_provenance(native):
    identifier, raw, receipt = query(native, rows=[point()])
    result = report(native, SLOT+pd.Timedelta(hours=1, minutes=1))
    selected = result["selected"]
    assert result["state"] == "AVAILABLE" and selected["rate"] == .000062
    assert selected["actual_at"] == SLOT.isoformat() and selected["received_at"] == receipt["received_at"]
    assert selected["raw_sha256"] == raw.stem and selected["revision_kind"] == "ORIGINAL_ITEM"
    assert selected["evidence"]["request_id"] == identifier and selected["evidence"]["raw_bytes_verified"]
    assert selected["evidence"]["source_identity"]["basis"] == "SETTLED_FUNDING_RATE"


def test_successful_query_proves_missing_only_after_publication_grace(native):
    query(native, rows=[])
    assert report(native, SLOT+pd.Timedelta(hours=1, minutes=2))["state"] == "PENDING"
    query(native, end=SLOT+pd.Timedelta(hours=2), rows=[])
    result = report(native)
    assert result["state"] == "MISSING" and result["reason_code"] == "NATIVE_SETTLED_RATE_MISSING_AFTER_GRACE"


def test_absent_confirming_query_remains_explicitly_unconfirmed(native):
    result = report(native)
    assert result["state"] == "UNAVAILABLE" and result["reason_code"] == "FUNDING_FINALITY_QUERY_UNCONFIRMED"
    assert not result["queries"] and not result["candidates"]


def test_failed_covering_query_is_not_misreported_as_missing_or_zero(native):
    query(native, status="RATE_LIMITED")
    result = report(native)
    assert result["state"] == "UNAVAILABLE" and result["reason_code"] == "FUNDING_COVERING_QUERY_FAILED"
    assert result["queries"][0]["status"] == "RATE_LIMITED" and result["selected"] is None


def test_correction_is_retained_and_cannot_silently_replace_original_rate(native):
    _, old_raw, _ = query(native, rows=[point(.000062)])
    _, new_raw, _ = query(native, end=SLOT+pd.Timedelta(hours=2), rows=[point(.000079)])
    result = report(native)
    assert result["state"] == "CONFLICT" and result["selected"] is None
    assert {row["raw_sha256"] for row in result["candidates"]} == {old_raw.stem, new_raw.stem}
    assert {row["revision_kind"] for row in result["candidates"]} == {"ORIGINAL_ITEM", "CORRECTION"}
    with closing(observation.db_open(native[0], readonly=True)) as db:
        assert json.loads(db.execute("SELECT body FROM item WHERE basis='funding'").fetchone()[0])["rate"] == .000062
        assert db.execute("SELECT count(*) FROM correction WHERE basis='funding'").fetchone()[0] == 1


def test_changed_native_timestamp_is_a_conflict_even_if_rate_is_unchanged(native):
    query(native, rows=[point()])
    query(native, end=SLOT+pd.Timedelta(hours=2), rows=[point(at=SLOT+pd.Timedelta(seconds=1))])
    result = report(native)
    assert result["state"] == "CONFLICT" and result["selected"] is None
    assert len({row["actual_at"] for row in result["candidates"]}) == 2


def test_source_clock_offset_within_native_tolerance_still_requires_exact_raw_match(native):
    native_at = SLOT-pd.Timedelta(seconds=2)
    query(native, end=SLOT, rows=[point(at=native_at)])
    result = report(native, SLOT+pd.Timedelta(minutes=1))
    assert result["state"] == "AVAILABLE" and result["selected"]["actual_at"] == native_at.isoformat()


@pytest.mark.parametrize("field,change,code", [
    ("source_close", (SLOT+pd.Timedelta(seconds=1)).isoformat(), "FUNDING_SETTLEMENT_TIME_INCONSISTENT"),
    ("received_at", (SLOT+pd.Timedelta(days=1)).isoformat(), "FUNDING_SETTLEMENT_TIME_INCONSISTENT"),
    ("body", '{"rate":0.000079,"settlement_at":"2026-11-19T00:00:00+00:00"}', "FUNDING_ITEM_DIFFERS_FROM_NATIVE_RAW"),
])
def test_mutated_source_rows_never_produce_selected_financing(native, field, change, code):
    query(native, rows=[point()])
    with closing(observation.db_open(native[0])) as db, db:
        db.execute("UPDATE item SET "+field+"=? WHERE basis='funding'", (change,))
    result = report(native)
    assert result["state"] == "UNAVAILABLE" and result["selected"] is None
    assert any(row["reason_code"] == code for row in result["rejected_evidence"])


@pytest.mark.parametrize("mutation", ["bytes", "missing", "request-source", "receipt-count"])
def test_unverifiable_native_raw_or_request_provenance_fails_closed(native, mutation):
    identifier, raw, _ = query(native, rows=[point()])
    if mutation == "bytes":
        raw.write_bytes(b"\x00"*88)
    elif mutation == "missing":
        raw.unlink()
    else:
        with closing(observation.db_open(native[0])) as db, db:
            spec, receipt = (json.loads(value) for value in db.execute('SELECT spec,receipt FROM request WHERE id=?',(identifier,)).fetchone())
            if mutation == "request-source":
                spec["url"] = "https://unreviewed.example/funding"
            else:
                receipt["rows"] = 99
            db.execute('UPDATE request SET spec=?,receipt=? WHERE id=?',(observation.encode(spec),observation.encode(receipt),identifier))
    result = report(native)
    assert result["state"] == "UNAVAILABLE" and result["selected"] is None
    assert result["rejected_evidence"]


def test_authority_mismatch_and_wrong_instrument_are_explicit(native):
    wrong = {**native[2], "phase14_amendment": "0"*64}
    assert sources.rates(wrong,[SLOT],SLOT+pd.Timedelta(hours=2))[SLOT.isoformat()]["reason_code"] == "FUNDING_SOURCE_AUTHORITY_UNAVAILABLE"
    native[1]["body"]["instrument"]["symbol"] = "ETHUSDTM"
    result = report(native)
    assert result["state"] == "UNAVAILABLE" and result["rejected_evidence"][0]["reason_code"] == "FUNDING_SOURCE_INSTRUMENT_MISMATCH"


def test_missing_source_store_is_not_created_or_replaced(native):
    path = observation.directory(native[0])/"observations.sqlite3"
    path.unlink()
    assert report(native)["state"] == "UNAVAILABLE"
    assert not path.exists()


def test_future_receipt_is_rejected_and_zero_rate_is_real_evidence(native):
    query(native, rows=[point(0.)])
    assert report(native,SLOT+pd.Timedelta(hours=1,seconds=10))["state"] == "UNAVAILABLE"
    result = report(native)
    assert result["state"] == "AVAILABLE" and result["selected"]["rate"] == 0.


def test_repeated_reads_and_late_failed_query_do_not_alter_a_verified_native_item(native):
    query(native,rows=[point()])
    query(native,end=SLOT+pd.Timedelta(hours=2),status="RATE_LIMITED")
    path=observation.directory(native[0])/"observations.sqlite3"
    before=path.read_bytes()
    first=report(native); second=report(native)
    assert first==second and first["state"]=="AVAILABLE" and path.read_bytes()==before
    assert sources.rates(native[2],[],SLOT)=={}


def test_invalid_correction_cannot_be_ignored_in_favor_of_valid_original(native):
    query(native,rows=[point()])
    _,raw,_=query(native,end=SLOT+pd.Timedelta(hours=2),rows=[point(.000079)])
    raw.write_bytes(b'corrupted correction bytes')
    result=report(native)
    assert result['state']=='UNAVAILABLE' and result['selected'] is None
    assert len(result['candidates'])==1 and result['candidates'][0]['revision_kind']=='ORIGINAL_ITEM'
    assert any(row['revision_kind']=='CORRECTION' for row in result['rejected_evidence'])


def test_unfinished_request_has_a_bounded_actionable_wait(native):
    identifier,_,_=query(native,rows=[])
    with closing(observation.db_open(native[0])) as db, db:
        db.execute('UPDATE request SET receipt=NULL WHERE id=?',(identifier,))
    assert report(native,SLOT+pd.Timedelta(hours=1,minutes=1))['state']=='PENDING'
    result=report(native,SLOT+pd.Timedelta(hours=1,minutes=3))
    assert result['state']=='UNAVAILABLE' and result['reason_code']=='FUNDING_QUERY_RECEIPT_MISSING'


def test_native_response_internal_conflict_is_reported_without_a_rate_winner(native):
    identifier,raw,receipt=query(native,rows=[point()])
    payload=json.loads(raw.read_bytes()); payload['data']['list'].append(point(.000079))
    encoded=json.dumps(payload,separators=(',',':')).encode(); digest=sha256(encoded).hexdigest()
    changed=raw.with_name(digest+'.json'); changed.write_bytes(encoded)
    receipt.update(raw_sha256=digest,rows=2)
    with closing(observation.db_open(native[0])) as db, db:
        db.execute('UPDATE request SET receipt=? WHERE id=?',(observation.encode(receipt),identifier))
        db.execute("UPDATE item SET raw_sha=? WHERE basis='funding'",(digest,))
    result=report(native)
    assert result['state']=='CONFLICT' and result['selected'] is None
    assert any(row['reason_code']=='FUNDING_NATIVE_RESPONSE_CONFLICT' for row in result['rejected_evidence'])


def test_revision_capacity_has_an_explicit_review_path_instead_of_silent_truncation(native,monkeypatch):
    query(native,rows=[point()]); query(native,end=SLOT+pd.Timedelta(hours=2),rows=[point(.000079)])
    monkeypatch.setattr(sources,'MAX_REVISIONS',1)
    result=report(native)
    assert result['state']=='UNAVAILABLE' and result['selected'] is None
    assert result['reason_code']=='FUNDING_REVISION_CAPACITY_REQUIRES_REVIEW'
    assert result['next_action']=='PRESERVE_AND_REVIEW_INVALID_OR_INACCESSIBLE_SOURCE_EVIDENCE'


def test_query_history_is_bounded_but_all_rate_revisions_are_separately_checked(native,monkeypatch):
    for number in range(4):
        query(native,rows=[],received=SLOT+pd.Timedelta(hours=1,seconds=50+number))
    monkeypatch.setattr(sources,'MAX_QUERIES',2)
    result=report(native,SLOT+pd.Timedelta(hours=1,minutes=2))
    assert result['state']=='PENDING' and len(result['queries'])==2 and result['query_history_truncated']


def test_inspection_never_starts_a_market_client_or_financial_write(native,monkeypatch):
    query(native,rows=[point()])
    path=observation.directory(native[0])/'observations.sqlite3'; before=path.read_bytes()
    def forbidden(*args,**kwargs):
        raise AssertionError('funding evidence inspection must not acquire market data')
    monkeypatch.setattr('httpx.Client',forbidden)
    assert report(native)['state']=='AVAILABLE'
    assert path.read_bytes()==before


def closeout(native,monkeypatch,*,boundary_suffix='/funding-closeout',delay=7201,left_shift=0):
    original,amendment,body=native
    original['body']['end_exclusive']=(SLOT-pd.Timedelta(hours=4)).isoformat()
    amendment['body']['end_exclusive']=(SLOT+pd.Timedelta(hours=1)).isoformat()
    tail=sources.outcomes.tail_protocol(amendment,original)
    tail['storage_root']=original['storage_root']
    monkeypatch.setattr(sources.outcomes,'tail_protocol',lambda *_:tail)
    end=utc(tail['body']['end_exclusive']); left=end-pd.Timedelta(hours=24)+pd.Timedelta(minutes=left_shift)
    spec=funding('amended-funding-closeout',left,end)
    received=end+pd.Timedelta(seconds=delay)
    raw=json.dumps({'code':'200000','data':{'symbol':'XBTUSDTM','list':[point()]}},separators=(',',':')).encode()
    digest=sha256(raw).hexdigest(); rawpath=observation.directory(tail)/'raw'/(digest+'.json')
    rawpath.parent.mkdir(parents=True,exist_ok=True); rawpath.write_bytes(raw)
    receipt={'status':'RECEIVED','http_status':200,'raw_sha256':digest,'received_at':received.isoformat(),'rows':1}
    with closing(observation.db_open(tail)) as db,db:
        observation.store_frame(db,'funding',settled_rates(json.loads(raw)['data'],spec),receipt,tail['body']['acquisition'])
        db.execute('INSERT INTO request(boundary,basis,spec,reserved,receipt) VALUES(?,?,?,?,?)',
                   (end.isoformat()+boundary_suffix,'funding',observation.encode(spec),
                    (received-pd.Timedelta(milliseconds=200)).isoformat(),observation.encode(receipt)))
    return tail


def test_existing_authorized_closeout_can_cross_tail_start_without_becoming_a_new_permission(native,monkeypatch):
    tail=closeout(native,monkeypatch)
    result=report(native,SLOT+pd.Timedelta(hours=3,minutes=1))
    assert result['state']=='AVAILABLE' and result['selected']['store_id']==tail['id']
    assert result['first_eligible_collection_at']==(SLOT+pd.Timedelta(hours=1,seconds=45)).isoformat()
    assert result['collection_deadline_at']==(SLOT+pd.Timedelta(hours=3)).isoformat()
    assert utc(result['queries'][0]['start'])<utc(tail['body']['start'])


@pytest.mark.parametrize('change',[{'boundary_suffix':'/unreviewed-closeout'},{'delay':3601},{'left_shift':1}])
def test_closeout_scope_boundary_and_grace_cannot_be_widened(native,monkeypatch,change):
    closeout(native,monkeypatch,**change)
    result=report(native,SLOT+pd.Timedelta(hours=3,minutes=1))
    assert result['state']=='UNAVAILABLE' and result['selected'] is None
    assert result['rejected_evidence']


@pytest.fixture
def archive():
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE observation(source TEXT,at TEXT,event_id TEXT,body TEXT,PRIMARY KEY(source,event_id))')
    lane={"source":sources.capabilities.MARK,"symbol":"XBTUSDTM","observation_kind":"HTTP_SNAPSHOT","price_basis":"KUCOIN_XBTUSDTM_MARK"}
    position={"id":"actual-position","entry_at":(SLOT-pd.Timedelta(minutes=5)).isoformat()}
    receipt=SLOT+pd.Timedelta(seconds=3.474286)
    point={"source":lane["source"],"symbol":lane["symbol"],"status":"RECEIVED","source_at":(SLOT+pd.Timedelta(seconds=2)).isoformat(),
           "received_at":receipt.isoformat(),"requested_at":(receipt-pd.Timedelta(milliseconds=469)).isoformat(),
           "request_mono_ns":100000000000,"received_mono_ns":100469000000,"elapsed_ns":469000000,
           "price":63031.88,"price_text":"63031.88","raw_sha256":"a"*64}
    point['event_id']=point['source_at']
    yield db,lane,position,point
    db.close()


def save_archive(archive):
    db,lane,_,point=archive
    encoded=json.dumps(point,sort_keys=True,separators=(',',':'))
    db.execute('INSERT INTO observation VALUES(?,?,?,?)',(lane['source'],point['source_at'],point['event_id'],encoded))
    db.commit()
    return encoded


def test_preserved_accepted_forward_observation_is_reviewable_without_recasting_as_fresh(archive):
    db,lane,position,point=archive
    encoded=save_archive(archive)
    candidate=sources.archived_forward(db,lane,position,SLOT,SLOT+pd.Timedelta(hours=3))[0]
    assert candidate['observation']==point and candidate['observation_sha256']==fingerprint(point)
    assert candidate['observation_body_sha256']==sha256(encoded.encode()).hexdigest()
    assert candidate['observation_event_id']==point['event_id'] and candidate['price']==63031.88
    assert candidate['recovery_label']=='PRESERVED_ACCEPTED_FORWARD_EVIDENCE' and not candidate['historical_reconstruction']
    assert candidate['raw_hash_status']==sources.RAW_HASH_LIMITATION
    assert db.execute('SELECT count(*) FROM observation').fetchone()[0]==1


@pytest.mark.parametrize('mutation',['stale','future','monotonic','identity','raw-hash','naive','wrong-symbol'])
def test_invalid_preserved_observation_never_becomes_a_recovery_candidate(archive,mutation):
    db,lane,position,point=archive
    if mutation=='stale':
        point['received_at']=(SLOT+pd.Timedelta(seconds=6)).isoformat()
        point['requested_at']=(SLOT+pd.Timedelta(seconds=5.531)).isoformat()
    elif mutation=='future':
        point['received_at']=(SLOT+pd.Timedelta(days=1)).isoformat()
    elif mutation=='monotonic':
        point['elapsed_ns']=1
    elif mutation=='identity':
        point['event_id']='another-event'
    elif mutation=='raw-hash':
        point['raw_sha256']='not-a-hash'
    elif mutation=='naive':
        point['requested_at']='2026-11-19T00:00:03'
    else:
        point['symbol']='ETHUSDTM'
    save_archive(archive)
    with pytest.raises(sources.FundingEvidenceError):
        sources.archived_forward(db,lane,position,SLOT,SLOT+pd.Timedelta(hours=3))


def test_candles_current_marks_and_out_of_window_points_are_not_a_forward_fallback(archive):
    db,lane,position,point=archive
    point['source_at']=(SLOT+pd.Timedelta(minutes=15)).isoformat(); point['event_id']=point['source_at']
    save_archive(archive)
    assert sources.archived_forward(db,lane,position,SLOT,SLOT+pd.Timedelta(hours=3))==[]
    wrong={**lane,'source':sources.capabilities.SPOT,'symbol':'BTC-USDT','price_basis':'KUCOIN_SPOT_LAST'}
    with pytest.raises(sources.FundingEvidenceError,match='ARCHIVED_FORWARD_SOURCE_UNSUPPORTED'):
        sources.archived_forward(db,wrong,position,SLOT,SLOT+pd.Timedelta(hours=3))


def test_archived_reference_must_match_original_exposure_interval(archive):
    db,lane,position,_=archive
    save_archive(archive)
    position['entry_at']=SLOT.isoformat()
    with pytest.raises(sources.FundingEvidenceError,match='POSITION_INTERVAL_INVALID'):
        sources.archived_forward(db,lane,position,SLOT,SLOT+pd.Timedelta(hours=3))
