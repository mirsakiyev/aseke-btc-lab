"""Atomic bounded loss witnesses for the managed consumer's local retry buffer.

This is local evidence, never an exchange disconnect or a source of fill prices.
A marker survives restart until the actual immutable witness journal proves its
matching revision committed. Corrupt state is reported with its original hash;
it is never replaced with an empty default.
"""

from contextlib import closing
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.storage import read_json, utc, write_json, writer_lock


SCHEMA = "PAPER_CONSUMER_LOSS_SPOOL_V1"
FILE = "consumer-losses.json"
MAX_BYTES = 64 * 1024
MAX_SCOPES = 8
MAX_BATCH = 4096
KIND = "CONSUMER_QUEUE_OVERFLOW"


class CorruptSpool(ValueError):
    def __init__(self, path, raw, reason):
        self.path, self.size, self.sha256 = str(path), len(raw), sha256(raw).hexdigest()
        super().__init__(f"CONSUMER_LOSS_SPOOL_CORRUPT: {reason}; path={self.path}; bytes={self.size}; sha256={self.sha256}")


def _stamp(value):
    value = pd.Timestamp(value)
    if pd.isna(value) or value.tzinfo is None:
        raise ValueError("CONSUMER_LOSS_TIMESTAMP_REQUIRES_OFFSET")
    return utc(value)


def _generation(root, generation):
    if not isinstance(generation, str) or not re.fullmatch('[0-9a-f]{64}', generation):
        raise ValueError('CONSUMER_LOSS_INVALID_GENERATION')
    pointer = root/'current.json'
    if pointer.exists() and read_json(pointer).get('id') != generation:
        raise ValueError('CONSUMER_LOSS_CURRENT_GENERATION_MISMATCH')


def _witness_id(witness):
    return fingerprint([SCHEMA, {key:value for key,value in witness.items() if key != 'id'}])


def _load(root, generation):
    _generation(root,generation)
    path=root/FILE
    if not path.exists():
        return {'schema':SCHEMA,'generation':generation,'revision':0,'pending':{},'checkpoints':{}}
    raw=path.read_bytes()
    try:
        if path.is_symlink() or len(raw)>MAX_BYTES:
            raise ValueError('invalid path or resource bound')
        envelope=json.loads(raw.decode('utf-8'))
        body=envelope['body']
        if envelope['sha256']!=fingerprint(body) or body['schema']!=SCHEMA:
            raise ValueError('schema or checksum mismatch')
        if body['generation']!=generation:
            raise ValueError('generation mismatch')
        if type(body['revision']) is not int or body['revision']<0:
            raise ValueError('invalid revision')
        if not isinstance(body['pending'],dict) or not isinstance(body['checkpoints'],dict):
            raise ValueError('invalid pending/checkpoint maps')
        scopes=set(body['pending'])|set(body['checkpoints'])
        if len(scopes)>MAX_SCOPES or any(not re.fullmatch('[0-9a-f]{64}',scope) for scope in scopes):
            raise ValueError('invalid source scope or capacity')
        for scope,witness in body['pending'].items():
            if (witness.get('id')!=_witness_id(witness) or witness.get('generation')!=generation
                    or witness.get('source_scope_id')!=scope or witness.get('kind')!=KIND):
                raise ValueError('witness identity mismatch')
        return body
    except (UnicodeError,ValueError,KeyError,TypeError,AttributeError) as exc:
        raise CorruptSpool(path,raw,str(exc)) from exc


def _save(root,body):
    body['revision']+=1
    envelope={'body':body,'sha256':fingerprint(body)}
    if len(json.dumps(envelope,separators=(',',':')).encode('utf-8'))>MAX_BYTES:
        raise ValueError('CONSUMER_LOSS_SPOOL_RESOURCE_LIMIT')
    write_json(root/FILE,envelope)


def pending(root,generation,source_scope_id=None):
    body=_load(Path(root),generation)
    return deepcopy([value for scope,value in sorted(body['pending'].items())
                     if source_scope_id is None or scope==source_scope_id])


def _observations(observations,at):
    if not isinstance(observations,(tuple,list)) or not 1<=len(observations)<=MAX_BATCH:
        raise ValueError('CONSUMER_LOSS_REQUIRES_BOUNDED_DROPPED_OBSERVATIONS')
    points=[]
    identity=None
    for obs in observations:
        if (not isinstance(obs,dict) or obs.get('observation_kind')!='WEBSOCKET_TRADE'
                or not isinstance(obs.get('event_id'),str) or not obs['event_id'].isdigit() or int(obs['event_id'])<1):
            raise ValueError('CONSUMER_LOSS_REQUIRES_ACTUAL_TRADE_IDENTITIES')
        source,receipt=(_stamp(obs[key]) for key in ('source_at','received_at'))
        if not source<=receipt<=at:
            raise ValueError('CONSUMER_LOSS_REFERENCE_CLOCK_INCONSISTENT')
        key=(obs.get('source'),obs.get('symbol'),obs['observation_kind'])
        if not all(isinstance(value,str) and value for value in key) or identity is not None and key!=identity:
            raise ValueError('CONSUMER_LOSS_MIXED_SOURCE_IDENTITY')
        identity=key
        points.append({'event_id':obs['event_id'],'source_at':source.isoformat(),'received_at':receipt.isoformat(),
                       'raw_sha256':obs.get('raw_sha256')})
    # The collector accepts ordered IDs. Keep the actual bounds even if a caller
    # presents the same batch twice; never count an already-covered prefix twice.
    points=sorted({item['event_id']:item for item in points}.values(),key=lambda value:int(value['event_id']))
    return identity,points


def record_loss(root,generation,source_scope_id,observations,*,at,reason=KIND):
    root=Path(root)
    if not isinstance(source_scope_id,str) or not re.fullmatch('[0-9a-f]{64}',source_scope_id):
        raise ValueError('CONSUMER_LOSS_INVALID_SOURCE_SCOPE')
    if not isinstance(reason,str) or not re.fullmatch('[A-Z0-9_]{1,128}',reason):
        raise ValueError('CONSUMER_LOSS_REQUIRES_STABLE_REASON_CODE')
    at=_stamp(at)
    identity,points=_observations(observations,at)
    with writer_lock(root/'.consumer-loss.lock'):
        body=_load(root,generation)
        prior=body['pending'].get(source_scope_id)
        checkpoint=body['checkpoints'].get(source_scope_id) or {}
        if len(set(body['pending'])|set(body['checkpoints'])|{source_scope_id})>MAX_SCOPES:
            raise ValueError('CONSUMER_LOSS_SOURCE_CAPACITY_REQUIRES_REVIEW')
        boundary=(prior or checkpoint).get('last_event_id')
        points=[point for point in points if boundary is None or int(point['event_id'])>int(boundary)]
        if not points:
            return {'state':'ALREADY_RECORDED' if prior else 'ALREADY_COMMITTED',
                    'witness':deepcopy(prior),'source_scope_id':source_scope_id}
        if prior and tuple(prior['reference_identity'][key] for key in ('source','symbol','observation_kind'))!=identity:
            raise ValueError('CONSUMER_LOSS_SOURCE_SCOPE_IDENTITY_CHANGED')
        first,last=points[0],points[-1]
        witness=deepcopy(prior) if prior else {
            'schema':SCHEMA,'generation':generation,'source_scope_id':source_scope_id,'kind':KIND,
            'reason':reason,'cause':'LOCAL_FINANCIAL_CONSUMER_BUFFER_CAPACITY','exchange_disconnect':False,
            'reference_identity':dict(zip(('source','symbol','observation_kind'),identity)),
            'at':at.isoformat(),'first_detected_at':at.isoformat(),
            'first_event_id':first['event_id'],'first_source_at':first['source_at'],
            'first_received_at':first['received_at'],
            'correlation_id':fingerprint([SCHEMA,generation,source_scope_id,first,at.isoformat()]),
            'dropped_count_lower_bound':0,
            'limitation':'Local queued references were dropped before durable per-book application; no raw prices or complete historical stream retained',
        }
        witness.update(last_detected_at=at.isoformat(),last_event_id=last['event_id'],
                       last_source_at=last['source_at'],last_received_at=last['received_at'],
                       dropped_count_lower_bound=witness['dropped_count_lower_bound']+len(points))
        witness['id']=_witness_id(witness)
        body['pending'][source_scope_id]=witness
        _save(root,body)
        return {'state':'RECORDED','witness':deepcopy(witness),'source_scope_id':source_scope_id}


def _journal_evidence(path,witness,at):
    if not path.is_file():
        raise ValueError('CONSUMER_LOSS_WITNESS_JOURNAL_UNAVAILABLE')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
        rows = db.execute("SELECT key,recorded_at,body FROM event WHERE kind='PROTECTION_TRANSPORT_WITNESS' "
                          "AND book=? AND idea=? ORDER BY rowid DESC",
                          ('SOURCE:'+witness['source_scope_id'], witness['id'])).fetchall()
        for key,recorded,raw in rows:
            body=json.loads(raw)
            saved=body.get('witness') or {}
            if (saved.get('id')==witness['id'] and saved==witness and body.get('generation')==witness['generation']
                    and _stamp(recorded)<=at):
                return {'event_id':key,'recorded_at':recorded,'witness_id':witness['id']}
    raise ValueError('CONSUMER_LOSS_WITNESS_NOT_COMMITTED')


def mark_committed(root,generation,witness_ids,*,at,journal_path=None):
    root=Path(root);at=_stamp(at)
    if not isinstance(witness_ids,(tuple,list)) or any(not isinstance(value,str) for value in witness_ids):
        raise ValueError('CONSUMER_LOSS_ACK_REQUIRES_WITNESS_IDENTITIES')
    journal_path=root/'paper.sqlite3' if journal_path is None else Path(journal_path)
    with writer_lock(root/'.consumer-loss.lock'):
        body=_load(root,generation)
        changed=[]
        for identifier in set(witness_ids):
            match=next(((scope,value) for scope,value in body['pending'].items() if value['id']==identifier),None)
            if match is None:
                continue
            scope,witness=match
            evidence=_journal_evidence(journal_path,witness,at)
            body['checkpoints'][scope]={'generation':generation,'source_scope_id':scope,'witness_id':identifier,
                                       'last_event_id':witness['last_event_id'],'last_source_at':witness['last_source_at'],
                                       'last_received_at':witness['last_received_at'],'marked_committed_at':at.isoformat(),
                                       'journal_evidence':evidence}
            del body['pending'][scope]
            changed.append(identifier)
        if changed:
            _save(root,body)
        return {'state':'COMMITTED' if changed else 'NO_MATCHING_PENDING_REVISION',
                'committed_ids':sorted(changed),'pending_ids':sorted(value['id'] for value in body['pending'].values())}
