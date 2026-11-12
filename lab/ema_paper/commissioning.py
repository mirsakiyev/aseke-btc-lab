"""Attach only reviewed independent sources to the three existing EMA books."""

from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

import pandas as pd

from lab.config import ROOT as PROJECT, fingerprint
from lab.ema_paper import catalog, proxy_protocol
from lab.kraken_paper import authorization
from lab.kraken_paper.catalog import _grant_body
from lab.storage import file_hash, utc_now, writer_lock
from lab.strategy_lab.cross_venue_warmup import POLICY_ID, freeze_proxy_seed, load_proxy_seed
from lab.strategy_lab.market_data import MarketData, ObservationKind, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.spot_sources import SpotCandleProvider, load_captured_receipt


VERSION = "EMA_KUCOIN_INDEPENDENT_HANDOFF_V1"
FILENAME = "commissioning.sqlite3"
RIGHTS = frozenset({"ontario", "automated_collection", "local_retention", "private_rules_only_paper",
                    "independent_ema_consumer", "kucoin_origin", "execution_evidence", "owner_only_alerts"})
SCHEMA = """
CREATE TABLE IF NOT EXISTS commission (id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS commission_update BEFORE UPDATE ON commission
    BEGIN SELECT RAISE(ABORT, 'immutable EMA commissioning record'); END;
CREATE TRIGGER IF NOT EXISTS commission_delete BEFORE DELETE ON commission
    BEGIN SELECT RAISE(ABORT, 'immutable EMA commissioning record'); END;
"""
utc = authorization.utc


def _pins():
    paths = ("lab/ema_paper/commissioning.py", "lab/ema_paper/dispatcher.py", "lab/ema_paper/proxy_execution.py",
             "lab/strategy_lab/source_spool.py", "lab/strategy_lab/source_handoff.py", "lab/ema_paper/notifications.py")
    return {**catalog.code_pins(), **proxy_protocol._pins(), **{name: file_hash(PROJECT / name) for name in paths}}


def _private(path):
    path = Path(path).resolve(strict=True)
    if path.is_relative_to(PROJECT.resolve()):
        raise PermissionError("The independently licensed source and correspondence must live outside the project stores")
    return path


def _local_file(folder, name):
    path = _private(folder / name)
    if not path.is_relative_to(folder):
        raise PermissionError("Source review files must resolve inside the reviewed private directory")
    return path


def _read_rights(review, now):
    review = _private(review)
    payload = authorization.bounded_bytes(review)
    body = json.loads(payload)
    if (body.get("version") != VERSION or frozenset(body.get("reviewed_scope", ())) != RIGHTS
            or body.get("upstream_venue") != "KuCoin" or body.get("protected_sources_excluded") is not True
            or body.get("paper_only") is not True or body.get("live_orders") is not False
            or any(not isinstance(body.get(key), str) or not body[key].strip()
                   for key in ("operator", "provider", "namespace", "rights_reference"))):
        raise PermissionError("A complete operator-reviewed independent KuCoin-origin license is required")
    granted, approved, expires = map(utc, (body["provider_effective_at"], body["approved_at"], body["expires_at"]))
    if not granted <= approved <= now < expires:
        raise PermissionError("KuCoin source review is outside its approval window")
    evidence = authorization.bounded_bytes(_local_file(review.parent, body["rights_evidence_file"]))
    if sha256(evidence).hexdigest() != body["rights_evidence_sha256"]:
        raise PermissionError("Private KuCoin source rights evidence checksum failed")
    spool = _local_file(review.parent, body["spool_directory"])
    return body, spool, sha256(payload).hexdigest()


def _grant(body):
    fields = dict(body)
    identity = SourceIdentity(**fields.pop("source"))
    return SourceGrant(source=identity, **fields)


def _native_grants(lane, review, seam):
    source = SourceIdentity(review["provider"], review["namespace"], lane["instrument"], lane["market"],
                            review["rights_reference"], review["rights_evidence_sha256"], "independent")
    trade = ObservationKind.TRADE_OHLCV
    roles = {trade, ObservationKind.CONTRACT_SPEC}
    if lane["market"] == "FUTURES":
        roles.update({ObservationKind.MARK_OHLCV, ObservationKind.FUNDING_SETTLEMENT})
    approved, expires = map(utc, (review["approved_at"], review["expires_at"]))
    scope = (seam, expires, approved, lane["registered_at"], review["rights_reference"])
    return (SourceGrant(source, lane["book_id"], Purpose.FORWARD_DECISION, {trade}, *scope),
            SourceGrant(source, lane["book_id"], Purpose.EXECUTION_EVIDENCE, roles, *scope))


def _fresh_seed(lane, previous, settings, review_path, now, seam):
    previous_grant = _grant(previous["body"]["warmup_grant"])
    source = previous_grant.source
    private_root = _private(review_path).parent
    permission = authorization.read_review(_local_file(private_root, settings["kraken_permission"]), at=now)
    if authorization.evidence_id(permission) != source.rights_evidence_sha256:
        raise PermissionError("Kraken evidence does not match the original reviewed feature source")
    paths = settings["kraken_receipts"]
    if not isinstance(paths, list) or not 1 <= len(paths) <= 5:
        raise ValueError("A bounded set of Kraken receipt manifests is required")
    receipts = []
    for name in paths:
        manifest = _local_file(private_root, name)
        descriptor = json.loads(authorization.bounded_bytes(manifest))
        _local_file(private_root, manifest.parent / descriptor["raw_path"])
        receipts.append(load_captured_receipt(manifest))
    # The producer captured these separately under the verified Kraken authorization.
    provider = SpotCandleProvider(source, receipts)
    interval = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    start = utc(settings["kraken_start"])
    kind = ObservationKind.TRADE_OHLCV
    grant = SourceGrant(source, lane["book_id"], Purpose.WARMUP, {kind}, start, seam, now, lane["registered_at"],
                        previous_grant.approval_reference)
    snapshot = MarketData([grant], {source.id: provider}).read(grant.id, lane["book_id"], Purpose.WARMUP, kind,
                                                               start, seam, now, interval)
    if snapshot.content_id != settings["kraken_content_sha256"]:
        raise ValueError("Refreshed Kraken feature snapshot differs from the reviewed content")
    return freeze_proxy_seed(snapshot, timeframe=interval), grant


def _spool(forward, execution, spool):
    from lab.strategy_lab.source_spool import ReviewedSqliteSpool

    provider = ReviewedSqliteSpool(forward.source, root=spool)
    provider.verify()
    return MarketData((forward, execution), {forward.source.id: provider})


def _read(path):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT id,body FROM commission").fetchall()
    if not rows:
        raise PermissionError("No immutable EMA handoff record found")
    groups, bound = [], set()
    pins = _pins()
    for identifier, payload in rows:
        body = json.loads(payload)
        if (fingerprint(body) != identifier or body["version"] != VERSION or body["code_pins"] != pins
                or bound.intersection(body["books"])):
            raise PermissionError("Commissioned EMA handoff checksum, book uniqueness or runtime pins failed")
        bound.update(body["books"])
        groups.append(body)
    return groups


def commission(review_path, *, now=None, root=None):
    """Bind reviewed permissions and a fresh exact seam. The scheduler later qualifies readiness."""
    root = proxy_protocol.ROOT if root is None else Path(root)
    now = utc_now() if now is None else utc(now)
    target = root / FILENAME
    with writer_lock(root / ".commission.lock"):
        groups = _read(target) if target.is_file() else []
        review, spool, digest = _read_rights(review_path, now)
        for current in groups:
            if digest == current["review_sha256"]:
                return {"state": "COMMISSIONED", "id": fingerprint(current)}
        registrations = proxy_protocol._original(catalog.registrations())
        profiles = set(review["books"])
        if not profiles or not profiles <= set(registrations):
            raise PermissionError("The review must name one or more original EMA books")
        if any(profiles.intersection(current["books"]) for current in groups):
            raise PermissionError("An original book already has a different frozen handoff")
        records = {}
        for profile in sorted(profiles):
            registration = registrations[profile]
            lane, settings = registration["body"], review["books"][profile]
            previous = proxy_protocol.get_amendment(profile, root=root)
            seam = utc(settings["seam"])
            step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
            if (settings["registration_id"] != registration["id"] or settings["book_id"] != lane["book_id"]
                    or settings["predecessor_amendment_id"] != previous["id"]
                    or not utc(previous["body"]["recorded_at"]) <= utc(review["approved_at"]) <= seam <= now
                    or seam != seam.floor(step)
                    or utc(review["expires_at"]) <= now + pd.Timedelta(days=lane["horizon_days"],
                                                                      minutes=lane["follow_through_minutes"]) + step):
                raise PermissionError("The review must pin the original book, approved exact seam and full study coverage")
            forward, execution = _native_grants(lane, review, seam)
            _spool(forward, execution, spool)  # Verify the independent producer manifest before attaching it.
            seed, warmup = _fresh_seed(lane, previous, settings, review_path, now, seam)
            if (seed.end != seam or seed.book_id != lane["book_id"] or seed.grant_id != warmup.id
                    or seed.source_id != warmup.source.id or seed.timeframe_seconds != int(step.total_seconds())):
                raise ValueError("Refreshed Kraken feature seed does not match the reviewed book and UTC seam")
            record = {"registration_id": registration["id"], "book_id": lane["book_id"],
                      "policy_id": POLICY_ID, "recorded_at": now.isoformat(), "seed": seed.body(), "seed_id": seed.id,
                      "warmup_grant": _grant_body(warmup), "predecessor_amendment_id": previous["id"],
                      "forward_grant": _grant_body(forward), "execution_grant": _grant_body(execution)}
            records[profile] = {"id": fingerprint(record), "body": record}
        body = {"version": VERSION, "recorded_at": now.isoformat(), "books": records,
                "review_path": str(_private(review_path)), "review_sha256": digest, "code_pins": _pins()}
        with closing(sqlite3.connect(target, timeout=3)) as db:
            db.executescript(SCHEMA)
            with db:
                db.execute("INSERT INTO commission VALUES (?,?)", (fingerprint(body), json.dumps(body, sort_keys=True)))
        return {"state": "COMMISSIONED", "id": fingerprint(body)}


def _attach_group(body, registrations, now, root):
    from lab.ema_paper.proxy_execution import KuCoinProxyExecution

    _, spool, digest = _read_rights(body["review_path"], now)
    if digest != body["review_sha256"] or not set(body["books"]) <= set(registrations):
        raise PermissionError("Commissioned EMA source rights or registration group changed")
    result = {}
    for profile, amendment in body["books"].items():
        record = amendment["body"]
        registration = registrations[profile]
        prior = proxy_protocol.get_amendment(profile, root=root)
        if (fingerprint(record) != amendment["id"] or record["registration_id"] != registration["id"]
                or prior["id"] != record["predecessor_amendment_id"]):
            raise PermissionError("Commissioned EMA book revision no longer matches the original registration")
        seed = load_proxy_seed(record["seed"], record["seed_id"])
        forward, execution = (_grant(record[key]) for key in ("forward_grant", "execution_grant"))
        if seed.book_id != forward.book_id or seed.end != forward.start or forward.source != execution.source:
            raise PermissionError("Commissioned Kraken seed or KuCoin seam differs from the frozen grant")
        reader = KuCoinProxyExecution(_spool(forward, execution, spool), registration,
                                      protocol_recorded_at=prior["body"]["recorded_at"],
                                      source_identity_id=forward.source.id, forward_grant_id=forward.id,
                                      execution_grant_id=execution.id)
        result[profile] = (amendment, seed, reader)
    return result


def attachments(registrations, now, *, root=None, errors=None):
    """Open reviewed spools read-only and reconstruct exact per-book runtime scopes."""
    root = proxy_protocol.ROOT if root is None else Path(root)
    path = root / FILENAME
    if not path.is_file():
        return {}
    result = {}
    for group in _read(path):
        try:
            result.update(_attach_group(group, registrations, now, root))
        except (OSError, ValueError, LookupError, sqlite3.Error) as exc:
            if errors is None:
                raise
            errors.update({profile: f"Commissioned source review failed ({type(exc).__name__})" for profile in group["books"]})
    return result
