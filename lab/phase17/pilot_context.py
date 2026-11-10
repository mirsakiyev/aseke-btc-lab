import numpy as np
import pandas as pd

from lab.config import ORIGIN
from lab.data import OHLCV, aggregate
from lab.learning.data import validate_prices
from lab.learning.features import build_features
from lab.phase9.data import candles
from lab.phase9.public import attempts, kline, latest
from lab.phase9.reference import reference
from lab.phase10.partition import partition_features
from lab.phase10.worker import OUTPUT, cache_read
from lab.phase13.registry import checked
from lab.phase14.features import FeatureBars
from lab.phase17.labels import RECEIPT_ALLOWANCE
from lab.phase17.pilot_registry import PILOT, SCOPE, find, save
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


def grid(frame, step, now):
    index = pd.DatetimeIndex(frame.open_at)
    if (frame.empty or str(index.tz) != "UTC" or not index.equals(pd.date_range(index[0], periods=len(index), freq=step))
            or (frame.received_at < frame.close_at).any() or (frame.received_at > now).any()):
        raise ValueError("Feature cache has gaps, invalid receipts or an unavailable bar")
    validate_prices(frame.set_index("open_at"), step)


def contiguous_suffix(frame, step):
    if frame.open_at.duplicated().any():
        raise ValueError("Duplicate source timestamps in the monitoring feature cache")
    breaks = np.flatnonzero(frame.open_at.diff().iloc[1:].ne(step))
    return frame.iloc[breaks[-1]+1:] if len(breaks) else frame


def raw_receipts(name, frame, mode):
    home = latest("current")
    prior = {}
    if home and read_json(home/"plan.json")["design"]["scope"] == "PROSPECTIVE_FEATURE_WARMUP_ONLY":
        prior = {row["raw_sha256"]: row for row in attempts(home) if row.get("status") == "RECEIVED"}
    selected = []
    for digest, rows in frame.groupby("raw_sha256", sort=False):
        kind = set(rows.receipt_class)
        left, end = rows.open_at.min(), rows.close_at.max()
        if kind == {"BOOTSTRAP_PHASE9"}:
            evidence = prior.get(digest)
            if not evidence or not rows.received_at.eq(utc(evidence["received_at"])).all():
                raise ValueError("Bootstrap feature receipt does not match its original public ledger")
            spec = evidence["request"]
            expected = {"tradeType":mode["trade_type"], "symbol":mode["symbol"], "klineType":"TRADE", "interval":mode["warmup_interval"]}
            if any(spec["params"].get(k) != v for k,v in expected.items()):
                raise ValueError("Bootstrap instrument, cadence or trade basis changed")
            asset = home / "raw" / f"{digest}.json"
        elif kind == {"LIVE_INCREMENT"}:
            spec = kline(name, mode["symbol"], mode["trade_type"], "TRADE", mode["warmup_interval"], left, end)
            asset = OUTPUT / "raw" / f"{digest}.json"
        else:
            raise ValueError("Unrecognized feature source provenance")
        payload = read_json(checked(asset, digest))["data"]
        parsed = candles(payload, spec, rows.received_at.max(), .001 if mode["trade_type"] == "FUTURES" else None)
        parsed = parsed.set_index("open_at").reindex(rows.open_at)
        if (parsed[list(OHLCV)].isna().any().any()
                or not np.allclose(parsed[list(OHLCV)], rows[list(OHLCV)], rtol=1e-12, atol=1e-12)):
            raise ValueError("Cached feature values differ from the checked native public response")
        selected.append({"sha256": digest, "path": str(asset), "receipt_class": next(iter(kind)),
                         "source_start": left.isoformat(), "source_end_exclusive": end.isoformat(),
                         "received_at": rows.received_at.max().isoformat()})
    return selected


def feature_frames(frame, cfg):
    step = pd.Timedelta(cfg["context_bar"])
    profile = cfg["profile_rules"]
    resolutions = dict.fromkeys([profile["decision"], *(x["frame"] for x in profile["context"])])
    return {pd.Timedelta(r): aggregate(frame, r, step) for r in resolutions}


def admit(pilot, name, *, cutoff=None, now=None):
    cfg = pilot["body"]["profiles"][name]
    now = utc_now() if now is None else utc(now)
    step = pd.Timedelta(cfg["context_bar"])
    mode = reference()["profiles"][name]
    if mode["venue"] != "KuCoin" or mode["symbol"] != cfg["instrument"] or mode["trade_type"] != cfg["market"]:
        raise ValueError("Monitoring reference does not match the frozen pilot market")
    frame = cache_read(name)
    if frame.empty:
        raise ValueError("The native monitoring feature cache is empty")
    if cutoff is not None:
        frame = frame.loc[frame.close_at <= cutoff]
    frame = contiguous_suffix(frame.sort_values("open_at"),step)
    grid(frame, step, now)
    if cutoff is not None and frame.close_at.iloc[-1] != cutoff:
        raise ValueError("The expected completed feature bridge has not reached the requested cutoff")
    receipts = raw_receipts(name, frame, mode)
    indexed = frame.set_index("open_at")
    expected, schema = build_features(indexed, cfg["profile_rules"], step)
    actual, columns = partition_features(FeatureBars(feature_frames(indexed, cfg)), cfg["profile_rules"])
    if (columns != schema or not actual.index.equals(expected.index)
            or not np.allclose(actual[columns], expected[schema], equal_nan=True, rtol=1e-12, atol=1e-12)
            or not actual.ready.equals(expected.ready) or not actual.ready.iloc[-1]):
        raise ValueError("Operational and pilot features lack parity or sufficient causal context")
    bars = [{key: val.isoformat() if isinstance(val, pd.Timestamp) else val for key,val in row.items()}
            for row in frame[["open_at", "close_at", "received_at", "raw_sha256", *OHLCV]].to_dict("records")]
    # The receipt dates are facts about the observation. Admission is a later
    # availability constraint and never makes the original dates tradable.
    return save("context", {"version":"phase17.1-native-context-v1", "pilot":pilot["id"], "profile":name, "scope":SCOPE,
        "venue":"KuCoin", "instrument":cfg["instrument"], "market":cfg["market"], "basis":"TRADE", "step":str(step),
        "start":frame.open_at.iloc[0].isoformat(), "cutoff":frame.close_at.iloc[-1].isoformat(), "rows":len(frame),
        "receipt_first":frame.received_at.min().isoformat(), "receipt_last":frame.received_at.max().isoformat(),
        "admitted_at":now.isoformat(), "raw_receipts":receipts, "bars":bars, "ready":True, "parity":True})


def pinned(pilot, name):
    path = PILOT / "pins" / f"{name}.json"
    if path.exists():
        seed = find("context", read_json(path)["id"])
        if seed["body"]["pilot"] != pilot["id"] or seed["body"]["profile"] != name:
            raise ValueError("Feature context pin belongs to another pilot or market")
        return seed


def pin(pilot, name, now):
    cfg = pilot["body"]["profiles"][name]
    if now < utc(cfg["label_start"]):
        return None
    with writer_lock(PILOT / ".context.lock"):
        if seed := pinned(pilot, name):
            return seed
        # A later complete operational cache may bridge an outage. Outcomes
        # then begin after this new admission, with no backdated decisions.
        cutoff = (now-pd.Timedelta(seconds=30)).floor(cfg["context_bar"])
        try:
            seed = admit(pilot, name, cutoff=cutoff, now=now)
        except ValueError as error:
            write_json(PILOT / "context-wait" / f"{name}.json", {"at":now.isoformat(), "reason":str(error), "expected_cutoff":cutoff.isoformat()})
            return None
        write_json(PILOT / "pins" / f"{name}.json", {"id":seed["id"]})
        return seed


def builder(pilot, name, seed):
    cfg = pilot["body"]["profiles"][name]
    body = seed["body"]
    if (body["scope"] != SCOPE or body["pilot"] != pilot["id"] or body["profile"] != name
            or body["instrument"] != cfg["instrument"] or body["market"] != cfg["market"] or body["basis"] != "TRADE"):
        raise ValueError("Pilot context cannot cross market or use scope")
    seed_frame = pd.DataFrame(body["bars"])
    for key in ("open_at", "close_at", "received_at"):
        seed_frame[key] = pd.to_datetime(seed_frame[key], utc=True)
    admitted, bridge = utc(body["admitted_at"]), utc(body["cutoff"])
    step = pd.Timedelta(cfg["context_bar"])
    grid(seed_frame, step, admitted)
    seed_frame = seed_frame.set_index("open_at")

    def build(data, receipts):
        market = data["market"]
        if hasattr(market, "bars"):
            bars = market.bars([cfg["context_bar"]])[cfg["context_bar"]]
            bars = bars.reset_index().set_index(bars.index-step) if len(bars) else pd.DataFrame(columns=[*OHLCV,"close_at"])
        else:
            bars = aggregate(market.trade, cfg["context_bar"], market.step)
        if len(bars):
            times = receipts.rename(index=lambda at: at+market.step).resample(step, origin=ORIGIN, label="right", closed="right").max()
            bars["received_at"] = times.reindex(pd.DatetimeIndex(bars.close_at)).to_numpy()
            bars = bars.loc[bars.index >= bridge]
        combined = pd.concat([seed_frame, bars]) if len(bars) else seed_frame
        features, columns = partition_features(FeatureBars(feature_frames(combined, cfg)), cfg["profile_rules"])
        available = combined.set_index("close_at").received_at.cummax().reindex(features.index).clip(lower=admitted)
        timely = features.ready & (features.index > bridge) & (features.index >= admitted) & (available <= features.index+RECEIPT_ALLOWANCE)
        return features, columns, available, timely
    return build
