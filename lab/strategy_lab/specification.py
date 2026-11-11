import re
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit

from lab.learning.features import REGISTRY
from lab.strategy_lab.registry import PROFILES, SOURCE_TYPES, identifier, register


MAX_SOURCE_BYTES = 2 * 1024**2
MAX_SOURCE_STORAGE = 64 * 1024**2
MAX_SPEC_BYTES = 64 * 1024
POLICY = "closed-input-ioc-onebar-v1"
ENTRIES = {
    "breakout_v1": ("prior_range", "Completed close strictly beyond the previous 20 decision bars, in the direction of every completed context SMA regime."),
    "pullback_v1": ("prior_retracement_atr", "All context trends agree, the prior close retraced by the configured ATR distance beyond its EMA26, and the current close reverses in the trend direction."),
    "range_reversion_v1": ("deviation_atr", "Context distance is within the configured absolute maximum. Close deviates beyond EMA26 by the configured ATR multiple and RSI14 is below 0.4 for long or above 0.6 for short."),
}
EXITS = {
    "fixed_r_v1": ("target_r", "Take profit at the configured multiple of distance between fill and initial stop."),
    "atr_trail_v1": ("trailing_atr", "Ratchet the stop by the configured ATR multiple behind completed decision closes."),
}
INDICATORS = {
    **REGISTRY, "atr": "14-bar arithmetic mean of true range.",
    "ema": "EMA(26), recursive weighting, full-span warmup.",
    "sma": "Completed higher-timeframe SMA with the declared window.",
    "prior_high": "Highest high of the preceding 20 bars, excluding current bar.",
    "prior_low": "Lowest low of the preceding 20 bars, excluding current bar.",
    "pivots": "2-left/2-right confirmed pivots, first usable two completed bars later.",
    "bollinger": "20-bar close SMA with bands at two population standard deviations.",
    "candle_vwap": "UTC-day cumulative typical price weighted by closed native candle volume. Candle proxy.",
}
DEPENDENCIES = {"trade_ohlcv", "mark_ohlc", "settled_funding", "dated_contract"}
DISCRETION = re.compile(r"\b(?:looks? strong|use judgment|discretionary|as needed|clean breakout|tbd|somewhat|eyeball)\b", re.I)
LOOKAHEAD = re.compile(r"(?:lookahead\s*=\s*barmerge\.lookahead_on|\blook.?ahead\b|\brepaint\w*\b|shift\(\s*-\d|\bfuture (?:bar|candle))", re.I)


def template(profile):
    market, symbol, decision, native = PROFILES[profile]
    futures = market == "FUTURES"
    session = {"timezone": "UTC", "max_hold_minutes": None}
    if profile == "day":
        session.update(entry_cutoff_minute_utc=1320, flat_minute_utc=1435)
    return {
        "strategy_id": "", "family_id": "", "name": "", "description": "", "profile": profile,
        "venue": "KuCoin", "market": market, "instrument": symbol, "timeframe": decision,
        "context": [], "indicators": {}, "sides": ["long", "short"] if futures else ["long"],
        "entry_rules": [], "exit_rules": [], "invalidation_rules": [], "risk": {},
        "session": session, "execution": {}, "cost": {},
        "data": {"native_seconds": native, "dependencies": sorted(DEPENDENCIES) if futures else ["trade_ohlcv"]},
        "source": {"kind": "manual", "reference": "", "license": "", "author": ""},
        "origin": "", "implementation_version": "", "recipe": None, "limitations": [], "examples": [],
    }


def validate(spec):
    issues = []

    def flag(field, reason, code="NEEDS_CLARIFICATION"):
        issues.append({"field": field, "code": code, "reason": reason})

    for field in ("strategy_id", "family_id"):
        try:
            identifier(spec.get(field))
        except ValueError as exc:
            flag(field, str(exc))
    for field in ("name", "description", "origin"):
        if not isinstance(spec.get(field), str) or not spec[field].strip():
            flag(field, "Provide a nonempty value")
    profile = spec.get("profile")
    cap = PROFILES.get(profile) if isinstance(profile, str) else None
    if cap is None or (spec.get("market"), spec.get("instrument"), spec.get("timeframe")) != cap[:3] or spec.get("venue") != "KuCoin":
        flag("profile", "Select compatible profile, market, venue, instrument and decision cadence")
    sides = spec.get("sides")
    if sides not in (["long"], ["short"], ["long", "short"]) or (
        spec.get("market") == "SPOT" and sides != ["long"]
    ):
        flag("sides", "Specify consistent direction eligibility. Spot is long-only")
    source = spec.get("source")
    if not isinstance(source, dict) or source.get("kind") not in SOURCE_TYPES or any(
        not isinstance(source.get(key), str) or not source[key].strip() for key in ("reference", "license", "author")
    ):
        flag("source", "Record the source kind, reference, license status and author")
    for field in ("entry_rules", "exit_rules", "invalidation_rules"):
        rules = spec.get(field)
        if not isinstance(rules, list) or not rules or any(not isinstance(x, str) or not x.strip() for x in rules):
            flag(field, "Supply explicit conditions")
        elif any(DISCRETION.search(rule) for rule in rules):
            flag(field, "Replace discretionary or unresolved language with a deterministic rule")
        elif any(LOOKAHEAD.search(rule) for rule in rules):
            flag(field, "Review potential repainting or future data")
    entry_rules = spec.get("entry_rules")
    if sides == ["long"] and isinstance(entry_rules, list) and any(
        re.search(r"\b(?:enter|open) (?:a )?short\b", rule, re.I)
        for rule in entry_rules if isinstance(rule, str)
    ):
        flag("sides", "A short entry conflicts with long-only eligibility")
    indicators = spec.get("indicators")
    if not isinstance(indicators, dict) or any(
        not isinstance(params, dict) or not params or any(value is None or value == "" for value in params.values())
        for params in indicators.values()
    ):
        flag("indicators", "Specify settings for every required indicator, or {} for no indicators")
    context = spec.get("context")
    if not isinstance(context, list) or any(not isinstance(item, dict) or not isinstance(item.get("frame"), str)
        or type(item.get("sma")) is not int or item["sma"] < 1 for item in context):
        flag("context", "Specify each completed higher timeframe and SMA lookback")
    data = spec.get("data")
    deps = data.get("dependencies") if isinstance(data, dict) else None
    if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps) or "trade_ohlcv" not in deps:
        flag("data", "Declare required data including trade OHLCV")
    else:
        unsupported = sorted(set(deps) - DEPENDENCIES)
        if unsupported:
            flag("data", f"Unavailable inputs: {', '.join(unsupported)}", "UNSUPPORTED_DATA")
        if cap and data.get("native_seconds") != cap[3]:
            flag("data", "Native execution resolution does not match the profile", "UNSUPPORTED_DATA")
        if cap and cap[0] == "FUTURES" and not DEPENDENCIES.issubset(deps):
            flag("data", "Futures requires trade, mark, settled funding and dated contract evidence", "UNSUPPORTED_DATA")
    for field in ("risk", "execution", "cost"):
        if not isinstance(spec.get(field), dict) or not spec[field]:
            flag(field, "Specify the policy and its parameters")
    session = spec.get("session")
    if not isinstance(session, dict) or session.get("timezone") != "UTC" or (
        type(session.get("max_hold_minutes")) is not int or session["max_hold_minutes"] < 1
    ):
        flag("session", "Specify UTC and a positive maximum hold in minutes")
    else:
        cutoff, flat = session.get("entry_cutoff_minute_utc"), session.get("flat_minute_utc")
        if (profile == "day" or cutoff is not None or flat is not None) and (
            type(cutoff) is not int or type(flat) is not int or not 0 < cutoff < flat < 1440
        ):
            flag("session", "Supply a valid entry cutoff strictly before the UTC flat time")
    if spec.get("source_review_flags"):
        flag("source", "Review source risk markers and record the resolution in a new version")
    return issues


def source_url(source):
    address = urlsplit(source["reference"])
    if address.scheme not in ("https", "http") or not address.hostname or address.username or address.password:
        raise ValueError("Supply an HTTP(S) reference without embedded credentials")
    if not all(isinstance(source.get(key), str) and source[key].strip() for key in ("title", "publisher")):
        raise ValueError("URL references require title and publisher metadata")


def stage(db, spec, *, expected=None, source_file: Path | None = None, source_upload=None):
    from lab.strategy_lab.registry import canonical

    source = spec.get("source")
    if not isinstance(source, dict) or source.get("kind") not in SOURCE_TYPES:
        raise ValueError("Provide a supported source kind")
    strategy_id = identifier(spec.get("strategy_id"))
    if spec.get("origin") in ("PHASE15_REGISTERED_PAPER", "PHASE16_INITIAL_HISTORICAL") or (
        strategy_id.startswith(("phase15/", "phase16/")) and not expected
    ):
        raise ValueError("Registered catalog identities are seeded from sealed sources. Use manual origin for revisions")
    spec, source = dict(spec), dict(source)
    spec["source"] = source
    if source["kind"] == "url":
        source_url(source)
    if source_file is not None:
        with source_file.open("rb") as stream:
            source_upload = (source_file.name, stream.read(MAX_SOURCE_BYTES + 1))
    if source_upload is not None:
        if source["kind"] not in ("document", "pine", "chart", "repository"):
            raise ValueError("A source file requires document, pine, chart or repository provenance")
        filename, content = source_upload
        if len(content) > MAX_SOURCE_BYTES:
            raise ValueError("Source exceeds 2 MiB. Record a local reference instead")
        key = sha256(content).hexdigest()
        if not db.execute("SELECT 1 FROM source_blob WHERE hash=?", (key,)).fetchone():
            used = db.execute("SELECT COALESCE(SUM(length(body)),0) FROM source_blob").fetchone()[0]
            if used + len(content) > MAX_SOURCE_STORAGE:
                raise ValueError("Source attachment storage ceiling reached")
        db.execute("INSERT OR IGNORE INTO source_blob VALUES(?,?)", (key, content))
        source.update(sha256=key, filename=Path(filename).name)
        if source["kind"] in ("pine", "repository") and LOOKAHEAD.search(content.decode("utf-8", errors="replace")):
            spec["source_review_flags"] = ["Possible future data or repainting in source"]
    if len(canonical(spec).encode("utf-8")) > MAX_SPEC_BYTES:
        raise ValueError("Strategy specification exceeds 64 KiB")
    return register(db, spec, expected=expected)


def recipe_description(entry, exit_name, params):
    return (
        [ENTRIES[entry][1]],
        [EXITS[exit_name][1], "Close on protective stop, applicable liquidation, or the frozen time/session deadline."],
        [f"Initial stop is {params['initial_atr']:g} × ATR from the decision close, replaced by a closer confirmed pivot only if it is at least one ATR away."],
    )
