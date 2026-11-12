"""Freeze the user-supplied EMA pullback source and its research profiles."""

from copy import deepcopy
from hashlib import sha256

from lab.config import ROOT as PROJECT
from lab.strategy_lab.evaluator import ema_code_hashes
from lab.strategy_lab.ema_pullback import ENTRY_PARAMETERS, EXIT, EXIT_PARAMETERS, FAMILY
from lab.strategy_lab.registry import get, register, source_content
from lab.strategy_lab.specification import EMA_INDICATORS, SOURCE_POLICY, ema_rule_description, template, validate


SOURCE_VERSION = "source_faithful_v1"
SOURCE_DIR = PROJECT / "strategies" / FAMILY
ARTIFACTS = (
    ("source_faithful_v1.pine", "31ea034ab2ebf4b722cfc56b5b6b22fbf0b074b015942ee3dce7a6e41818684f"),
    ("user_description.txt", "eec7fff419455e74b62afce409111c9a48841fe60d0b84b566ec8cbfff4d0a53"),
)
SOURCE_DIFFERENCES = {
    "A": "Trend uses the ordering of EMA50 and EMA200 without an independent price-versus-EMA200 filter.",
    "B": "Both EMAs use the decision chart timeframe. There is no higher-timeframe request.",
    "C": "The prior candle predicate accepts any qualifying low or close for a long, or high or close for a short. "
         "It does not establish the direction of approach.",
    "D": "Repeated qualifying touches are eligible when flat. There is no first-pullback counter.",
    "E": "There is no EMA slope or minimum separation filter.",
    "F": "The five-bar high and low include the confirmation candle.",
    "G": "The 20-bar volume SMA includes the confirmation candle.",
    "H": "Long RSI must be strictly above 40. Short RSI must be strictly below 60.",
    "I": "The target is anchored to the confirmation close and remains fixed through entry.",
    "J": "The target distance is 2.5 ATR. The reward-to-risk multiple depends on stop distance and fill.",
    "K": "The source has no session cutoff, maximum hold, or daily flat rule.",
    "L": "The source specifies 10% equity allocation and a 0.05% commission per fill side. "
         "Account risk depends on the position size and stop distance.",
    "M": "Chart labels, plots, and backgrounds are presentation. Alert text is also preserved as source text only.",
}
POTENTIAL_V2 = [
    "Require a genuine from-above or from-below approach.",
    "Add a minimum EMA slope.",
    "Add a minimum EMA separation.",
    "Add an ADX trend-strength filter.",
    "Limit entries to the first qualifying pullback in a trend.",
    "Test alternate ATR risk and reward settings.",
]


def _stage_artifacts(db):
    paths = []
    for filename, key in ARTIFACTS:
        path = SOURCE_DIR / filename
        content = path.read_bytes()
        if sha256(content).hexdigest() != key:
            raise ValueError(f"Immutable EMA pullback source checksum mismatch for {filename}")
        db.execute("INSERT OR IGNORE INTO source_blob VALUES(?,?)", (key, content))
        source_content(db, key)
        paths.append(path.relative_to(PROJECT).as_posix())
    return {
        "kind": "pine",
        "author": "Original author unspecified. Supplied by the user.",
        "license": "User-provided reference for local review. Redistribution license unspecified.",
        "reference": paths[0],
        "filename": ARTIFACTS[0][0],
        "sha256": ARTIFACTS[0][1],
        "description_reference": paths[1],
        "description_filename": ARTIFACTS[1][0],
        "description_sha256": ARTIFACTS[1][1],
    }


def _profile_baseline(db, profile):
    row = db.execute("SELECT hash FROM strategy_version WHERE strategy_id=? AND version=1",
                     (f"phase15/{profile}/fixed",)).fetchone()
    if row is None:
        raise ValueError(f"Seed the frozen Phase 15 {profile} rules-only baseline in the Strategy Lab first")
    baseline = get(db, row["hash"])
    if baseline["spec"]["origin"] != "PHASE15_REGISTERED_PAPER":
        raise ValueError(f"Unexpected origin for the frozen {profile} baseline")
    return baseline


def _spec(db, variant, source, code):
    profile = "position" if variant == "position" else "swing"
    spec = template(profile)
    entry, exit_rules, invalidation = ema_rule_description(spec["sides"])
    role = "SOURCE_ALLOCATION_BENCHMARK" if variant == "source" else "ASEKE_PROFILE_RESEARCH"
    version = SOURCE_VERSION if variant == "source" else f"aseke_{profile}_v1"
    spec.update(
        strategy_id=f"{FAMILY}/{variant}", family_id=FAMILY, implementation_version=version,
        name=f"EMA Pullback Trend Continuation · {variant.title()}",
        description="A same-timeframe EMA pullback and close confirmation strategy. Better performance in trending "
                    "markets is the author's research hypothesis.",
        origin="USER_SUPPLIED_PINE_V6", source=source, entry_rules=entry, exit_rules=exit_rules,
        invalidation_rules=invalidation, indicators=deepcopy(EMA_INDICATORS),
        recipe={"entry": FAMILY, "entry_parameters": dict(ENTRY_PARAMETERS), "exit": EXIT,
                "exit_parameters": dict(EXIT_PARAMETERS)},
        evaluator={"entrypoint": "lab.strategy_lab.evaluator.evaluate",
                   "code_sha256": code, "source_version": SOURCE_VERSION},
        source_differences=SOURCE_DIFFERENCES, potential_v2=POTENTIAL_V2,
        profile_compatibility={
            "source": "Chart-timeframe reference registered on the Swing 4h grid. Long and short.",
            "position": "BTC-USDT Spot on 1D decisions. Long only.",
            "swing": "XBTUSDTM futures on 4h decisions. Long and short.",
            "day": "Deferred. A separate 15m version would need its own session review.",
        },
        historical_readiness={
            "at_registration": "BLOCKED_DATA",
            "required": "Approved pre-holdout development dataset with the declared native dependencies and "
                        "causal availability evidence. See DATA_ADMISSION_PLAN.md.",
        },
        variant={"role": role, "version": version, "signal_version": SOURCE_VERSION},
        limitations=[
            "Synthetic fixtures support signal and execution checks. TradingView trade-by-trade parity is unverified.",
            "Native OHLC simulation uses the lab's explicit fill, rounding and intrabar ambiguity policies.",
            "The source hypothesis has no permitted economic result recorded for this family.",
        ],
    )
    if variant == "source":
        spec["risk"] = {"starting_equity": 10000, "risk_fraction": .0025, "notional_cap": .10, "leverage": 1,
                        "ordinary_cap": 1, "liquidation_buffer_atr": 0}
        spec["execution"] = {"version": SOURCE_POLICY, "processing_seconds": 0, "entry_limit_atr": 0,
                             "funding_reserve_bps": 0}
        spec["cost"] = {"taker": .0005, "adverse_proxy": 0, "basis": "PINE_SOURCE_DECLARATION"}
        spec["variant"]["sizing"] = {"mode": "PERCENT_EQUITY_NOTIONAL", "allocation_fraction": .10,
                                     "risk_fraction_used": False}
    else:
        baseline = _profile_baseline(db, profile)
        spec.update({key: baseline["spec"][key] for key in ("risk", "cost", "execution", "session")})
        spec["variant"].update(baseline_hash=baseline["hash"], sizing={"mode": "ASEKE_PROFILE_STOP_RISK"})
        spec["limitations"].append("The profile applies its own costs, bounded native IOC entry and maximum hold.")
        if profile == "position":
            spec["limitations"].append("The Spot variant suppresses short proposals.")
    return spec


def register_ema_pullback(db):
    source = _stage_artifacts(db)
    code = ema_code_hashes()
    records = []
    for variant in ("source", "position", "swing"):
        spec = _spec(db, variant, source, code)
        if issues := validate(spec):
            raise ValueError(f"EMA pullback {variant} specification requires review: {issues}")
        prior = db.execute("SELECT hash FROM strategy_version WHERE strategy_id=? AND version=1",
                           (spec["strategy_id"],)).fetchone()
        if prior:
            record = get(db, prior["hash"])
            if record["spec"] != spec:
                raise ValueError(f"Frozen {spec['strategy_id']} v1 differs. Review a new version explicitly")
        else:
            record = register(
                db, spec, initial="SPECIFIED", reason="Reviewed port of immutable user-supplied Pine v6 rules",
                flags={"evidence_quality": "REVIEWED_SOURCE", "data_completeness": "BLOCKED_DATA"},
                reference={"pine_sha256": source["sha256"], "description_sha256": source["description_sha256"],
                           "evaluator_code_sha256": code},
            )
        records.append(record)
    return records
