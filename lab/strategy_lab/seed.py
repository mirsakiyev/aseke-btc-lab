from lab.config import ROOT as PROJECT
from lab.phase13.registry import CAMPAIGN, PARENT, sealed
from lab.phase15.catalog import registered
from lab.phase16.plan import ROOT as PHASE16, SPOT, folder
from lab.phase9.reference import OUTPUT as PHASE9
from lab.storage import file_hash, read_json
from lab.strategy_lab.registry import register
from lab.strategy_lab.specification import INDICATORS, recipe_description, template


CODE_PATHS = (
    "lab/learning/features.py", "lab/learning/simulator.py", "lab/phase10/trading.py",
    "lab/phase15/catalog.py", "lab/phase15/opportunities.py", "lab/phase15/paper.py",
)
LABELS = {"position": "Position", "swing": "Swing", "day": "Day"}


def recipe_spec(profile, entry, exit_name, entry_params, exit_params, rules, risk, execution, cost):
    spec = template(profile)
    spec["context"] = rules["context"]
    spec["sides"] = ["long"] if rules["long_only"] else ["long", "short"]
    spec["session"].update({key: rules[key] for key in
                            ("max_hold_minutes", "entry_cutoff_minute_utc", "flat_minute_utc") if key in rules})
    spec["indicators"] = {key: {"definition": value} for key, value in INDICATORS.items()
                          if key not in ("bollinger", "candle_vwap")}
    spec["indicators"]["sma"]["windows"] = rules["context"]
    spec["risk"], spec["execution"], spec["cost"] = risk, execution, cost
    spec["entry_rules"], spec["exit_rules"], spec["invalidation_rules"] = recipe_description(entry, exit_name, exit_params)
    spec["recipe"] = {"entry": entry, "entry_parameters": entry_params, "exit": exit_name, "exit_parameters": exit_params}
    return spec


def source(reference, paths):
    return {"kind": "repository", "reference": reference, "license": "Repository terms. No license grant recorded here",
            "author": "ASEKE BTC Lab", "code_sha256": {path: file_hash(PROJECT / path) for path in paths}}


def seed(db):
    records, missing = [], []

    def add(spec, state, reason, flags, reference):
        prior = db.execute("SELECT hash FROM strategy_version WHERE strategy_id=? AND version=1",
                           (spec["strategy_id"],)).fetchone()
        if prior:
            from lab.strategy_lab.registry import get
            records.append(get(db, prior["hash"]))
        else:
            records.append(register(db, spec, initial=state, reason=reason, flags=flags, reference=reference))

    if registration := registered():
        body = registration["body"]
        provenance = source("PHASE15_RUNBOOK.md and sealed Phase 15 registration", CODE_PATHS)
        provenance.update(registration=registration["id"], registered_at=body["registered_at"],
                          document_sha256=file_hash(PROJECT / "PHASE15_RUNBOOK.md"))
        for key, lane in sorted(body["lanes"].items()):
            name = lane["profile"]
            spec = recipe_spec(name, lane["strategy"], lane["exit"], lane["entry_parameters"], lane["exit_parameters"],
                               lane["profile_rules"], lane["risk"], lane["execution"], lane["cost"])
            spec.update(strategy_id=f"phase15/{key}", family_id=lane["strategy"],
                        name=f"{LABELS[name]} {'ATR trailing' if lane['exit'] == 'atr_trail_v1' else 'fixed R'}",
                        description="Frozen Phase 15 rules-only breakout lane", origin="PHASE15_REGISTERED_PAPER",
                        source=provenance, implementation_version=lane["strategy_version"],
                        variant={"parameters": lane["entry_parameters"], "exit": lane["exit"], "policy": body["policy"]},
                        original_registration={**{field: body[field] for field in (
                            "policy", "start", "publication_end_exclusive", "follow_end_exclusive")},
                            "lane": lane, "publication_policy": body["config"]["ideas"]},
                        limitations=["The historical IOC adapter uses its own order model and cannot reproduce Phase 15 publications."])
            spec["entry_rules"].append(
                "Publication requires fresh executable quote evidence, current instrument rules, an unexpired UTC session, "
                "risk feasibility, and a valid trigger-basis price. Fill requires a strictly subsequent eligible trade minute "
                "with the breakout still confirmed and the adverse-adjusted open inside the frozen range."
            )
            spec["exit_rules"].append("Paper protection uses Spot trade or futures mark triggers and modeled trade-price fills.")
            add(spec, "IMPLEMENTED", "Indexed existing paper implementation; sandbox adapter test is independent",
                {"evidence_quality": "REGISTERED_PAPER", "reproducibility": "SEALED_SOURCE"},
                {"registration": registration["id"], "book": lane["book_id"], "test_reference": "tests/test_phase15.py"})
    else:
        missing.append("Phase 15 sealed registration")

    pointer = PHASE16 / "initial.json"
    if pointer.exists():
        identity = read_json(pointer)["id"]
        location = folder(identity)
        campaign = sealed(location / "manifest.json", identity)["body"]
        result_path = location / "result.json"
        result = read_json(result_path) if result_path.exists() else {}
        for profile, item in sorted(campaign["profiles"].items()):
            trial = item["trial"]
            if profile == "position":
                parent = sealed(PHASE9 / "campaigns" / SPOT / "manifest.json", SPOT)
            else:
                parent = sealed(CAMPAIGN / "manifest.json", PARENT)
            settings = parent["body"]["design"]["settings"]
            entry, exit_name = trial["entry"], trial["exit_policy"]
            spec = recipe_spec(profile, entry, exit_name, settings["strategies"][entry], settings["exits"][exit_name],
                               item["profile"], item["risk"], settings["execution"], item["costs"])
            provenance = source("PHASE16_RUNBOOK.md and initial sealed campaign", ("lab/phase16/plan.py", "lab/phase16/evaluate.py"))
            provenance.update(campaign=identity, parent=parent["id"], dataset=item["data_manifest"],
                              document_sha256=file_hash(PROJECT / "PHASE16_RUNBOOK.md"))
            outcome = result.get("profiles", {}).get(profile, {}) if result.get("campaign") == identity else {}
            reference = {"campaign": identity, "trial": trial["id"], "folds": item["folds"],
                         "test_reference": "tests/test_phase16.py"}
            if outcome:
                reference.update(conclusion=outcome.get("conclusion"), recorded_result_sha256=file_hash(result_path))
            spec.update(strategy_id=f"phase16/{profile}/initial-ridge", family_id=entry,
                        name=f"{LABELS[profile]} initial Ridge challenger", description=item["hypothesis"],
                        origin="PHASE16_INITIAL_HISTORICAL", source=provenance, implementation_version=campaign["code"],
                        variant={"model": trial["model"], "exit": exit_name, "features": item["features"]},
                        original_evaluation=item, historical_reference=reference,
                        limitations=["Historical catalog reference only. The sandbox evaluator does not load fitted models."])
            spec["data"]["original_scope"] = item["source_scope"]
            if item["source_scope"] == "BINANCE_DEVELOPMENT_RESEARCH_ONLY":
                spec["venue"], spec["instrument"] = "Binance", "BTCUSDT"
            spec["entry_rules"].append(
                f"Apply the frozen Ridge subset with alpha {campaign['policy']['alpha']:g} and require a prediction "
                f"strictly above {campaign['policy']['entry_threshold_net_r']:g} net R."
            )
            add(spec, "HISTORICAL_RESEARCH", "Indexed exact initial Phase 16 trial; original evaluation stays in Phase 16",
                {"evidence_quality": "CONSUMED_DEVELOPMENT", "reproducibility": "SEALED_SOURCE"}, reference)
    else:
        missing.append("Phase 16 initial campaign")
    return {"versions": records, "missing_sources": missing}
