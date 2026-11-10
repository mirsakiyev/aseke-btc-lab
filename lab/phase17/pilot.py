from pathlib import Path

import joblib
import pandas as pd

from lab.config import fingerprint
from lab.learning.evaluation import errors, matured, scores
from lab.phase11.campaign import start_clock
from lab.phase13.registry import checked, sealed
from lab.phase16.allocation import completed_for
from lab.phase16.control import settings as controls
from lab.phase16.evaluate import account, conclusion, feature_matrix, fitted_model
from lab.phase16.plan import code_version
from lab.phase17.admission import training_identity
from lab.phase17.labels import label
from lab.phase17.pilot_context import builder, pinned
from lab.phase17.pilot_registry import PILOT, current, find, input_record, save
from lab.phase17.registry import ROOT, registered
from lab.phase17.source import snapshot
from lab.storage import file_hash, read_json, utc, utc_now, write_json


DAY = pd.Timedelta(days=1)
GRACE = pd.Timedelta(hours=2)


def optional(path):
    return read_json(path) if path.exists() else None


def profile_dir(name):
    return PILOT / "profiles" / name


def record_pointer(root, name, record):
    write_json(root / (name+".json"), {"id":record["id"]})


def pointed(root, name, kind):
    pointer = optional(root / (name+".json"))
    return find(kind, pointer["id"]) if pointer else None


def procedure_identity():
    paths = ("pilot.py","pilot_registry.py","pilot_context.py","labels.py","source.py")
    return fingerprint([code_version(),{name:file_hash(Path(__file__).with_name(name)) for name in paths}])


def label_input(pilot, name, seed, end, *, decision_start=None, saved_source=None):
    base = input_record(pilot)
    if saved_source:
        source = sealed(ROOT / "sources" / (saved_source+".json"), saved_source)
    else:
        source = snapshot(base, name, end, utc(end)+GRACE, namespace=PILOT / "inputs", storage_record=registered())
    if not source["body"]["parts"]:
        raise ValueError("No closed eligible native source partitions")
    if source["body"]["start"] != pilot["body"]["profiles"][name]["label_start"]:
        raise ValueError("Pilot source crosses its prospective allocation")
    prepared = label(base, source, feature_builder=builder(pilot, name, seed), decision_start=decision_start)
    return source, prepared


def samples(cfg, labels, cutoff, split=None):
    start = utc(cfg["label_start"])
    cutoff = utc(cutoff)
    train_stop = utc(split["body"]["cutoff"]) if split else cutoff
    train = matured(labels, start, train_stop)
    inner = matured(labels, train_stop, cutoff) if split else labels.iloc[:0]
    total = len(matured(labels, start, cutoff))
    return train, inner, total


def fit_gate(cfg, train, inner, total, due, now):
    if not cfg["queue"]:
        return "BASELINE_ONLY", "Swing challenger remains shelved"
    missing = [f"{key} {n}/{minimum}" for key,n,minimum in (
        ("train",len(train),cfg["minimum_train"]),("inner",len(inner),cfg["minimum_inner"]),("new mature",total,cfg["minimum_new_mature"])) if n < minimum]
    if missing:
        return "WAITING_FOR_MATURE_SAMPLES", ", ".join(missing)
    if now < due:
        return "WAITING_FOR_RETRAINING_INTERVAL", f"Existing profile interval ends {due.isoformat()}"
    return "FIT_ELIGIBLE", "Frozen train, inner and new-maturity floors and the profile interval cleared"


def forward_registration(pilot, name, candidate, now):
    cfg = pilot["body"]["profiles"][name]
    source_stop = utc(candidate["body"]["development_cutoff"])+GRACE
    start = max(utc(now), source_stop).floor("1D")+DAY
    end = start+cfg["evaluation_days"]*DAY
    if end > utc(cfg["source_end_exclusive"]):
        return None
    return save("evaluations", {"version":"phase17.1-fixed-pilot-evaluation-v1", "pilot":pilot["id"], "profile":name,
        "candidate":candidate["id"], "registered_at":utc(now).isoformat(), "start":start.isoformat(), "end_exclusive":end.isoformat(),
        "scoring_available_at":(end+GRACE).isoformat(), "scope":"RESEARCH_ONLY", "minimum_outer":cfg["minimum_outer"],
        "minimum_closed":pilot["body"]["policy"]["minimum_closed"], "controls":pilot["body"]["policy"]["controls"],
        "cases":pilot["body"]["policy"]["cases"], "stopping_rule":"Fixed horizon. One frozen model, no retraining, early performance access or automatic promotion."})


def ensure_forward(pilot, name, candidate, now):
    root = profile_dir(name)
    receipt = pointed(root, "evaluation", "evaluations")
    if receipt:
        if receipt["body"]["candidate"] != candidate["id"]:
            raise ValueError("Pilot forward registration belongs to another model")
        return receipt
    receipt = forward_registration(pilot, name, candidate, now)
    if receipt:
        record_pointer(root, "evaluation", receipt)
    return receipt


def prepare(pilot, name, cutoff, work):
    now = utc_now()
    cfg = pilot["body"]["profiles"][name]
    root = profile_dir(name)
    end = utc(cutoff)
    if end > (now-GRACE).floor("1D") or end <= utc(cfg["label_start"]):
        raise ValueError("Use a closed prospective cutoff after its receipt grace")
    if candidate := pointed(root, "candidate", "candidates"):
        evaluation = ensure_forward(pilot, name, candidate, now)
        return {"state":"CANDIDATE_FROZEN", "reason":"One pinned pilot candidate has completed", "candidate":candidate["id"],
                "evaluation":evaluation["id"] if evaluation else None, **candidate["body"]["readiness"]}
    seed = pinned(pilot, name)
    if not seed:
        return {"state":"WAITING_FOR_FEATURE_CONTEXT", "reason":"A causal bridge has not been pinned at or after the label start"}
    intent = pointed(root, "fit-intent", "intents")
    if intent and intent["body"]["procedure"] != procedure_identity():
        raise ValueError("Frozen pilot fitting procedure changed")
    check_end = utc(intent["body"]["cutoff"]) if intent else end
    try:
        source, (design, data, batch, _, _) = label_input(pilot,name,seed,check_end,
                        saved_source=intent["body"]["source"] if intent else None)
    except ValueError as error:
        return {"state":"WAITING_FOR_NATIVE_PATH", "reason":str(error), "context":seed["id"]}
    setups, cost, labels, _, counts = batch
    split = pointed(root, "split", "splits")
    train, inner, total = samples(cfg, labels, check_end, split)
    if split and training_identity(train) != split["body"]["training_identity"]:
        return {"state":"TRAINING_REVISION_REQUIRES_REVIEW", "reason":"A revised path differs from the frozen training sample",
                "context":seed["id"], "source":source["id"], "split":split["id"], "mature_train":len(train), "mature_inner":len(inner)}
    if not split and cfg["queue"] and len(train) >= cfg["minimum_train"]:
        split = save("splits", {"pilot":pilot["id"], "profile":name, "cutoff":check_end.isoformat(), "source":source["id"],
                                "seed":seed["id"], "training_identity":training_identity(train), "mature_train":len(train)})
        record_pointer(root, "split", split)
    due = utc(completed_for(name)["completed_at"])+cfg["interval_days"]*DAY if cfg["queue"] else now
    state, reason = fit_gate(cfg,train,inner,total,due,now)
    readiness = {"context":seed["id"], "source":source["id"], "cutoff":check_end.isoformat(),
                 "mature_train":len(train), "mature_inner":len(inner), "mature_total":total, "counts":counts,
                 "training_split":split["body"]["cutoff"] if split else None}
    if state != "FIT_ELIGIBLE":
        return {**readiness,"state":state,"reason":reason}
    if not controls()["recurring"]:
        return {**readiness,"state":"FIT_ELIGIBLE_TRAINING_OFF","reason":"Mature samples qualify. Phase 16 recurring training is off"}
    if intent is None:
        intent = save("intents", {"pilot":pilot["id"], "profile":name, "cutoff":check_end.isoformat(), "source":source["id"],
                                  "seed":seed["id"], "split":split["id"], "inner_identity":training_identity(inner),"procedure":procedure_identity()})
        record_pointer(root,"fit-intent",intent)
    if training_identity(inner) != intent["body"]["inner_identity"]:
        raise ValueError("Pilot fitting intent and assessment rows differ")
    fit_root = root / "fit"
    clock = read_json(work / "deadline.json")
    start_clock(fit_root, pilot["body"]["budget"]["wall_seconds"], utc(clock["started_at"]))
    write_json(work / "fitting.json", {"fit_root":str(fit_root)})
    schema = cfg["features"]
    features = {"features":feature_matrix(data["features"],schema,setups,cost), "columns":schema}
    fold = {"id":"pilot", "inner_start":split["body"]["cutoff"]}
    if not controls()["recurring"]:
        return {**readiness,"state":"FIT_ELIGIBLE_TRAINING_OFF","reason":"Training disabled before fit"}
    model, receipt = fitted_model(fit_root,intent,name,fold,"inner",train,features,pilot["body"]["policy"],design["seed"])
    mean = float(train.net_r.mean())
    forecast = {"candidate":errors(scores(model,features,inner.index),inner), "training_mean":errors(scores(mean,features,inner.index),inner)}
    candidate = save("candidates", {"pilot":pilot["id"],"profile":name,"scope":"RESEARCH_ONLY","fit":receipt,"intent":intent["id"],
                    "development_cutoff":check_end.isoformat(),"training_mean":mean,"inner_forecast":forecast,"readiness":readiness,
                    "comparators":pilot["body"]["policy"]["controls"],"recipe":cfg["recipe"],"procedure":intent["body"]["procedure"]})
    record_pointer(root,"candidate",candidate)
    evaluation = ensure_forward(pilot,name,candidate,utc_now())
    return {**readiness,"state":"CANDIDATE_FROZEN","reason":"One immutable research-only fit saved", "candidate":candidate["id"],
            "evaluation":evaluation["id"] if evaluation else None}


def evaluate(pilot, name, work, now=None):
    now = utc_now() if now is None else utc(now)
    root = profile_dir(name)
    protocol = pointed(root,"evaluation","evaluations")
    if not protocol:
        return {"state":"WAITING_FOR_FORWARD_REGISTRATION"}
    cohort = protocol["body"]
    if now < utc(cohort["scoring_available_at"]):
        return {"state":"FORWARD_HORIZON_OPEN","due":cohort["scoring_available_at"]}
    cfg = pilot["body"]["profiles"][name]
    candidate = find("candidates",cohort["candidate"])["body"]
    if candidate["procedure"] != procedure_identity():
        raise ValueError("Frozen pilot evaluation procedure changed")
    seed = find("context",candidate["readiness"]["context"])
    start,end = utc(cohort["start"]),utc(cohort["end_exclusive"])
    source, (design,data,batch,executor,_) = label_input(pilot,name,seed,end,decision_start=start)
    setups,cost,labels,_,_ = batch
    outer = matured(labels,start,end)
    if len(outer) < cfg["minimum_outer"]:
        return {"state":"INSUFFICIENT_EVALUATION_SAMPLE","mature":len(outer),"required":cfg["minimum_outer"],"source":source["id"]}
    fit = candidate["fit"]
    artifact = joblib.load(checked(root/"fit"/"models"/(fit["version"]+".joblib"),fit["sha256"]))
    if artifact["version"] != fit["version"] or artifact["campaign"] != candidate["intent"] or artifact["features"] != cfg["features"]:
        raise ValueError("Pilot model pin differs from the forward registration")
    schema = cfg["features"]
    features = {"features":feature_matrix(data["features"],schema,setups,cost), "columns":schema}
    index = features["features"].index
    lane = {"challenger":scores(artifact["model"],features,index), "training_mean":scores(candidate["training_mean"],features,index),
            "unfiltered":pd.Series(1.,index=index), "half_risk_rule":pd.Series(1.,index=index)}
    design = {**design,"folds":[{"id":"forward","outer_start":start,"outer_end":end}]}
    policy = pilot["body"]["policy"]
    accounts = {}
    for stress in policy["cases"]:
        for role,pred in lane.items():
            accounts[f"{role}/{stress}"] = account(work,name,role,stress,design,data,cfg["recipe"],setups,cost,{"forward":pred},
                    {"forward":fit["version"]} if role=="challenger" else {},policy,executor)
    verdict, reason = conclusion(accounts,True,policy["cases"])
    return {"state":"EVALUATED_RESEARCH_ONLY","protocol":protocol["id"],"model":fit["version"],"source":source["id"],
            "mature":len(outer),"conclusion":verdict,"reason":reason,"cash":0.,"accounts":accounts,
            "forecasts":{key:errors(lane[key],outer) for key in ("challenger","training_mean")}}
