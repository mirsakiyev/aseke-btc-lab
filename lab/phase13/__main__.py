import argparse
import json
from contextlib import nullcontext


def _observe(launcher, audit=None):
    stage = audit.stage if audit else lambda _: nullcontext()
    with stage("source_import"):
        from lab.phase14.collection import managed_observe
        from lab.storage import RUNS

    native = None
    if audit and (RUNS / "phase17" / "registration.json").exists():
        try:
            with stage("native_registration"):
                from lab.phase17.registry import registered
                native = registered()
        except (KeyError, OSError, RuntimeError, ValueError) as error:
            from lab.storage import utc_now, write_json
            write_json(RUNS / "phase17" / "managed-error.json", {"at": utc_now().isoformat(), "reason": str(error)})
    with stage("shared_source_collection"):
        if native:
            from lab.phase17.collection import price_observe
            result = price_observe(native, launcher)
        else:
            result = managed_observe(launcher)
    if not audit:
        return result

    with stage("phase15_import"):
        from lab.phase15.catalog import ROOT as paper_root
    if (paper_root / "registration.json").exists():
        try:
            with stage("phase15_paper_cycle"):
                from lab.phase15.worker import cycle as paper_cycle
                paper = paper_cycle(launcher, source_cycle=result,
                                    allow_spot=result.get("stop_reason") not in {"RATE_LIMITED", "ACCESS_RESTRICTED"}
                                    and result.get("state") not in {"RETRY_AFTER", "ACCESS_RESTRICTED", "COLLECTION_OFF"})
        except Exception as error:
            from lab.storage import utc_now, write_json
            write_json(paper_root / "managed-error.json", {"at": utc_now().isoformat(), "launcher": launcher,
                                                          "type": type(error).__name__, "reason": str(error)})
        else:
            audit.phase15(paper)
    if native:
        try:
            with stage("native_managed"):
                from lab.phase17.managed import tick
                tick(native, launcher, result)
        except (KeyError, OSError, RuntimeError, ValueError) as error:
            from lab.storage import utc_now, write_json
            write_json(RUNS / "phase17" / "managed-error.json", {"at": utc_now().isoformat(), "reason": str(error)})
    challenger_controls = RUNS / "phase16" / "controls.json"
    try:
        with stage("challenger_dispatch"):
            from lab.storage import read_json
            if challenger_controls.exists() and read_json(challenger_controls).get("recurring"):
                from lab.phase16.control import managed_tick
                managed_tick()
    except (OSError, RuntimeError, ValueError) as error:
        from lab.storage import utc_now, write_json
        write_json(RUNS / "phase16" / "dispatch-error.json", {"at": utc_now().isoformat(), "reason": str(error)})
    return result


def _managed_observe(launch):
    from pathlib import Path
    from lab.phase10.observer import ObservationAudit
    from lab.phase10.repair_guard import _lease

    project = Path(__file__).resolve().parents[2]
    state = project / "data" / "phase10"
    audit = ObservationAudit(project / "runs" / "phase10", launch)
    with audit.open(), audit.stage("managed_observation"):
        with audit.stage("ownership"):
            import psutil

            worker = json.loads((state / "runtime.json").read_text(encoding="utf-8"))
            if (not any(parent.pid == worker["pid"] and parent.create_time() == worker["created"]
                        for parent in psutil.Process().parents()) or worker["launch_id"] != launch):
                raise ValueError("Managed observation requires the current task worker as its ancestor")
            launcher = f"{worker['launcher']}:{worker['launch_id']}"
        with audit.stage("observer_lease"), _lease(audit.output / ".observer-cycle.lock"):
            if json.loads((state / "control.json").read_text(encoding="utf-8"))["paused"] is not False:
                return {"state": "PAUSED"}
            return _observe(launcher, audit)


def main():
    parser = argparse.ArgumentParser(prog="phase13")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("freeze", "register", "observe-once", "status", "report", "diagnose", "technical-review",
                 "prepare-evaluation", "evaluate"):
        sub.add_parser(name)
    sub.add_parser("collection").add_argument("state", choices=("on", "off"))
    sub.add_parser("resume").add_argument("diagnostic")
    sub.add_parser("_diagnose").add_argument("diagnostic")
    sub.add_parser("_observe").add_argument("worker_launch")
    infer = sub.add_parser("infer")
    infer.add_argument("lane")
    infer.add_argument("input")
    infer.add_argument("--fold")
    args = parser.parse_args()
    name = args.action
    if name in ("diagnose", "resume", "_diagnose"):
        from lab.phase13 import diagnostics
        from lab.phase13.registry import sealed
        if name == "_diagnose":
            diagnostics.work(args.diagnostic)
            return
        manifest = diagnostics.plan() if name == "diagnose" else sealed(
            diagnostics.folder(args.diagnostic) / "manifest.json", args.diagnostic)
        result = diagnostics.supervise(manifest)
    elif name in ("status", "report", "technical-review"):
        from lab.phase13 import report
        result = {"status": report.status, "report": report.report, "technical-review": report.technical}[name]()
    elif name == "prepare-evaluation":
        from lab.phase13.future import prepare_evaluation
        result = prepare_evaluation()
    elif name == "evaluate":
        from lab.phase13.replay import evaluate
        result = evaluate()
    elif name == "freeze":
        from lab.phase13.registry import freeze
        reg = freeze()
        result = {"registry": reg["id"], "lanes": {key: item["id"] for key, item in reg["body"]["candidates"].items()}}
    elif name in ("register", "collection"):
        from lab.phase13 import protocol
        result = protocol.register() if name == "register" else protocol.set_control(args.state == "on")
    elif name in ("observe-once", "_observe"):
        result = _managed_observe(args.worker_launch) if name == "_observe" else _observe("operator")
    else:
        from pathlib import Path
        import pandas as pd
        from lab.phase13.inference import decision
        from lab.phase13.registry import freeze
        from lab.storage import utc
        item = freeze()["body"]["candidates"][args.lane]
        proof = json.loads(Path(args.input).read_text(encoding="utf-8-sig"))
        cutoff = utc(proof["input_cutoff"])
        feature = pd.DataFrame([proof["features"]], index=[cutoff]) if proof.get("features") else None
        result = decision(item, feature, cutoff, proof["decision_at"],
                          {**proof["evidence"], "scope": "SYNTHETIC"}, fold=args.fold)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
