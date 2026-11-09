import argparse
import json


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
        from lab.phase14.collection import managed_observe
        launcher = "operator"
        if name == "_observe":
            import psutil
            from lab.phase10.worker import RUNTIME
            from lab.storage import read_json
            worker = read_json(RUNTIME)
            ancestors = {proc.pid for proc in psutil.Process().parents()}
            if worker["pid"] not in ancestors or worker["launch_id"] != args.worker_launch:
                raise ValueError("Managed observation requires the current task worker as its ancestor")
            launcher = f"{worker['launcher']}:{worker['launch_id']}"
        result = managed_observe(launcher)
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
