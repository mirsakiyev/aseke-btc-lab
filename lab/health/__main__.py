import sys

_scheduled_trace = None
if __name__ == "__main__" and "check-once" in sys.argv and "--scheduled" in sys.argv:
    from lab.health.diagnostics import bootstrap

    _scheduled_trace = bootstrap()

import argparse
from contextlib import closing, suppress
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from uuid import uuid4

import pandas as pd
import psutil

from lab.health import dashboard_service as dashboard
from lab.health.delivery import send
from lab.health.state import DEFAULTS, ROOT, connect, enqueue, flush, get_meta, local, observe, policy, set_meta, set_policy, status
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


TASKS = ("ASEKE BTC Lab - Forward Research", "ASEKE BTC Lab - KuCoin Monitoring",
         "ASEKE BTC Lab - Dashboard", "ASEKE BTC Lab - Health Watchdog")
WATCHDOG_GRACE_SECONDS = 420


def _stage(name, *, reason=None, decisions=None):
    if _scheduled_trace is not None:
        _scheduled_trace.record(name, reason=reason, decisions=decisions)


def task_evidence():
    names = ",".join(f"'{name}'" for name in TASKS)
    script = f"""$ErrorActionPreference = 'Stop'
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $folder = $service.GetFolder('\\')
    foreach ($name in @({names})) {{
        try {{
            $task = $folder.GetTask($name)
            $logon = try {{ $task.Definition.Principal.LogonType }} catch {{ '' }}
            [Console]::WriteLine(('{{0}}|{{1}}|{{2}}|{{3}}|{{4:o}}|{{5:o}}' -f
                $name,$task.State,$logon,$task.LastTaskResult,$task.LastRunTime,$task.NextRunTime))
        }} catch {{
            $exception = $_.Exception
            while ($exception.InnerException) {{ $exception = $exception.InnerException }}
            $state = if ($exception.HResult -eq -2147024894) {{ 'MISSING' }} else {{ 'UNKNOWN' }}
            [Console]::WriteLine(('{{0}}|{{1}}' -f $name,$state))
        }}
    }}"""
    exe=Path(os.environ["WINDIR"])/"System32/WindowsPowerShell/v1.0/powershell.exe"
    try:
        out=subprocess.run([str(exe),"-NoProfile","-NonInteractive","-Command",script],capture_output=True,timeout=12,
                           check=True,creationflags=subprocess.CREATE_NO_WINDOW)
    except (OSError,subprocess.SubprocessError):
        return {}
    # Windows PowerShell 5.1 JSON serialization can fail even when the native
    # scheduler query succeeds. The fields emitted above are fixed and ASCII.
    states = {"1": "DISABLED", "2": "QUEUED", "3": "READY", "4": "RUNNING"}
    logons = {"1": "Password", "2": "S4U", "3": "Interactive", "4": "Group",
              "5": "ServiceAccount", "6": "InteractiveOrPassword"}
    no_task_time = "1899-12-30T00:00:00.0000000"
    result = {}
    for line in out.stdout.decode("ascii", errors="replace").splitlines():
        name, *fields = line.split("|")
        if name not in TASKS:
            continue
        result[name] = {"state": "UNKNOWN"}
        if fields == ["MISSING"]:
            result[name]["state"] = "MISSING"
        elif len(fields) == 5:
            code, logon, exit_code, last, next_run = fields
            try:
                result[name] = {"state": states.get(code, "UNKNOWN"), "logon": logons.get(logon),
                                "result": int(exit_code), "last": last if last != no_task_time else "",
                                "next": next_run if next_run != no_task_time else ""}
            except ValueError:
                pass
    return result


def _check_kraken_paper(db, cfg, root):
    errors = (ImportError, OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error, psutil.Error)
    keys = ("kraken_source_producer", "kraken_paper", "kraken_paper_notifications")
    try:
        from lab.kraken_paper import retirement

        if retirement.history(root=root):
            return {key: {"state": retirement.STATE} for key in keys}
    except errors as exc:
        return {key: {"state": "CHECK_FAILED", "reason": type(exc).__name__} for key in keys}
    result = {}
    try:
        from lab.kraken_paper import managed

        result[keys[0]] = managed.ensure()
    except errors as exc:
        result[keys[0]] = {"state": "CHECK_FAILED", "reason": type(exc).__name__}
    try:
        from lab.kraken_paper import worker

        result[keys[1]] = worker.cycle()
    except errors as exc:
        result[keys[1]] = {"state": "CHECK_FAILED", "reason": type(exc).__name__}
    try:
        from lab.kraken_paper import notifications

        result[keys[2]] = notifications.collect(db, utc_now(), cfg)
    except errors as exc:
        result[keys[2]] = {"state": "CHECK_FAILED", "reason": type(exc).__name__}
    return result


def _kraken_reference_continuity(reference, now, source_root):
    """Check due minutes by first availability, ignoring minutes still within their allowance."""
    from lab.kraken_paper import source

    heartbeat_at = reference.get("heartbeat", {}).get("at")
    minute = next((item for item in reference.get("evidence", {}).get("checkpoints", ())
                   if item.get("interval_minutes") == 1), {})
    stream = minute.get("source_id")
    if not heartbeat_at or not stream or not pd.Timedelta(0) <= now - utc(heartbeat_at) <= source.TIMELY_CHART:
        return False
    # Check the previous watchdog interval at the registered receipt deadline.
    step = pd.Timedelta(minutes=1)
    due = (now - source.TIMELY_CHART).floor(step)
    first = due - 3 * step
    db = source._read_connection(source_root)
    if db is None:
        return False
    with closing(db):
        timely = db.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT o.opened FROM observation o JOIN receipt r ON r.id=o.receipt_id "
            "WHERE o.stream=? AND o.minutes=1 AND o.opened>=? AND o.opened<? AND r.available<=? "
            "GROUP BY o.opened HAVING COUNT(*)=1 AND MIN(r.available)<=o.opened+?)",
            (stream, first.value, due.value, now.value, (step + source.TIMELY_CHART).value),
        ).fetchone()[0]
    return timely == 3


def _dispatch_ema():
    evidence = {}
    baseline = None
    errors = (ImportError, OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error, psutil.Error)
    try:
        _stage("evaluation_baseline")
        from lab.health import evaluation

        baseline = evaluation.baseline(ROOT.parent)
        _stage("ema_decisions_before_dispatch", decisions={
            profile: {"boundary": boundary.isoformat(), "observed_at": observed.isoformat(), "outcome": outcome}
            for profile, (boundary, observed, outcome) in baseline.items()})
    except errors as exc:
        _stage("evaluation_baseline_error", reason=type(exc).__name__)
    try:
        _stage("ema_import")
        from lab.ema_paper import synthetic_protocol

        if synthetic_protocol.active():
            from lab.ema_paper import recovery_protocol, synthetic_runtime

            if recovery_protocol.active():
                from lab.ema_paper import position_seam

                cycle = position_seam.cycle
            else:
                cycle = synthetic_runtime.cycle
        else:
            from lab.ema_paper import dispatcher, proxy_protocol, worker

            cycle = dispatcher.cycle if proxy_protocol.amended() else worker.cycle
        _stage("ema_dispatch")
        evidence["ema_paper"] = cycle()
        _stage("ema_dispatch_complete", reason=evidence["ema_paper"].get("state"))
    except errors as exc:
        _stage("ema_dispatch_error", reason=type(exc).__name__)
        evidence["ema_paper"] = {"state": "CHECK_FAILED", "reason": f"{type(exc).__name__}: {exc}"}
    if _scheduled_trace is not None:
        try:
            from lab.health import evaluation

            decisions = {profile: {"boundary": boundary.isoformat(), "observed_at": observed.isoformat(),
                                   "outcome": outcome, "advanced": (baseline or {}).get(profile, (None,))[0] != boundary}
                         for profile, (boundary, observed, outcome) in evaluation.baseline(ROOT.parent).items()}
            _stage("ema_decisions_committed", decisions=decisions)
        except errors as exc:
            _stage("ema_decisions_error", reason=type(exc).__name__)
    try:
        from lab.ema_paper import synthetic_protocol

        if synthetic_protocol.active():
            _stage("kraken_reference_supervision")
            from lab.ema_paper import kraken_reference

            synthetic_protocol.load()
            reference = kraken_reference.ensure()
            evidence["ema_kraken_reference"] = reference
            evidence["ema_kraken_continuity"] = _kraken_reference_continuity(
                reference, utc_now(), kraken_reference.ROOT / "source")
            _stage("kraken_reference_complete", reason=reference.get("state"))
    except errors as exc:
        _stage("kraken_reference_error", reason=type(exc).__name__)
        evidence.setdefault("ema_kraken_reference", {"state": "CHECK_FAILED", "reason": type(exc).__name__})
    return evidence, baseline


def check(*, deliver=True, mode="operator"):
    before=time.monotonic()
    cpu=time.process_time()
    _stage("check_lock")
    with writer_lock(ROOT/".check.lock"):
        evidence, baseline = {}, None
        if mode == "scheduled" and (ROOT.parent / "ema_paper" / "paper.sqlite3").is_file():
            evidence, baseline = _dispatch_ema()
        return _check_health(evidence, baseline, before=before, cpu=cpu, deliver=deliver, mode=mode)


def _check_health(evidence, baseline, *, before, cpu, deliver, mode):
    from lab.health import incidents
    _stage("health_import")
    from lab.health.monitor import collect, finding
    from lab.health import private_telegram
    from lab.ema_paper import private_notifications

    now=utc_now()
    epoch=int(now.timestamp())
    _stage("outbox_open")
    with connect() as db:
        cfg=policy()
        started=int(get_meta(db,"started",epoch))
        set_meta(db,"started",started)
        _stage("task_scheduler_probe")
        tasks=task_evidence()
        _stage("research_and_service_probes")
        components,findings=collect(db,now,started,cfg,tasks)
        evidence.update(components)
        try:
            private_settings = private_notifications.route(db)
        except (ValueError, KeyError, OSError):
            private_settings = None
            evidence["ema_private_route"] = "INVALID_LOCAL_CONFIGURATION"
        private_route = private_settings["route_id"] if private_settings else None
        if "ema_paper" in evidence:
            _stage("ema_notification_scan")
            try:
                from lab.ema_paper import notifications

                ema = evidence["ema_paper"]
                for profile, book_status in ema.get("books", {}).items():
                    if book_status.get("source_monitor") in {"HEALTHY", "DEGRADED"}:
                        finding(findings, f"EMA {profile.title()} KuCoin source",
                                "WARNING" if book_status["source_monitor"] == "DEGRADED" else None,
                                "Independently licensed KuCoin paper evidence is delayed or incomplete."
                                if book_status["source_monitor"] == "DEGRADED" else "Timely KuCoin paper evidence restored.")
                evidence["ema_paper_notifications"] = notifications.collect(db, utc_now(), cfg)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error, psutil.Error) as exc:
                evidence["ema_paper_notifications"] = {"state": "CHECK_FAILED", "reason": type(exc).__name__}
            if private_route:
                try:
                    current = utc_now()
                    evidence["ema_private_notifications"] = private_notifications.collect(db, current)
                    reference = evidence.get("ema_kraken_reference", {})
                    ema = evidence.get("ema_paper", {})
                    observations = {
                        "collector": reference.get("state") in {"RUNNING", "RUNNING_IDENTITY_UNVERIFIED", "STARTED"},
                        "staleness": evidence.get("ema_kraken_continuity", False),
                        "evaluation": ema.get("state") == "CHECKED" and all(
                            book.get("state") not in {"BLOCKED_PERMISSION", "BLOCKED_DATA", "WAITING_FOR_RESOURCES"}
                            and not (book.get("state") == "WAITING_FOR_DATA" and book.get("first_eligible_at")
                                     and not ((book.get("source_protocol") or {}).get("chart_recovery") or {}).get("has_discontinuity"))
                            for book in ema.get("books", {}).values()),
                        "watchdog": epoch - int(get_meta(db, "ema_private_watchdog_previous", epoch)) <= WATCHDOG_GRACE_SECONDS,
                    }
                    if ema.get("recovery_amendment_id"):
                        observations["chart_recovery"] = all(
                            not ((book.get("source_protocol") or {}).get("chart_recovery") or {}).get("has_discontinuity")
                            or (book.get("source_protocol") or {}).get("chart_recovery", {}).get("state") == "READY"
                            for book in ema.get("books", {}).values())
                    private_notifications.observe(db, current, observations)
                    set_meta(db, "ema_private_watchdog_previous", epoch)
                except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
                    evidence["ema_private_notifications"] = {"state": "LOCAL_ERROR", "reason": type(exc).__name__}
        kraken_root = ROOT.parent / "kraken_paper"
        if mode == "scheduled" and (kraken_root / "paper.sqlite3").is_file():
            _stage("retired_kraken_status")
            evidence.update(_check_kraken_paper(db, cfg, kraken_root))
        if mode == "scheduled":
            _stage("evaluation_freshness")
            db.execute("SAVEPOINT evaluation_freshness")
            try:
                from lab.health import evaluation

                evidence["evaluation_freshness"] = evaluation.record(
                    db, ROOT.parent, utc_now(), private_route=private_route, baseline=baseline, group_breakout=True)
            except (ImportError, OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
                db.execute("ROLLBACK TO evaluation_freshness")
                evidence["evaluation_freshness"] = {"state": "EVIDENCE_UNAVAILABLE", "reason": type(exc).__name__}
            finally:
                db.execute("RELEASE evaluation_freshness")
        queried = all(tasks.get(name, {}).get("state", "UNKNOWN") != "UNKNOWN" for name in TASKS[:2])
        finding(findings, "Task Scheduler evidence", None if queried else "WARNING",
                "Collector task states checked." if queried else "Could not verify both collector task states.")
        if 0 <= epoch - psutil.boot_time() < cfg["boot_grace_seconds"]:
            for key in ("Coinbase worker", "KuCoin worker"):
                if key in findings:
                    severity, detail, _ = findings[key]
                    finding(findings, key, severity, detail)
        evidence["health_checks"] = findings
        grouped = incidents.consolidate(findings, evidence, utc_now())
        incidents.adopt_legacy(db, grouped, epoch)
        observe(db, grouped, epoch, cfg, episode_only=incidents.GROUPS)
        db.commit()
        if deliver:
            _stage("telegram_delivery")
            route = None
            if private_route and db.execute("SELECT 1 FROM outbox WHERE id LIKE 'ema-private:%' "
                                            "AND status IN ('PENDING','RETRY') AND due<=? LIMIT 1", (epoch,)).fetchone():
                try:
                    candidate = private_telegram.destination()
                    if candidate.identifier == private_route:
                        route = candidate
                    else:
                        evidence["ema_private_route"] = "ROUTE_CHANGED_LOCAL_ONLY"
                except (OSError, ValueError):
                    evidence["ema_private_route"] = "ROUTE_VERIFICATION_FAILED_LOCAL_ONLY"
            flush(db,time.time(),cfg,send,clock=time.time, private_route_id=route.identifier if route else None,
                  private_send=(lambda text: private_telegram.send(route, text)) if route else None)
        completed=utc_now()
        _stage("watchdog_receipt")
        result={**evidence,"mode":mode,"completed_at":completed.isoformat(),"pid":os.getpid(),
                "wall_seconds":round(time.monotonic()-before,2),"cpu_seconds":round(time.process_time()-cpu,2),
                **status(db)}
        write_json(ROOT/"last.json",result)
        return result


def view():
    # The dashboard is a reader of the watchdog's atomic receipt. It never
    # queries Telegram or drives checks.
    path=ROOT/"last.json"
    return read_json(path) if path.exists() else {"state":"NO_WATCHDOG_RECEIPT"}


def _ema_evaluation_status(books, now):
    result = {}
    for profile, (saved, decision_seconds) in books.items():
        checked = saved.get("last_checked_at")
        evaluated = saved.get("last_evaluated_at")
        signal = saved.get("last_signal") or {}
        state = saved.get("state", "NOT_CHECKED")
        if state == "ACTIVE":
            cutoff = now - pd.Timedelta(seconds=WATCHDOG_GRACE_SECONDS)
            overdue = not evaluated or utc(evaluated) <= cutoff - pd.Timedelta(seconds=2 * decision_seconds)
            if not checked or utc(checked) < cutoff or overdue:
                state = "DISPATCHER_STALLED"
            elif signal.get("decision") == "NO_TRADE" and signal.get("observed_at"):
                state = "NO_TRADE"
            else:
                state = "EVALUATING"
        result[profile] = {
            "state": state,
            "last_checked_at": checked,
            "last_evaluated_at": evaluated,
            "last_decision": signal.get("decision"),
            "next_evaluation_at": saved.get("next_evaluation_at"),
        }
    return result


def _operational_status():
    """Read current component receipts independently of the last full watchdog check."""
    result = {}
    ema_root = ROOT.parent / "ema_paper"
    if (ema_root / "paper.sqlite3").is_file():
        try:
            from lab.ema_paper import catalog, worker

            with closing(catalog.db_open(readonly=True, root=ema_root)) as db:
                books = {}
                for profile, registration in catalog.registrations(db).items():
                    lane = registration["body"]
                    books[profile] = (worker._record(db, "readiness", lane["book_id"]),
                                      lane["execution_clock"]["decision_seconds"])
            result["ema_evaluation"] = _ema_evaluation_status(books, utc_now())
        except (OSError, RuntimeError, ValueError, KeyError, sqlite3.Error):
            result["ema_evaluation"] = {"state": "EVIDENCE_UNAVAILABLE"}
    reference_root = ROOT.parent / "ema_kraken_reference"
    if (reference_root / "reference-heartbeat.json").is_file():
        try:
            from lab.ema_paper import kraken_reference

            reference = kraken_reference.status(root=reference_root)
            healthy = _kraken_reference_continuity(reference, utc_now(), reference_root / "source")
            result["kraken_reference"] = {
                "state": "HEALTHY" if healthy else "INTERRUPTED",
                "heartbeat_at": reference.get("heartbeat", {}).get("at"),
            }
        except (OSError, RuntimeError, ValueError, KeyError, sqlite3.Error):
            result["kraken_reference"] = {"state": "EVIDENCE_UNAVAILABLE"}
    outbox = ROOT / "watchdog.sqlite3"
    if outbox.is_file():
        try:
            with closing(sqlite3.connect(outbox.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
                result["telegram_trade_delivery"] = [
                    {"category": category, "state": state, "count": count}
                    for category, state, count in db.execute(
                        "SELECT category,status,COUNT(*) FROM outbox "
                        "WHERE category IN ('ideas','paper_entry','paper_no_entry','paper_exit','open_position') "
                        "GROUP BY category,status ORDER BY category,status")
                ]
        except (OSError, sqlite3.Error):
            result["telegram_trade_delivery"] = {"state": "EVIDENCE_UNAVAILABLE"}
    return result


def stop_local():
    stopped=[]
    marker=ROOT/"local-watch.json"
    if marker.exists():
        row=read_json(marker)
        with suppress(psutil.NoSuchProcess):
            p=psutil.Process(row["pid"])
            if p.create_time()==row["created"] and Path(p.cwd()).resolve()==dashboard.PROJECT.resolve() and "lab.health.local_service" in p.cmdline():
                p.terminate()
                p.wait(8)
                stopped.append("local watchdog")
    marker=ROOT/"dashboard-supervisor.json"
    if marker.exists():
        row=read_json(marker)
        with suppress(psutil.NoSuchProcess):
            p=psutil.Process(row["supervisor_pid"])
            if p.create_time() != row.get("supervisor_created"):
                return {"stopped":stopped}
            args=p.cmdline()
            web=dashboard.inspect(probe=False)
            if Path(p.cwd()).resolve()==dashboard.PROJECT.resolve() and "lab.health.dashboard_service" in args and "--scheduled" not in args:
                child = None
                if web["state"] == "EXPECTED_LISTENER":
                    listener = psutil.Process(web["pid"])
                    if p.pid in {parent.pid for parent in listener.parents()}:
                        child = listener
                p.terminate()
                p.wait(8)
                stopped.append("local dashboard supervisor")
                if child:
                    with suppress(psutil.NoSuchProcess):
                        if child.create_time()==web["created"] and dashboard.expected(child):
                            child.terminate()
                            child.wait(8)
                            stopped.append("local dashboard child")
    return {"stopped":stopped}


def main():
    parser=argparse.ArgumentParser(prog="aseke-health")
    sub=parser.add_subparsers(dest="command",required=True)
    for name in ("status","test-telegram","dashboard-status","recover-dashboard","stop-local","task-status"):
        sub.add_parser(name)
    sub.add_parser("enable-ema-private-telegram", help="verify the existing private route and baseline new EMA events")
    sub.add_parser("alerts").add_argument("--idea", help="Show delivery attempts for one full paper idea ID")
    run=sub.add_parser("check-once")
    run.add_argument("--no-send",action="store_true")
    mode = run.add_mutually_exclusive_group()
    mode.add_argument("--scheduled",action="store_true")
    mode.add_argument("--local",action="store_true")
    sub.add_parser("audit").add_argument("start",nargs="?",default="2026-11-10T02:00:00Z")
    sub.add_parser("mute").add_argument("state",choices=("on","off"))
    cat=sub.add_parser("category")
    cat.add_argument("category",choices=DEFAULTS["categories"]); cat.add_argument("state",choices=("on","off"))
    interval = sub.add_parser("position-interval")
    interval.add_argument("profile", choices=DEFAULTS["open_position_minutes"])
    interval.add_argument("minutes", type=int, choices=range(1, 1441), metavar="1–1440")
    args=parser.parse_args()
    command=args.command
    if command=="check-once":
        result=check(deliver=not args.no_send,mode="scheduled" if args.scheduled else "local" if args.local else "operator")
    elif command=="status":
        result={"latest":view(),"current":_operational_status(),"settings":policy()}
    elif command=="task-status":
        result=task_evidence()
    elif command=="dashboard-status":
        result=dashboard.inspect()
    elif command=="recover-dashboard":
        result=dashboard.recover()
    elif command=="stop-local":
        result=stop_local()
    elif command=="audit":
        from lab.health.monitor import overnight
        result=overnight(utc(args.start),utc_now())
    elif command == "enable-ema-private-telegram":
        from lab.ema_paper import private_notifications
        from lab.health import private_telegram

        route = private_telegram.destination()
        with writer_lock(ROOT/".check.lock"), connect() as db, db:
            enabled = private_notifications.enable(db, route.identifier, utc_now())
            result = {"state": "PRIVATE_SUMMARIES_ENABLED", "enabled_at": enabled["enabled_at"]}
    elif command in {"alerts","test-telegram","mute","category","position-interval"}:
        with writer_lock(ROOT/".check.lock"),connect() as db:
            if command=="alerts":
                result=status(db,idea=args.idea)
            elif command=="mute":
                result=set_policy(mute=args.state=="on")
            elif command=="category":
                result=set_policy(categories={**policy()["categories"],args.category:args.state=="on"})
            elif command=="position-interval":
                result=set_policy(open_position_minutes={**policy()["open_position_minutes"],args.profile:args.minutes})
            else:
                now=time.time()
                identifier="test:"+uuid4().hex
                enqueue(db,identifier,"test",f"ASEKE BTC LAB — TEST NOTIFICATION\nIndependent watchdog delivery test.\n{local(now)}",now)
                db.commit()
                flush(db,now,policy(),send,only=identifier,clock=time.time)
                result=next(row for row in status(db)["recent_alerts"] if row["id"]==identifier)
    print(json.dumps(result,indent=2,default=str,allow_nan=False))


if __name__=="__main__":
    try:
        _stage("imports_complete")
        main()
        _stage("completed", reason="EXIT_0")
    except (OSError,RuntimeError,ValueError,sqlite3.Error,psutil.Error) as exc:
        _stage("failed", reason=type(exc).__name__)
        print(f"Health command could not complete: {type(exc).__name__}",file=sys.stderr)
        raise SystemExit(1) from None
