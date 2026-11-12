import argparse
from contextlib import suppress
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from uuid import uuid4

import psutil

from lab.health import dashboard_service as dashboard
from lab.health.delivery import send
from lab.health.state import DEFAULTS, ROOT, connect, enqueue, flush, get_meta, local, observe, policy, set_meta, set_policy, status
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


TASKS = ("ASEKE BTC Lab - Forward Research", "ASEKE BTC Lab - KuCoin Monitoring",
         "ASEKE BTC Lab - Dashboard", "ASEKE BTC Lab - Health Watchdog")


def task_evidence():
    names = ",".join(f"'{name}'" for name in TASKS)
    script = f"""$result = @{{}}
    foreach ($name in @({names})) {{
        try {{
            $task = Get-ScheduledTask -TaskName $name -TaskPath '\\' -ErrorAction Stop
            $result[$name] = @{{state=([string]$task.State).ToUpperInvariant(); logon=[string]$task.Principal.LogonType}}
            try {{
                $info = Get-ScheduledTaskInfo -TaskName $name -TaskPath '\\' -ErrorAction Stop
                $result[$name].result = $info.LastTaskResult
                $result[$name].last = [string]$info.LastRunTime
                $result[$name].next = [string]$info.NextRunTime
            }} catch {{ $result[$name].result = $null }}
        }} catch {{
            $result[$name] = @{{state=$(if ($_.CategoryInfo.Category -eq 'ObjectNotFound') {{'MISSING'}} else {{'UNKNOWN'}})}}
        }}
    }}
    $result | ConvertTo-Json -Compress -Depth 4"""
    exe=Path(os.environ["WINDIR"])/"System32/WindowsPowerShell/v1.0/powershell.exe"
    try:
        out=subprocess.run([str(exe),"-NoProfile","-NonInteractive","-Command",script],capture_output=True,timeout=12,
                           check=True,creationflags=subprocess.CREATE_NO_WINDOW)
        return json.loads(out.stdout)
    except (OSError,subprocess.SubprocessError,ValueError):
        return {}


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


def check(*, deliver=True, mode="operator"):
    from lab.health.monitor import collect, finding
    from lab.health import private_telegram
    from lab.ema_paper import private_notifications

    before=time.monotonic()
    cpu=time.process_time()
    now=utc_now()
    epoch=int(now.timestamp())
    with writer_lock(ROOT/".check.lock"), connect() as db:
        cfg=policy()
        started=int(get_meta(db,"started",epoch))
        set_meta(db,"started",started)
        tasks=task_evidence()
        evidence,findings=collect(db,now,started,cfg,tasks)
        try:
            private_settings = private_notifications.route(db)
        except (ValueError, KeyError, OSError):
            private_settings = None
            evidence["ema_private_route"] = "INVALID_LOCAL_CONFIGURATION"
        private_route = private_settings["route_id"] if private_settings else None
        if mode == "scheduled" and (ROOT.parent / "ema_paper" / "paper.sqlite3").is_file():
            try:
                from lab.ema_paper import dispatcher, notifications, proxy_protocol, synthetic_protocol, worker

                if synthetic_protocol.active():
                    from lab.ema_paper import kraken_reference, recovery_protocol, recovery_runtime, synthetic_runtime

                    synthetic_protocol.load()
                    evidence["ema_kraken_reference"] = kraken_reference.ensure()
                    evidence["ema_paper"] = recovery_runtime.cycle() if recovery_protocol.active() else synthetic_runtime.cycle()
                else:
                    evidence["ema_paper"] = dispatcher.cycle() if proxy_protocol.amended() else worker.cycle()
                ema = evidence["ema_paper"]
                for profile, book_status in ema.get("books", {}).items():
                    if book_status.get("source_monitor") in {"HEALTHY", "DEGRADED"}:
                        finding(findings, f"EMA {profile.title()} KuCoin source",
                                "WARNING" if book_status["source_monitor"] == "DEGRADED" else None,
                                "Independently licensed KuCoin paper evidence is delayed or incomplete."
                                if book_status["source_monitor"] == "DEGRADED" else "Timely KuCoin paper evidence restored.")
                evidence["ema_paper_notifications"] = notifications.collect(db, utc_now(), cfg)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error, psutil.Error) as exc:
                evidence["ema_paper"] = {"state": "CHECK_FAILED", "reason": f"{type(exc).__name__}: {exc}"}
            if private_route:
                try:
                    current = utc_now()
                    evidence["ema_private_notifications"] = private_notifications.collect(db, current)
                    reference = evidence.get("ema_kraken_reference", {})
                    heartbeat = reference.get("heartbeat", {})
                    heartbeat_at = heartbeat.get("at")
                    minute = next((item for item in reference.get("evidence", {}).get("checkpoints", ())
                                   if item.get("interval_minutes") == 1), {})
                    closed_at = minute.get("closed_through")
                    ema = evidence.get("ema_paper", {})
                    observations = {
                        "collector": reference.get("state") in {"RUNNING", "RUNNING_IDENTITY_UNVERIFIED", "STARTED"},
                        "staleness": bool(heartbeat_at and closed_at and minute.get("missing_rows") == 0
                                          and 0 <= (current - utc(heartbeat_at)).total_seconds() <= 120
                                          and 0 <= (current - utc(closed_at)).total_seconds() <= 180),
                        "evaluation": ema.get("state") == "CHECKED" and all(
                            book.get("state") not in {"BLOCKED_PERMISSION", "BLOCKED_DATA", "WAITING_FOR_RESOURCES"}
                            and not (book.get("state") == "WAITING_FOR_DATA" and book.get("first_eligible_at")
                                     and not ((book.get("source_protocol") or {}).get("chart_recovery") or {}).get("has_discontinuity"))
                            for book in ema.get("books", {}).values()),
                        "watchdog": epoch - int(get_meta(db, "ema_private_watchdog_previous", epoch)) <= 420,
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
            evidence.update(_check_kraken_paper(db, cfg, kraken_root))
        queried = all(tasks.get(name, {}).get("state", "UNKNOWN") != "UNKNOWN" for name in TASKS[:2])
        finding(findings, "Task Scheduler evidence", None if queried else "WARNING",
                "Collector task states checked." if queried else "Could not verify both collector task states.")
        if 0 <= epoch - psutil.boot_time() < cfg["boot_grace_seconds"]:
            for key in ("Coinbase worker", "KuCoin worker"):
                if key in findings:
                    severity, detail, _ = findings[key]
                    finding(findings, key, severity, detail)
        observe(db,findings,epoch,cfg)
        db.commit()
        if deliver:
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
        result={"latest":view(),"settings":policy()}
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
        main()
    except (OSError,RuntimeError,ValueError,psutil.Error) as exc:
        print(f"Health command could not complete: {type(exc).__name__}",file=sys.stderr)
        raise SystemExit(1) from None
