import argparse
from contextlib import suppress
import json
import os
from pathlib import Path
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


def check(*, deliver=True, mode="operator"):
    from lab.health.monitor import collect, finding

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
            flush(db,time.time(),cfg,send,clock=time.time)
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
