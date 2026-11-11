from contextlib import closing
import json
import shutil
import sqlite3

import pandas as pd
import psutil

from lab.config import ROOT as PROJECT
from lab.health import dashboard_service as dashboard
from lab.health.sources import coinbase_interval, native_interval, spot_interval
from lab.health.state import enqueue, get_meta, local, set_meta
from lab.storage import RUNS, read_json, utc


FORWARD = "ASEKE BTC Lab - Forward Research"
KUCOIN = "ASEKE BTC Lab - KuCoin Monitoring"


def optional(path):
    return read_json(path) if path.exists() else None


def age(now, value, scale=1):
    return max(0,(now-utc(value)).total_seconds())/scale if value is not None else float("inf")


def rss(pid):
    try:
        return round(psutil.Process(pid).memory_info().rss/1024**2,1) if pid else None
    except (OSError,psutil.Error):
        return None


def level(value, limits):
    return "CRITICAL" if value >= limits[1] else "WARNING" if value >= limits[0] else None


def finding(findings, key, severity, detail, *, immediate=False):
    findings[key] = (severity,detail,immediate)


def task_check(tasks, name, label, findings):
    row = tasks.get(name,{})
    state = row.get("state","UNKNOWN")
    if state == "UNKNOWN":
        return state
    failed = state in {"DISABLED","MISSING"} or (state == "READY" and row.get("result") not in {None,0,267009,267011})
    finding(findings,label+" task", "CRITICAL" if failed else None, f"Task {state}; last result {row.get('result','unknown')}. Inspect Task Scheduler.", immediate=failed)
    return state


def coinbase(now, tasks, cfg, findings):
    from lab.phase7.operation import health
    from lab.phase2 import store

    data = health()
    worker = data["worker"]
    paused = worker["intentionally_paused"]
    state = task_check(tasks,FORWARD,"Coinbase collector",findings) if not paused else tasks.get(FORWARD,{}).get("state","UNKNOWN")
    pid = worker.get("pid")
    age_s = data["heartbeat_age_seconds"]
    stopped = not paused and (not worker["process_alive"] or not data["worker_lock_held"])
    finding(findings,"Coinbase worker","CRITICAL" if stopped else None,
            f"PID {pid}, task {state}, state {data['process_state']}, writer lock {data['worker_lock_held']}. Check research-collector.log.",immediate=stopped)
    running = not paused and not stopped
    if running:
        finding(findings,"Coinbase heartbeat",level(age_s if age_s is not None else float('inf'),cfg["coinbase_heartbeat_seconds"]),
                f"Heartbeat age {age_s!s} s, PID {pid}. Inspect the collector heartbeat.")
    with store.connect(readonly=True) as db:
        for seconds,key in ((300,"5m"),(3600,"1h")):
            feed = data["feeds"][str(seconds)]
            when = feed["last_observed_close"]
            scanned = feed["scanned_through"]
            missing = age(now,when,60)
            if running:
                finding(findings,f"Coinbase {key} progression",level(missing,cfg[f"coinbase_{key}_minutes"]),
                        f"Latest close {when}, scanned through {scanned}, delay {missing:.0f} min. Check the Coinbase feed.")
            responses = db.execute("SELECT problem FROM responses WHERE seconds=? ORDER BY id DESC LIMIT 5",(seconds,)).fetchall()
            consecutive = next((i for i,row in enumerate(responses) if not row[0]),len(responses))
            finding(findings,f"Coinbase {key} API failures", ("CRITICAL" if consecutive>=5 else "WARNING" if consecutive>=3 else None) if not paused else None,
                    f"{consecutive} consecutive failed responses for this cadence. Check Coinbase connectivity.")
    return {"task":state,"state":data["process_state"],"pid":pid,"lock":data["worker_lock_held"],"heartbeat_age_seconds":age_s,
            "identity_verified":bool(data["process"] and data["process"]["expected_launch_identity"]),"rss_mib":rss(pid),"feeds":data["feeds"]}


def kucoin(now,tasks,cfg,findings):
    from lab.phase10.worker import process_state, controls
    from lab.phase13.protocol import control
    from lab.phase14.context import pointer
    from lab.phase17.registry import registered

    worker,locked = process_state()
    switches = controls()
    paused = switches["paused"]
    enabled = switches["monitor"] and control()["enabled"]
    state = task_check(tasks,KUCOIN,"KuCoin collector",findings) if not paused else tasks.get(KUCOIN,{}).get("state","UNKNOWN")
    stopped = not paused and (not locked or worker.get("launcher") != "scheduled")
    hb = age(now,worker.get("heartbeat"))
    finding(findings,"KuCoin worker","CRITICAL" if stopped else None,
            f"PID {worker.get('pid')}, task {state}, writer ownership {locked}. Check the KuCoin worker log.",immediate=stopped)
    if not paused and not stopped:
        finding(findings,"KuCoin heartbeat",level(hb,cfg["kucoin_heartbeat_seconds"]),
                f"Heartbeat age {hb:.0f} s, PID {worker.get('pid')}.")
    backoff = max(0,(utc(worker["retry_at"])-now).total_seconds()/60) if worker.get("retry_at") else 0
    failures = worker.get("consecutive_errors",0)
    finding(findings,"KuCoin persistent API backoff",("CRITICAL" if failures>=5 else "WARNING" if failures>=3 else level(backoff,cfg["retry_minutes"]))
            if enabled and not paused else None,f"Consecutive errors {failures}, next retry {worker.get('retry_at')}. Check KuCoin connectivity.")
    amendment,native = pointer("amendment"),registered()
    if not amendment or not native:
        raise ValueError("Required research observation registration is unavailable")
    end14=utc(amendment["body"]["end_exclusive"])
    later=utc(native["body"]["profiles"]["day"]["start"])
    final=max(utc(native["body"]["profiles"][p]["end_exclusive"]) for p in ("day","swing"))
    futures_due = now < end14 or later <= now < final
    boundary=utc(amendment["body"]["start"]) if now<end14 else later
    since=max(now.floor("15min")-pd.Timedelta(minutes=60),boundary) if futures_due else now.floor("15min")
    fut=native_interval(amendment,native,since,now,funding_since=max(now-pd.Timedelta(hours=26),boundary) if futures_due else since)
    spot=spot_interval(native,now-pd.Timedelta(hours=5),now)
    should_monitor = not paused and not stopped and enabled
    if should_monitor and futures_due and fut["expected"]:
        delay = age(now, fut["last_checkpoint"], 60)
        finding(findings, "KuCoin shared observer checkpoint", level(delay, cfg["kucoin_15m_minutes"]),
                f"Latest finalized checkpoint {fut['last_checkpoint']}, delay {delay:.0f} min. Check the shared Phase 14 and native observer.")
        for basis,view in fut["prices"].items():
            stale=age(now,view["last_close"],60)
            finding(findings,f"KuCoin futures {basis} progression",level(stale,cfg["kucoin_15m_minutes"]),
                    f"Latest {basis} close {view['last_close']}, delay {stale:.0f} min. Check shared capture.")
            finding(findings,f"KuCoin futures {basis} coverage",level(view["missing"],cfg["futures_gap_minutes"]),
                    f"{view['missing']} missing {basis} minutes in the recent completed window. Check affected research or paper paths.")
            if view["max_receipt_delay_seconds"] is not None:
                late=view["max_receipt_delay_seconds"]/60-15
                finding(findings,f"KuCoin futures {basis} receipt delay",level(late,cfg["receipt_delay_minutes"]),
                        f"Maximum receipt lag {view['max_receipt_delay_seconds']} s, including a 15-minute capture window.")
    spot_due=now<utc(native["body"]["profiles"]["position"]["end_exclusive"])
    if should_monitor and spot_due and spot["expected"]:
        spot_age=age(now,spot["last_close"],60)
        finding(findings,"KuCoin Spot hourly progression",level(spot_age,cfg["spot_1h_minutes"]),
                f"Latest BTC-USDT hourly close {spot['last_close']}, delay {spot_age:.0f} min. Check the hourly receipt ledger.")
        finding(findings,"KuCoin Spot hourly coverage",level(spot["missing"],[2,3]),
                f"{spot['missing']} missing trade hours in the recent window. Mark and funding are N/A.")
    funding=fut["funding"]
    missing=[s for s in funding["slots"] if s["rate"] is None and age(now,s["scheduled_at"],60)>120]
    finding(findings,"KuCoin funding settlements","CRITICAL" if missing and should_monitor and futures_due else None,
            f"{len(missing)} due settlement(s) remain unverified after two-hour grace. Check public funding receipts.")
    finding(findings,"KuCoin funding schedule","WARNING" if should_monitor and futures_due and fut["expected"] and funding["state"]=="FUNDING_SCHEDULE_UNAVAILABLE" else None,
            f"Dated funding schedule state {funding['state']}.")
    bad=sum(n for k,n in fut["request_status"].items() if k!="RECEIVED")
    finding(findings,"KuCoin shared observer API errors","CRITICAL" if should_monitor and bad>=5 else "WARNING" if should_monitor and bad>=3 else None,
            f"{bad} failed shared observer requests in the recent window. Check recorded API status and retries.")
    finding(findings,"KuCoin public collection disabled","WARNING" if not enabled and not paused else None,
            f"Public monitor {switches['monitor']}, shared collection {control()['enabled']}.")
    return {"task":state,"state":"PAUSED" if paused else "RUNNING" if locked else "STOPPED","pid":worker.get("pid"),
            "launch_id":worker.get("launch_id"),"lock":locked,"heartbeat_age_seconds":hb if hb!=float('inf') else None,
            "rss_mib":rss(worker.get("pid")),"futures":fut,"spot":spot,"monitor":enabled,"retry_at":worker.get("retry_at")},amendment,native


def phase14(db,now,amendment,collection,findings):
    from lab.phase14.context import pointer

    ident = amendment["id"]
    baseline=get_meta(db,"phase14_registration",ident)
    set_meta(db,"phase14_registration",baseline)
    finding(findings,"Phase 14 registration", "CRITICAL" if ident!=baseline else None,
            "The sealed Phase 14 amendment identity differs from watchdog commissioning.",immediate=ident!=baseline)
    early = now < utc(amendment["body"]["end_exclusive"]) and pointer("evaluation") is not None
    finding(findings,"Phase 14 premature evaluation","CRITICAL" if early else None,"An evaluation pointer appeared before the frozen horizon.",immediate=early)
    return {"id":ident,"registration_matches":ident==baseline,"collection_enabled":collection["monitor"],
            "checkpoint":collection["futures"]["last_checkpoint"],"primary_open":now<utc(amendment["body"]["end_exclusive"]),"early_evaluation":early}


def idea_text(lane,idea):
    plan=idea["plan"]
    entry=plan["entry"]
    designation = (
        f"Illustrative {plan['target_r']:g}R target {plan['fixed_target']}. Recomputed from the modeled fill."
        if plan["fixed_target"] is not None else f"ATR trailing ({plan['trailing_atr']})"
    )
    flat=f"; session flat {local(int(utc(plan['session_flat']).timestamp()))}" if plan.get("session_flat") else ""
    return (f"ASEKE BTC LAB — EXPERIMENTAL IDEA\n{lane['profile'].title()} / {idea['exit_policy']} · {lane['mode']['symbol']}\n"
            f"{idea['direction']} · Trigger {entry['trigger']} · Range {entry['minimum']}–{entry['maximum']}\n"
            f"Stop {plan['stop']} · Exit {designation}\nExpires {local(int(utc(plan['entry_expiry']).timestamp()))}\n"
            f"Holding limit {plan['max_hold_minutes']} min{flat}\nPublished {local(int(utc(idea['published_at']).timestamp()))}\n"
            f"Strategy {idea['strategy_version']}\nIdea {idea['id']}\nExperimental — No validated model\nManual outside lab")


def publications(paper,db,lanes,started,now):
    for row in paper.execute("SELECT book,idea,body FROM event WHERE kind='PUBLISHED' AND recorded_at>=? ORDER BY recorded_at",
                             (pd.Timestamp(started,unit="s",tz="UTC").isoformat(),)):
        book,identifier,document=row
        if book not in lanes:
            continue
        idea=json.loads(document)
        if idea["id"] != identifier or idea["direction"] not in {"LONG","SHORT"}:
            raise ValueError("Saved publication identity or direction is invalid")
        enqueue(db,"idea:"+identifier,"ideas",idea_text(lanes[book],idea),now)


def paper(now,db,started,findings):
    from lab.phase15.catalog import registered,controls,db_open
    from lab.phase15.paper import book_report

    reg=registered()
    if not reg:
        raise ValueError("No Phase 15 registration")
    body=reg["body"]
    ctl=controls()
    open_window=utc(body["start"])<=now<utc(body["publication_end_exclusive"])
    for key in ("analysis","publication","paper","spot_minutes"):
        finding(findings,f"Phase 15 {key}","WARNING" if (key!="publication" or open_window) and not ctl[key] else None,
                f"Phase 15 {key} switch is {'on' if ctl[key] else 'off'}.")
    books={}
    with closing(db_open(readonly=True)) as conn:
        saved={row[0] for row in conn.execute("SELECT id FROM book")}
        for key,lane in body["lanes"].items():
            info=book_report(conn,lane)
            missing=lane["book_id"] not in saved
            severe=bool(missing or info["permanent_block"])
            unresolved=bool(info["problem"] or not info["accounting_complete"])
            finding(findings,f"Phase 15 {key} accounting","CRITICAL" if severe else "WARNING" if unresolved else None,
                    f"Book {'missing' if missing else info['book_status']}. {info['problem'] or 'Review paper ledger.'}",immediate=severe)
            books[key]={"state":info["book_status"],"problem":info["problem"],"permanent_block":info["permanent_block"],
                        "published":info["counts"].get("PUBLISHED",0),"accounting_complete":info["accounting_complete"]}
        publications(conn,db,{lane["book_id"]:lane for lane in body["lanes"].values()},started,int(now.timestamp()))
    return {"id":reg["id"],"controls":ctl,"publication_open":open_window,"books":books}


def learning(now,db,started,cfg,findings):
    from lab.learning.control import process_identity
    from lab.phase16.control import active,settings
    from lab.phase16.plan import ROOT as LEARN
    from lab.phase17.pilot_registry import PILOT
    from lab.phase17.registry import ROOT as NATIVE

    enabled=settings()["recurring"]
    initial=get_meta(db,"recurring_expected",str(enabled))=="True"
    set_meta(db,"recurring_expected",initial)
    expected=cfg.get("recurring_expected",initial)
    finding(findings,"Learning eligibility disabled","WARNING" if expected and not enabled else None,
            f"Phase 16/17.1 recurring eligibility is {'on' if enabled else 'off'}. Check whether an operator paused it.")
    last=optional(LEARN/"last-managed-check.json") or {}
    child=active()
    worker=optional(PILOT/"worker.json")
    pilot=worker if worker and process_identity(worker["pid"],worker["created"]) else None
    busy=child or pilot
    minutes=age(now,busy["at"],60) if busy else 0
    finding(findings,"Learning job stalled","CRITICAL" if busy and minutes>125 else None,
            f"Managed research job has run {minutes:.0f} min. Inspect supervisor and resource logs.")
    delay=age(now,last.get("at"),60)
    finding(findings,"Learning eligibility checkpoint",level(delay,cfg["learning_check_minutes"]) if enabled and not busy else None,
            f"Latest managed eligibility check {last.get('at')}, delay {delay:.0f} min.")
    resources = {}
    for name, paths in (("Phase 16", LEARN.glob("campaigns/*/resources.json")),
                        ("Phase 17", NATIVE.glob("checks/*/*/resources.json")),
                        ("Phase 17.1", PILOT.glob("pilot-*/*/*/resources.json"))):
        latest = max(paths, key=lambda path: path.stat().st_mtime, default=None)
        if not latest:
            continue
        record = read_json(latest)
        resources[name] = record
        failed = record.get("exit_code") not in {None, 0} and not record.get("stop_reason")
        observed = utc(record["at"]).timestamp() >= started
        if failed and observed:
            finding(findings, f"{name} unexpected worker failure", "CRITICAL",
                    f"Supervised worker exited with {record['exit_code']} at {record['at']}. Inspect worker.log.", immediate=True)
        elif record.get("exit_code") == 0 and not record.get("stop_reason"):
            finding(findings, f"{name} unexpected worker failure", None, f"Supervised worker completed at {record['at']}.")
        if name == "Phase 16" and observed and record.get("exit_code") == 0 and (latest.parent / "result.json").is_file():
            campaign = latest.parent.name
            enqueue(db, "campaign:"+campaign, "training_completion",
                    f"ASEKE BTC LAB — RESEARCH CAMPAIGN COMPLETE\nPhase 16 · {campaign}\nResearch only. No validated model.", int(now.timestamp()))
    for path in PILOT.glob("profiles/*/candidate.json"):
        if path.stat().st_mtime>=started:
            model=read_json(path)["id"]
            enqueue(db,"pilot:"+model,"training_completion",f"ASEKE BTC LAB — RESEARCH FIT COMPLETE\n{path.parent.name.title()} · {model}\nResearch only. No validated model.",int(now.timestamp()))
    return {"recurring":enabled,"managed_check":last,"active":busy,"latest_resources":resources,
            "native_guard":optional(NATIVE/"label-admission.json"),"pilot_guard":optional(PILOT/"last-guard.json")}


def collect(db,now,started,cfg,tasks):
    findings={}
    result={"at":now.isoformat(),"tasks":tasks}
    def inspect(label, callable_):
        try:
            value=callable_()
        except (OSError,ValueError,KeyError,RuntimeError,TypeError,sqlite3.Error) as error:
            finding(findings,label+" evidence","WARNING",f"Could not verify {label}: {type(error).__name__}. Check local status.")
            return {"state":"EVIDENCE_UNAVAILABLE"}
        finding(findings,label+" evidence",None,f"{label} evidence available.")
        return value

    result["coinbase"]=inspect("Coinbase",lambda:coinbase(now,tasks,cfg,findings))
    ku=inspect("KuCoin",lambda:kucoin(now,tasks,cfg,findings))
    if isinstance(ku,tuple):
        result["kucoin"],amendment,_=ku
        result["phase14"]=inspect("Phase 14",lambda:phase14(db,now,amendment,result["kucoin"],findings))
    else:
        result["kucoin"]=ku
    result["phase15"]=inspect("Phase 15",lambda:paper(now,db,started,findings))
    result["learning"]=inspect("Learning",lambda:learning(now,db,started,cfg,findings))
    web=dashboard.inspect()
    supervisor=optional(dashboard.ROOT/"dashboard-supervisor.json")
    terminal=supervisor and supervisor["state"]=="RETRY_BUDGET_EXHAUSTED" and age(now,supervisor["at"],60)<30
    reachable = web["state"] in {"HEALTHY", "HTTP_HEALTHY_IDENTITY_UNVERIFIED"}
    finding(findings,"Dashboard listener",None if reachable else "CRITICAL" if terminal or web["state"]=="PORT_CONFLICT" else "WARNING",
            f"Loopback dashboard {web['state']}. Check dashboard.log and its scheduled task.",immediate=bool(terminal or web["state"]=="PORT_CONFLICT"))
    result["dashboard"]={**web,"supervisor":supervisor}
    ram,disk=psutil.virtual_memory().available//1024**2,shutil.disk_usage(PROJECT).free//1024**2
    for key,value in (("RAM",ram),("disk",disk)):
        warn,critical=cfg[key.lower()+"_mib"]
        finding(findings,"Workstation "+key,"CRITICAL" if value<critical else "WARNING" if value<warn else None,
                f"Available {value} MiB. Warning reserve {warn} MiB, critical threshold {critical} MiB.")
    result["resources"]={"ram_mib":ram,"disk_mib":disk,"watchdog_rss_mib":rss(psutil.Process().pid)}
    return result,findings


def overnight(start,now):
    from lab.phase14.context import pointer
    from lab.phase17.registry import registered

    native=registered()
    return {"start":start.isoformat(),"at":now.isoformat(),"coinbase":coinbase_interval(start,now),
            "kucoin_futures":native_interval(pointer("amendment"),native,start,now),"kucoin_spot":spot_interval(native,start,now)}
