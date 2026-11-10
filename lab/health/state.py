from contextlib import closing
from datetime import datetime, timezone
import sqlite3
from zoneinfo import ZoneInfo

from lab.health.dashboard_service import ROOT
from lab.storage import read_json, write_json


DEFAULTS = {"categories":{"health":True,"ideas":True,"training_completion":False},"mute":False,"reminder_minutes":240,
    "warning_checks":3,"critical_checks":2,"recovery_checks":2,"ram_mib":[1250,750],"disk_mib":[2048,1024],
    "boot_grace_seconds":180,
    "coinbase_heartbeat_seconds":[45,120],"kucoin_heartbeat_seconds":[90,240],
    "coinbase_5m_minutes":[20,35],"coinbase_1h_minutes":[85,135],"kucoin_15m_minutes":[30,50],"spot_1h_minutes":[85,135],
    "futures_gap_minutes":[3,12],"receipt_delay_minutes":[4,12],"retry_minutes":[8,20],"learning_check_minutes":[35,60]}

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS incident (key TEXT PRIMARY KEY, episode INTEGER, first INTEGER, last INTEGER,
    failures INTEGER, healthy INTEGER, severity TEXT, opened TEXT, notified INTEGER, recovered INTEGER, detail TEXT);
CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, category TEXT, text TEXT, incident TEXT, kind TEXT,
    status TEXT, created INTEGER, due INTEGER, attempts INTEGER DEFAULT 0, detail TEXT, delivered INTEGER, message_id INTEGER);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status,due);
"""


def policy():
    path = ROOT / "controls.json"
    saved = read_json(path) if path.exists() else {}
    return {**DEFAULTS, **saved, "categories":{**DEFAULTS["categories"], **saved.get("categories",{})}}


def set_policy(**values):
    updated = {**policy(), **values}
    write_json(ROOT / "controls.json", updated)
    return updated


def connect():
    ROOT.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(ROOT / "watchdog.sqlite3", timeout=3)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return closing(db)


def local(value):
    return datetime.fromtimestamp(value, timezone.utc).astimezone(ZoneInfo("America/Toronto")).strftime("%Y-%m-%d %H:%M:%S %Z")


def set_meta(db, key, value):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key,str(value)))


def get_meta(db, key, default=None):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def enqueue(db, identifier, category, message, now, *, incident=None, kind="event"):
    db.execute("INSERT OR IGNORE INTO outbox(id,category,text,incident,kind,status,created,due) VALUES (?,?,?,?,?,'PENDING',?,?)",
               (identifier,category,message,incident,kind,now,now))


def observe(db, findings, now, cfg):
    for key, (severity, detail, immediate) in findings.items():
        row = db.execute("SELECT * FROM incident WHERE key=?", (key,)).fetchone()
        if not severity:
            if not row or row["recovered"] is not None:
                continue
            healthy = row["healthy"]+1
            if row["opened"] and healthy < cfg["recovery_checks"]:
                db.execute("UPDATE incident SET healthy=?,failures=0 WHERE key=?", (healthy,key))
                continue
            episode = f"{key}:{row['episode']}"
            if row["notified"] is not None:
                elapsed = max(0,(now-row["first"])//60)
                enqueue(db,episode+":recovery","health",f"ASEKE BTC LAB — RECOVERY\n{key}\n{detail}\n"
                        f"Degraded for approximately {elapsed} min. Recovered {local(now)}.",now,incident=key,kind="recovery")
            db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE incident=? AND kind='incident' AND status IN ('PENDING','RETRY')", (key,))
            db.execute("UPDATE incident SET recovered=?,healthy=? WHERE key=?", (now,healthy,key))
            continue
        if not row or row["recovered"] is not None:
            episode = 1 if not row else row["episode"]+1
            db.execute("INSERT OR REPLACE INTO incident VALUES (?,?,?,?,1,0,?,NULL,NULL,NULL,?)",
                       (key,episode,now,now,severity,detail))
        else:
            failures = row["failures"]+1 if row["severity"]==severity else 1
            db.execute("UPDATE incident SET last=?,failures=?,healthy=0,severity=?,detail=? WHERE key=?", (now,failures,severity,detail,key))
        row = db.execute("SELECT * FROM incident WHERE key=?",(key,)).fetchone()
        floor = cfg["critical_checks"] if severity=="CRITICAL" else cfg["warning_checks"]
        if not immediate and row["failures"] < floor:
            continue
        episode = f"{key}:{row['episode']}"
        pending = db.execute("SELECT 1 FROM outbox WHERE incident=? AND kind='incident' AND status IN ('PENDING','RETRY','IN_FLIGHT','UNCERTAIN')",(key,)).fetchone()
        escalation = row["opened"] == "WARNING" and severity == "CRITICAL"
        if escalation:
            db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE incident=? AND kind='incident' AND status IN ('PENDING','RETRY')",(key,))
        first = not row["opened"]
        reminder = (not pending and row["notified"] is not None and now-row["notified"]>=cfg["reminder_minutes"]*60)
        if first or escalation or reminder:
            suffix = severity if first or escalation else f"reminder:{now}"
            enqueue(db,episode+":"+suffix,"health",f"ASEKE BTC LAB — {severity}\n{key}\n{detail}\n"
                    f"First detected {local(row['first'])}. Checked {local(now)}.",now,incident=key,kind="incident")
            db.execute("UPDATE incident SET opened=? WHERE key=?", (severity,key))


def flush(db, now, cfg, send, *, limit=5, only=None):
    db.execute("UPDATE outbox SET status='UNCERTAIN',detail='Interrupted during a Telegram request' WHERE status='IN_FLIGHT'")
    db.commit()
    if (cfg["mute"] and not only) or now < int(get_meta(db,"telegram_cooldown_until",0)):
        return
    rows = db.execute("SELECT * FROM outbox WHERE status IN ('PENDING','RETRY') AND due<=? ORDER BY created,id",(now,)).fetchall()
    due = [row for row in rows if (row["id"] == only if only else
           row["category"] == "test" or cfg["categories"].get(row["category"],False))][:limit]
    for row in due:
        db.execute("UPDATE outbox SET status='IN_FLIGHT',attempts=attempts+1 WHERE id=?",(row["id"],))
        db.commit()
        result = send(row["text"])
        delay = max(result.retry_after,min(900,30*2**min(row["attempts"],5)))
        delivered = now if result.state == "DELIVERED" else None
        db.execute("UPDATE outbox SET status=?,detail=?,due=?,delivered=?,message_id=? WHERE id=?",
                   (result.state,result.detail,now+delay,delivered,result.message_id,row["id"]))
        if delivered is not None:
            set_meta(db,"last_delivery",now)
            if row["incident"] and row["kind"]=="incident":
                db.execute("UPDATE incident SET notified=? WHERE key=?",(now,row["incident"]))
        if result.detail == "Telegram rate limit":
            set_meta(db,"telegram_cooldown_until",now+delay)
        db.commit()
        if result.detail == "Telegram rate limit" or result.state == "RETRY":
            break


def status(db):
    incidents = [dict(r) for r in db.execute("SELECT * FROM incident WHERE recovered IS NULL ORDER BY first")]
    counts = dict(db.execute("SELECT status,count(*) FROM outbox GROUP BY status"))
    alerts = [dict(r) for r in db.execute("SELECT id,category,kind,status,created,attempts,detail,delivered,message_id FROM outbox ORDER BY created DESC LIMIT 25")]
    return {"incidents":incidents,"delivery_counts":counts,"recent_alerts":alerts,
            "last_delivery":get_meta(db,"last_delivery"),"last_recovery":db.execute("SELECT max(recovered) FROM incident WHERE opened IS NOT NULL").fetchone()[0]}
