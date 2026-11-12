from contextlib import closing
from datetime import datetime, timezone
import json
import sqlite3
from zoneinfo import ZoneInfo

from lab.health.dashboard_service import ROOT
from lab.storage import read_json, write_json


DEFAULTS = {"categories":{"health":True,"ideas":True,"training_completion":False,
                          "paper_entry":True,"paper_no_entry":True,"paper_exit":True,"open_position":False},
    "open_position_minutes":{"day":30,"swing":60,"position":240},"telegram_max_attempts":6,
    "mute":False,"reminder_minutes":240,
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
CREATE TABLE IF NOT EXISTS outbox_attempt (id TEXT, number INTEGER, dispatch_at REAL, request_started REAL,
    request_completed REAL, api_accepted_at REAL, telegram_date INTEGER, status TEXT, detail TEXT,
    PRIMARY KEY(id,number));
"""


def policy():
    path = ROOT / "controls.json"
    saved = read_json(path) if path.exists() else {}
    return {**DEFAULTS, **saved, "categories":{**DEFAULTS["categories"], **saved.get("categories",{})},
            "open_position_minutes":{**DEFAULTS["open_position_minutes"], **saved.get("open_position_minutes",{})}}


def set_policy(**values):
    updated = {**policy(), **values}
    write_json(ROOT / "controls.json", updated)
    return updated


def connect():
    ROOT.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(ROOT / "watchdog.sqlite3", timeout=3)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    columns = {row["name"] for row in db.execute("PRAGMA table_info(outbox)")}
    for name in ("idea_id", "transition", "event_json"):
        if name not in columns:
            db.execute(f"ALTER TABLE outbox ADD COLUMN {name} TEXT")
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS outbox_transition ON outbox(transition)")
    db.execute("CREATE INDEX IF NOT EXISTS outbox_idea ON outbox(idea_id)")
    if "idea_id" not in columns:
        db.execute("UPDATE outbox SET idea_id=substr(id,6) WHERE category='ideas' AND id LIKE 'idea:%'")
    db.commit()
    return closing(db)


def local(value):
    return datetime.fromtimestamp(value, timezone.utc).astimezone(ZoneInfo("America/Toronto")).strftime("%Y-%m-%d %H:%M:%S %Z")


def set_meta(db, key, value):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key,str(value)))


def get_meta(db, key, default=None):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def enqueue(db, identifier, category, message, now, *, incident=None, kind="event",
            idea_id=None, transition=None, event_json=None):
    db.execute("INSERT OR IGNORE INTO outbox(id,category,text,incident,kind,status,created,due,idea_id,transition,event_json) "
               "VALUES (?,?,?,?,?,'PENDING',?,?,?,?,?)",
               (identifier,category,message,incident,kind,now,now,idea_id,transition,event_json))


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


def flush(db, now, cfg, send, *, limit=5, only=None, clock=None, private_send=None, private_route_id=None):
    db.execute("UPDATE outbox_attempt SET status='UNCERTAIN',detail='Interrupted during a Telegram request' "
               "WHERE status='IN_FLIGHT'")
    db.execute("UPDATE outbox SET status='UNCERTAIN',detail='Interrupted during a Telegram request' WHERE status='IN_FLIGHT'")
    db.commit()
    if (cfg["mute"] and not only) or now < int(get_meta(db,"telegram_cooldown_until",0)):
        return
    rows = db.execute("SELECT rowid AS queue_order,* FROM outbox WHERE status IN ('PENDING','RETRY') AND due<=? "
                      "ORDER BY CASE WHEN category='health' THEN 0 ELSE 1 END, rowid",(now,)).fetchall()
    attempted = 0
    for row in rows:
        if attempted >= limit:
            break
        if not (row["id"] == only if only else row["category"] == "test" or cfg["categories"].get(row["category"],False)):
            continue
        sender = send
        if row["id"].startswith("ema-private:"):
            try:
                route = json.loads(row["event_json"] or "{}").get("private_route_id")
            except (ValueError, AttributeError):
                route = None
            if private_send is None or not private_route_id or route != private_route_id:
                continue
            sender = private_send
        if row["idea_id"]:
            blockers = db.execute("SELECT id,category FROM outbox WHERE idea_id=? AND rowid<? "
                                  "AND status IN ('PENDING','RETRY') AND category!='open_position'",
                                  (row["idea_id"],row["queue_order"])).fetchall()
            if any(cfg["categories"].get(parent["category"],False) for parent in blockers):
                continue
            db.executemany("UPDATE outbox SET status='SUPERSEDED',detail='Later lifecycle alert sent while category was disabled' "
                           "WHERE id=?", [(parent["id"],) for parent in blockers])
        attempted += 1
        dispatched = clock() if clock else now
        number = row["attempts"]+1
        db.execute("UPDATE outbox SET status='IN_FLIGHT',attempts=attempts+1 WHERE id=?",(row["id"],))
        db.execute("INSERT INTO outbox_attempt(id,number,dispatch_at,status) VALUES (?,?,?,'IN_FLIGHT')",
                   (row["id"],number,dispatched))
        db.commit()
        result = sender(row["text"])
        finished = clock() if clock else now
        delay = max(result.retry_after,min(900,30*2**min(row["attempts"],5)))
        delivered = result.api_accepted_at if result.api_accepted_at is not None else (
            finished if result.state == "DELIVERED" else None)
        status_value, detail = result.state, result.detail
        if status_value == "RETRY" and number >= cfg["telegram_max_attempts"]:
            status_value, detail = "FAILED", f"Retry limit reached. {detail}"
        db.execute("UPDATE outbox_attempt SET request_started=?,request_completed=?,api_accepted_at=?,telegram_date=?,"
                   "status=?,detail=? WHERE id=? AND number=?",
                   (result.request_started,result.request_completed,delivered,result.telegram_date,
                    status_value,detail,row["id"],number))
        db.execute("UPDATE outbox SET status=?,detail=?,due=?,delivered=?,message_id=? WHERE id=?",
                   (status_value,detail,finished+delay,delivered,result.message_id,row["id"]))
        if delivered is not None:
            set_meta(db,"last_delivery",delivered)
            if row["incident"] and row["kind"]=="incident":
                db.execute("UPDATE incident SET notified=? WHERE key=?",(delivered,row["incident"]))
        if result.detail == "Telegram rate limit":
            set_meta(db,"telegram_cooldown_until",int(finished+delay+0.999999))
        db.commit()
        if result.detail == "Telegram rate limit" or result.state == "RETRY":
            break


def status(db, *, idea=None):
    incidents = [dict(r) for r in db.execute("SELECT * FROM incident WHERE recovered IS NULL ORDER BY first")]
    counts = dict(db.execute("SELECT status,count(*) FROM outbox GROUP BY status"))
    alerts = [dict(r) for r in db.execute(
        "SELECT o.id,o.category,o.kind,o.status,o.created,o.attempts,o.detail,o.delivered,o.message_id,"
        "o.idea_id,o.event_json,a.dispatch_at,a.request_started,a.request_completed,a.api_accepted_at,a.telegram_date "
        "FROM outbox o LEFT JOIN outbox_attempt a ON a.id=o.id AND a.number=o.attempts "
        "WHERE ? IS NULL OR o.idea_id=? ORDER BY o.created DESC,o.rowid DESC LIMIT 25", (idea,idea))]
    for row in alerts:
        row["ledger_event"] = json.loads(row.pop("event_json") or "null")
        if idea:
            row["attempt_history"] = [dict(attempt) for attempt in db.execute(
                "SELECT number,dispatch_at,request_started,request_completed,api_accepted_at,telegram_date,status,detail "
                "FROM outbox_attempt WHERE id=? ORDER BY number", (row["id"],))]
    return {"incidents":incidents,"delivery_counts":counts,"recent_alerts":alerts,
            "last_delivery":get_meta(db,"last_delivery"),"last_recovery":db.execute("SELECT max(recovered) FROM incident WHERE opened IS NOT NULL").fetchone()[0],
            "paper_notifications":json.loads(get_meta(db,"paper_lifecycle","null"))}
