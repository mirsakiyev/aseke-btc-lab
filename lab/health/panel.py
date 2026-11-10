import streamlit as st

from lab.health.dashboard_service import ROOT
from lab.health.state import local, policy
from lab.storage import read_json, utc, utc_now


def display_time(value):
    return local(int(value)) if value is not None else "—"


@st.fragment(run_every="30s")
def panel():
    st.title("Operational health / Telegram alerts")
    st.caption("Read-only view of the independent watchdog. Times are Toronto local time.")
    path = ROOT / "last.json"
    if not path.exists():
        st.warning("Waiting for the first watchdog receipt. Run health.ps1 check-once to check now.")
        return
    report = read_json(path)
    checked = utc(report["completed_at"])
    if (utc_now() - checked).total_seconds() > 420:
        st.warning("The watchdog receipt is more than seven minutes old.")
    st.caption(f"Last check {display_time(checked.timestamp())} · {report['mode'].title()} mode")

    coinbase = report.get("coinbase", {})
    kucoin = report.get("kucoin", {})
    learning = report.get("learning", {})
    components = (
        ("Coinbase", coinbase.get("state"), f"Task {coinbase.get('task', 'unknown')} · PID {coinbase.get('pid', '—')}"),
        ("KuCoin", kucoin.get("state"), f"Task {kucoin.get('task', 'unknown')} · PID {kucoin.get('pid', '—')}"),
        ("Dashboard", report.get("dashboard", {}).get("state"), "Loopback HTTP and listener identity"),
        ("Learning", "Recurring on" if learning.get("recurring") else "Recurring off" if "recurring" in learning else "Unknown",
         "Research job active" if learning.get("active") else "No active research job recorded"),
    )
    st.dataframe([{"Component": name, "State": state or "Unknown", "Detail": detail}
                  for name, state, detail in components], hide_index=True, width="stretch")
    resources = report["resources"]
    ram, disk = st.columns(2)
    ram.metric("Available RAM", f"{resources['ram_mib']:,} MiB")
    disk.metric("Free disk", f"{resources['disk_mib']:,} MiB")

    st.subheader("Active incidents")
    incidents = report["incidents"]
    if incidents:
        st.dataframe([{"Component": row["key"], "Severity": row["severity"], "State": "Open" if row["opened"] else "Debouncing",
                       "First seen": display_time(row["first"]), "Detail": row["detail"]} for row in incidents],
                     hide_index=True, width="stretch")
    else:
        st.write("No active incidents.")

    st.subheader("Telegram delivery")
    cfg = policy()
    enabled = ", ".join(name.replace("_", " ") for name, active in cfg["categories"].items() if active)
    st.write(f"{'Muted' if cfg['mute'] else 'Enabled'} · Categories {enabled or 'none'}")
    st.write(f"Last delivery {display_time(report['last_delivery'])} · Last recovery {display_time(report['last_recovery'])}")
    alerts = report["recent_alerts"]
    if alerts:
        st.dataframe([{"Event": row["id"], "State": row["status"], "Recorded": display_time(row["created"]),
                       "Detail": row["detail"]} for row in alerts], hide_index=True, width="stretch")
