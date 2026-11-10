from lab.phase15.catalog import ROOT
from lab.phase15.worker import status
from lab.storage import put_bytes, read_json


def num(value):
    return f"{value:,.4f}" if value is not None else "—"


def report():
    data = status()
    reg = data["registration"]
    lines = ["# Phase 15 prospective paper comparison", "", "Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**.", "",
             "Each strategy book has independent capital and occupancy. Marked account values are minute-based hypothetical observations.", ""]
    if reg:
        body = reg["body"]
        lines += [f"Catalog `{reg['id']}`. Prospective start {body['start']}. Publication through {body['publication_end_exclusive']} "
                  f"exclusive. Existing positions have observation support through {body['follow_end_exclusive']} exclusive.", ""]
    lines += ["## Current setup checks", "", "| Lane | Closed bar UTC | State | Direction | Confirmation level | Reason |",
              "| --- | --- | --- | --- | ---: | --- |"]
    for lane,row in data["opportunities"].items():
        lines.append(f"| {lane} | {row['latest_completed'] or '—'} | {row['state']} | {row['direction'] or '—'} | "
                     f"{num(row.get('confirmation_level'))} | {row['reason']} |")
    lines += ["", "## Independent books", "", "| Lane | Published | Paper fills | Closed | Equity | Closed net | Gross at modeled fills | Fees | Funding | Drawdown | Turnover | Open exposure | Net expectancy / R |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for key,book in data["books"].items():
        c = book.get("counts", {})
        lines.append(f"| {key} | {c.get('PUBLISHED',0)} | {c.get('HYPOTHETICAL_FILL',0)} | {c.get('CLOSED',0)} | {num(book['equity'])} | "
                     f"{num(book.get('closed_net'))} | {num(book['gross'])} | {num(book['fees'])} | {num(book['funding'])} | "
                     f"{book['drawdown']:.2%} | {num(book['turnover'])} | {num(book.get('exposure'))} | "
                     f"{num(book.get('expectancy'))} / {num(book.get('expectancy_r'))} |")
    lines.append("")
    for key,book in data["books"].items():
        if "counts" not in book:
            continue
        lines += [f"**{key}**. {book['book_status']}. Last accounted native close {book['last_source_close'] or '—'}. "
                  f"Available cash {num(book['available_cash'])}, margin {num(book['reserved_margin'])}, reserved risk {num(book['reserved_risk'])}, "
                  f"unrealized {num(book['unrealized'])}, adverse cost already embedded in gross {num(book['adverse_cost_in_realized_gross'])}. "
                  f"Cash reconciliation difference {num(book['cash_reconciliation_difference'])} USDT. "
                  f"Average hold {num(book['average_hold_minutes'])} minutes. Top three share of positive gains "
                  f"{num(book['top_three_share_of_positive_gains'])}. Unresolved reason {book['problem'] or '—'}. "
                  f"Rejections and expiries {book['reasons'] or '—'}.", ""]
    lines += ["## Operating evidence", "", f"Managed Phase 15 cycle verified {data['worker']['managed_phase15_verified']}. "
              f"Existing KuCoin launch `{data['worker']['launch_id']}`. Spot public requests {data['collection']['spot_requests']}. "
              f"Continuous training off {data['continuous_training_off']}. Phase 14 amendment `{(data['phase14'] or {}).get('amendment')}`. "
              "There were zero new historical estimator fits.", ""]
    if cycle := data["last_managed"]:
        lines += [f"Last managed cycle {cycle['at']} via `{cycle['launcher']}`. Native Spot result {cycle['spot'].get('status')}. "
                  f"Publications {cycle['published'] or 'none'}.", ""]
    if (proof_path := ROOT / "preserve-after.json").exists():
        proof = read_json(proof_path)
        lines += [f"Preservation checked {proof['files_checked']} existing files. {proof['checks']}.", ""]
    path = ROOT / "REPORT.md"
    put_bytes(path, "\n".join(lines).encode())
    return {"report": str(path), "approval": data["approval"], "managed": data["worker"],
            "continuous_training_off": data["continuous_training_off"], "books": {k:v.get("counts", {}) for k,v in data["books"].items()}}
