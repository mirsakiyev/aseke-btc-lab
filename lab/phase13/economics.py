from bisect import bisect_right
from collections import Counter, defaultdict
import math

import numpy as np

from lab.storage import utc


CLOSED = ("COMPLETE", "AMBIGUOUS")
YEAR_SECONDS = 365.2425 * 86400
BREAKDOWN_VERSION = "saved-ledger-mark-reconstruction-v1"


def marks_column(market, lo, hi, basis, column):
    while lo < hi:
        part = bisect_right(market.starts, lo) - 1
        offset = lo - market.starts[part]
        values = market.read(part)[basis]
        n = min(hi - lo, len(values) - offset)
        yield values[offset:offset+n, column]
        lo += n


def max_drawdown(equity, start, step):
    peak = np.maximum.accumulate(equity)
    drawdown = equity / peak - 1.
    i = int(np.argmin(drawdown))
    p = int(np.flatnonzero(equity[:i+1] == peak[i])[-1])
    recovery = np.flatnonzero(equity[i:] >= peak[i])
    r = int(recovery[0]) + i if len(recovery) else None
    return {"drawdown": float(drawdown[i]), "peak_at": (start+p*step).isoformat(), "trough_at": (start+i*step).isoformat(),
            "recovered_at": (start+r*step).isoformat() if r is not None else None,
            "trough_to_recovery_minutes": int((r-i)*step.total_seconds()/60) if r is not None else None,
            "peak_to_recovery_minutes": int((r-p)*step.total_seconds()/60) if r is not None else None,
            "unrecovered_minutes_through_endpoint": int((len(equity)-1-i)*step.total_seconds()/60) if r is None else None}


def distribution(rows):
    by_exit, by_side, months = Counter(), defaultdict(lambda: {"trades": 0, "net": 0.}), defaultdict(float)
    for row in rows:
        by_exit[row["reason"]] += 1
        side = by_side["long" if row["direction"] == 1 else "short"]
        side["trades"] += 1
        side["net"] += row["net"]
        months[utc(row["exit_at"]).strftime("%Y-%m")] += row["net"]
    winners = sorted((row for row in rows if row["net"] > 0), key=lambda row: row["net"], reverse=True)[:5]
    return {"by_exit": dict(by_exit), "by_side": dict(by_side),
            "largest_wins": [{key: row[key] for key in ("entry_at", "exit_at", "direction", "net")} for row in winners],
            "top_five_winning_net": sum(row["net"] for row in winners),
            "largest_winning_months": sorted(((month, net) for month, net in months.items() if net > 0),
                                             key=lambda pair: -pair[1])[:5],
            "monthly_net": dict(sorted(months.items()))}


def annualized(value, seconds):
    return math.expm1(math.log1p(value)*YEAR_SECONDS/seconds) if value > -1 else -1.


def compare(item, record, ledger, market, design):
    metrics = record["metrics"]
    left, right = utc(design["folds"][0]["outer_start"]), utc(design["folds"][-1]["outer_end"])
    step = market.step
    n = int((right-left)//step)
    cash = float(item["risk"]["starting_equity"])
    equity = np.full(n+1, cash, dtype=float)
    closed = [row for row in ledger if row["status"] in CLOSED]
    if any(row.get("remaining_base") for row in ledger):
        raise ValueError("Unresolved saved exposure requires a different evidence interpretation")
    notional_minutes = 0.
    components = Counter()
    folds = defaultdict(Counter)
    exposure = 0
    wallet_at, wallet = 0, cash
    for row in closed:
        start, end = utc(row["entry_at"]), utc(row["exit_at"])
        i = int(market.times.searchsorted(start.value))
        a, b = int((start-left)//step), int((end-left)//step)
        if not 0 <= wallet_at <= a <= b <= n or market.times[i] != start.value:
            raise ValueError("Saved trade chronology is not aligned")
        if len(row["fills"]) != 1:
            raise ValueError("Primary frozen lane requires its one full exit receipt")
        q = row["signed_base_quantity"]
        fill = row["fills"][0]
        actual_in = row["entry_fill_price"]
        fee = abs(q)*actual_in*item["cost"]["taker"]
        start_wallet = row["sizing_equity"] - fee
        if not math.isclose(row["sizing_equity"], wallet, abs_tol=1e-7):
            raise ValueError("Phase 12 frozen account continuity changed")
        equity[wallet_at:a+1] = wallet
        if b > a:
            fair = np.concatenate(list(marks_column(market, i, i + b-a, 1, 3)))
            if len(fair) != b-a or not np.isfinite(fair).all():
                raise ValueError("Missing required mark in saved exposure reconstruction")
            path = start_wallet + q*(fair - actual_in)
            notional_minutes += float(np.sum(fair) * abs(q))
            exposure += len(fair)
        else:
            path = np.empty(0)
        raw_in = float(market.trade_prices[i][0])
        no_adverse_price_gross = q*(fill["raw_trade_price"] - raw_in)
        funding_received = funding_paid = 0.
        for event in row.get("settlements", []):
            amount = -q*event["rate"]*event["mark_proxy"]
            funding_received += max(amount, 0.)
            funding_paid -= min(amount, 0.)
            offset = int((utc(event["settlement_at"]).floor(step) - start)//step)
            if 0 <= offset < len(path):
                path[offset:] += amount
        if len(path):
            equity[a+1:b+1] = path
        wallet_at, wallet = b, row["ending_equity"]
        equity[b] = wallet
        gross = row["gross_realized"]
        vals = {"price_pnl_before_adverse": no_adverse_price_gross, "gross_fill_price_pnl": gross,
                "adverse_execution_cost": no_adverse_price_gross - gross, "commissions": row["fees"],
                "funding_paid": funding_paid, "funding_received": funding_received, "funding_net": row["funding"],
                "liquidation_cost": row["liquidation_cost"], "unfunded_deficit": row["unfunded_deficit"],
                "net": row["net"], "turnover": row["turnover"], "planned_risk": row["planned_risk"],
                "realized_losing_pnl_absolute": max(0., -row["net"]), "closed_trades": 1,
                "loss_over_plan_count": int(-row["net"] > row["planned_risk"]),
                "loss_over_risk_budget_count": int(row["realized_loss_overrun"] > 0)}
        components.update(vals)
        folds[row["fold"]].update(vals)
    equity[wallet_at:] = wallet
    dd = max_drawdown(equity, left, step)
    ret = wallet/cash - 1.
    if not math.isclose(ret, metrics["return"], abs_tol=1e-10) or not math.isclose(dd["drawdown"], metrics["drawdown_observed_marks"], abs_tol=1e-9):
        raise ValueError(f"Saved account return/drawdown disagrees with ledger reconstruction: {item['trial']['id']}: {ret}, {dd['drawdown']}")
    checksum = (components["price_pnl_before_adverse"] - components["adverse_execution_cost"] - components["commissions"]
                + components["funding_net"] - components["liquidation_cost"] + components["unfunded_deficit"])
    if not math.isclose(checksum, components["net"], abs_tol=1e-7):
        raise ValueError("Net P&L attribution did not reconcile")
    fold_records = {}
    for fold in design["folds"]:
        a, b = utc(fold["outer_start"]), utc(fold["outer_end"])
        x, y = int((a-left)//step), int((b-left)//step)
        code = fold["id"]
        accepted = [row for row in ledger if row["fold"] == code]
        s = sum(row["status"] != "SKIPPED" for row in accepted)
        fold_records[code] = {"start": a.isoformat(), "end_exclusive": b.isoformat(),
                              "net_return": float(equity[y]/equity[x]-1.),
                              "annualized_return": annualized(float(equity[y]/equity[x]-1.), (b-a).total_seconds()),
                              **max_drawdown(equity[x:y+1], a, step), "eligible": len(accepted), "accepted": s,
                              "acceptance_rate": s/len(accepted) if accepted else None, "economics": dict(folds[code])}
    total_accepted = sum(row["status"] != "SKIPPED" for row in ledger)
    risk = [max(0., -row["net"])/row["planned_risk"] for row in closed if row["planned_risk"] > 0]
    return {"candidate": item["id"], "trial": item["trial"]["id"], "period": [left.isoformat(), right.isoformat()],
            "net_return": ret, "annualized_return": annualized(ret, (right-left).total_seconds()),
            "drawdown_basis": "Phase 12 one-minute mark-close equity and realized exit updates",
            **dd, "eligible": len(ledger), "accepted": total_accepted,
            "acceptance_rate": total_accepted/len(ledger) if ledger else None,
            "entry_fills": sum("entry_at" in row for row in ledger),
            "observed_mark_exposure_fraction": metrics.get("exposure_fraction_of_observed_marks", 0.),
            "held_minutes": exposure, "average_notional_all_minutes": notional_minutes/n,
            "average_notional_while_held": notional_minutes/exposure if exposure else 0.,
            "notional_basis": "Absolute filled base units times minute mark close over modeled holding intervals",
            "economics": dict(components), "exit_distribution": distribution(closed),
            "worst_realized_loss_as_planned_risk": max(risk, default=None),
            "filter_counts": metrics.get("filter_counts", {}),
            "unresolved_exposure": metrics.get("unresolved_reason"), "folds": fold_records,
            "uncertainty": metrics.get("uncertainty", {"status": "CASH_CONTROL"}),
            "evidence": "DESCRIPTIVE_CONSUMED_DEVELOPMENT", "method": BREAKDOWN_VERSION}
