import numpy as np
import pandas as pd

from lab.config import Execution, Profile


def quantity_plan(close: float, stop: float, equity: float, costs: Execution, profile: Profile) -> dict:
    # Independent reconciliation of the constraints, using the signal close rather than the future fill.
    entry_estimate = close * (1 + costs.proxy)
    stop_estimate = stop * (1 - costs.proxy)
    outlay = entry_estimate + entry_estimate * costs.fee
    stop_proceeds = stop_estimate - stop_estimate * costs.fee
    loss = outlay - stop_proceeds
    if min(loss, equity) <= 0:
        raise ValueError("Cannot reconcile a nonpositive planned risk or account equity")
    caps = {
        "risk": equity * profile.risk / loss,
        "entry_notional": equity * profile.allocation / entry_estimate,
        "cash": equity / outlay,
    }
    binding = min(caps, key=caps.get)
    units = caps[binding]
    return {
        "decision_equity_usd": equity, "observable_signal_close_usd": close, "raw_protective_stop_usd": stop,
        "estimated_adverse_entry_usd_per_btc": entry_estimate, "estimated_adverse_stop_usd_per_btc": stop_estimate,
        "planned_risk_budget_usd": equity * profile.risk, "planned_cost_inclusive_loss_per_btc_usd": loss,
        "planned_entry_notional_cap_usd": equity * profile.allocation, "maximum_cash_outlay_usd": equity,
        "uncapped_risk_quantity_btc": caps["risk"], "exposure_cap_quantity_btc": caps["entry_notional"],
        "cash_cap_quantity_btc": caps["cash"], "binding_constraint": binding,
        "rounding": "Fractional BTC in the original float ledger; no exchange lot-size rounding.",
        "planned_quantity_btc": units, "planned_cost_inclusive_risk_usd": units * loss,
        "planned_entry_notional_usd": units * entry_estimate, "planned_cash_outlay_usd": units * outlay,
    }


def audit_ledger(sim, candles, signals, costs: Execution, profile: Profile) -> tuple[dict, dict]:
    cash = profile.starting_cash
    last_exit = None
    evidence = {}
    if sim.trades.empty:
        return evidence, {"trades_checked": 0, "binding_constraints": {}, "overruns": {}}
    for trade in sim.trades.itertuples():
        if last_exit is not None and last_exit >= trade.available_at:
            raise ValueError("A prior exit was unavailable at the next sizing decision")
        signal = signals.loc[trade.signal_at]
        np.testing.assert_allclose(signal.stop, trade.stop, atol=1e-10)
        plan = quantity_plan(float(signal.close), float(signal.stop), cash, costs, profile)
        units = trade.units_btc
        np.testing.assert_allclose(
            [plan["planned_risk_budget_usd"], plan["planned_cost_inclusive_risk_usd"], plan["planned_quantity_btc"]],
            [trade.risk_budget_usd, trade.planned_risk_usd, units], atol=1e-9, rtol=1e-10,
        )
        raw_buy = float(candles.loc[trade.entry_at, "open"])
        buy = raw_buy * (1 + costs.proxy)
        sell, raw_sell, exit_fee, net = None, None, None, None
        entry_fee = buy * units * costs.fee
        actual_cost = buy * units + entry_fee
        if actual_cost > cash + 1e-8:
            raise ValueError("Recorded fill exceeded affordable cash")
        if pd.notna(trade.exit_at):
            raw_sell = float(trade.stop if trade.reason == "stop_intrabar" else candles.loc[trade.exit_bar_open, "open"])
            sell = raw_sell * (1 - costs.proxy)
            exit_fee = sell * units * costs.fee
            net = sell * units - exit_fee - actual_cost
            np.testing.assert_allclose([sell, net], [trade.exit_price, trade.net_usd], atol=1e-9, rtol=1e-10)
            last_exit = trade.exit_at
        else:
            last_exit = pd.Timestamp.max.tz_localize("UTC")
        stop_proceeds = float(trade.stop) * (1 - costs.proxy) * (1 - costs.fee)
        entry_risk = actual_cost - stop_proceeds * units
        risk_overrun = max(0., entry_risk - plan["planned_risk_budget_usd"])
        allocation_overrun = max(0., buy * units - plan["planned_entry_notional_cap_usd"])
        realized_overrun = max(0., -net - plan["planned_risk_budget_usd"]) if net is not None else None
        np.testing.assert_allclose(
            [buy, buy * units, entry_fee + (exit_fee or 0.), entry_risk, risk_overrun, allocation_overrun],
            [trade.entry_price, trade.entry_notional, trade.fees_usd, trade.fill_to_stop_risk_usd,
             trade.fill_risk_overrun_usd, trade.allocation_overrun_usd], atol=1e-8,
        )
        if realized_overrun is not None:
            np.testing.assert_allclose(realized_overrun, trade.realized_loss_overrun_usd, atol=1e-8)
        evidence[trade.signal_at] = {
            **plan, "signal_at": trade.signal_at, "information_cutoff": trade.available_at, "hypothetical_entry_at": trade.entry_at,
            "hypothetical_exit_at": trade.exit_at if pd.notna(trade.exit_at) else None,
            "entry_candle_raw_open_usd": raw_buy, "exit_raw_usd": raw_sell,
            "entry_after_proxy_usd": buy, "exit_after_proxy_usd": sell, "exit_reason": trade.reason,
            "entry_fee_usd": entry_fee, "exit_fee_usd": exit_fee, "final_quantity_btc": units, "net_usd": net,
            "actual_entry_cost_usd": actual_cost, "actual_entry_notional_usd": buy * units,
            "actual_entry_weight_of_decision_equity": buy * units / cash,
            "actual_entry_stop_risk_usd": entry_risk, "fill_risk_overrun_usd": risk_overrun,
            "realized_risk_overrun_usd": realized_overrun, "fill_allocation_overrun_usd": allocation_overrun,
        }
        cash += net if net is not None else -actual_cost
    np.testing.assert_allclose(cash, sim.equity.cash.iloc[-1], atol=1e-8)
    rows = list(evidence.values())
    return evidence, {
        "trades_checked": len(rows),
        "binding_constraints": {key: sum(row["binding_constraint"] == key for row in rows)
                                for key in ("risk", "entry_notional", "cash")},
        "max_actual_entry_notional_as_fraction_of_decision_equity": max(
            row["actual_entry_weight_of_decision_equity"] for row in rows),
        "overruns": {key: {"count": sum((row[key] or 0.) > 1e-8 for row in rows),
                           "largest_usd": max(row[key] or 0. for row in rows)}
                     for key in ("fill_risk_overrun_usd", "realized_risk_overrun_usd", "fill_allocation_overrun_usd")},
    }


def sizing_report(accounts, manifest, candles, signals, profile) -> dict:
    audits, details = {}, {}
    for key, sim in accounts.items():
        if key != "fixed_ledger":
            details[key], audits[key] = audit_ledger(sim, candles, signals, Execution(**manifest["costs"][key]), profile)
    old = accounts["reference"].trades.set_index("signal_at")
    fixed = accounts["fixed_ledger"].trades.set_index("signal_at")
    low = accounts["lower_fee"].trades.set_index("signal_at")
    fields = ["entry_at", "exit_at", "entry_price", "exit_price", "stop"]
    for other in (fixed, low):
        pd.testing.assert_frame_equal(old[fields], other[fields])
    pd.testing.assert_series_equal(old.units_btc, fixed.units_btc)
    if old.exit_at.isna().any():
        raise ValueError("Fixed-quantity decomposition requires the preserved closed trade set")
    earlier_cash = profile.starting_cash
    low_costs = Execution(**manifest["costs"]["lower_fee"])
    components = {"cost_inclusive_sizing_at_original_equity_usd": 0.,
                  "reinvested_fixed_ledger_fee_savings_usd": 0.,
                  "additional_sizing_compounding_usd": 0.}
    for at, original in old.iterrows():
        final, repriced = low.loc[at], fixed.loc[at]
        unit_net = (final.exit_price * (1 - low_costs.fee)) - (final.entry_price * (1 + low_costs.fee))
        close = details["reference"][at]["observable_signal_close_usd"]
        q_cost = quantity_plan(close, original.stop, details["reference"][at]["decision_equity_usd"],
                               low_costs, profile)["planned_quantity_btc"]
        q_fee_equity = quantity_plan(close, original.stop, earlier_cash, low_costs, profile)["planned_quantity_btc"]
        components["cost_inclusive_sizing_at_original_equity_usd"] += (q_cost - original.units_btc) * unit_net
        components["reinvested_fixed_ledger_fee_savings_usd"] += (q_fee_equity - q_cost) * unit_net
        components["additional_sizing_compounding_usd"] += (final.units_btc - q_fee_equity) * unit_net
        earlier_cash += repriced.net_usd
    fee_saving = float(old.fees_usd.sum() - fixed.fees_usd.sum())
    increment = float(low.net_usd.sum() - fixed.net_usd.sum())
    np.testing.assert_allclose(sum(components.values()), increment, atol=1e-8)
    difference = (low.units_btc - old.units_btc).abs()
    typical = next((at for at in old.index if all(details[key][at]["binding_constraint"] == "risk"
                  and details[key][at]["fill_risk_overrun_usd"] < 1e-8 for key in ("reference", "lower_fee"))), old.index[0])
    examples = {"ordinary_first_matching_non_overrun_trade": typical, "largest_absolute_btc_quantity_change": difference.idxmax()}
    cap = next((at for at in old.index if any(details[key][at]["binding_constraint"] != "risk"
                                             for key in ("reference", "lower_fee"))), None)
    if cap is not None:
        examples["first_cap_binding_trade"] = cap
    return {
        "same_entry_exit_and_raw_stop_identities": len(old), "quantities_changed": int(difference.ne(0).sum()),
        "fixed_ledger_fee_savings_usd": fee_saving, "additional_replay_pnl_usd_unrounded": increment,
        "sizing_effect_components_usd": components,
        "attribution": "Arithmetic on the same trades and new per-unit outcomes. Reconstruct sizing using original equity, "
                       "then fixed-ledger equity, then replay equity. No changes to signal selection.",
        "audits": audits, "real_cap_binding_example_found": cap is not None,
        "examples": {name: {key: details[key][at] for key in ("reference", "lower_fee")} for name, at in examples.items()},
    }
