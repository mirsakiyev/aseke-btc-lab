import numpy as np
import pandas as pd
import pytest

from lab.learning.data import Market
from lab.learning.design import config
from lab.learning.simulator import Contract, Cost, Risk, Setup
from lab.phase10.trading import run_trade
from lab.phase11.campaign import start_clock
from lab.phase17 import labels, pilot, pilot_context, pilot_managed, pilot_registry, pilot_report
from lab.storage import file_hash, read_json, utc, write_json


def test_pilot_registration_keeps_futures_window_and_parent(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot_registry, "PILOT", tmp_path)
    base = {"start":"2027-02-09T00:00:00+00:00", "end_exclusive":"2028-01-01T00:00Z", "minimum_train":35,
            "minimum_inner":10,"minimum_outer":10,"minimum_new_mature":40,"interval_days":1,"queue":True,
            "recipe":{},"features":[],"market":"FUTURES","instrument":"XBTUSDTM"}
    original = {"id":"unchanged", "body":{"profiles":{"day":base,"swing":{**base,"queue":False},
                  "position":{**base,"start":"2026-11-09T23:00Z","market":"SPOT","instrument":"BTC-USDT"}},
                  "budget":{}, "holdout":{},"phase14_evaluation":{"end_exclusive":"2027-02-07T20:00Z"},"phase15_spot_overlap":{}}}
    monkeypatch.setattr(pilot_registry,"registered",lambda:original)
    monkeypatch.setattr(pilot_registry,"freeze_initial",lambda:{"id":"phase16","body":{"profiles":{
        name:{"profile":{}} for name in ("day","position","swing")},"policy":{}}})
    p=pilot_registry.register(utc("2026-11-09T23:28Z"))
    assert p["body"]["activation"] == "2026-11-10T00:00:00+00:00"
    assert p["body"]["profiles"]["day"]["label_start"] == base["start"]
    assert p["body"]["profiles"]["position"]["label_start"] == p["body"]["activation"]
    assert not p["body"]["profiles"]["swing"]["queue"]
    assert p["body"]["parent_phase17"] == original["id"]
    assert pilot_registry.register(utc("2026-11-11T00:00Z"))["id"] == p["id"]


def test_feature_context_sets_actual_availability_and_no_historical_signals():
    cfg = {"profile_rules":config()["profiles"]["position_spot_research_v1"],"market":"SPOT","instrument":"BTC-USDT","context_bar":"1D"}
    registration = {"id":"pilot", "body":{"profiles":{"position":cfg}}}
    start = utc("2027-01-01T00:00Z")
    bridge = start+pd.Timedelta(days=178)
    days = pd.date_range(start,bridge,freq="1D",inclusive="left")
    values=10000+np.arange(len(days))*70+np.sin(np.arange(len(days))/3)*300
    past=pd.DataFrame({"open_at":days,"close_at":days+pd.Timedelta(days=1),"open":values-3.,"high":values+5.,"low":values-10.,
                       "close":values,"volume":10.+np.sin(np.arange(len(days))/3),"received_at":bridge+pd.Timedelta(minutes=1)})
    admitted=bridge+pd.Timedelta(minutes=5)
    seed={"id":"context", "body":{"pilot":"pilot","profile":"position","scope":pilot_registry.SCOPE,"market":"SPOT",
                                     "instrument":"BTC-USDT","basis":"TRADE","cutoff":bridge.isoformat(),"admitted_at":admitted.isoformat(),
                                     "bars":past.to_dict("records")}}
    hours=pd.date_range(bridge,bridge+pd.Timedelta(days=3),freq="1h",inclusive="left")
    price=values[-1]+np.arange(len(hours))*2
    trades=pd.DataFrame({"open":price,"high":price+5,"low":price-5,"close":price,"volume":2.,"close_at":hours+pd.Timedelta(hours=1)},index=hours)
    receipts=trades.close_at+pd.Timedelta(seconds=50)
    receipts.iloc[23]+=pd.Timedelta(minutes=3)
    market=Market(trades,trades,{},pd.Timedelta(hours=1),"SYNTHETIC")
    build=pilot_context.builder(registration,"position",seed)
    features,_,available,timely=build({"market":market},receipts)
    assert available.loc[bridge] == admitted and not timely.loc[:bridge].any()
    assert features.ready.loc[bridge+pd.Timedelta(days=1)] and not timely.loc[bridge+pd.Timedelta(days=1)]
    assert timely.loc[bridge+pd.Timedelta(days=2)]
    first_decision = bridge + pd.Timedelta(days=1)
    late_seed = {**seed, "body": {**seed["body"], "admitted_at": (first_decision+pd.Timedelta(seconds=30)).isoformat()}}
    _, _, late_available, late_timely = pilot_context.builder(registration,"position",late_seed)(
        {"market":market}, trades.close_at+pd.Timedelta(seconds=50))
    assert late_available.loc[first_decision] <= first_decision+labels.RECEIPT_ALLOWANCE
    assert not late_timely.loc[first_decision] and late_timely.loc[first_decision+pd.Timedelta(days=1)]
    assert market.index[0] == bridge and len(market.trade)==len(hours)
    with pytest.raises(ValueError, match="cross market or use scope"):
        pilot_context.builder(registration,"position",{**seed,"body":{**seed["body"],"scope":"FEATURE_CONTEXT_ONLY"}})


def test_pilot_status_presents_legacy_spot_gaps_as_inapplicable(tmp_path, monkeypatch):
    for key in ("ROOT", "PILOT"):
        monkeypatch.setattr(pilot_report, key, tmp_path)
    monkeypatch.setattr(pilot_report, "current", lambda: {"id":"fixture", "body":{"parent_phase17":"parent", "profiles":{},
                                                                                   "activation":"2027-01-01T00:00Z"}})
    monkeypatch.setattr(pilot_report, "settings", lambda: {"recurring":True})
    evidence = tmp_path / "first-spot-managed.json"
    write_json(evidence, {"spot_coverage":{"bases":{"trade":{"rows":1}, "mark":{"rows":0}, "funding":{"rows":0}},
                                          "trade_gaps":0, "mark_gaps":1, "paired":0},
                          "futures":{"mark_gaps":1, "paired":14}})
    original = evidence.read_bytes()
    view = pilot_report.status(utc("2027-01-01T00:01Z"))
    assert view["recurring_training"] is True
    capture = view["first_spot_managed"]
    spot = capture["spot_coverage"]
    assert spot["trade_gaps"] == 0 and spot["mark_gaps"] is None and spot["paired"] is None
    assert spot["applicability"] == {"trade":"REQUIRED", "mark":"NOT_APPLICABLE", "funding":"NOT_APPLICABLE"}
    assert list(spot["bases"]) == ["trade"] and capture["futures"] == {"mark_gaps":1, "paired":14}
    assert evidence.read_bytes() == original


def test_gap_only_invalidates_dependent_native_path():
    start=utc("2027-03-01T00:00Z")
    step=pd.Timedelta(minutes=1)
    index=pd.date_range(start,periods=13,freq=step).delete(2)
    price=pd.DataFrame({"open":100.,"high":101.,"low":99.,"close":100.,"volume":10.,"close_at":index+step},index=index)
    market=Market(price,price,{},step,"SYNTHETIC")
    market.intrabar_funding={}
    receipts=price.close_at+pd.Timedelta(seconds=30)
    executor=labels.audited_executor(run_trade,[(start-step,start+20*step)],{},[],receipts,receipts,step=step)
    contract=Contract("linear_perp","USDT",.001,1.,.1,1.,100000.,20.,.005,.001)
    settings=config()
    risk=Risk(**settings["risk"]["perp_equal_risk_v1"])
    def trade(at):
        setup=Setup(at-step,at-step,at,at+step,at+3*step,100.,5.,90.,1,2.,None,"breakout_v1","fixed_r_v1")
        return executor(market,setup,contract,Cost(.0006,.0005),risk,10000.,settings["execution"],start+12*step)
    first=trade(start)
    assert first["status"]=="INCOMPLETE" and first["reason"]=="trade_feed_gap_while_exposed" and first["remaining_base"]>0
    assert trade(start+4*step)["status"]=="COMPLETE"
    later = pilot_context.contiguous_suffix(pd.DataFrame({"open_at":index}),step)
    assert later.open_at.iloc[0]==start+3*step and len(later)==10


def test_pilot_fits_once_before_outer_and_locks_evaluation(tmp_path, monkeypatch):
    for module in (pilot,pilot_registry):
        monkeypatch.setattr(module,"PILOT",tmp_path)
    start=utc("2027-03-01T00:00Z")
    split=start+pd.Timedelta(days=1)
    end=split+pd.Timedelta(days=1)
    now=split+pd.Timedelta(hours=3)
    recurring=False
    cfg={"label_start":start.isoformat(),"minimum_train":35,"minimum_inner":10,"minimum_new_mature":40,"minimum_outer":10,"queue":True,
         "interval_days":1,"source_end_exclusive":"2028-01-01T00:00Z","evaluation_days":90,"features":["relative_volume"],"recipe":{"cost":"fixture"}}
    p={"id":"fixture","body":{"profiles":{"day":cfg},"budget":{"wall_seconds":7200},"policy":{
        "alpha":10.,"max_fit_attempts":2,"max_attempts_per_fit":2,"minimum_closed":25,"controls":["unfiltered","training_mean","cash","half_risk_rule"],
        "cases":["base","double_cost","delay"],"max_account_attempts":24,"max_attempts_per_account":2,"risk_control_multiplier":.5}}}
    monkeypatch.setattr(pilot,"utc_now",lambda:now)
    monkeypatch.setattr(pilot,"pinned",lambda *_:{"id":"seed"})
    monkeypatch.setattr(pilot,"controls",lambda:{"recurring":recurring})
    monkeypatch.setattr(pilot,"completed_for",lambda _:{"completed_at":(start-pd.Timedelta(days=20)).isoformat()})
    train_at=pd.date_range(start,periods=35,freq="15min")
    inner_at=pd.date_range(split,periods=10,freq="15min")
    # This row matures at the split and cannot enter training or inner.
    old_late=split-pd.Timedelta(minutes=15)
    calls=[]
    def provider(_,name,seed,cutoff,**kw):
        calls.append(cutoff)
        assert name=="day" and cutoff<=end
        index=train_at.append(pd.DatetimeIndex([old_late]))
        if cutoff==end:
            index=index.append(inner_at)
        frame=pd.DataFrame({"net_r":np.arange(len(index))/100.,"mature_at":index+pd.Timedelta(minutes=15)},index=index)
        features=pd.DataFrame({"relative_volume":np.arange(len(index),dtype=float)},index=index)
        return {"id":"source"},({"seed":308},{"features":features},(dict.fromkeys(index),Cost(.001,.001),frame,[],{}),None,{})
    monkeypatch.setattr(pilot,"label_input",provider)
    first=pilot.prepare(p,"day",split,tmp_path/"check1")
    assert first["mature_train"]==35 and first["mature_inner"]==0 and first["state"]=="WAITING_FOR_MATURE_SAMPLES"
    now=end+pd.Timedelta(hours=3)
    work=tmp_path/"check2"
    start_clock(work,7200,now)
    ready=pilot.prepare(p,"day",end,work)
    assert ready["state"]=="FIT_ELIGIBLE_TRAINING_OFF" and ready["mature_inner"]==10
    recurring=True
    fitted=pilot.prepare(p,"day",end,work)
    assert fitted["state"]=="CANDIDATE_FROZEN" and fitted["evaluation"]
    profile=pilot.profile_dir("day")
    candidate=pilot.pointed(profile,"candidate","candidates")["body"]
    path=profile/"fit"/"models"/(candidate["fit"]["version"]+".joblib")
    sha=file_hash(path)
    assert pilot.prepare(p,"day",end,work)["candidate"]==fitted["candidate"] and file_hash(path)==sha
    assert len(list((profile/"fit"/"reservations"/"fits").glob("*.json")))==1
    assert pilot.evaluate(p,"day",tmp_path/"future",now)["state"]=="FORWARD_HORIZON_OPEN"
    assert len(calls)==3

    evaluation=pilot.pointed(profile,"evaluation","evaluations")["body"]
    left,right=(utc(evaluation[key]) for key in ("start","end_exclusive"))
    now=utc(evaluation["scoring_available_at"])
    step=pd.Timedelta(minutes=1)
    decisions=pd.date_range(left+step,periods=12,freq="1D")
    minutes=pd.DatetimeIndex([at+k*step for at in decisions for k in range(8)]+[right-step])
    prices=pd.DataFrame({"open":100.,"high":101.,"low":99.,"close":100.,"volume":10.,"close_at":minutes+step},index=minutes)
    market=Market(prices,prices,{},step,"SYNTHETIC")
    market.intrabar_funding={}
    contract=Contract("linear_perp","USDT",.001,1.,.1,1.,100000.,20.,.005,.001)
    settings=config()
    data={"market":market,"features":pd.DataFrame({"relative_volume":np.ones(len(decisions))},index=decisions),
          "risk":Risk(**settings["risk"]["perp_equal_risk_v1"]),"rules":settings["execution"],"trail":{},"profile":{},"contract":contract}
    setups={at:Setup(at,at,at+step,at+2*step,at+4*step,100.,5.,90.,1,2.,None,"breakout_v1","fixed_r_v1") for at in decisions}
    samples=pd.DataFrame({"net_r":.05,"mature_at":decisions+5*step},index=decisions)
    design={"seed":308,"settings":settings,"feature_version":"synthetic","label_version":"synthetic"}
    def future_provider(*args, decision_start, **kwargs):
        assert decision_start==left and args[-1]==right
        return {"id":"untouched-fixture"},(design,data,(setups,Cost(.001,.001),samples,[],{}),run_trade,{})
    frozen_find=pilot.find
    monkeypatch.setattr(pilot,"find",lambda kind,key: {"id":"seed"} if kind=="context" else frozen_find(kind,key))
    monkeypatch.setattr(pilot,"label_input",future_provider)
    future=tmp_path/"future"
    start_clock(future,7200,now)
    outcome=pilot.evaluate(p,"day",future,now)
    assert outcome["state"]=="EVALUATED_RESEARCH_ONLY" and outcome["model"]==candidate["fit"]["version"]
    assert outcome["mature"]==12 and len(outcome["accounts"])==12 and outcome["conclusion"]=="INCONCLUSIVE"
    assert len(list((profile/"fit"/"reservations"/"fits").glob("*.json")))==1


def test_enabling_uses_saved_fit_eligible_cutoff_with_first_fit_budget(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from lab.learning import control as shared
    from lab.phase16 import control

    start=utc("2027-03-01T00:00Z")
    now=start+pd.Timedelta(days=4)
    cutoff=start+pd.Timedelta(days=1)
    cfg={"label_start":start.isoformat(),"source_end_exclusive":"2027-12-01T00:00Z"}
    p={"body":{"profiles":{"day":cfg},"activation":start.isoformat(),"budget":{}}}
    monkeypatch.setattr(pilot_managed,"PILOT",tmp_path)
    monkeypatch.setattr(pilot_managed,"current",lambda:p)
    monkeypatch.setattr(shared,"admission",lambda _:None)
    monkeypatch.setattr(shared,"process_identity",lambda pid,created:pid==123 and created==1.)
    recurring=False
    monkeypatch.setattr(control,"settings",lambda:{"recurring":recurring})
    monkeypatch.setattr(pilot_managed.psutil,"Process",lambda pid:SimpleNamespace(pid=pid,create_time=lambda:1.))
    calls=[]
    monkeypatch.setattr(pilot_managed.subprocess,"Popen",lambda args,**_:calls.append(args) or SimpleNamespace(pid=123))
    write_json(tmp_path/"pins"/"day.json",{"id":"fixture"})
    write_json(tmp_path/"profiles"/"day"/"readiness.json",{"state":"FIT_ELIGIBLE_TRAINING_OFF","checked_cutoff":cutoff.isoformat()})
    # Suppress the newly closed label-only dispatch for the disabled case.
    write_json(tmp_path/"worker.json",{"pid":1,"created":1.,"profile":"day","at":now.isoformat()})
    pilot_managed.tick(now)
    assert not calls
    recurring=True
    now+=pd.Timedelta(minutes=31)
    pilot_managed.tick(now)
    pilot_managed.tick(now)
    assert len(calls)==1 and calls[0][-4:]==["day",cutoff.isoformat(),"--kind","pilot-fit"]
