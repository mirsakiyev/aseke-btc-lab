from hashlib import sha256
import json
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator
import yaml

ROOT = Path(__file__).resolve().parents[1]
PROFILE_IDS = ("position_spot", "swing", "day")
Venue = Literal["coinbase_exchange"]
Frame = Literal["5min", "15min", "1h", "4h", "1D", "7D"]
ORIGIN = pd.Timestamp("1970-01-05", tz="UTC")  # Monday


def fingerprint(value: object) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Execution(Frozen):
    fee_bps: int = Field(default=50, ge=0, lt=10000)
    proxy_bps: int = Field(default=5, ge=0, lt=10000)
    processing_seconds: int = Field(default=30, ge=0, le=3600)
    delay_bars: int = Field(default=0, ge=0, le=12)

    @property
    def fee(self) -> float:
        return self.fee_bps / 10000

    @property
    def proxy(self) -> float:
        return self.proxy_bps / 10000


class Session(Frozen):
    cutoff_minute_utc: int = Field(default=22 * 60, ge=1, lt=1440)
    flat_minute_utc: int = Field(default=23 * 60 + 55, ge=1, lt=1440)


class Context(Frozen):
    frame: Frame
    average: Literal["sma", "ema"]
    slow: int = Field(ge=2, le=500)
    fast: int | None = Field(default=None, ge=2, le=499)

    @model_validator(mode="after")
    def check_periods(self):
        if self.fast is not None and (self.average != "ema" or self.fast >= self.slow):
            raise ValueError("Fast context requires EMA and a shorter lookback")
        return self


class Profile(Frozen):
    id: Literal["position_spot", "swing", "day"]
    label: str = Field(min_length=1)
    version: str
    venue: Venue = "coinbase_exchange"
    instrument: Literal["BTC-USD"] = "BTC-USD"
    market_type: Literal["spot"] = "spot"
    decision: Frame
    execution: Literal["5min", "1h"]
    context: tuple[Context, ...] = Field(min_length=1)
    holding_label: str
    max_minutes: int = Field(gt=0, le=60 * 24 * 60)
    session: Session | None = None
    entry_range: int = Field(default=20, ge=2, le=500)
    exit_range: int = Field(default=10, ge=2, le=500)
    atr_period: int = Field(default=14, ge=2, le=500)
    atr_multiple: float = Field(default=2, gt=0)
    momentum_period: int = Field(ge=1, le=500)
    volume_period: int = Field(ge=2, le=500)
    feature_version: str = "1"
    target_version: str = "1"
    strategy_version: str = "1"
    starting_cash: float = Field(default=10000, gt=0)
    risk: float = Field(default=0.0025, gt=0, le=0.05)
    allocation: float = Field(default=0.25, gt=0, le=1)
    ridge_alpha: float = Field(default=1, gt=0)
    min_train: int = Field(default=20, ge=2)
    min_eval: int = Field(default=5, ge=1)
    min_train_days: int = Field(gt=0)
    costs: Execution = Execution()

    @model_validator(mode="after")
    def check_frames(self):
        resolution = self.step
        frames = tuple(item.frame for item in self.context)
        decision = pd.Timedelta(self.decision)
        if decision < resolution or decision % resolution:
            raise ValueError("Decision bars must be composed of full execution bars")
        if len(set(frames)) != len(frames) or any(
            pd.Timedelta(frame) <= decision or pd.Timedelta(frame) % resolution for frame in frames
        ):
            raise ValueError("Context frames must be unique, larger, and aggregate from execution")
        if bool(self.session) != (self.id == "day") or (self.id == "day" and self.execution != "5min"):
            raise ValueError("Only Day uses a UTC session and Day requires genuine 5-minute data")
        required = (2 + self.costs.delay_bars) * resolution  # Include the one-bar stress case.
        if self.costs.delay_bars >= 12 or pd.Timedelta(minutes=self.max_minutes) < required:
            raise ValueError("Maximum hold must accommodate reference and stress execution delays")
        if self.session:
            cutoff = pd.Timedelta(minutes=self.session.cutoff_minute_utc)
            flat = pd.Timedelta(minutes=self.session.flat_minute_utc)
            if flat <= cutoff or cutoff % resolution or flat % resolution:
                raise ValueError("Use aligned UTC session times with flat-by after cutoff")
            if flat - cutoff + resolution < required:
                raise ValueError("The session must allow scheduled exits after the last eligible entry")
        return self

    @property
    def step(self) -> pd.Timedelta:
        return pd.Timedelta(self.execution)

    @property
    def seconds(self) -> int:
        return int(self.step.total_seconds())

    @property
    def identity(self) -> str:
        return fingerprint(self.model_dump(mode="json"))


def load_profiles(path: Path = ROOT / "profiles.yaml") -> dict[str, Profile]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if set(raw) != set(PROFILE_IDS):
        raise ValueError("Configuration must contain exactly the three research profiles")
    return {key: Profile(id=key, **raw[key]) for key in PROFILE_IDS}
