from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
import yaml

from lab.config import Execution, Frozen, ROOT, fingerprint

Channel = Literal["unconfirmed", "manual_web_app", "api"]
LABEL = "Coinbase BTC-USD price history with a hypothetical published-MEXC-fee scenario."
REFERENCE = Execution(fee_bps=50, proxy_bps=5, processing_seconds=30, delay_bars=0)


def bps_fraction(bps: int | Decimal) -> Decimal:
    return Decimal(bps) / 10000


class Eligibility(Frozen):
    residency_country_context: str | None = Field(pattern=r"^[A-Z]{2}$")
    locality_context: str | None
    terms_url: Literal["https://www.mexc.com/terms"]
    terms_revision: date
    checked_on: date
    additional_eligibility_evidence: str | None

    @property
    def state(self) -> str:
        if self.residency_country_context == "CA":
            return "RESTRICTED"
        # A different or unknown location does not itself verify entitlement to use MEXC.
        return "UNRESOLVED"


class PublishedFees(Frozen):
    name: Literal["mexc_published_spot_taker_research"]
    intended_venue: Literal["MEXC"]
    product: Literal["spot"]
    market_type: Literal["unlevered_spot"]
    proposed_symbol: str = Field(pattern=r"^BTC[A-Z0-9]{2,12}$")
    proposed_pair_confirmed: bool
    proposed_quote_currency: str = Field(pattern=r"^[A-Z0-9]{2,12}$")
    maker_fee_bps: Literal[0]
    taker_fee_bps: Literal[5]
    simulated_entry_liquidity: Literal["taker"]
    simulated_exit_liquidity: Literal["taker"]
    mx_deduction_enabled: Literal[False]
    mx_balance_eligibility_verified: Literal[False]
    promotional_zero_fee_assumed: Literal[False]
    execution_channel: Channel
    fee_provenance: Literal["official_published_standard"]
    account_pair_channel_fee_verified: Literal[False]
    combined_adverse_price_proxy_bps_per_side: Literal[5]
    adverse_proxy_provenance: Literal["inherited_hypothetical_assumption"]
    standard_source: str
    standard_published_on: date
    fee_overview: str
    checked_on: date
    promotion_source: str
    promotion_published_on: date
    promotion_scope_effective_on: date
    promotion_expires_on: date | None
    promotional_api_users_eligible: Literal[False]

    @model_validator(mode="after")
    def validate_quote_and_sources(self):
        if self.proposed_symbol != f"BTC{self.proposed_quote_currency}":
            raise ValueError("The MEXC symbol must identify BTC and the declared quote")
        if any(not url.startswith("https://www.mexc.com/") for url in (
            self.standard_source, self.fee_overview, self.promotion_source,
        )):
            raise ValueError("Published evidence must identify official MEXC sources")
        if not self.promotion_published_on <= self.promotion_scope_effective_on <= self.checked_on:
            raise ValueError("Promotion source dates are out of sequence")
        if self.standard_published_on > self.checked_on:
            raise ValueError("The fee source review predates its publication")
        if self.promotion_expires_on is not None and self.promotion_expires_on < self.promotion_scope_effective_on:
            raise ValueError("Promotion expiry predates the effective scope")
        return self

    def execution_sensitivity(self, reference: Execution) -> Execution:
        if reference != REFERENCE:
            raise ValueError("Phase 4 requires the preserved reference execution")
        return Execution(**{**reference.model_dump(), "fee_bps": self.taker_fee_bps})


class AccountFeeEvidence(Frozen):
    symbol: str
    channel: Literal["manual_web_app", "api"]
    maker_fee_bps: Decimal = Field(ge=0, lt=10000)
    taker_fee_bps: Decimal = Field(ge=0, lt=10000)
    evidence_date: date
    evidence_reference: str = Field(min_length=1)
    account_pair_qualification: str = Field(min_length=1)
    promotion_applied: bool
    promotion_expires_on: date | None
    mx_deduction_enabled: bool
    mx_balance_qualification_evidence: str | None

    @model_validator(mode="after")
    def validate_channel_qualifications(self):
        if self.promotion_applied and self.channel == "api":
            raise ValueError("The published zero-fee offer excludes API users")
        if self.promotion_applied and (self.promotion_expires_on is None or self.promotion_expires_on < self.evidence_date):
            raise ValueError("Promotion needs applicable dated evidence and a known expiry for this study")
        if self.mx_deduction_enabled and not self.mx_balance_qualification_evidence:
            raise ValueError("MX deduction requires separate eligibility and balance evidence")
        return self


class MatchingData(Frozen):
    adapter_version: Literal["mexc-spot-v3-offline-v1"]
    access: Literal["offline"]
    coverage: Literal["INSUFFICIENT_DATA"]
    advertised_archive_start: date
    observed_start: None
    observed_end: None
    observed_gap_count: None
    snapshot_sha256: None
    live_schema_verified: Literal[False]
    bucket_grid_verified: Literal[False]


class ResearchConfig(Frozen):
    version: Literal["mexc-spot-research-v1"]
    eligibility: Eligibility
    published: PublishedFees
    account_fee_evidence: AccountFeeEvidence | None
    matching_data: MatchingData

    @model_validator(mode="after")
    def verify_separation(self):
        if self.eligibility.terms_revision > self.eligibility.checked_on:
            raise ValueError("Terms review predates the cited agreement revision")
        if self.account_fee_evidence is not None:
            evidence, scope = self.account_fee_evidence, self.published
            if (self.eligibility.state != "ELIGIBLE" or not scope.proposed_pair_confirmed
                    or evidence.symbol != scope.proposed_symbol or evidence.channel != scope.execution_channel):
                raise ValueError("Account evidence requires permitted use and the confirmed matching pair and channel")
        return self

    @property
    def identity(self) -> str:
        return fingerprint(self.model_dump(mode="json"))


class MarketIdentity(Frozen):
    venue: Literal["coinbase_exchange", "mexc"]
    product: Literal["spot"]
    market_type: Literal["unlevered_spot"]
    symbol: str
    base_currency: Literal["BTC"]
    quote_currency: str
    data_version: str
    strategy_version: str
    cost_version: str
    execution_version: str

    @model_validator(mode="after")
    def verify_product(self):
        expected = "BTC-USD" if self.venue == "coinbase_exchange" else f"BTC{self.quote_currency}"
        if self.symbol != expected or (self.venue == "coinbase_exchange" and self.quote_currency != "USD"):
            raise ValueError("Venue, symbol and currency must agree")
        return self

    def require_compatible(self, other: "MarketIdentity") -> None:
        if self != other:
            compared = other.model_dump()
            fields = [key for key, value in self.model_dump().items() if compared[key] != value]
            raise ValueError("Incompatible artifact fields: " + ", ".join(fields))


def load_config(path: Path = ROOT / "mexc-research.yaml") -> ResearchConfig:
    return ResearchConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
