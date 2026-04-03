from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class InstrumentQuote:
    symbol: str
    bid: float
    ask: float
    last: float
    volume: float
    ts: datetime

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        return self.last

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        if mid <= 0:
            return 0.0
        return ((self.ask - self.bid) / mid) * 10000.0


@dataclass
class PairSignal:
    left: str
    right: str
    ratio: float
    zscore: float
    signal: str
    edge_bps: float
    confidence: float
    eligible: bool = True
    reject_reason: str = ""
    left_volume: float = 0.0
    right_volume: float = 0.0
    left_spread_bps: float = 0.0
    right_spread_bps: float = 0.0


@dataclass
class HedgePlan:
    instrument: str
    contracts: int
    side: str
    notional: float
    rationale: str


@dataclass
class RiskReport:
    capital: float
    max_risk_per_trade: float
    max_daily_risk: float
    current_gross_exposure: float
    exposure_limit: float
    utilization_pct: float
    status: str


@dataclass
class PaperPosition:
    pair: str
    side: str
    entry_ratio: float
    entry_zscore: float
    opened_at: str
    size_notional: float
    status: str = "OPEN"
    exit_ratio: Optional[float] = None
    exit_zscore: Optional[float] = None
    closed_at: Optional[str] = None
    pnl_bps: Optional[float] = None
    pnl_currency: Optional[float] = None


@dataclass
class PaperTradeEvent:
    event_type: str
    pair: str
    side: str
    time: str
    ratio: float
    zscore: float
    pnl_bps: float = 0.0
    pnl_currency: float = 0.0


@dataclass
class OpportunityLogItem:
    id: int
    time: str
    pair: str
    left: str
    right: str
    signal: str
    ratio: float
    zscore: float
    edge_bps: float
    left_last: float
    right_last: float
    left_bid: float
    right_bid: float
    left_ask: float
    right_ask: float
    series_index: int
    eligible: bool = True
    reject_reason: str = ""


@dataclass
class DeskState:
    mode: str
    quotes: dict[str, InstrumentQuote]
    ratios: dict[str, float]
    zscores: dict[str, float]
    pair_signals: list[PairSignal]
    hedge_plans: list[HedgePlan]
    risk_report: RiskReport
    pnl_series: list[float]
    timestamps: list[str]
    notes: list[str]
    ratio_history: dict[str, list[float]] = field(default_factory=dict)
    zscore_history: dict[str, list[float]] = field(default_factory=dict)
    series_timestamps: list[str] = field(default_factory=list)
    opportunity_log: list[dict] = field(default_factory=list)
    paper_positions: list[dict] = field(default_factory=list)
    paper_events: list[dict] = field(default_factory=list)
    alerts_sent: list[dict] = field(default_factory=list)
