from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Dict, List

from .models import PairSignal, PaperPosition


class PaperTrader:
    def __init__(self, capital: float, trade_notional_pct: float = 0.05, exit_z_abs: float = 0.5) -> None:
        self.capital = float(capital)
        self.trade_notional_pct = float(trade_notional_pct)
        self.exit_z_abs = float(exit_z_abs)
        self.positions: Dict[str, PaperPosition] = {}
        self.events: List[dict] = []

    def _now(self) -> str:
        return datetime.utcnow().strftime("%H:%M:%S")

    def process(self, signals: List[PairSignal]) -> List[dict]:
        generated: List[dict] = []
        for s in signals:
            pair = f"{s.left}/{s.right}"
            pos = self.positions.get(pair)

            if pos is None and s.eligible and s.signal != "NO TRADE":
                side = "SHORT_RATIO" if s.signal.startswith("SELL") else "LONG_RATIO"
                pos = PaperPosition(
                    pair=pair,
                    side=side,
                    entry_ratio=s.ratio,
                    entry_zscore=s.zscore,
                    opened_at=self._now(),
                    size_notional=self.capital * self.trade_notional_pct,
                )
                self.positions[pair] = pos
                event = {
                    "event_type": "OPEN",
                    "pair": pair,
                    "side": side,
                    "time": pos.opened_at,
                    "ratio": round(s.ratio, 8),
                    "zscore": round(s.zscore, 4),
                    "pnl_bps": 0.0,
                    "pnl_currency": 0.0,
                }
                self.events.append(event)
                generated.append(event)
                continue

            if pos is not None:
                should_close = abs(s.zscore) <= self.exit_z_abs or s.signal == "NO TRADE"
                if should_close:
                    pnl_bps = (pos.entry_ratio - s.ratio) * 10000.0 if pos.side == "SHORT_RATIO" else (s.ratio - pos.entry_ratio) * 10000.0
                    pnl_ccy = pos.size_notional * pnl_bps / 10000.0
                    pos.status = "CLOSED"
                    pos.exit_ratio = s.ratio
                    pos.exit_zscore = s.zscore
                    pos.closed_at = self._now()
                    pos.pnl_bps = pnl_bps
                    pos.pnl_currency = pnl_ccy
                    event = {
                        "event_type": "CLOSE",
                        "pair": pair,
                        "side": pos.side,
                        "time": pos.closed_at,
                        "ratio": round(s.ratio, 8),
                        "zscore": round(s.zscore, 4),
                        "pnl_bps": round(pnl_bps, 2),
                        "pnl_currency": round(pnl_ccy, 2),
                    }
                    self.events.append(event)
                    generated.append(event)
                    del self.positions[pair]
        self.events = self.events[-300:]
        return generated

    def open_positions(self) -> List[dict]:
        return [asdict(p) for p in self.positions.values()]
