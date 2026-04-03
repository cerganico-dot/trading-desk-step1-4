from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from .models import InstrumentQuote, PairSignal


@dataclass
class FilterConfig:
    z_entry_threshold: float = float(os.getenv("Z_ENTRY_THRESHOLD", "1.5"))
    min_volume: float = float(os.getenv("MIN_VOLUME", "500"))
    max_spread_bps: float = float(os.getenv("MAX_SPREAD_BPS", "25"))
    roundtrip_cost_bps: float = float(os.getenv("ROUNDTRIP_COST_BPS", "30"))
    confidence_divisor: float = float(os.getenv("CONFIDENCE_DIVISOR", "3"))


class SignalFilter:
    def __init__(self, config: FilterConfig | None = None) -> None:
        self.config = config or FilterConfig()

    def build_signals(
        self,
        pairs: Iterable[Tuple[str, str]],
        ratios: Dict[str, float],
        zscores: Dict[str, float],
        quotes: Dict[str, InstrumentQuote],
    ) -> List[PairSignal]:
        out: List[PairSignal] = []
        c = self.config

        for left, right in pairs:
            pair_name = f"{left}/{right}"
            ratio = float(ratios.get(pair_name, 0.0))
            z = float(zscores.get(pair_name, 0.0))
            ql = quotes.get(left)
            qr = quotes.get(right)
            left_vol = float(ql.volume) if ql else 0.0
            right_vol = float(qr.volume) if qr else 0.0
            left_spread = float(ql.spread_bps) if ql else 0.0
            right_spread = float(qr.spread_bps) if qr else 0.0

            signal = "NO TRADE"
            eligible = True
            reject_reason = ""
            gross_edge_bps = abs(z) * 10.0
            edge_bps = max(gross_edge_bps - c.roundtrip_cost_bps, 0.0)
            confidence = min(abs(z) / c.confidence_divisor, 1.0)

            if abs(z) >= c.z_entry_threshold:
                signal = f"SELL {left} / BUY {right}" if z > 0 else f"BUY {left} / SELL {right}"

            if signal != "NO TRADE":
                if left_vol < c.min_volume or right_vol < c.min_volume:
                    eligible = False
                    reject_reason = "Volumen insuficiente"
                elif left_spread > c.max_spread_bps or right_spread > c.max_spread_bps:
                    eligible = False
                    reject_reason = "Spread excesivo"
                elif edge_bps <= 0:
                    eligible = False
                    reject_reason = "Edge no supera costos"

            out.append(
                PairSignal(
                    left=left,
                    right=right,
                    ratio=ratio,
                    zscore=z,
                    signal=signal,
                    edge_bps=edge_bps,
                    confidence=confidence,
                    eligible=eligible,
                    reject_reason=reject_reason,
                    left_volume=left_vol,
                    right_volume=right_vol,
                    left_spread_bps=left_spread,
                    right_spread_bps=right_spread,
                )
            )
        return out
