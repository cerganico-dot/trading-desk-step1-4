from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import sin
from typing import Dict, List

from .models import DeskState, HedgePlan, InstrumentQuote, PairSignal, RiskReport


class DeskSimulator:
    def __init__(self) -> None:
        self.base_time = datetime.now(UTC).replace(second=0, microsecond=0)
        self.series_timestamps = [
            (self.base_time - timedelta(minutes=79 - i)).strftime("%H:%M")
            for i in range(80)
        ]
        self.ratio_history = {"AL30/GD30": [], "AL30D/GD30D": []}
        self.zscore_history = {"AL30/GD30": [], "AL30D/GD30D": []}
        self.opportunity_log: List[dict] = []
        self._build_histories()
        self.pnl_series = [round(sum(abs(self.zscore_history[p][-1] if i == len(self.series_timestamps)-1 else 0) for p in self.zscore_history)*1000, 2) for i in range(len(self.series_timestamps))]

    def _build_histories(self) -> None:
        oid = 0
        for i, ts in enumerate(self.series_timestamps):
            r1 = 0.965 + 0.006 * sin(i / 4)
            r2 = 0.966 + 0.005 * sin(i / 5 + 0.8)
            z1 = 2.2 * sin(i / 4)
            z2 = 1.8 * sin(i / 5 + 0.8)
            self.ratio_history["AL30/GD30"].append(r1)
            self.ratio_history["AL30D/GD30D"].append(r2)
            self.zscore_history["AL30/GD30"].append(z1)
            self.zscore_history["AL30D/GD30D"].append(z2)
            if abs(z1) >= 1.5:
                oid += 1
                self.opportunity_log.append({
                    "id": oid, "time": ts, "pair": "AL30/GD30", "left": "AL30", "right": "GD30",
                    "signal": "SELL AL30 / BUY GD30" if z1 > 0 else "BUY AL30 / SELL GD30",
                    "ratio": round(r1, 8), "zscore": round(z1, 4), "edge_bps": round(max(abs(z1)*10 - 30, 0), 2),
                    "left_last": round(690 + i * 0.2, 6), "right_last": round(715 + i * 0.18, 6),
                    "left_bid": round(689.5 + i * 0.2, 6), "right_bid": round(714.5 + i * 0.18, 6),
                    "left_ask": round(690.5 + i * 0.2, 6), "right_ask": round(715.5 + i * 0.18, 6),
                    "series_index": i, "eligible": True, "reject_reason": "",
                })
            if abs(z2) >= 1.5:
                oid += 1
                self.opportunity_log.append({
                    "id": oid, "time": ts, "pair": "AL30D/GD30D", "left": "AL30D", "right": "GD30D",
                    "signal": "SELL AL30D / BUY GD30D" if z2 > 0 else "BUY AL30D / SELL GD30D",
                    "ratio": round(r2, 8), "zscore": round(z2, 4), "edge_bps": round(max(abs(z2)*10 - 30, 0), 2),
                    "left_last": round(0.57 + i * 0.0002, 6), "right_last": round(0.59 + i * 0.0002, 6),
                    "left_bid": round(0.569 + i * 0.0002, 6), "right_bid": round(0.589 + i * 0.0002, 6),
                    "left_ask": round(0.571 + i * 0.0002, 6), "right_ask": round(0.591 + i * 0.0002, 6),
                    "series_index": i, "eligible": True, "reject_reason": "",
                })

    def _quote(self, symbol: str, last: float, volume: float) -> InstrumentQuote:
        spread = max(last * 0.0015, 0.001)
        return InstrumentQuote(symbol=symbol, bid=last - spread / 2, ask=last + spread / 2, last=last, volume=volume, ts=datetime.now(UTC))

    def build_state(self) -> DeskState:
        quotes = {
            "AL30": self._quote("AL30", 697.0935880020878, 1390),
            "GD30": self._quote("GD30", 722.5055114156904, 1395),
            "AL30D": self._quote("AL30D", 0.5748560324031948, 1400),
            "GD30D": self._quote("GD30D", 0.5952589289056065, 1405),
            "S31L6": self._quote("S31L6", 141.87451042072743, 1410),
            "DLR": self._quote("DLR", 1286.5168999281375, 1415),
            "SOJ": self._quote("SOJ", 311723.25605110364, 1420),
            "CEDX": self._quote("CEDX", 18384.44507333695, 1425),
        }
        ratios = {k: v[-1] for k, v in self.ratio_history.items()}
        zscores = {k: v[-1] for k, v in self.zscore_history.items()}
        pair_signals = [
            PairSignal(left="AL30", right="GD30", ratio=ratios["AL30/GD30"], zscore=zscores["AL30/GD30"], signal="NO TRADE", edge_bps=0.0, confidence=min(abs(zscores["AL30/GD30"])/3,1.0)),
            PairSignal(left="AL30D", right="GD30D", ratio=ratios["AL30D/GD30D"], zscore=zscores["AL30D/GD30D"], signal="NO TRADE", edge_bps=0.0, confidence=min(abs(zscores["AL30D/GD30D"])/3,1.0)),
        ]
        hedge_plans = [
            HedgePlan(instrument="DLR", contracts=3, side="SELL", notional=3859550.699784412, rationale="Cobertura de exposición dólar / sintético."),
            HedgePlan(instrument="SOJ", contracts=0, side="SELL", notional=0.0, rationale="Cobertura táctica de exposición agrícola."),
            HedgePlan(instrument="CEDX", contracts=7, side="SELL", notional=1286911.1551335864, rationale="Cobertura índice equity/CEDEAR táctica."),
        ]
        risk_report = RiskReport(capital=25_000_000.0, max_risk_per_trade=125_000.0, max_daily_risk=375_000.0, current_gross_exposure=29_500_000.0, exposure_limit=50_000_000.0, utilization_pct=0.59, status="GREEN")
        notes = [
            "El dashboard corre con market data simulada para validación offline.",
            "La capa de scanner, señales, hedge y riesgo ya está desacoplada del proveedor.",
            "El adaptador real de broker se puede conectar sin tocar la UI.",
            f"Oportunidades simuladas registradas: {len(self.opportunity_log)}",
        ]
        return DeskState(mode="SIM", quotes=quotes, ratios=ratios, zscores=zscores, pair_signals=pair_signals, hedge_plans=hedge_plans, risk_report=risk_report, pnl_series=self.pnl_series, timestamps=self.series_timestamps[-60:], notes=notes, ratio_history=self.ratio_history, zscore_history=self.zscore_history, series_timestamps=self.series_timestamps, opportunity_log=self.opportunity_log)
