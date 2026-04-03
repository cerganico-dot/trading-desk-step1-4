from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import sin
from typing import Dict, List

from .models import HedgePlan, InstrumentQuote, PairSignal, RiskReport


@dataclass
class DeskState:
    mode: str
    quotes: Dict[str, InstrumentQuote]
    ratios: Dict[str, float]
    zscores: Dict[str, float]
    pair_signals: List[PairSignal]
    hedge_plans: List[HedgePlan]
    risk_report: RiskReport
    pnl_series: List[float]
    timestamps: List[str]
    notes: List[str]
    ratio_history: Dict[str, List[float]]
    zscore_history: Dict[str, List[float]]
    series_timestamps: List[str]
    opportunity_log: List[dict]


class DeskSimulator:
    def __init__(self) -> None:
        self.max_points = 120
        self.step = 0
        self.base_time = datetime.now(UTC).replace(second=0, microsecond=0)

        self.series_timestamps: List[str] = []
        self.ratio_history: Dict[str, List[float]] = {
            "AL30/GD30": [],
            "AL30D/GD30D": [],
        }
        self.zscore_history: Dict[str, List[float]] = {
            "AL30/GD30": [],
            "AL30D/GD30D": [],
        }
        self.pnl_series: List[float] = []
        self.opportunity_log: List[dict] = []
        self._log_id = 0

        # warm-up inicial para que el dashboard ya arranque con historia
        for _ in range(40):
            self._advance()

    def _quote(self, symbol: str, last: float, volume: float) -> InstrumentQuote:
        spread = max(last * 0.0015, 0.001)
        return InstrumentQuote(
            symbol=symbol,
            bid=last - spread / 2,
            ask=last + spread / 2,
            last=last,
            volume=volume,
            ts=datetime.now(UTC),
        )

    def _append_opportunity(
        self,
        ts: str,
        pair: str,
        left: str,
        right: str,
        signal: str,
        ratio: float,
        zscore: float,
        left_last: float,
        right_last: float,
        left_bid: float,
        right_bid: float,
        left_ask: float,
        right_ask: float,
        series_index: int,
    ) -> None:
        self._log_id += 1
        self.opportunity_log.append(
            {
                "id": self._log_id,
                "time": ts,
                "pair": pair,
                "left": left,
                "right": right,
                "signal": signal,
                "ratio": round(ratio, 8),
                "zscore": round(zscore, 4),
                "edge_bps": round(abs(zscore) * 10.0, 2),
                "left_last": round(left_last, 6),
                "right_last": round(right_last, 6),
                "left_bid": round(left_bid, 6),
                "right_bid": round(right_bid, 6),
                "left_ask": round(left_ask, 6),
                "right_ask": round(right_ask, 6),
                "series_index": series_index,
            }
        )
        self.opportunity_log = self.opportunity_log[-300:]

    def _advance(self) -> None:
        i = self.step
        now = self.base_time + timedelta(minutes=i)
        ts = now.strftime("%H:%M")

        # Series dinámicas
        ratio_1 = 0.965 + 0.007 * sin(i / 4.0)
        ratio_2 = 0.966 + 0.006 * sin(i / 5.0 + 0.8)
        z_1 = 2.2 * sin(i / 4.0)
        z_2 = 1.8 * sin(i / 5.0 + 0.8)

        self.series_timestamps.append(ts)
        self.ratio_history["AL30/GD30"].append(ratio_1)
        self.ratio_history["AL30D/GD30D"].append(ratio_2)
        self.zscore_history["AL30/GD30"].append(z_1)
        self.zscore_history["AL30D/GD30D"].append(z_2)
        self.pnl_series.append(round((abs(z_1) + abs(z_2)) * 1000.0, 2))

        # recorte
        self.series_timestamps = self.series_timestamps[-self.max_points :]
        self.pnl_series = self.pnl_series[-self.max_points :]
        for k in self.ratio_history:
            self.ratio_history[k] = self.ratio_history[k][-self.max_points :]
        for k in self.zscore_history:
            self.zscore_history[k] = self.zscore_history[k][-self.max_points :]

        series_index = len(self.series_timestamps) - 1

        # precios sintéticos consistentes con los ratios
        gd30_last = 720.0 + 2.0 * sin(i / 6.0)
        al30_last = gd30_last * ratio_1

        gd30d_last = 0.595 + 0.003 * sin(i / 7.0)
        al30d_last = gd30d_last * ratio_2

        if abs(z_1) >= 1.5:
            self._append_opportunity(
                ts=ts,
                pair="AL30/GD30",
                left="AL30",
                right="GD30",
                signal="SELL AL30 / BUY GD30" if z_1 > 0 else "BUY AL30 / SELL GD30",
                ratio=ratio_1,
                zscore=z_1,
                left_last=al30_last,
                right_last=gd30_last,
                left_bid=al30_last * 0.999,
                right_bid=gd30_last * 0.999,
                left_ask=al30_last * 1.001,
                right_ask=gd30_last * 1.001,
                series_index=series_index,
            )

        if abs(z_2) >= 1.5:
            self._append_opportunity(
                ts=ts,
                pair="AL30D/GD30D",
                left="AL30D",
                right="GD30D",
                signal="SELL AL30D / BUY GD30D" if z_2 > 0 else "BUY AL30D / SELL GD30D",
                ratio=ratio_2,
                zscore=z_2,
                left_last=al30d_last,
                right_last=gd30d_last,
                left_bid=al30d_last * 0.999,
                right_bid=gd30d_last * 0.999,
                left_ask=al30d_last * 1.001,
                right_ask=gd30d_last * 1.001,
                series_index=series_index,
            )

        self.step += 1

    def build_state(self) -> DeskState:
        # cada request mueve la simulación
        self._advance()

        al30 = self.ratio_history["AL30/GD30"][-1] * (720.0 + 2.0 * sin(self.step / 6.0))
        gd30 = 720.0 + 2.0 * sin(self.step / 6.0)

        gd30d = 0.595 + 0.003 * sin(self.step / 7.0)
        al30d = self.ratio_history["AL30D/GD30D"][-1] * gd30d

        quotes = {
            "AL30": self._quote("AL30", al30, 1390 + self.step),
            "GD30": self._quote("GD30", gd30, 1395 + self.step),
            "AL30D": self._quote("AL30D", al30d, 1400 + self.step),
            "GD30D": self._quote("GD30D", gd30d, 1405 + self.step),
            "S31L6": self._quote("S31L6", 141.8 + 0.2 * sin(self.step / 8.0), 1410 + self.step),
            "DLR": self._quote("DLR", 1286.0 + 5 * sin(self.step / 9.0), 1415 + self.step),
            "SOJ": self._quote("SOJ", 311700 + 500 * sin(self.step / 10.0), 1420 + self.step),
            "CEDX": self._quote("CEDX", 18380 + 50 * sin(self.step / 11.0), 1425 + self.step),
        }

        ratios = {
            "AL30/GD30": self.ratio_history["AL30/GD30"][-1],
            "AL30D/GD30D": self.ratio_history["AL30D/GD30D"][-1],
        }

        zscores = {
            "AL30/GD30": self.zscore_history["AL30/GD30"][-1],
            "AL30D/GD30D": self.zscore_history["AL30D/GD30D"][-1],
        }

        pair_signals = [
            PairSignal(
                left="AL30",
                right="GD30",
                ratio=ratios["AL30/GD30"],
                zscore=zscores["AL30/GD30"],
                signal="SELL AL30 / BUY GD30" if zscores["AL30/GD30"] > 1.5 else (
                    "BUY AL30 / SELL GD30" if zscores["AL30/GD30"] < -1.5 else "NO TRADE"
                ),
                edge_bps=abs(zscores["AL30/GD30"]) * 10 if abs(zscores["AL30/GD30"]) >= 1.5 else 0.0,
                confidence=min(abs(zscores["AL30/GD30"]) / 3, 1.0),
            ),
            PairSignal(
                left="AL30D",
                right="GD30D",
                ratio=ratios["AL30D/GD30D"],
                zscore=zscores["AL30D/GD30D"],
                signal="SELL AL30D / BUY GD30D" if zscores["AL30D/GD30D"] > 1.5 else (
                    "BUY AL30D / SELL GD30D" if zscores["AL30D/GD30D"] < -1.5 else "NO TRADE"
                ),
                edge_bps=abs(zscores["AL30D/GD30D"]) * 10 if abs(zscores["AL30D/GD30D"]) >= 1.5 else 0.0,
                confidence=min(abs(zscores["AL30D/GD30D"]) / 3, 1.0),
            ),
        ]

        hedge_plans = [
            HedgePlan(
                instrument="DLR",
                contracts=3,
                side="SELL",
                notional=3859550.699784412,
                rationale="Cobertura de exposición dólar / sintético.",
            ),
            HedgePlan(
                instrument="SOJ",
                contracts=0,
                side="SELL",
                notional=0.0,
                rationale="Cobertura táctica de exposición agrícola.",
            ),
            HedgePlan(
                instrument="CEDX",
                contracts=7,
                side="SELL",
                notional=1286911.1551335864,
                rationale="Cobertura índice equity/CEDEAR táctica.",
            ),
        ]

        risk_report = RiskReport(
            capital=25_000_000.0,
            max_risk_per_trade=125_000.0,
            max_daily_risk=375_000.0,
            current_gross_exposure=29_500_000.0,
            exposure_limit=50_000_000.0,
            utilization_pct=0.59,
            status="GREEN",
        )

        notes = [
            "El dashboard corre con market data simulada para validación offline.",
            "La simulación avanza un paso en cada request para generar movimiento continuo.",
            "La capa de scanner, señales, hedge y riesgo ya está desacoplada del proveedor.",
            "El adaptador real de broker se puede conectar sin tocar la UI.",
            f"Oportunidades simuladas registradas: {len(self.opportunity_log)}",
        ]

        return DeskState(
            mode="SIM",
            quotes=quotes,
            ratios=ratios,
            zscores=zscores,
            pair_signals=pair_signals,
            hedge_plans=hedge_plans,
            risk_report=risk_report,
            pnl_series=self.pnl_series,
            timestamps=self.series_timestamps[-60:],
            notes=notes,
            ratio_history=self.ratio_history,
            zscore_history=self.zscore_history,
            series_timestamps=self.series_timestamps,
            opportunity_log=self.opportunity_log,
        )