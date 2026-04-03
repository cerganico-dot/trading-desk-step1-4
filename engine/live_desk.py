from __future__ import annotations

import os
import random
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Tuple

from .alerts import AlertManager
from .models import DeskState, HedgePlan, InstrumentQuote, PairSignal, RiskReport
from .paper_trader import PaperTrader
from .persistence import SQLiteStore
from .scanner import RatioScanner
from .signal_filter import FilterConfig, SignalFilter

TEST_PERTURB = os.getenv("TEST_PERTURB", "0") == "1"


class LiveDesk:
    def __init__(self, provider: Any, capital: float, pairs: List[Tuple[str, str]], window: int = 20) -> None:
        self.provider = provider
        self.capital = float(capital)
        self.pairs = list(pairs)
        self.window = int(window)
        self.scanner = RatioScanner(self.pairs, window=self.window)
        self.filter = SignalFilter(FilterConfig())
        self.alerts = AlertManager()
        self.paper = PaperTrader(capital=self.capital)
        self.store = SQLiteStore(os.getenv("SQLITE_PATH", "data/trading_desk.db"))
        self.pnl_series: List[float] = []
        self.timestamps: List[str] = []
        self.last_quotes: Dict[str, InstrumentQuote] = {}
        self.series_timestamps: List[str] = []
        self.ratio_history: Dict[str, List[float]] = {f"{l}/{r}": [] for l, r in self.pairs}
        self.zscore_history: Dict[str, List[float]] = {f"{l}/{r}": [] for l, r in self.pairs}
        self.opportunity_log: List[dict] = []
        self._log_id = 0

    def _perturb(self, quotes: Dict[str, InstrumentQuote]) -> Dict[str, InstrumentQuote]:
        if not TEST_PERTURB:
            return quotes
        out: Dict[str, InstrumentQuote] = {}
        for sym, q in quotes.items():
            noise = random.uniform(-0.002, 0.002)
            mid = q.mid * (1 + noise)
            out[sym] = InstrumentQuote(symbol=sym, bid=mid * 0.999, ask=mid * 1.001, last=mid, volume=q.volume, ts=q.ts)
        return out

    def _append_histories(self, ts_label: str, ratios: Dict[str, float], zscores: Dict[str, float]) -> None:
        self.series_timestamps.append(ts_label)
        self.series_timestamps = self.series_timestamps[-200:]
        for pair_name in self.ratio_history:
            self.ratio_history[pair_name].append(float(ratios.get(pair_name, 0.0)))
            self.ratio_history[pair_name] = self.ratio_history[pair_name][-200:]
        for pair_name in self.zscore_history:
            self.zscore_history[pair_name].append(float(zscores.get(pair_name, 0.0)))
            self.zscore_history[pair_name] = self.zscore_history[pair_name][-200:]

    def _append_opportunity_log(self, ts_label: str, signals: List[PairSignal], quotes: Dict[str, InstrumentQuote]) -> List[dict]:
        new_items: List[dict] = []
        for s in signals:
            if s.signal == "NO TRADE":
                continue
            self._log_id += 1
            lq = quotes.get(s.left)
            rq = quotes.get(s.right)
            item = {
                "id": self._log_id,
                "time": ts_label,
                "pair": f"{s.left}/{s.right}",
                "left": s.left,
                "right": s.right,
                "signal": s.signal,
                "ratio": round(float(s.ratio), 8),
                "zscore": round(float(s.zscore), 4),
                "edge_bps": round(float(s.edge_bps), 2),
                "left_last": round(float(lq.last), 6) if lq else None,
                "right_last": round(float(rq.last), 6) if rq else None,
                "left_bid": round(float(lq.bid), 6) if lq else None,
                "right_bid": round(float(rq.bid), 6) if rq else None,
                "left_ask": round(float(lq.ask), 6) if lq else None,
                "right_ask": round(float(rq.ask), 6) if rq else None,
                "series_index": len(self.series_timestamps) - 1,
                "eligible": s.eligible,
                "reject_reason": s.reject_reason,
            }
            self.opportunity_log.append(item)
            new_items.append(item)
        self.opportunity_log = self.opportunity_log[-300:]
        return new_items

    def build_state(self) -> DeskState:
        quotes = self._perturb(self.provider.snapshot())
        if not quotes:
            raise RuntimeError("El proveedor no devolvió cotizaciones válidas.")
        self.last_quotes = quotes
        ratios = self.scanner.update(quotes)
        zscores = self.scanner.zscores()
        pair_signals = self.filter.build_signals(self.pairs, ratios, zscores, quotes)

        now_str = datetime.utcnow().strftime("%H:%M:%S")
        self.timestamps.append(now_str)
        self.timestamps = self.timestamps[-60:]
        self._append_histories(now_str, ratios, zscores)
        new_log_items = self._append_opportunity_log(now_str, pair_signals, quotes)

        alert_events = self.alerts.process(pair_signals)
        paper_events = self.paper.process(pair_signals)
        open_positions = self.paper.open_positions()

        pnl_value = sum((p.get("pnl_currency") or 0.0) for p in self.paper.events[-20:])
        self.pnl_series.append(round(pnl_value, 2))
        self.pnl_series = self.pnl_series[-60:]

        max_risk_per_trade = self.capital * 0.005
        max_daily_risk = self.capital * 0.015
        current_gross_exposure = self.capital * (1.0 + 0.05 * len(open_positions))
        exposure_limit = self.capital * 2.0
        utilization_pct = 0.0 if exposure_limit <= 0 else current_gross_exposure / exposure_limit
        risk_report = RiskReport(
            capital=self.capital,
            max_risk_per_trade=max_risk_per_trade,
            max_daily_risk=max_daily_risk,
            current_gross_exposure=current_gross_exposure,
            exposure_limit=exposure_limit,
            utilization_pct=utilization_pct,
            status="GREEN" if utilization_pct < 0.8 else "YELLOW",
        )

        hedge_plans = []
        if "DLR" in quotes:
            hedge_plans.append(HedgePlan(instrument="DLR", contracts=1, side="SELL", notional=quotes["DLR"].last * 1000, rationale="Cobertura demo."))

        self.store.save_snapshot(now_str, ratios, zscores)
        if new_log_items:
            self.store.save_opportunities(new_log_items)
        if alert_events:
            self.store.save_alerts(alert_events)
        if paper_events:
            self.store.save_paper_events(paper_events)

        notes = [
            "Paso 1 activo: alertas en consola y Telegram opcional.",
            "Paso 2 activo: persistencia SQLite de oportunidades, alertas y paper events.",
            "Paso 3 activo: filtro profesional por z-score, volumen, spread y edge neto de costos.",
            "Paso 4 activo: paper trading con apertura/cierre por reversión a la media.",
            f"Observaciones acumuladas: {max((len(v) for v in self.scanner.history.values()), default=0)}",
            f"Open paper positions: {len(open_positions)}",
        ]
        if TEST_PERTURB:
            notes.append("TEST_PERTURB=1 activo: se agregan micro-variaciones sintéticas para probar señales fuera de rueda.")

        return DeskState(
            mode="LIVE",
            quotes=quotes,
            ratios=ratios,
            zscores=zscores,
            pair_signals=pair_signals,
            hedge_plans=hedge_plans,
            risk_report=risk_report,
            pnl_series=self.pnl_series,
            timestamps=self.timestamps,
            notes=notes,
            ratio_history=self.ratio_history,
            zscore_history=self.zscore_history,
            series_timestamps=self.series_timestamps,
            opportunity_log=self.opportunity_log,
            paper_positions=open_positions,
            paper_events=self.paper.events[-200:],
            alerts_sent=self.alerts.sent_events[-200:],
        )
