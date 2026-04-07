from __future__ import annotations

import os
import random
from datetime import datetime
from typing import Any, Dict, List, Tuple

from .alerts import AlertManager
from .models import DeskState, HedgePlan, InstrumentQuote, PairSignal, RiskReport
from .paper_trader import PaperTrader
from .persistence import SQLiteStore
from .scanner import RatioScanner
from .signal_filter import FilterConfig, SignalFilter

TEST_PERTURB = os.getenv("TEST_PERTURB", "0") == "1"

PAIR_CONFIGS: Dict[str, Dict[str, float]] = {
    "AL30/AL30D": {"entry": 1.8, "exit": 0.6, "weight": 1.0},
    "GD30/GD30D": {"entry": 1.8, "exit": 0.6, "weight": 1.0},
    "AL30/GD30": {"entry": 1.6, "exit": 0.5, "weight": 0.9},
    "AL35/GD35": {"entry": 1.6, "exit": 0.5, "weight": 0.7},
    "AL30/AL35": {"entry": 2.0, "exit": 0.7, "weight": 0.6},
    "GD30/GD35": {"entry": 2.0, "exit": 0.7, "weight": 0.6},
    "AL35/AL35D": {"entry": 2.0, "exit": 0.7, "weight": 0.4},
    "GD35/GD35D": {"entry": 2.0, "exit": 0.7, "weight": 0.4},
    "AL41/GD41": {"entry": 2.2, "exit": 0.8, "weight": 0.3},
}
DEFAULT_PAIR_CONFIG = {"entry": 1.6, "exit": 0.5, "weight": 1.0}


class LiveDesk:
    def __init__(self, provider: Any, capital: float, pairs: List[Tuple[str, str]], window: int = 20) -> None:
        self.provider = provider
        self.capital = float(capital)
        self.window = int(window)

        clean_pairs: List[Tuple[str, str]] = []
        seen: set[str] = set()
        for left, right in pairs:
            key = f"{left}/{right}"
            if key in seen:
                continue
            seen.add(key)
            clean_pairs.append((left, right))

        self.pairs = clean_pairs
        self.pair_names = [f"{left}/{right}" for left, right in self.pairs]

        self.scanner = RatioScanner(self.pairs, window=self.window)
        self.filter = SignalFilter(FilterConfig())
        self.alerts = AlertManager()
        self.paper = PaperTrader(capital=self.capital)
        self.store = SQLiteStore(os.getenv("SQLITE_PATH", "data/trading_desk.db"))

        self.pnl_series: List[float] = []
        self.timestamps: List[str] = []
        self.last_quotes: Dict[str, InstrumentQuote] = {}

        self.series_timestamps: List[str] = []
        self.ratio_history: Dict[str, List[float]] = {pair_name: [] for pair_name in self.pair_names}
        self.zscore_history: Dict[str, List[float]] = {pair_name: [] for pair_name in self.pair_names}

        self.opportunity_log: List[dict] = []
        self._log_id = 0

    def _pair_cfg(self, left: str, right: str) -> Dict[str, float]:
        return PAIR_CONFIGS.get(f"{left}/{right}", DEFAULT_PAIR_CONFIG)

    def _perturb(self, quotes: Dict[str, InstrumentQuote]) -> Dict[str, InstrumentQuote]:
        if not TEST_PERTURB:
            return quotes

        out: Dict[str, InstrumentQuote] = {}
        for sym, q in quotes.items():
            noise = random.uniform(-0.002, 0.002)
            mid = q.mid * (1 + noise)
            out[sym] = InstrumentQuote(
                symbol=sym,
                bid=mid * 0.999,
                ask=mid * 1.001,
                last=mid,
                volume=q.volume,
                ts=q.ts,
            )
        return out

    def _apply_pair_policy(self, signals: List[PairSignal]) -> List[PairSignal]:
        out: List[PairSignal] = []

        for s in signals:
            cfg = self._pair_cfg(s.left, s.right)
            entry = float(cfg["entry"])
            weight = float(cfg["weight"])
            z = float(getattr(s, "zscore", 0.0) or 0.0)

            signal = "NO TRADE"
            if abs(z) >= entry:
                if z > 0:
                    signal = f"SELL {s.left} / BUY {s.right}"
                else:
                    signal = f"BUY {s.left} / SELL {s.right}"

            s.signal = signal
            s.edge_bps = round(abs(z) * 10.0 * weight, 2)
            s.confidence = min(abs(z) / max(entry, 0.01), 1.0)

            if hasattr(s, "eligible") and signal == "NO TRADE":
                s.eligible = False
                if hasattr(s, "reject_reason"):
                    s.reject_reason = f"|z|<{entry:.2f}"

            out.append(s)

        return out

    def _append_histories(self, ts_label: str, ratios: Dict[str, float], zscores: Dict[str, float]) -> None:
        self.series_timestamps.append(ts_label)
        self.series_timestamps = self.series_timestamps[-200:]

        for pair_name in self.ratio_history:
            self.ratio_history[pair_name].append(float(ratios.get(pair_name, 0.0) or 0.0))
            self.ratio_history[pair_name] = self.ratio_history[pair_name][-200:]

        for pair_name in self.zscore_history:
            self.zscore_history[pair_name].append(float(zscores.get(pair_name, 0.0) or 0.0))
            self.zscore_history[pair_name] = self.zscore_history[pair_name][-200:]

    def _append_opportunity_log(self, ts_label: str, signals: List[PairSignal], quotes: Dict[str, InstrumentQuote]) -> List[dict]:
        new_items: List[dict] = []

        for s in signals:
            if s.signal == "NO TRADE":
                continue

            self._log_id += 1
            lq = quotes.get(s.left)
            rq = quotes.get(s.right)
            cfg = self._pair_cfg(s.left, s.right)

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
                "entry_threshold": float(cfg["entry"]),
                "exit_threshold": float(cfg["exit"]),
                "weight": float(cfg["weight"]),
                "left_last": round(float(lq.last), 6) if lq else None,
                "right_last": round(float(rq.last), 6) if rq else None,
                "left_bid": round(float(lq.bid), 6) if lq else None,
                "right_bid": round(float(rq.bid), 6) if rq else None,
                "left_ask": round(float(lq.ask), 6) if lq else None,
                "right_ask": round(float(rq.ask), 6) if rq else None,
                "series_index": len(self.series_timestamps) - 1,
                "eligible": getattr(s, "eligible", True),
                "reject_reason": getattr(s, "reject_reason", ""),
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

        raw_signals = self.filter.build_signals(self.pairs, ratios, zscores, quotes)
        pair_signals = self._apply_pair_policy(raw_signals)

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

        hedge_plans: List[HedgePlan] = []
        if "DLR" in quotes:
            hedge_plans.append(
                HedgePlan(
                    instrument="DLR",
                    contracts=1,
                    side="SELL",
                    notional=quotes["DLR"].last * 1000,
                    rationale="Cobertura demo.",
                )
            )

        self.store.save_snapshot(now_str, ratios, zscores)
        if new_log_items:
            self.store.save_opportunities(new_log_items)
        if alert_events:
            self.store.save_alerts(alert_events)
        if paper_events:
            self.store.save_paper_events(paper_events)

        notes = [
            "Modo LIVE con configuración dinámica de pares.",
            "Persistencia SQLite activa para snapshots, oportunidades, alertas y paper events.",
            "Los pares manuales arrancan con histórico vacío y necesitan warm-up.",
            f"Pares activos: {', '.join(self.pair_names)}",
            f"Observaciones acumuladas: {max((len(v) for v in self.scanner.history.values()), default=0)}",
            f"Open paper positions: {len(open_positions)}",
        ]
        if TEST_PERTURB:
            notes.append("TEST_PERTURB=1 activo.")

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