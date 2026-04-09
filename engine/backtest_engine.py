from __future__ import annotations
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Dict


@dataclass
class Trade:
    entry_date: str
    exit_date: str
    side: str
    entry_z: float
    exit_z: float
    entry_ratio: float
    exit_ratio: float
    pnl: float


class BacktestEngine:

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def load_data(self):
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT trade_date, ratio, zscore_20
                FROM historical_pair_metrics
                WHERE pair_name = 'AL30/GD30'
                ORDER BY trade_date ASC
            """).fetchall()

        return [dict(r) for r in rows]

    def run_backtest(
        self,
        entry_z: float = 2.0,
        exit_z: float = 0.0,
        cost: float = 0.003,
        capital: float = 1_000_000
    ):

        data = self.load_data()

        position = None
        trades: List[Trade] = []
        equity = []
        pnl_total = 0

        for i, row in enumerate(data):
            z = row["zscore_20"]
            ratio = row["ratio"]
            date = row["trade_date"]

            if z is None:
                equity.append(pnl_total)
                continue

            # ENTRY
            if position is None:
                if z > entry_z:
                    position = {
                        "side": "SELL",
                        "entry_z": z,
                        "entry_ratio": ratio,
                        "entry_date": date
                    }
                elif z < -entry_z:
                    position = {
                        "side": "BUY",
                        "entry_z": z,
                        "entry_ratio": ratio,
                        "entry_date": date
                    }

            # EXIT
            else:
                exit_condition = (
                    (position["side"] == "SELL" and z <= exit_z) or
                    (position["side"] == "BUY" and z >= -exit_z)
                )

                if exit_condition:
                    pnl = (
                        (position["entry_ratio"] - ratio)
                        if position["side"] == "SELL"
                        else (ratio - position["entry_ratio"])
                    )

                    pnl -= cost

                    pnl_money = pnl * capital

                    pnl_total += pnl_money

                    trades.append(Trade(
                        entry_date=position["entry_date"],
                        exit_date=date,
                        side=position["side"],
                        entry_z=position["entry_z"],
                        exit_z=z,
                        entry_ratio=position["entry_ratio"],
                        exit_ratio=ratio,
                        pnl=pnl_money
                    ))

                    position = None

            equity.append(pnl_total)

        return self._metrics(trades, equity)

    def _metrics(self, trades: List[Trade], equity: List[float]):

        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl <= 0]

        total_pnl = sum(t.pnl for t in trades)

        win_rate = len(wins) / len(trades) if trades else 0

        profit_factor = (
            sum(t.pnl for t in wins) /
            abs(sum(t.pnl for t in losses))
            if losses else 0
        )

        max_dd = 0
        peak = 0

        for v in equity:
            if v > peak:
                peak = v
            dd = peak - v
            if dd > max_dd:
                max_dd = dd

        return {
            "trades": [t.__dict__ for t in trades],
            "equity": equity,
            "metrics": {
                "total_pnl": total_pnl,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "max_drawdown": max_dd,
                "trades": len(trades)
            }
        }

    def optimize(self):

        results = []

        for window in [10, 20, 30, 40]:
            for entry in [1.5, 2.0, 2.5, 3.0]:
                for exit in [0, 0.5, 1.0]:

                    r = self.run_backtest(entry_z=entry, exit_z=exit)

                    results.append({
                        "entry": entry,
                        "exit": exit,
                        "trades": r["metrics"]["trades"],
                        "pnl": r["metrics"]["total_pnl"],
                        "pf": r["metrics"]["profit_factor"],
                        "win_rate": r["metrics"]["win_rate"]
                    })

        results.sort(key=lambda x: x["pnl"], reverse=True)

        return results[:20]