from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class Trade:
    entry_date: str
    exit_date: str
    side: str
    entry_z: float
    exit_z: float
    entry_ratio: float
    exit_ratio: float
    pnl_ratio: float
    pnl_money: float


class BacktestEngine:
    PAIR_NAME = "AL30/GD30"

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def load_data(self) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, ratio, zscore_20, left_close, right_close, spread
                FROM historical_pair_metrics
                WHERE pair_name = ?
                ORDER BY trade_date ASC
                """,
                (self.PAIR_NAME,),
            ).fetchall()

        return [dict(r) for r in rows]

    def run_backtest(
        self,
        entry_z: float = 2.0,
        exit_z: float = 0.0,
        cost: float = 0.003,
        capital: float = 1_000_000.0,
    ) -> Dict[str, Any]:
        data = self.load_data()

        if not data:
            return {
                "ok": True,
                "pair_name": self.PAIR_NAME,
                "params": {
                    "entry_z": entry_z,
                    "exit_z": exit_z,
                    "cost": cost,
                    "capital": capital,
                },
                "trades": [],
                "equity_curve": [],
                "metrics": {
                    "total_pnl": 0.0,
                    "total_return_pct": 0.0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown": 0.0,
                    "avg_trade": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                },
            }

        position = None
        trades: List[Trade] = []
        equity_curve: List[Dict[str, Any]] = []
        running_pnl = 0.0

        for row in data:
            z = row["zscore_20"]
            ratio = float(row["ratio"])
            trade_date = row["trade_date"]

            if z is not None:
                z = float(z)

            if position is None and z is not None:
                if z >= entry_z:
                    position = {
                        "side": "SELL",
                        "entry_date": trade_date,
                        "entry_z": z,
                        "entry_ratio": ratio,
                    }
                elif z <= -entry_z:
                    position = {
                        "side": "BUY",
                        "entry_date": trade_date,
                        "entry_z": z,
                        "entry_ratio": ratio,
                    }

            elif position is not None and z is not None:
                should_exit = False

                if position["side"] == "SELL" and z <= exit_z:
                    should_exit = True
                elif position["side"] == "BUY" and z >= -exit_z:
                    should_exit = True

                if should_exit:
                    if position["side"] == "SELL":
                        pnl_ratio = position["entry_ratio"] - ratio
                    else:
                        pnl_ratio = ratio - position["entry_ratio"]

                    pnl_ratio -= cost
                    pnl_money = pnl_ratio * capital
                    running_pnl += pnl_money

                    trades.append(
                        Trade(
                            entry_date=position["entry_date"],
                            exit_date=trade_date,
                            side=position["side"],
                            entry_z=float(position["entry_z"]),
                            exit_z=float(z),
                            entry_ratio=float(position["entry_ratio"]),
                            exit_ratio=ratio,
                            pnl_ratio=pnl_ratio,
                            pnl_money=pnl_money,
                        )
                    )
                    position = None

            equity_curve.append(
                {
                    "date": trade_date,
                    "equity": running_pnl,
                    "zscore_20": z,
                    "ratio": ratio,
                }
            )

        return {
            "ok": True,
            "pair_name": self.PAIR_NAME,
            "params": {
                "entry_z": entry_z,
                "exit_z": exit_z,
                "cost": cost,
                "capital": capital,
            },
            "trades": [t.__dict__ for t in trades],
            "equity_curve": equity_curve,
            "metrics": self._metrics(trades, equity_curve, capital),
        }

    def _metrics(
        self,
        trades: List[Trade],
        equity_curve: List[Dict[str, Any]],
        capital: float,
    ) -> Dict[str, Any]:
        if not trades:
            return {
                "total_pnl": 0.0,
                "total_return_pct": 0.0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "avg_trade": 0.0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
            }

        wins = [t for t in trades if t.pnl_money > 0]
        losses = [t for t in trades if t.pnl_money <= 0]

        total_pnl = sum(t.pnl_money for t in trades)
        total_return_pct = (total_pnl / capital) * 100 if capital else 0.0
        win_rate = len(wins) / len(trades) if trades else 0.0

        gross_profit = sum(t.pnl_money for t in wins)
        gross_loss = abs(sum(t.pnl_money for t in losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

        peak = 0.0
        max_dd = 0.0
        for point in equity_curve:
            equity = float(point["equity"])
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        avg_trade = total_pnl / len(trades) if trades else 0.0

        return {
            "total_pnl": total_pnl,
            "total_return_pct": total_return_pct,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_dd,
            "avg_trade": avg_trade,
            "trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
        }

    def optimize(
        self,
        cost: float = 0.003,
        capital: float = 1_000_000.0,
    ) -> Dict[str, Any]:
        results = []

        for entry in [1.5, 2.0, 2.5, 3.0]:
            for exit_ in [0.0, 0.5, 1.0]:
                bt = self.run_backtest(
                    entry_z=entry,
                    exit_z=exit_,
                    cost=cost,
                    capital=capital,
                )
                m = bt["metrics"]
                results.append(
                    {
                        "entry_z": entry,
                        "exit_z": exit_,
                        "total_pnl": m["total_pnl"],
                        "total_return_pct": m["total_return_pct"],
                        "profit_factor": m["profit_factor"],
                        "win_rate": m["win_rate"],
                        "max_drawdown": m["max_drawdown"],
                        "trades": m["trades"],
                        "avg_trade": m["avg_trade"],
                    }
                )

        results.sort(key=lambda x: x["total_pnl"], reverse=True)

        return {
            "ok": True,
            "pair_name": self.PAIR_NAME,
            "results": results,
            "best": results[0] if results else None,
        }