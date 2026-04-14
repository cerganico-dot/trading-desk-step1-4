from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests


@dataclass
class DailyBar:
    symbol: str
    trade_date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    adjusted_close: float
    amount: float | None
    nominal_volume: float | None
    source: str = "IOL_PUBLIC_HTML"


class HistoricalPhase1Service:
    SYMBOLS = ("AL30", "GD30")
    PAIR_NAME = "AL30/GD30"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_tables(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_daily_bars (
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    adjusted_close REAL NOT NULL,
                    amount REAL,
                    nominal_volume REAL,
                    source TEXT NOT NULL,
                    inserted_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, trade_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_pair_metrics (
                    pair_name TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    left_symbol TEXT NOT NULL,
                    right_symbol TEXT NOT NULL,
                    left_close REAL NOT NULL,
                    right_close REAL NOT NULL,
                    ratio REAL NOT NULL,
                    spread REAL NOT NULL,
                    zscore_20 REAL,
                    inserted_at TEXT NOT NULL,
                    PRIMARY KEY (pair_name, trade_date)
                )
                """
            )
            conn.commit()

    @staticmethod
    def _to_float(raw: str) -> float:
        return float(raw.replace(",", ""))

    @staticmethod
    def _to_iso_date(mmddyyyy: str) -> str:
        return datetime.strptime(mmddyyyy, "%m/%d/%Y").date().isoformat()

    def _historical_url(self, symbol: str) -> str:
        return f"https://iol.invertironline.com/titulo/datoshistoricos?mercado=bcba&simbolo={symbol.lower()}"

    def _extract_text(self, html: str) -> str:
        text = re.sub(r"<script.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text

    def _parse_bars_from_html(self, symbol: str, html: str) -> list[DailyBar]:
        text = self._extract_text(html)

        pattern = re.compile(
            r"(\d{2}/\d{2}/\d{4})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})\s+"
            r"([0-9,]+\.\d{2})"
        )

        rows: dict[str, DailyBar] = {}
        for match in pattern.finditer(text):
            trade_date = self._to_iso_date(match.group(1))
            bar = DailyBar(
                symbol=symbol,
                trade_date=trade_date,
                open_price=self._to_float(match.group(2)),
                high_price=self._to_float(match.group(3)),
                low_price=self._to_float(match.group(4)),
                close_price=self._to_float(match.group(5)),
                adjusted_close=self._to_float(match.group(6)),
                amount=self._to_float(match.group(7)),
                nominal_volume=self._to_float(match.group(8)),
            )
            rows[trade_date] = bar

        return sorted(rows.values(), key=lambda x: x.trade_date)

    def fetch_public_history(self, symbol: str) -> list[DailyBar]:
        url = self._historical_url(symbol)
        response = requests.get(
            url,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            },
        )
        response.raise_for_status()

        bars = self._parse_bars_from_html(symbol=symbol, html=response.text)
        if not bars:
            raise RuntimeError(f"No se pudieron parsear datos históricos para {symbol}")
        return bars

    def upsert_bars(self, bars: list[DailyBar]) -> int:
        if not bars:
            return 0

        inserted_at = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT INTO historical_daily_bars (
                    symbol, trade_date, open_price, high_price, low_price, close_price,
                    adjusted_close, amount, nominal_volume, source, inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    open_price=excluded.open_price,
                    high_price=excluded.high_price,
                    low_price=excluded.low_price,
                    close_price=excluded.close_price,
                    adjusted_close=excluded.adjusted_close,
                    amount=excluded.amount,
                    nominal_volume=excluded.nominal_volume,
                    source=excluded.source,
                    inserted_at=excluded.inserted_at
                """,
                [
                    (
                        b.symbol,
                        b.trade_date,
                        b.open_price,
                        b.high_price,
                        b.low_price,
                        b.close_price,
                        b.adjusted_close,
                        b.amount,
                        b.nominal_volume,
                        b.source,
                        inserted_at,
                    )
                    for b in bars
                ],
            )
            conn.commit()
        return len(bars)

    def _load_bars(self, symbol: str, start_date: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT symbol, trade_date, open_price, high_price, low_price, close_price,
                   adjusted_close, amount, nominal_volume, source
            FROM historical_daily_bars
            WHERE symbol = ?
        """
        params: list[Any] = [symbol]

        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)

        query += " ORDER BY trade_date ASC"

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        return [dict(r) for r in rows]

    def _compute_pair_metrics(self, left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        right_map = {r["trade_date"]: r for r in right_rows}
        aligned: list[dict[str, Any]] = []

        for left in left_rows:
            right = right_map.get(left["trade_date"])
            if not right:
                continue

            left_close = float(left["close_price"])
            right_close = float(right["close_price"])
            if right_close == 0:
                continue

            aligned.append(
                {
                    "pair_name": self.PAIR_NAME,
                    "trade_date": left["trade_date"],
                    "left_symbol": "AL30",
                    "right_symbol": "GD30",
                    "left_close": left_close,
                    "right_close": right_close,
                    "ratio": left_close / right_close,
                    "spread": left_close - right_close,
                }
            )

        ratios = [row["ratio"] for row in aligned]
        for i, row in enumerate(aligned):
            if i < 19:
                row["zscore_20"] = None
                continue

            window = ratios[i - 19:i + 1]
            mean = sum(window) / 20.0
            variance = sum((x - mean) ** 2 for x in window) / 20.0
            std = math.sqrt(variance)

            row["zscore_20"] = 0.0 if std == 0 else (row["ratio"] - mean) / std

        return aligned

    def upsert_pair_metrics(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        inserted_at = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT INTO historical_pair_metrics (
                    pair_name, trade_date, left_symbol, right_symbol,
                    left_close, right_close, ratio, spread, zscore_20, inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pair_name, trade_date) DO UPDATE SET
                    left_close=excluded.left_close,
                    right_close=excluded.right_close,
                    ratio=excluded.ratio,
                    spread=excluded.spread,
                    zscore_20=excluded.zscore_20,
                    inserted_at=excluded.inserted_at
                """,
                [
                    (
                        row["pair_name"],
                        row["trade_date"],
                        row["left_symbol"],
                        row["right_symbol"],
                        row["left_close"],
                        row["right_close"],
                        row["ratio"],
                        row["spread"],
                        row["zscore_20"],
                        inserted_at,
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def bootstrap_phase1(self, years: int = 2) -> dict[str, Any]:
        self.ensure_tables()

        cutoff = (date.today() - timedelta(days=365 * years + 15)).isoformat()

        summary: dict[str, Any] = {
            "ok": True,
            "symbols": {},
            "pair_name": self.PAIR_NAME,
            "years_requested": years,
        }

        for symbol in self.SYMBOLS:
            fetched = self.fetch_public_history(symbol)
            filtered = [b for b in fetched if b.trade_date >= cutoff]
            self.upsert_bars(filtered)

            summary["symbols"][symbol] = {
                "rows_fetched": len(fetched),
                "rows_saved": len(filtered),
                "first_date": filtered[0].trade_date if filtered else None,
                "last_date": filtered[-1].trade_date if filtered else None,
                "source_url": self._historical_url(symbol),
            }

        left_rows = self._load_bars("AL30", start_date=cutoff)
        right_rows = self._load_bars("GD30", start_date=cutoff)
        pair_rows = self._compute_pair_metrics(left_rows, right_rows)
        self.upsert_pair_metrics(pair_rows)

        summary["pair_metrics"] = {
            "rows_saved": len(pair_rows),
            "first_date": pair_rows[0]["trade_date"] if pair_rows else None,
            "last_date": pair_rows[-1]["trade_date"] if pair_rows else None,
        }

        return summary

    def status(self) -> dict[str, Any]:
        self.ensure_tables()

        with self._conn() as conn:
            bars = conn.execute(
                """
                SELECT symbol, COUNT(*) AS cnt, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
                FROM historical_daily_bars
                WHERE symbol IN ('AL30', 'GD30')
                GROUP BY symbol
                ORDER BY symbol
                """
            ).fetchall()

            pair = conn.execute(
                """
                SELECT COUNT(*) AS cnt, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
                FROM historical_pair_metrics
                WHERE pair_name = ?
                """,
                (self.PAIR_NAME,),
            ).fetchone()

        return {
            "ok": True,
            "db_path": self.db_path,
            "symbols": [dict(r) for r in bars],
            "pair": dict(pair) if pair else {"cnt": 0, "first_date": None, "last_date": None},
        }

    def get_symbol_history(self, symbol: str, limit: int = 600) -> dict[str, Any]:
        self.ensure_tables()

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trade_date, open_price, high_price, low_price, close_price,
                       adjusted_close, amount, nominal_volume, source
                FROM historical_daily_bars
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (symbol, limit),
            ).fetchall()

        out = [dict(r) for r in rows]
        out.reverse()

        return {
            "ok": True,
            "symbol": symbol,
            "rows": out,
            "count": len(out),
        }

def get_pair_history(self, limit=2000):
    conn = self._get_conn()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT 
            l.trade_date,
            l.close as left_close,
            r.close as right_close,
            (l.close / r.close) as ratio,
            (l.close - r.close) as spread
        FROM daily_prices l
        JOIN daily_prices r 
            ON l.trade_date = r.trade_date
        WHERE l.symbol = 'AL30'
          AND r.symbol = 'GD30'
        ORDER BY l.trade_date ASC
        LIMIT ?
    """, (limit,)).fetchall()

    result = []
    ratios = []

    for row in rows:
        ratio = row["ratio"]
        ratios.append(ratio)

        # zscore rolling 20
        if len(ratios) >= 20:
            window = ratios[-20:]
            mean = sum(window) / 20
            std = (sum((x - mean) ** 2 for x in window) / 20) ** 0.5
            z = (ratio - mean) / std if std != 0 else 0
        else:
            z = None

        result.append({
            "trade_date": row["trade_date"],
            "left_close": row["left_close"],
            "right_close": row["right_close"],
            "ratio": ratio,
            "spread": row["spread"],
            "zscore_20": z
        })

    conn.close()

    return {
        "ok": True,
        "rows": result,
        "count": len(result)
    }
    