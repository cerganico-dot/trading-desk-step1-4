from __future__ import annotations

import ast
import json
import math
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


class HistoricalPhase1Service:
    BASE_URL = "https://iol.invertironline.com/titulo/datoshistoricos?mercado=bcba&simbolo={symbol}"
    SUPPORTED_SYMBOLS = ["AL30", "GD30"]

    def __init__(self, db_path: str, timezone: str = "America/Argentina/Buenos_Aires") -> None:
        self.db_path = db_path
        self.timezone = timezone
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_daily_bars (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL NOT NULL,
                    volume REAL,
                    source TEXT DEFAULT 'IOL_PUBLIC',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, trade_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_pair_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_key TEXT NOT NULL,
                    left_symbol TEXT NOT NULL,
                    right_symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    left_close REAL NOT NULL,
                    right_close REAL NOT NULL,
                    ratio REAL NOT NULL,
                    spread REAL NOT NULL,
                    zscore_20 REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(pair_key, trade_date)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_hdb_symbol_date ON historical_daily_bars(symbol, trade_date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_hpm_pair_date ON historical_pair_metrics(pair_key, trade_date)"
            )
            conn.commit()

    def _parse_date(self, raw: Any) -> Optional[str]:
        if raw is None:
            return None

        text = str(raw).strip()
        if not text:
            return None

        if "T" in text:
            text = text.split("T")[0]

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None

        text = str(value).strip()
        if text == "":
            return None

        text = text.replace("\xa0", "").replace(" ", "")

        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        else:
            text = text.replace(",", ".")

        try:
            return float(text)
        except Exception:
            return None

    def _extract_close(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ["ultimoPrecio", "ultimo", "cierre", "close", "precioCierre"]:
            value = self._to_float(item.get(key))
            if value is not None:
                return value
        return None

    def _extract_open(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ["apertura", "open", "precioApertura"]:
            value = self._to_float(item.get(key))
            if value is not None:
                return value
        return None

    def _extract_high(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ["maximo", "high", "precioMaximo"]:
            value = self._to_float(item.get(key))
            if value is not None:
                return value
        return None

    def _extract_low(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ["minimo", "low", "precioMinimo"]:
            value = self._to_float(item.get(key))
            if value is not None:
                return value
        return None

    def _extract_volume(self, item: Dict[str, Any]) -> Optional[float]:
        for key in ["volumenNominal", "volumen", "volume", "volumenOperado"]:
            value = self._to_float(item.get(key))
            if value is not None:
                return value
        return None

    def _extract_candidate_payload_text(self, text: str) -> str:
        clean = text.strip()
        clean = re.sub(r"^\)\]\}',?\s*", "", clean)

        if clean.startswith("[") or clean.startswith("{"):
            return clean

        array_match = re.search(r"(\[\s*[\s\S]*\])", clean)
        if array_match:
            return array_match.group(1)

        object_match = re.search(r"(\{\s*[\s\S]*\})", clean)
        if object_match:
            return object_match.group(1)

        return clean

    def _quote_unquoted_keys(self, text: str) -> str:
        pattern = re.compile(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)')
        prev = None
        cur = text
        while prev != cur:
            prev = cur
            cur = pattern.sub(r'\1"\2"\3', cur)
        return cur

    def _try_parse_json_family(self, raw_text: str) -> Any:
        candidate = self._extract_candidate_payload_text(raw_text)

        try:
            return json.loads(candidate)
        except Exception:
            pass

        quoted_keys = self._quote_unquoted_keys(candidate)
        try:
            return json.loads(quoted_keys)
        except Exception:
            pass

        pythonish = quoted_keys
        pythonish = re.sub(r"\btrue\b", "True", pythonish, flags=re.IGNORECASE)
        pythonish = re.sub(r"\bfalse\b", "False", pythonish, flags=re.IGNORECASE)
        pythonish = re.sub(r"\bnull\b", "None", pythonish, flags=re.IGNORECASE)

        try:
            return ast.literal_eval(pythonish)
        except Exception:
            pass

        single_to_double = quoted_keys.replace("'", '"')
        try:
            return json.loads(single_to_double)
        except Exception:
            pass

        raise ValueError(f"IOL payload parse error: {candidate[:400]}")

    def _fetch_payload(self, symbol: str) -> Any:
        url = self.BASE_URL.format(symbol=symbol)

        response = requests.get(
            url,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://iol.invertironline.com/",
            },
        )
        response.raise_for_status()

        try:
            return response.json()
        except Exception:
            return self._try_parse_json_family(response.text)

    def _fetch_symbol_history(self, symbol: str, years: int = 2) -> List[Dict[str, Any]]:
        symbol = symbol.upper()
        if symbol not in self.SUPPORTED_SYMBOLS:
            raise ValueError(f"unsupported symbol: {symbol}")

        payload = self._fetch_payload(symbol)
        items: List[Dict[str, Any]] = []

        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            for key in ["data", "items", "result", "results"]:
                maybe = payload.get(key)
                if isinstance(maybe, list):
                    items = maybe
                    break

        cutoff = (datetime.utcnow() - timedelta(days=365 * years + 15)).date()

        rows: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue

            trade_date = self._parse_date(item.get("fecha") or item.get("tradeDate") or item.get("date"))
            close_price = self._extract_close(item)

            if trade_date is None or close_price is None:
                continue

            dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
            if dt < cutoff:
                continue

            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": self._extract_open(item),
                    "high": self._extract_high(item),
                    "low": self._extract_low(item),
                    "close": close_price,
                    "volume": self._extract_volume(item),
                }
            )

        rows.sort(key=lambda x: x["trade_date"])
        return rows

    def _upsert_symbol_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO historical_daily_bars (
                    symbol, trade_date, open, high, low, close, volume, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'IOL_PUBLIC')
                ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    source='IOL_PUBLIC'
                """,
                [
                    (
                        row["symbol"],
                        row["trade_date"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                    )
                    for row in rows
                ],
            )
            conn.commit()

        return len(rows)

    def _load_symbol_rows(self, symbol: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trade_date, open, high, low, close, volume
                FROM historical_daily_bars
                WHERE symbol = ?
                ORDER BY trade_date ASC
                """,
                (symbol.upper(),),
            ).fetchall()
        return [dict(row) for row in rows]

    def _rolling_mean(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def _rolling_std(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = self._rolling_mean(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)

    def _build_pair_metrics(self, left_rows: List[Dict[str, Any]], right_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        left_map = {row["trade_date"]: row for row in left_rows}
        right_map = {row["trade_date"]: row for row in right_rows}

        common_dates = sorted(set(left_map.keys()) & set(right_map.keys()))
        metrics: List[Dict[str, Any]] = []
        ratio_window: List[float] = []

        for trade_date in common_dates:
            left_close = self._to_float(left_map[trade_date]["close"])
            right_close = self._to_float(right_map[trade_date]["close"])

            if left_close is None or right_close is None or right_close == 0:
                continue

            ratio = left_close / right_close
            spread = left_close - right_close

            ratio_window.append(ratio)
            recent = ratio_window[-20:]
            mean20 = self._rolling_mean(recent)
            std20 = self._rolling_std(recent)
            zscore_20 = None if std20 == 0 else (ratio - mean20) / std20

            metrics.append(
                {
                    "pair_key": "AL30-GD30",
                    "left_symbol": "AL30",
                    "right_symbol": "GD30",
                    "trade_date": trade_date,
                    "left_close": round(left_close, 8),
                    "right_close": round(right_close, 8),
                    "ratio": round(ratio, 10),
                    "spread": round(spread, 10),
                    "zscore_20": None if zscore_20 is None else round(zscore_20, 10),
                }
            )

        return metrics

    def _replace_pair_metrics(self, metrics: List[Dict[str, Any]]) -> int:
        with self._connect() as conn:
            conn.execute("DELETE FROM historical_pair_metrics WHERE pair_key = 'AL30-GD30'")
            if metrics:
                conn.executemany(
                    """
                    INSERT INTO historical_pair_metrics (
                        pair_key, left_symbol, right_symbol, trade_date,
                        left_close, right_close, ratio, spread, zscore_20
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row["pair_key"],
                            row["left_symbol"],
                            row["right_symbol"],
                            row["trade_date"],
                            row["left_close"],
                            row["right_close"],
                            row["ratio"],
                            row["spread"],
                            row["zscore_20"],
                        )
                        for row in metrics
                    ],
                )
            conn.commit()
        return len(metrics)

    def bootstrap_phase1(self, years: int = 2) -> Dict[str, Any]:
        symbol_counts: Dict[str, int] = {}

        for symbol in self.SUPPORTED_SYMBOLS:
            rows = self._fetch_symbol_history(symbol=symbol, years=years)
            symbol_counts[symbol] = self._upsert_symbol_rows(rows)

        left_rows = self._load_symbol_rows("AL30")
        right_rows = self._load_symbol_rows("GD30")
        metrics = self._build_pair_metrics(left_rows, right_rows)
        pair_count = self._replace_pair_metrics(metrics)

        return {
            "ok": True,
            "years": years,
            "db_path": self.db_path,
            "timezone": self.timezone,
            "symbols": symbol_counts,
            "pair": "AL30-GD30",
            "pair_rows": pair_count,
        }

    def status(self) -> Dict[str, Any]:
        with self._connect() as conn:
            al30_count = conn.execute(
                "SELECT COUNT(*) FROM historical_daily_bars WHERE symbol = 'AL30'"
            ).fetchone()[0]
            gd30_count = conn.execute(
                "SELECT COUNT(*) FROM historical_daily_bars WHERE symbol = 'GD30'"
            ).fetchone()[0]
            pair_count = conn.execute(
                "SELECT COUNT(*) FROM historical_pair_metrics WHERE pair_key = 'AL30-GD30'"
            ).fetchone()[0]

        return {
            "ok": True,
            "db_path": self.db_path,
            "timezone": self.timezone,
            "symbols": {
                "AL30": al30_count,
                "GD30": gd30_count,
            },
            "pair": {
                "AL30-GD30": pair_count,
            },
        }

    def get_symbol_history(self, symbol: str, limit: int = 2000) -> List[Dict[str, Any]]:
        symbol = symbol.upper()

        with self._connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM historical_daily_bars WHERE symbol = ?",
                (symbol,),
            ).fetchone()[0]

        if count == 0 and symbol in self.SUPPORTED_SYMBOLS:
            try:
                self.bootstrap_phase1(years=2)
            except Exception:
                pass

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trade_date, open, high, low, close, volume
                FROM historical_daily_bars
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (symbol, int(limit)),
            ).fetchall()

        result = [dict(row) for row in rows]
        result.reverse()
        return result

    def _rebuild_pair_metrics_if_needed(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT trade_date, left_close, right_close, ratio, spread, zscore_20
                FROM historical_pair_metrics
                WHERE pair_key = 'AL30-GD30'
                ORDER BY trade_date ASC
                """
            ).fetchall()

        if existing:
            return [dict(row) for row in existing]

        left_rows = self._load_symbol_rows("AL30")
        right_rows = self._load_symbol_rows("GD30")

        if not left_rows or not right_rows:
            try:
                self.bootstrap_phase1(years=2)
            except Exception:
                pass
            left_rows = self._load_symbol_rows("AL30")
            right_rows = self._load_symbol_rows("GD30")

        if not left_rows or not right_rows:
            return []

        metrics = self._build_pair_metrics(left_rows, right_rows)
        if metrics:
            self._replace_pair_metrics(metrics)

        return [
            {
                "trade_date": row["trade_date"],
                "left_close": row["left_close"],
                "right_close": row["right_close"],
                "ratio": row["ratio"],
                "spread": row["spread"],
                "zscore_20": row["zscore_20"],
            }
            for row in metrics
        ]

    def get_pair_history(self, limit: int = 2000) -> List[Dict[str, Any]]:
        metrics = self._rebuild_pair_metrics_if_needed()
        trimmed = metrics[-int(limit):] if metrics else []
        return [
            {
                "trade_date": row["trade_date"],
                "left_close": row["left_close"],
                "right_close": row["right_close"],
                "ratio": row["ratio"],
                "spread": row["spread"],
                "zscore_20": row["zscore_20"],
            }
            for row in trimmed
        ]