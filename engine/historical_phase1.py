from __future__ import annotations

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
            conn.execute("""
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
            """)
            conn.execute("""
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
            """)
            conn.commit()

    def _clean_json_text(self, text: str) -> str:
        text = text.strip()

        # eliminar basura tipo )]}'
        text = re.sub(r"^\)\]\}',?\s*", "", text)

        # convertir comillas simples a dobles
        text = text.replace("'", '"')

        # eliminar trailing commas
        text = re.sub(r",\s*}", "}", text)
        text = re.sub(r",\s*]", "]", text)

        return text

    def _parse_payload(self, text: str) -> Any:
        try:
            return json.loads(text)
        except:
            pass

        clean = self._clean_json_text(text)

        try:
            return json.loads(clean)
        except:
            pass

        # fallback: extraer array
        match = re.search(r"\[\s*\{.*\}\s*\]", clean, re.DOTALL)
        if match:
            return json.loads(match.group(0))

        raise ValueError(f"JSON inválido IOL: {text[:200]}")

    def _fetch_symbol_history(self, symbol: str, years: int = 2) -> List[Dict[str, Any]]:
        url = self.BASE_URL.format(symbol=symbol)

        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )
        r.raise_for_status()

        payload = None
        try:
            payload = r.json()
        except:
            payload = self._parse_payload(r.text)

        items = []
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            for k in ["data", "items", "result"]:
                if isinstance(payload.get(k), list):
                    items = payload[k]
                    break

        cutoff = (datetime.utcnow() - timedelta(days=365 * years)).date()

        rows = []
        for item in items:
            fecha = item.get("fecha") or item.get("date")
            cierre = item.get("ultimoPrecio") or item.get("cierre")

            if not fecha or not cierre:
                continue

            try:
                dt = datetime.strptime(fecha.split("T")[0], "%Y-%m-%d").date()
            except:
                continue

            if dt < cutoff:
                continue

            try:
                close = float(str(cierre).replace(",", "."))
            except:
                continue

            rows.append({
                "symbol": symbol,
                "trade_date": dt.strftime("%Y-%m-%d"),
                "close": close
            })

        rows.sort(key=lambda x: x["trade_date"])
        return rows

    def _save_bars(self, rows):
        if not rows:
            return 0

        with self._connect() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO historical_daily_bars
                (symbol, trade_date, close)
                VALUES (?, ?, ?)
            """, [(r["symbol"], r["trade_date"], r["close"]) for r in rows])
            conn.commit()

        return len(rows)

    def _load(self, symbol):
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT trade_date, close
                FROM historical_daily_bars
                WHERE symbol = ?
                ORDER BY trade_date
            """, (symbol,)).fetchall()
        return [dict(r) for r in rows]

    def bootstrap_phase1(self, years: int = 2) -> Dict[str, Any]:
        counts = {}

        for s in self.SUPPORTED_SYMBOLS:
            rows = self._fetch_symbol_history(s, years)
            counts[s] = self._save_bars(rows)

        left = self._load("AL30")
        right = self._load("GD30")

        right_map = {r["trade_date"]: r for r in right}

        ratios = []
        window = []

        for l in left:
            d = l["trade_date"]
            if d not in right_map:
                continue

            lc = l["close"]
            rc = right_map[d]["close"]

            if rc == 0:
                continue

            ratio = lc / rc
            spread = lc - rc

            window.append(ratio)
            w = window[-20:]
            mean = sum(w) / len(w)
            std = math.sqrt(sum((x - mean) ** 2 for x in w) / len(w)) if len(w) > 1 else 0
            z = None if std == 0 else (ratio - mean) / std

            ratios.append((d, lc, rc, ratio, spread, z))

        with self._connect() as conn:
            conn.execute("DELETE FROM historical_pair_metrics")

            conn.executemany("""
                INSERT INTO historical_pair_metrics
                (pair_key,left_symbol,right_symbol,trade_date,left_close,right_close,ratio,spread,zscore_20)
                VALUES ('AL30-GD30','AL30','GD30',?,?,?,?,?,?)
            """, ratios)

            conn.commit()

        return {
            "ok": True,
            "symbols": counts,
            "pair_rows": len(ratios)
        }

    def status(self):
        with self._connect() as conn:
            a = conn.execute("SELECT COUNT(*) FROM historical_daily_bars WHERE symbol='AL30'").fetchone()[0]
            g = conn.execute("SELECT COUNT(*) FROM historical_daily_bars WHERE symbol='GD30'").fetchone()[0]
            p = conn.execute("SELECT COUNT(*) FROM historical_pair_metrics").fetchone()[0]

        return {"AL30": a, "GD30": g, "PAIR": p}

    def get_pair_history(self, limit=2000):
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT trade_date,left_close,right_close,ratio,spread,zscore_20
                FROM historical_pair_metrics
                ORDER BY trade_date DESC
                LIMIT ?
            """, (limit,)).fetchall()

        res = [dict(r) for r in rows]
        res.reverse()
        return res

    def get_symbol_history(self, symbol, limit=2000):
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT *
                FROM historical_daily_bars
                WHERE symbol=?
                ORDER BY trade_date DESC
                LIMIT ?
            """, (symbol, limit)).fetchall()

        res = [dict(r) for r in rows]
        res.reverse()
        return res