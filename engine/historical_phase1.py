from __future__ import annotations

import importlib
import inspect
import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


class HistoricalPhase1Service:
    PUBLIC_URL = "https://iol.invertironline.com/titulo/datoshistoricos?mercado=bcba&simbolo={symbol}"
    API_URL_CANDIDATES = [
        "https://api.invertironline.com/api/v2/bCBA/Titulos/{symbol}/Cotizacion/historica",
        "https://api.invertironline.com/api/v2/bCBA/Titulos/{symbol}/cotizacion/historica",
        "https://api.invertironline.com/api/v2/bcba/titulos/{symbol}/cotizacion/historica",
    ]
    SUPPORTED_SYMBOLS = ["AL30", "GD30"]

    def __init__(self, db_path: str, timezone: str = "America/Argentina/Buenos_Aires") -> None:
        self.db_path = db_path
        self.timezone = timezone
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._provider = self._load_provider()
        self._init_db()

    def _load_provider(self) -> Any:
        try:
            return importlib.import_module("engine.iol_provider")
        except Exception:
            return None

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
                    source TEXT DEFAULT 'IOL',
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_hdb_symbol_date ON historical_daily_bars(symbol, trade_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_hpm_pair_date ON historical_pair_metrics(pair_key, trade_date)")
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
        text = str(value).strip().replace("\xa0", "").replace(" ", "")
        if text == "":
            return None
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

    def _extract_first_float(self, item: Dict[str, Any], keys: List[str]) -> Optional[float]:
        for key in keys:
            if key in item:
                value = self._to_float(item.get(key))
                if value is not None:
                    return value
        return None

    def _extract_items(self, payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ["data", "items", "result", "results", "serie", "historicos", "cotizaciones"]:
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
        return []

    def _normalize_rows(self, symbol: str, payload: Any, years: int) -> List[Dict[str, Any]]:
        items = self._extract_items(payload)
        cutoff = (datetime.utcnow() - timedelta(days=365 * years + 15)).date()
        rows: List[Dict[str, Any]] = []

        for item in items:
            trade_date = self._parse_date(
                item.get("fecha")
                or item.get("tradeDate")
                or item.get("date")
                or item.get("Fecha")
            )
            close_price = self._extract_first_float(
                item,
                ["ultimoPrecio", "ultimo", "cierre", "close", "precioCierre", "ultimoOperado"],
            )
            if trade_date is None or close_price is None:
                continue

            dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
            if dt < cutoff:
                continue

            open_price = self._extract_first_float(item, ["apertura", "open", "precioApertura"])
            high_price = self._extract_first_float(item, ["maximo", "high", "precioMaximo"])
            low_price = self._extract_first_float(item, ["minimo", "low", "precioMinimo"])
            volume = self._extract_first_float(item, ["volumenNominal", "volumen", "volume", "volumenOperado"])

            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                }
            )

        rows.sort(key=lambda x: x["trade_date"])
        dedup: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            dedup[row["trade_date"]] = row
        return [dedup[k] for k in sorted(dedup.keys())]

    def _provider_instances(self) -> List[Any]:
        instances: List[Any] = []
        mod = self._provider
        if mod is None:
            return instances

        for attr_name in ["iol_provider", "provider", "client", "iol", "IOL", "session_client"]:
            obj = getattr(mod, attr_name, None)
            if obj is not None:
                instances.append(obj)

        for class_name in ["IOLProvider", "IOLClient", "IOLApi", "Client", "Provider"]:
            cls = getattr(mod, class_name, None)
            if cls is None or not inspect.isclass(cls):
                continue
            try:
                instances.append(cls())
            except Exception:
                continue

        uniq: List[Any] = []
        seen = set()
        for inst in instances:
            ident = id(inst)
            if ident not in seen:
                seen.add(ident)
                uniq.append(inst)
        return uniq

    def _invoke_callable(self, fn: Any, **kwargs: Any) -> Any:
        try:
            sig = inspect.signature(fn)
            accepted = {}
            for name in sig.parameters.keys():
                if name in kwargs:
                    accepted[name] = kwargs[name]
            return fn(**accepted)
        except Exception:
            try:
                return fn(kwargs.get("symbol"))
            except Exception:
                return None

    def _fetch_from_provider_methods(self, symbol: str, years: int) -> Optional[Any]:
        method_names = [
            "get_historical_data",
            "get_historical_prices",
            "fetch_historical_data",
            "historical_data",
            "get_daily_history",
            "get_history",
            "get_symbol_history",
            "get_titulo_datos_historicos",
        ]

        since_date = (datetime.utcnow() - timedelta(days=365 * years + 15)).strftime("%Y-%m-%d")
        to_date = datetime.utcnow().strftime("%Y-%m-%d")

        for inst in self._provider_instances():
            for name in method_names:
                fn = getattr(inst, name, None)
                if not callable(fn):
                    continue
                payload = self._invoke_callable(
                    fn,
                    symbol=symbol,
                    years=years,
                    limit=years * 365,
                    market="bcba",
                    exchange="bcba",
                    plaza="BCBA",
                    since=since_date,
                    from_date=since_date,
                    to_date=to_date,
                )
                if payload is not None:
                    return payload
        return None

    def _fetch_from_provider_session(self, symbol: str) -> Optional[Any]:
        mod = self._provider
        if mod is None:
            return None

        session = None
        for inst in self._provider_instances():
            for attr_name in ["session", "http", "client", "_session"]:
                sess = getattr(inst, attr_name, None)
                if sess is not None and hasattr(sess, "get"):
                    session = sess
                    break
            if session is not None:
                break

        if session is None:
            for attr_name in ["session", "http", "client"]:
                sess = getattr(mod, attr_name, None)
                if sess is not None and hasattr(sess, "get"):
                    session = sess
                    break

        if session is None:
            return None

        headers = {"Accept": "application/json,text/plain,*/*"}
        for url_tpl in self.API_URL_CANDIDATES + [self.PUBLIC_URL]:
            url = url_tpl.format(symbol=symbol)
            try:
                response = session.get(url, timeout=30, headers=headers)
            except TypeError:
                response = session.get(url)
            except Exception:
                continue

            text = getattr(response, "text", "")
            status_code = getattr(response, "status_code", 200)

            if status_code >= 400:
                continue
            if isinstance(text, str) and "<html" in text.lower():
                continue

            try:
                return response.json()
            except Exception:
                try:
                    return json.loads(text)
                except Exception:
                    continue

        return None

    def _fetch_payload(self, symbol: str, years: int) -> Any:
        payload = self._fetch_from_provider_methods(symbol=symbol, years=years)
        if payload is not None:
            return payload

        payload = self._fetch_from_provider_session(symbol=symbol)
        if payload is not None:
            return payload

        raise ValueError("IOL provider unavailable for authenticated historical fetch")

    def _fetch_symbol_history(self, symbol: str, years: int = 2) -> List[Dict[str, Any]]:
        symbol = symbol.upper()
        if symbol not in self.SUPPORTED_SYMBOLS:
            raise ValueError(f"unsupported symbol: {symbol}")
        payload = self._fetch_payload(symbol=symbol, years=years)
        return self._normalize_rows(symbol=symbol, payload=payload, years=years)

    def _upsert_symbol_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO historical_daily_bars (
                    symbol, trade_date, open, high, low, close, volume, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'IOL')
                ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    source='IOL'
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
            if not rows:
                raise ValueError(f"IOL historical returned 0 rows for {symbol}")
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
            al30_count = conn.execute("SELECT COUNT(*) FROM historical_daily_bars WHERE symbol = 'AL30'").fetchone()[0]
            gd30_count = conn.execute("SELECT COUNT(*) FROM historical_daily_bars WHERE symbol = 'GD30'").fetchone()[0]
            pair_count = conn.execute("SELECT COUNT(*) FROM historical_pair_metrics WHERE pair_key = 'AL30-GD30'").fetchone()[0]

        return {
            "ok": True,
            "db_path": self.db_path,
            "timezone": self.timezone,
            "symbols": {"AL30": al30_count, "GD30": gd30_count},
            "pair": {"AL30-GD30": pair_count},
        }

    def get_symbol_history(self, symbol: str, limit: int = 2000) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trade_date, open, high, low, close, volume
                FROM historical_daily_bars
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (symbol.upper(), int(limit)),
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