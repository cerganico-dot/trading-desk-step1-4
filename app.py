from __future__ import annotations

import os
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from zoneinfo import ZoneInfo

from engine.backtest_engine import BacktestEngine
from engine.historical_phase1 import HistoricalPhase1Service
from engine.iol_provider import IOLConfig, IOLMarketProvider
from engine.live_desk import LiveDesk
from engine.simulator import DeskSimulator

app = FastAPI(title="ARG Trading Desk", version="3.4.0")
templates = Jinja2Templates(directory="templates")

USE_IOL = os.getenv("USE_IOL", "0") == "1"
IOL_USERNAME = os.getenv("IOL_USERNAME", "")
IOL_PASSWORD = os.getenv("IOL_PASSWORD", "")
CAPITAL = float(os.getenv("CAPITAL", "25000000"))
PAIR_WINDOW = int(os.getenv("PAIR_WINDOW", "20"))

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Argentina/Buenos_Aires")
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/trading_desk.db")

DEFAULT_PAIRS = [
    ("AL30", "AL30D"),
    ("GD30", "GD30D"),
    ("AL30", "GD30"),
    ("AL35", "GD35"),
    ("AL30", "AL35"),
    ("GD30", "GD35"),
    ("AL35", "AL35D"),
    ("GD35", "GD35D"),
    ("AL41", "GD41"),
]

TIME_KEYS = {
    "ts",
    "time",
    "timestamp",
    "event_time",
    "entry_time",
    "exit_time",
    "datetime",
}
TIME_LIST_KEYS = {
    "timestamps",
    "series_timestamps",
}

_live_engine: LiveDesk | None = None
_sim_engine = DeskSimulator()


def _ensure_db_dir() -> None:
    db_path = Path(SQLITE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _db_conn() -> sqlite3.Connection:
    _ensure_db_dir()
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_config_table() -> None:
    with _db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS configured_pairs (
                left_symbol TEXT NOT NULL,
                right_symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (left_symbol, right_symbol)
            )
            """
        )
        conn.commit()


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_pair(left: str, right: str) -> tuple[str, str]:
    l = _normalize_symbol(left)
    r = _normalize_symbol(right)
    if not l or not r:
        raise ValueError("Ticker vacío.")
    if l == r:
        raise ValueError("Los tickers no pueden ser iguales.")
    return l, r


def _load_manual_pairs() -> list[tuple[str, str]]:
    _ensure_config_table()
    with _db_conn() as conn:
        rows = conn.execute(
            """
            SELECT left_symbol, right_symbol
            FROM configured_pairs
            ORDER BY created_at ASC, left_symbol ASC, right_symbol ASC
            """
        ).fetchall()
    return [(str(r["left_symbol"]), str(r["right_symbol"])) for r in rows]


def _save_manual_pair(left: str, right: str) -> tuple[str, str]:
    pair = _normalize_pair(left, right)
    _ensure_config_table()
    with _db_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO configured_pairs (left_symbol, right_symbol, created_at)
            VALUES (?, ?, ?)
            """,
            (pair[0], pair[1], datetime.now(UTC).isoformat()),
        )
        conn.commit()
    return pair


def _delete_manual_pair(left: str, right: str) -> tuple[str, str]:
    pair = _normalize_pair(left, right)
    _ensure_config_table()
    with _db_conn() as conn:
        conn.execute(
            """
            DELETE FROM configured_pairs
            WHERE left_symbol = ? AND right_symbol = ?
            """,
            (pair[0], pair[1]),
        )
        conn.commit()
    return pair


def _all_pairs() -> list[tuple[str, str]]:
    seen: set[str] = set()
    out: list[tuple[str, str]] = []

    for left, right in DEFAULT_PAIRS + _load_manual_pairs():
        key = f"{left}/{right}"
        if key in seen:
            continue
        seen.add(key)
        out.append((left, right))

    return out


def _all_symbols() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    for left, right in _all_pairs():
        for sym in (left, right):
            if sym not in seen:
                seen.add(sym)
                out.append(sym)

    if "S31L6" not in seen:
        out.append("S31L6")

    return out


def _pair_names(pairs: list[tuple[str, str]]) -> list[str]:
    return [f"{a}/{b}" for a, b in pairs]


def _build_provider(symbols: list[str]) -> IOLMarketProvider:
    return IOLMarketProvider(
        symbols=symbols,
        config=IOLConfig(
            username=IOL_USERNAME,
            password=IOL_PASSWORD,
            base_url=os.getenv("IOL_BASE_URL", "https://api.invertironline.com").rstrip("/"),
            token_path=os.getenv("IOL_TOKEN_PATH", "/token"),
            market=os.getenv("IOL_MARKET", "bCBA"),
            timeout=int(os.getenv("IOL_TIMEOUT", "15")),
        ),
    )


def _validate_symbols_live(left: str, right: str) -> dict[str, Any]:
    if not (USE_IOL and IOL_USERNAME and IOL_PASSWORD):
        return {
            "validated": False,
            "mode": "SKIPPED",
            "reason": "LIVE desactivado o credenciales faltantes.",
            "symbols_found": [],
        }

    provider = _build_provider([left, right])
    try:
        quotes = provider.snapshot()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo validar en IOL: {exc}") from exc

    found: list[str] = []
    missing: list[str] = []

    for sym in [left, right]:
        q = quotes.get(sym)
        if q is None:
            missing.append(sym)
            continue

        has_price = any(
            [
                getattr(q, "bid", None) not in (None, 0),
                getattr(q, "ask", None) not in (None, 0),
                getattr(q, "last", None) not in (None, 0),
                getattr(q, "mid", None) not in (None, 0),
            ]
        )

        if has_price:
            found.append(sym)
        else:
            missing.append(sym)

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Ticker inválido o sin cotización válida en IOL: {', '.join(missing)}",
        )

    return {
        "validated": True,
        "mode": "LIVE",
        "reason": "",
        "symbols_found": found,
    }


def _rebuild_live_engine() -> LiveDesk:
    global _live_engine

    pairs = _all_pairs()
    symbols = _all_symbols()

    provider = _build_provider(symbols)
    _live_engine = LiveDesk(
        provider=provider,
        capital=CAPITAL,
        pairs=pairs,
        window=PAIR_WINDOW,
    )
    return _live_engine


def get_engine():
    global _live_engine
    if USE_IOL and IOL_USERNAME and IOL_PASSWORD:
        if _live_engine is None:
            return _rebuild_live_engine()
        return _live_engine
    return _sim_engine


def _history_service() -> HistoricalPhase1Service:
    return HistoricalPhase1Service(db_path=SQLITE_PATH)


def _to_local_iso_string(value: str) -> str:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(LOCAL_TZ).isoformat(timespec="seconds")


def _to_local_time_label(value: str) -> str:
    raw = value.strip()

    parsed_time = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed_time = datetime.strptime(raw, fmt).time()
            break
        except ValueError:
            continue

    if parsed_time is None:
        return value

    dt_utc = datetime.combine(date.today(), parsed_time, tzinfo=UTC)
    return dt_utc.astimezone(LOCAL_TZ).strftime("%H:%M:%S")


def _convert_time_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    raw = value.strip()
    if not raw:
        return value

    try:
        if "T" in raw or raw.endswith("Z") or "+" in raw[10:]:
            return _to_local_iso_string(raw)
    except Exception:
        pass

    try:
        if len(raw) in (5, 8) and raw.count(":") in (1, 2):
            return _to_local_time_label(raw)
    except Exception:
        pass

    return value


def _localize_payload(obj: Any, parent_key: str | None = None) -> Any:
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if key in TIME_KEYS:
                out[key] = _convert_time_value(value)
            elif key in TIME_LIST_KEYS and isinstance(value, list):
                out[key] = [_convert_time_value(v) for v in value]
            else:
                out[key] = _localize_payload(value, key)
        return out

    if isinstance(obj, list):
        if parent_key in TIME_LIST_KEYS:
            return [_convert_time_value(v) for v in obj]
        return [_localize_payload(v, parent_key) for v in obj]

    return obj


@app.on_event("startup")
def startup() -> None:
    _ensure_config_table()
    _history_service().ensure_tables()


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health() -> JSONResponse:
    pairs = _all_pairs()
    return JSONResponse(
        {
            "ok": True,
            "timezone": APP_TIMEZONE,
            "db_path": SQLITE_PATH,
            "symbols": _all_symbols(),
            "pairs": _pair_names(pairs),
            "manual_pairs": _pair_names(_load_manual_pairs()),
        }
    )


@app.get("/api/config")
def get_config() -> JSONResponse:
    return JSONResponse(
        {
            "timezone": APP_TIMEZONE,
            "db_path": SQLITE_PATH,
            "default_pairs": _pair_names(DEFAULT_PAIRS),
            "manual_pairs": _pair_names(_load_manual_pairs()),
            "pairs": _pair_names(_all_pairs()),
            "symbols": _all_symbols(),
        }
    )


@app.post("/api/config/pairs/validate")
def validate_pair(payload: dict = Body(...)) -> JSONResponse:
    left = payload.get("left")
    right = payload.get("right")

    try:
        pair = _normalize_pair(left, right)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    validation = _validate_symbols_live(pair[0], pair[1])

    return JSONResponse(
        {
            "ok": True,
            "pair": f"{pair[0]}/{pair[1]}",
            **validation,
        }
    )


@app.post("/api/config/pairs")
def add_pair(payload: dict = Body(...)) -> JSONResponse:
    left = payload.get("left")
    right = payload.get("right")

    try:
        pair = _normalize_pair(left, right)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    validation = _validate_symbols_live(pair[0], pair[1])
    pair = _save_manual_pair(pair[0], pair[1])

    if USE_IOL and IOL_USERNAME and IOL_PASSWORD:
        _rebuild_live_engine()

    return JSONResponse(
        {
            "ok": True,
            "added": f"{pair[0]}/{pair[1]}",
            "validation": validation,
            "manual_pairs": _pair_names(_load_manual_pairs()),
            "pairs": _pair_names(_all_pairs()),
            "symbols": _all_symbols(),
        }
    )


@app.delete("/api/config/pairs")
def delete_pair(payload: dict = Body(...)) -> JSONResponse:
    left = payload.get("left")
    right = payload.get("right")

    try:
        pair = _delete_manual_pair(left, right)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if USE_IOL and IOL_USERNAME and IOL_PASSWORD:
        _rebuild_live_engine()

    return JSONResponse(
        {
            "ok": True,
            "deleted": f"{pair[0]}/{pair[1]}",
            "manual_pairs": _pair_names(_load_manual_pairs()),
            "pairs": _pair_names(_all_pairs()),
            "symbols": _all_symbols(),
        }
    )


@app.post("/api/history/phase1/bootstrap")
def bootstrap_phase1_history(payload: dict = Body(default={})) -> JSONResponse:
    years = int(payload.get("years", 2) or 2)
    service = _history_service()

    try:
        result = service.bootstrap_phase1(years=years)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Error bootstrap histórico: {exc}") from exc

    return JSONResponse(result)


@app.get("/api/history/phase1/status")
def phase1_status() -> JSONResponse:
    service = _history_service()
    return JSONResponse(service.status())


@app.get("/api/history/phase1/al30-gd30")
def phase1_pair_history(limit: int = Query(default=600, ge=1, le=5000)) -> JSONResponse:
    service = _history_service()
    return JSONResponse(service.get_pair_history(limit=limit))


@app.get("/api/history/phase1/bars/{symbol}")
def phase1_symbol_bars(symbol: str, limit: int = Query(default=600, ge=1, le=5000)) -> JSONResponse:
    service = _history_service()
    return JSONResponse(service.get_symbol_history(symbol=symbol.upper(), limit=limit))


@app.get("/api/history/backtest/al30-gd30")
def backtest(
    entry_z: float = 2.0,
    exit_z: float = 0.0,
    cost: float = 0.003,
    capital: float = 1_000_000.0,
):
    engine = BacktestEngine(SQLITE_PATH)
    return engine.run_backtest(
        entry_z=entry_z,
        exit_z=exit_z,
        cost=cost,
        capital=capital,
    )


@app.get("/api/history/optimize/al30-gd30")
def optimize(
    cost: float = 0.003,
    capital: float = 1_000_000.0,
):
    engine = BacktestEngine(SQLITE_PATH)
    return engine.optimize(
        cost=cost,
        capital=capital,
    )


@app.get("/api/state")
def state() -> JSONResponse:
    engine = get_engine()
    desk = engine.build_state()

    pairs = _all_pairs()
    payload = {
        "mode": getattr(desk, "mode", "SIM"),
        "timezone": APP_TIMEZONE,
        "db_path": SQLITE_PATH,
        "symbols": _all_symbols(),
        "pairs": _pair_names(pairs),
        "default_pairs": _pair_names(DEFAULT_PAIRS),
        "manual_pairs": _pair_names(_load_manual_pairs()),
        "quotes": {
            sym: {
                "bid": q.bid,
                "ask": q.ask,
                "last": q.last,
                "mid": q.mid,
                "spread_bps": getattr(q, "spread_bps", None),
                "volume": q.volume,
                "ts": q.ts.isoformat(),
            }
            for sym, q in desk.quotes.items()
        },
        "ratios": desk.ratios,
        "zscores": desk.zscores,
        "pair_signals": [s.__dict__ for s in desk.pair_signals],
        "hedge_plans": [h.__dict__ if hasattr(h, "__dict__") else h for h in desk.hedge_plans],
        "risk_report": desk.risk_report.__dict__,
        "pnl_series": desk.pnl_series,
        "timestamps": getattr(desk, "timestamps", []),
        "notes": getattr(desk, "notes", []),
        "ratio_history": getattr(desk, "ratio_history", {}),
        "zscore_history": getattr(desk, "zscore_history", {}),
        "series_timestamps": getattr(desk, "series_timestamps", getattr(desk, "timestamps", [])),
        "opportunity_log": getattr(desk, "opportunity_log", []),
        "paper_positions": getattr(desk, "paper_positions", []),
        "paper_events": getattr(desk, "paper_events", []),
        "paper_trades": getattr(desk, "paper_events", getattr(desk, "paper_trades", [])),
        "alerts_sent": getattr(desk, "alerts_sent", []),
    }

    payload = _localize_payload(payload)
    return JSONResponse(payload)