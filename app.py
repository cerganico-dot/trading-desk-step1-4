from __future__ import annotations

import os
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from engine.iol_provider import IOLConfig, IOLMarketProvider
from engine.live_desk import LiveDesk
from engine.simulator import DeskSimulator

app = FastAPI(title="ARG Trading Desk", version="2.1.0")
HTML_PATH = Path(__file__).parent / "templates" / "index.html"

USE_IOL = os.getenv("USE_IOL", "0") == "1"
IOL_USERNAME = os.getenv("IOL_USERNAME", "")
IOL_PASSWORD = os.getenv("IOL_PASSWORD", "")
CAPITAL = float(os.getenv("CAPITAL", "25000000"))

DEFAULT_PAIRS = [("AL30", "GD30"), ("AL30D", "GD30D")]
LIVE_SYMBOLS = ["AL30", "GD30", "AL30D", "GD30D", "S31L6"]

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Argentina/Buenos_Aires")
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)

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

_live_engine = None
_sim_engine = DeskSimulator()


def get_engine():
    global _live_engine
    if USE_IOL and IOL_USERNAME and IOL_PASSWORD:
        if _live_engine is None:
            provider = IOLMarketProvider(
                symbols=LIVE_SYMBOLS,
                config=IOLConfig(
                    username=IOL_USERNAME,
                    password=IOL_PASSWORD,
                    base_url=os.getenv("IOL_BASE_URL", "https://api.invertironline.com").rstrip("/"),
                    token_path=os.getenv("IOL_TOKEN_PATH", "/token"),
                    market=os.getenv("IOL_MARKET", "bCBA"),
                    timeout=int(os.getenv("IOL_TIMEOUT", "15")),
                ),
            )
            _live_engine = LiveDesk(provider, capital=CAPITAL, pairs=DEFAULT_PAIRS, window=20)
        return _live_engine
    return _sim_engine


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


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(HTML_PATH.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"ok": True, "timezone": APP_TIMEZONE})


@app.get("/api/state")
def state() -> JSONResponse:
    engine = get_engine()
    desk = engine.build_state()

    payload = {
        "mode": getattr(desk, "mode", "SIM"),
        "timezone": APP_TIMEZONE,
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