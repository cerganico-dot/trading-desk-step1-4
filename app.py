from __future__ import annotations

import os
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, HTTPException, Query, Body
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from zoneinfo import ZoneInfo

from engine.backtest_engine import BacktestEngine
from engine.historical_phase1 import HistoricalPhase1Service
from engine.iol_provider import IOLConfig, IOLMarketProvider
from engine.live_desk import LiveDesk
from engine.simulator import DeskSimulator

# =========================
# APP INIT
# =========================
app = FastAPI(title="ARG Trading Desk", version="3.4.1")

templates = Jinja2Templates(directory="templates")

# =========================
# CONFIG
# =========================
USE_IOL = os.getenv("USE_IOL", "0") == "1"
IOL_USERNAME = os.getenv("IOL_USERNAME", "")
IOL_PASSWORD = os.getenv("IOL_PASSWORD", "")
CAPITAL = float(os.getenv("CAPITAL", "25000000"))
PAIR_WINDOW = int(os.getenv("PAIR_WINDOW", "20"))

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Argentina/Buenos_Aires")
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/trading_desk.db")

# =========================
# DEFAULT DATA
# =========================
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

# =========================
# ENGINE
# =========================
_live_engine: LiveDesk | None = None
_sim_engine = DeskSimulator()


def _all_symbols():
    s = set()
    for a, b in DEFAULT_PAIRS:
        s.add(a)
        s.add(b)
    s.add("S31L6")
    return list(s)


def _pair_names(pairs):
    return [f"{a}/{b}" for a, b in pairs]


def _build_provider(symbols):
    return IOLMarketProvider(
        symbols=symbols,
        config=IOLConfig(
            username=IOL_USERNAME,
            password=IOL_PASSWORD,
            base_url="https://api.invertironline.com",
            token_path="/token",
            market="bCBA",
            timeout=15,
        ),
    )


def get_engine():
    global _live_engine
    if USE_IOL and IOL_USERNAME and IOL_PASSWORD:
        if _live_engine is None:
            provider = _build_provider(_all_symbols())
            _live_engine = LiveDesk(
                provider=provider,
                capital=CAPITAL,
                pairs=DEFAULT_PAIRS,
                window=PAIR_WINDOW,
            )
        return _live_engine
    return _sim_engine


# =========================
# ROOT (FIX CLAVE)
# =========================
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return {
        "ok": True,
        "timezone": APP_TIMEZONE,
        "db_path": SQLITE_PATH,
        "symbols": _all_symbols(),
        "pairs": _pair_names(DEFAULT_PAIRS),
        "manual_pairs": [],
    }


# =========================
# STATE
# =========================
@app.get("/api/state")
def state():
    engine = get_engine()
    desk = engine.build_state()

    payload = {
        "mode": getattr(desk, "mode", "SIM"),
        "timezone": APP_TIMEZONE,
        "db_path": SQLITE_PATH,
        "symbols": _all_symbols(),
        "pairs": _pair_names(DEFAULT_PAIRS),
        "manual_pairs": [],
        "ratios": desk.ratios,
        "zscores": desk.zscores,
        "series_timestamps": getattr(desk, "series_timestamps", []),
        "zscore_history": getattr(desk, "zscore_history", {}),
        "opportunity_log": getattr(desk, "opportunity_log", []),
        "paper_positions": getattr(desk, "paper_positions", []),
        "paper_events": getattr(desk, "paper_events", []),
        "alerts_sent": getattr(desk, "alerts_sent", []),
        "risk_report": desk.risk_report.__dict__,
    }

    return JSONResponse(payload)


# =========================
# HISTORY
# =========================
def _history_service():
    return HistoricalPhase1Service(db_path=SQLITE_PATH)


@app.get("/api/history/phase1/al30-gd30")
def history(limit: int = 2000):
    svc = _history_service()
    rows = svc.get_pair_history(limit=limit)
    return {"ok": True, "rows": rows, "count": len(rows)}


@app.get("/api/history/phase1/bars/{symbol}")
def bars(symbol: str, limit: int = 2000):
    svc = _history_service()
    rows = svc.get_symbol_history(symbol=symbol.upper(), limit=limit)
    return {"ok": True, "rows": rows}


# =========================
# BACKTEST
# =========================
@app.get("/api/history/backtest/al30-gd30")
def backtest(entry_z: float = 2.0, exit_z: float = 0.0):
    engine = BacktestEngine(SQLITE_PATH)
    return engine.run_backtest(entry_z=entry_z, exit_z=exit_z)


@app.get("/api/history/optimize/al30-gd30")
def optimize():
    engine = BacktestEngine(SQLITE_PATH)
    return engine.optimize()