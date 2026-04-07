from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from engine.iol_provider import IOLConfig, IOLMarketProvider
from engine.live_desk import LiveDesk
from engine.simulator import DeskSimulator

app = FastAPI(title="ARG Trading Desk", version="2.0.0")
HTML_PATH = Path(__file__).parent / "templates" / "index.html"

USE_IOL = os.getenv("USE_IOL", "0") == "1"
IOL_USERNAME = os.getenv("IOL_USERNAME", "")
IOL_PASSWORD = os.getenv("IOL_PASSWORD", "")
CAPITAL = float(os.getenv("CAPITAL", "25000000"))

DEFAULT_PAIRS = [("AL30", "GD30"), ("AL30D", "GD30D")]
LIVE_SYMBOLS = ["AL30", "GD30", "AL30D", "GD30D", "S31L6"]

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


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(HTML_PATH.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.get("/api/state")
def state() -> JSONResponse:
    engine = get_engine()
    desk = engine.build_state()
    payload = {
        "mode": getattr(desk, "mode", "SIM"),
        "quotes": {
            sym: {
                "bid": q.bid,
                "ask": q.ask,
                "last": q.last,
                "mid": q.mid,
                "spread_bps": q.spread_bps,
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
        "timestamps": desk.timestamps,
        "notes": desk.notes,
        "ratio_history": getattr(desk, "ratio_history", {}),
        "zscore_history": getattr(desk, "zscore_history", {}),
        "series_timestamps": getattr(desk, "series_timestamps", desk.timestamps),
        "opportunity_log": getattr(desk, "opportunity_log", []),
        "paper_positions": getattr(desk, "paper_positions", []),
        "paper_events": getattr(desk, "paper_events", []),
        "alerts_sent": getattr(desk, "alerts_sent", []),
    }
    return JSONResponse(payload)