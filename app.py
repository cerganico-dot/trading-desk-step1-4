from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
import os

from engine.live_desk import LiveDesk
from engine.historical_phase1 import HistoricalPhase1Service
from engine.backtest_engine import BacktestEngine

app = FastAPI()

# =========================
# CONFIG
# =========================
SQLITE_PATH = os.getenv("SQLITE_PATH", "data/trading_desk.db")

templates = Jinja2Templates(directory="templates")

desk = LiveDesk(db_path=SQLITE_PATH)

# =========================
# ROOT (FIX FRONTEND)
# =========================
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# =========================
# STATE
# =========================
@app.get("/api/state")
def get_state():
    try:
        state = desk.get_state()
        return JSONResponse(state)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

# =========================
# PAIRS CONFIG
# =========================
class PairRequest(BaseModel):
    left: str
    right: str

@app.post("/api/config/pairs")
def add_pair(req: PairRequest):
    try:
        pair = desk.add_manual_pair(req.left, req.right)
        return {"ok": True, "added": pair}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.delete("/api/config/pairs")
def delete_pair(req: PairRequest):
    try:
        pair = desk.remove_manual_pair(req.left, req.right)
        return {"ok": True, "removed": pair}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =========================
# HISTORY PHASE 1
# =========================
def _history_service():
    return HistoricalPhase1Service(db_path=SQLITE_PATH)

@app.post("/api/history/phase1/bootstrap")
def bootstrap_history(years: Optional[int] = 2):
    try:
        svc = _history_service()
        svc.bootstrap(years=years)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/history/phase1/status")
def history_status():
    try:
        svc = _history_service()
        return svc.status()
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/history/phase1/al30-gd30")
def history_pair(limit: Optional[int] = 2000):
    try:
        svc = _history_service()
        rows = svc.get_pair_history("AL30", "GD30", limit=limit)
        return {
            "ok": True,
            "pair_name": "AL30/GD30",
            "rows": rows,
            "count": len(rows)
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/history/phase1/bars/{symbol}")
def history_bars(symbol: str, limit: Optional[int] = 2000):
    try:
        svc = _history_service()
        rows = svc.get_bars(symbol.upper(), limit=limit)
        return {
            "ok": True,
            "symbol": symbol.upper(),
            "rows": rows,
            "count": len(rows)
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =========================
# BACKTEST
# =========================
@app.get("/api/history/backtest/al30-gd30")
def run_backtest(
    entry_z: float = 2.0,
    exit_z: float = 0.0,
    capital: float = 1_000_000,
    cost: float = 0.003
):
    try:
        engine = BacktestEngine(SQLITE_PATH)
        result = engine.run_backtest(
            left="AL30",
            right="GD30",
            entry_z=entry_z,
            exit_z=exit_z,
            capital=capital,
            cost=cost
        )
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =========================
# OPTIMIZER
# =========================
@app.get("/api/history/optimize/al30-gd30")
def optimize_backtest():
    try:
        engine = BacktestEngine(SQLITE_PATH)
        results = engine.optimize(
            left="AL30",
            right="GD30"
        )
        return {
            "ok": True,
            "results": results
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}