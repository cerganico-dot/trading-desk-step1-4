from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from engine.historical_phase1 import HistoricalPhase1Service

try:
    from engine.live_desk import live_desk as LIVE_DESK  # type: ignore
except Exception:
    LIVE_DESK = None

try:
    from engine.live_desk import LiveDeskService  # type: ignore
except Exception:
    LiveDeskService = None  # type: ignore


APP_TZ = "America/Argentina/Buenos_Aires"
BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "trading_desk.db")
HTML_PATH = BASE_DIR / "templates" / "index.html"

DEFAULT_SYMBOLS = ["AL30", "GD30", "AL30D", "GD30D", "AL35", "GD35", "AL41", "GD41"]
DEFAULT_PAIRS = [
    ["AL30", "GD30"],
    ["AL30D", "GD30D"],
    ["AL35", "GD35"],
    ["AL41", "GD41"],
]


app = FastAPI(title="Trading Desk Argentino", version="stable-live-historical-multipair")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

historical_service = HistoricalPhase1Service(db_path=DB_PATH, timezone=APP_TZ)


def _normalize_pair_item(pair: Any) -> Optional[List[str]]:
    if isinstance(pair, str):
        text = pair.strip().upper().replace("/", "-").replace("_", "-")
        parts = [p.strip() for p in text.split("-") if p.strip()]
        if len(parts) == 2:
            return [parts[0], parts[1]]
        return None

    if isinstance(pair, (list, tuple)) and len(pair) == 2:
        left = str(pair[0]).strip().upper()
        right = str(pair[1]).strip().upper()
        if left and right:
            return [left, right]

    if isinstance(pair, dict):
        left = str(pair.get("left", "")).strip().upper()
        right = str(pair.get("right", "")).strip().upper()
        if left and right:
            return [left, right]

    return None


def _pairs_to_strings(pairs: List[List[str]]) -> List[str]:
    return [f"{left}-{right}" for left, right in pairs]


def _extract_state_dict(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if hasattr(raw, "dict") and callable(getattr(raw, "dict")):
        try:
            return raw.dict()
        except Exception:
            pass
    if hasattr(raw, "__dict__"):
        try:
            return dict(raw.__dict__)
        except Exception:
            pass
    return {}


def _call_if_exists(obj: Any, names: List[str], *args: Any, **kwargs: Any) -> Any:
    for name in names:
        fn = getattr(obj, name, None)
        if callable(fn):
            try:
                return fn(*args, **kwargs)
            except TypeError:
                try:
                    return fn()
                except Exception:
                    continue
            except Exception:
                continue
    return None


def _ensure_live_desk() -> Any:
    global LIVE_DESK

    if LIVE_DESK is not None:
        return LIVE_DESK

    if LiveDeskService is not None:
        try:
            LIVE_DESK = LiveDeskService(db_path=DB_PATH, timezone=APP_TZ)  # type: ignore
            return LIVE_DESK
        except Exception:
            return None

    return None


def _default_state() -> Dict[str, Any]:
    return {
        "mode": "LIVE",
        "timezone": APP_TZ,
        "db_path": DB_PATH,
        "symbols": DEFAULT_SYMBOLS,
        "pairs": DEFAULT_PAIRS,
        "default_pairs": DEFAULT_PAIRS,
        "manual_pairs": [],
        "quotes": {},
        "ratios": {},
        "zscores": {},
        "pair_signals": {},
        "hedge_plans": {},
        "risk_report": {},
        "pnl_series": [],
        "timestamps": [],
        "notes": [],
        "ratio_history": {},
        "zscore_history": {},
        "series_timestamps": [],
        "opportunity_log": [],
        "paper_positions": [],
        "paper_events": [],
        "paper_trades": [],
        "alerts_sent": [],
    }


def _get_live_state() -> Dict[str, Any]:
    desk = _ensure_live_desk()
    state = _default_state()

    if desk is None:
        state["notes"] = ["live_desk unavailable"]
        return state

    raw_state = None
    for method_name in ["get_state", "snapshot", "state", "export_state"]:
        attr = getattr(desk, method_name, None)
        if callable(attr):
            try:
                raw_state = attr()
                break
            except Exception:
                continue
        elif attr is not None:
            raw_state = attr
            break

    live_state = _extract_state_dict(raw_state)

    state["mode"] = live_state.get("mode", state["mode"])
    state["timezone"] = live_state.get("timezone", APP_TZ)
    state["db_path"] = live_state.get("db_path", DB_PATH)
    state["symbols"] = live_state.get("symbols", DEFAULT_SYMBOLS)

    pairs = live_state.get("pairs", DEFAULT_PAIRS)
    if not pairs:
        pairs = DEFAULT_PAIRS
    state["pairs"] = pairs
    state["default_pairs"] = live_state.get("default_pairs", pairs)

    manual_pairs = live_state.get("manual_pairs")
    if manual_pairs is None:
        manual_pairs = getattr(app.state, "manual_pairs", [])
    state["manual_pairs"] = manual_pairs

    for key in [
        "quotes",
        "ratios",
        "zscores",
        "pair_signals",
        "hedge_plans",
        "risk_report",
        "pnl_series",
        "timestamps",
        "notes",
        "ratio_history",
        "zscore_history",
        "series_timestamps",
        "opportunity_log",
        "paper_positions",
        "paper_events",
        "paper_trades",
        "alerts_sent",
    ]:
        state[key] = live_state.get(key, state[key])

    if not state["pairs"] and state["manual_pairs"]:
        state["pairs"] = state["manual_pairs"]

    return state


def _apply_manual_pairs_to_live_desk(manual_pairs: List[List[str]]) -> None:
    desk = _ensure_live_desk()
    if desk is None:
        return
    _call_if_exists(
        desk,
        ["set_manual_pairs", "update_manual_pairs", "set_pairs", "configure_pairs"],
        manual_pairs,
    )


@app.on_event("startup")
def startup_event() -> None:
    app.state.manual_pairs = []
    _ensure_live_desk()


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse(HTML_PATH.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> Dict[str, Any]:
    manual_pairs = getattr(app.state, "manual_pairs", [])
    return {
        "ok": True,
        "timezone": APP_TZ,
        "db_path": DB_PATH,
        "symbols": DEFAULT_SYMBOLS,
        "pairs": DEFAULT_PAIRS,
        "manual_pairs": manual_pairs,
    }


@app.get("/api/state")
def api_state() -> Dict[str, Any]:
    return _get_live_state()


@app.get("/api/config")
def api_config() -> Dict[str, Any]:
    manual_pairs = getattr(app.state, "manual_pairs", [])
    return {
        "timezone": APP_TZ,
        "db_path": DB_PATH,
        "symbols": DEFAULT_SYMBOLS,
        "pairs": DEFAULT_PAIRS,
        "default_pairs": DEFAULT_PAIRS,
        "manual_pairs": manual_pairs,
    }


@app.get("/api/config/pairs/validate")
def api_validate_pairs(pair: List[str] = Query(default=[])) -> Dict[str, Any]:
    parsed: List[List[str]] = []
    errors: List[str] = []

    for item in pair:
        normalized = _normalize_pair_item(item)
        if normalized is None:
            errors.append(f"invalid pair: {item}")
            continue
        if normalized[0] == normalized[1]:
            errors.append(f"duplicate symbols in pair: {item}")
            continue
        parsed.append(normalized)

    unique: List[List[str]] = []
    seen = set()
    for left, right in parsed:
        key = f"{left}-{right}"
        if key not in seen:
            seen.add(key)
            unique.append([left, right])

    return {
        "ok": len(errors) == 0,
        "pairs": unique,
        "pair_strings": _pairs_to_strings(unique),
        "errors": errors,
    }


@app.post("/api/config/pairs")
def api_add_pairs(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    raw_pairs = payload.get("pairs", [])
    if not isinstance(raw_pairs, list):
        raise HTTPException(status_code=400, detail="pairs must be a list")

    current: List[List[str]] = list(getattr(app.state, "manual_pairs", []))
    seen = {f"{left}-{right}" for left, right in current}

    for item in raw_pairs:
        normalized = _normalize_pair_item(item)
        if normalized is None:
            continue
        key = f"{normalized[0]}-{normalized[1]}"
        if key not in seen:
            seen.add(key)
            current.append(normalized)

    app.state.manual_pairs = current
    _apply_manual_pairs_to_live_desk(current)

    return {
        "ok": True,
        "manual_pairs": current,
        "pair_strings": _pairs_to_strings(current),
    }


@app.delete("/api/config/pairs")
def api_delete_pairs(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    raw_pairs = payload.get("pairs", [])
    if not isinstance(raw_pairs, list):
        raise HTTPException(status_code=400, detail="pairs must be a list")

    remove_set = set()
    for item in raw_pairs:
        normalized = _normalize_pair_item(item)
        if normalized is None:
            continue
        remove_set.add(f"{normalized[0]}-{normalized[1]}")

    current: List[List[str]] = list(getattr(app.state, "manual_pairs", []))
    filtered = [[left, right] for left, right in current if f"{left}-{right}" not in remove_set]

    app.state.manual_pairs = filtered
    _apply_manual_pairs_to_live_desk(filtered)

    return {
        "ok": True,
        "manual_pairs": filtered,
        "pair_strings": _pairs_to_strings(filtered),
    }


@app.post("/api/history/phase1/bootstrap")
def api_history_phase1_bootstrap(years: int = Query(default=2, ge=1, le=10)) -> Dict[str, Any]:
    return historical_service.bootstrap_phase1(years=years)


@app.get("/api/history/phase1/status")
def api_history_phase1_status() -> Dict[str, Any]:
    return historical_service.status()


@app.get("/api/history/phase1/al30-gd30")
def api_history_phase1_al30_gd30(limit: int = Query(default=2000, ge=1, le=10000)) -> Dict[str, Any]:
    rows = historical_service.get_pair_history(limit=limit)
    return {
        "ok": True,
        "pair": ["AL30", "GD30"],
        "count": len(rows),
        "rows": rows,
    }


@app.get("/api/history/phase1/bars/{symbol}")
def api_history_phase1_bars(symbol: str, limit: int = Query(default=2000, ge=1, le=10000)) -> Dict[str, Any]:
    rows = historical_service.get_symbol_history(symbol=symbol, limit=limit)
    return {
        "ok": True,
        "symbol": symbol.upper(),
        "count": len(rows),
        "rows": rows,
    }


@app.exception_handler(Exception)
def global_exception_handler(_: Any, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": str(exc),
        },
    )