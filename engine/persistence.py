from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


class SQLiteStore:
    def __init__(self, path: str = "data/trading_desk.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS opportunity_log (
                    id INTEGER PRIMARY KEY,
                    ts TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    ts TEXT PRIMARY KEY,
                    ratios_json TEXT NOT NULL,
                    zscores_json TEXT NOT NULL
                )
                """
            )
            con.commit()

    def save_opportunities(self, items: Iterable[dict]) -> None:
        rows = [(int(item["id"]), item["time"], json.dumps(item, ensure_ascii=False)) for item in items]
        with self._connect() as con:
            con.executemany(
                "INSERT OR REPLACE INTO opportunity_log (id, ts, payload) VALUES (?, ?, ?)",
                rows,
            )
            con.commit()

    def save_alerts(self, items: Iterable[dict]) -> None:
        rows = [(item["time"], json.dumps(item, ensure_ascii=False)) for item in items]
        with self._connect() as con:
            con.executemany("INSERT INTO alert_log (ts, payload) VALUES (?, ?)", rows)
            con.commit()

    def save_paper_events(self, items: Iterable[dict]) -> None:
        rows = [(item["time"], json.dumps(item, ensure_ascii=False)) for item in items]
        with self._connect() as con:
            con.executemany("INSERT INTO paper_events (ts, payload) VALUES (?, ?)", rows)
            con.commit()

    def save_snapshot(self, ts: str, ratios: dict, zscores: dict) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO snapshots (ts, ratios_json, zscores_json) VALUES (?, ?, ?)",
                (ts, json.dumps(ratios, ensure_ascii=False), json.dumps(zscores, ensure_ascii=False)),
            )
            con.commit()
