from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

from .models import PairSignal


class AlertManager:
    def __init__(self) -> None:
        self.enable_console = os.getenv("ENABLE_CONSOLE_ALERTS", "1") == "1"
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.cooldown_seconds = int(os.getenv("ALERT_COOLDOWN_SECONDS", "60"))
        self.last_sent: Dict[str, datetime] = {}
        self.sent_events: List[dict] = []

    def _key(self, signal: PairSignal) -> str:
        return f"{signal.left}/{signal.right}:{signal.signal}"

    def _in_cooldown(self, signal: PairSignal, now: datetime) -> bool:
        key = self._key(signal)
        last = self.last_sent.get(key)
        if not last:
            return False
        return now - last < timedelta(seconds=self.cooldown_seconds)

    def _format_message(self, signal: PairSignal, now: datetime) -> str:
        return (
            f"[{now.strftime('%H:%M:%S')}] {signal.signal}\n"
            f"Par: {signal.left}/{signal.right}\n"
            f"Z = {signal.zscore:.3f}\n"
            f"Ratio = {signal.ratio:.6f}\n"
            f"Edge neto = {signal.edge_bps:.2f} bps\n"
            f"Vols = {signal.left_volume:.0f} / {signal.right_volume:.0f}\n"
            f"Spreads = {signal.left_spread_bps:.2f} / {signal.right_spread_bps:.2f} bps"
        )

    def _send_telegram(self, message: str) -> Optional[str]:
        if not self.telegram_bot_token or not self.telegram_chat_id:
            return None
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        resp = requests.post(url, json={"chat_id": self.telegram_chat_id, "text": message}, timeout=10)
        resp.raise_for_status()
        return "telegram"

    def process(self, signals: List[PairSignal]) -> List[dict]:
        events: List[dict] = []
        now = datetime.utcnow()
        for s in signals:
            if not s.eligible or s.signal == "NO TRADE":
                continue
            if self._in_cooldown(s, now):
                continue
            message = self._format_message(s, now)
            channels: List[str] = []
            if self.enable_console:
                print(message)
                channels.append("console")
            tg = self._send_telegram(message)
            if tg:
                channels.append(tg)
            event = {
                "time": now.strftime("%H:%M:%S"),
                "pair": f"{s.left}/{s.right}",
                "signal": s.signal,
                "zscore": round(s.zscore, 4),
                "ratio": round(s.ratio, 8),
                "edge_bps": round(s.edge_bps, 2),
                "channels": channels,
            }
            self.last_sent[self._key(s)] = now
            self.sent_events.append(event)
            events.append(event)
        self.sent_events = self.sent_events[-200:]
        return events
