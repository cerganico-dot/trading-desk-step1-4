from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Dict, Iterable, Optional

import requests

from .models import InstrumentQuote


class IOLAuthError(RuntimeError):
    pass


@dataclass
class IOLConfig:
    username: str
    password: str
    base_url: str = "https://api.invertironline.com"
    token_path: str = "/token"
    market: str = "bCBA"
    timeout: int = 15


class IOLMarketProvider:
    def __init__(self, symbols: Iterable[str], config: Optional[IOLConfig] = None) -> None:
        self.symbols = list(symbols)
        self.config = config or IOLConfig(
            username=os.getenv("IOL_USERNAME", ""),
            password=os.getenv("IOL_PASSWORD", ""),
        )
        self.session = requests.Session()
        self._access_token: Optional[str] = None

    def _authenticate(self) -> None:
        if not self.config.username or not self.config.password:
            raise IOLAuthError("Faltan credenciales. Definí IOL_USERNAME e IOL_PASSWORD.")
        response = self.session.post(
            f"{self.config.base_url}{self.config.token_path}",
            data={
                "username": self.config.username,
                "password": self.config.password,
                "grant_type": "password",
            },
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload.get("access_token")
        if not self._access_token:
            raise IOLAuthError("IOL no devolvió access_token.")

    def _ensure_token(self) -> str:
        if not self._access_token:
            self._authenticate()
        assert self._access_token is not None
        return self._access_token

    def _get_json(self, path: str) -> dict:
        token = self._ensure_token()
        response = self.session.get(
            f"{self.config.base_url}/{path.lstrip('/')}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=self.config.timeout,
        )
        if response.status_code == 401:
            self._authenticate()
            response = self.session.get(
                f"{self.config.base_url}/{path.lstrip('/')}",
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=self.config.timeout,
            )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_quote(symbol: str, payload: dict) -> InstrumentQuote:
        bid = float(payload.get("precioCompra") or 0)
        ask = float(payload.get("precioVenta") or 0)
        last = float(payload.get("ultimoPrecio") or 0)
        volume = float(payload.get("montoOperado") or 0)
        ts = datetime.now(UTC)
        if bid <= 0 and ask <= 0 and last > 0:
            spread = max(last * 0.001, 0.0001)
            bid = last - spread / 2
            ask = last + spread / 2
        elif bid <= 0 < ask:
            bid = ask
        elif ask <= 0 < bid:
            ask = bid
        return InstrumentQuote(symbol=symbol, bid=bid, ask=ask, last=last if last > 0 else (bid + ask) / 2.0, volume=volume, ts=ts)

    def snapshot(self, step: int = 0) -> Dict[str, InstrumentQuote]:
        quotes: Dict[str, InstrumentQuote] = {}
        for symbol in self.symbols:
            if symbol.startswith(("DLR", "SOJ", "CEDX")):
                continue
            payload = self._get_json(f"api/v2/{self.config.market}/Titulos/{symbol}/Cotizacion")
            quotes[symbol] = self._parse_quote(symbol, payload)
        return quotes
