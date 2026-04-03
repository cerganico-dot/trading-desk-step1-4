from __future__ import annotations

from collections import defaultdict, deque
from statistics import mean, pstdev
from typing import Deque, Dict, Iterable, List, Tuple

from .models import InstrumentQuote


class RatioScanner:
    def __init__(self, pairs: Iterable[Tuple[str, str]], window: int = 20) -> None:
        self.pairs = list(pairs)
        self.window = window
        self.history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window))

    def update(self, quotes: Dict[str, InstrumentQuote]) -> Dict[str, float]:
        ratios: Dict[str, float] = {}
        for left, right in self.pairs:
            if left not in quotes or right not in quotes:
                continue
            ql = quotes[left]
            qr = quotes[right]
            if qr.mid <= 0:
                continue
            pair_name = f"{left}/{right}"
            ratio = ql.mid / qr.mid
            self.history[pair_name].append(ratio)
            ratios[pair_name] = ratio
        return ratios

    def zscores(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for pair_name, hist in self.history.items():
            values: List[float] = list(hist)
            if len(values) < 5:
                out[pair_name] = 0.0
                continue
            mu = mean(values)
            sigma = pstdev(values)
            out[pair_name] = 0.0 if sigma == 0 else (values[-1] - mu) / sigma
        return out
