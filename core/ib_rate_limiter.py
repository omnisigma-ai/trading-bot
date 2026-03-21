"""
IB API Rate Limiter
-------------------
Prevents IB API shadowbans by throttling requests, caching results,
and tracking market data subscription budgets.

IB hard limits:
  - 50 messages/second (we target 30/sec for headroom)
  - 6 reqHistoricalData per 2 seconds
  - ~100 concurrent market data subscriptions (paper account)

Usage:
    from core.ib_rate_limiter import (
        throttled_qualify_contracts,
        throttled_req_historical_data,
        throttled_req_contract_details,
        throttled_req_tickers,
        mkt_data_budget,
    )
"""
import threading
import time
from datetime import datetime, timedelta


# ── Token Bucket Rate Limiter ───────────────────────────────────────────

class TokenBucketRateLimiter:
    """Thread-safe token bucket enforcing a max requests/second rate."""

    def __init__(self, rate: float = 30.0, burst: int = 10):
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 5.0) -> bool:
        """Block until a token is available or timeout."""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
            if time.monotonic() >= deadline:
                print("[RateLimiter] WARNING: Token bucket timeout — proceeding anyway")
                return False
            time.sleep(0.02)

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now


# ── Historical Data Throttle ────────────────────────────────────────────

class HistoricalDataThrottle:
    """Enforce IB's 6 historical data requests per 2 seconds."""

    def __init__(self, max_requests: int = 6, window_seconds: float = 2.0):
        self._timestamps: list[float] = []
        self._max = max_requests
        self._window = window_seconds
        self._lock = threading.Lock()

    def acquire(self):
        """Block until a historical data slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < self._window]
                if len(self._timestamps) < self._max:
                    self._timestamps.append(now)
                    return
            time.sleep(0.1)


# ── Historical Data Cache ───────────────────────────────────────────────

class _CacheEntry:
    __slots__ = ("data", "timestamp", "ttl_seconds")

    def __init__(self, data, ttl_seconds: int):
        self.data = data
        self.timestamp = time.monotonic()
        self.ttl_seconds = ttl_seconds

    @property
    def expired(self) -> bool:
        return (time.monotonic() - self.timestamp) > self.ttl_seconds


class HistoricalDataCache:
    """In-memory TTL cache for reqHistoricalData results."""

    def __init__(self):
        self._cache: dict[tuple, _CacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, key: tuple):
        with self._lock:
            entry = self._cache.get(key)
            if entry and not entry.expired:
                return entry.data
            if entry:
                del self._cache[key]
            return None

    def put(self, key: tuple, data, ttl_seconds: int = 300):
        with self._lock:
            self._cache[key] = _CacheEntry(data, ttl_seconds)

    @property
    def size(self) -> int:
        return len(self._cache)


# ── Contract Cache ──────────────────────────────────────────────────────

class ContractCache:
    """Session-scoped cache for qualifyContracts and reqContractDetails."""

    def __init__(self):
        self._qualified: dict[str, object] = {}
        self._details: dict[str, list] = {}
        self._lock = threading.Lock()

    def get_qualified(self, key: str):
        with self._lock:
            return self._qualified.get(key)

    def put_qualified(self, key: str, contract):
        with self._lock:
            self._qualified[key] = contract

    def get_details(self, key: str):
        with self._lock:
            return self._details.get(key)

    def put_details(self, key: str, details):
        with self._lock:
            self._details[key] = details


# ── Market Data Budget ──────────────────────────────────────────────────

class MarketDataBudget:
    """Tracks active reqMktData subscriptions against IB's limit."""

    def __init__(self, limit: int = 95):
        self._limit = limit
        self._active: set[str] = set()
        self._lock = threading.Lock()

    def can_subscribe(self, contract_key: str) -> bool:
        with self._lock:
            if contract_key in self._active:
                return True  # already subscribed
            if len(self._active) >= self._limit:
                print(f"[RateLimiter] WARNING: Market data limit reached ({len(self._active)}/{self._limit})")
                return False
            return True

    def subscribe(self, contract_key: str):
        with self._lock:
            self._active.add(contract_key)

    def unsubscribe(self, contract_key: str):
        with self._lock:
            self._active.discard(contract_key)

    def reset(self):
        with self._lock:
            self._active.clear()

    @property
    def count(self) -> int:
        return len(self._active)


# ── Module-Level Singletons ────────────────────────────────────────────

_global_limiter = TokenBucketRateLimiter(rate=30.0, burst=10)
_hist_throttle = HistoricalDataThrottle(max_requests=6, window_seconds=2.0)
_hist_cache = HistoricalDataCache()
_contract_cache = ContractCache()
mkt_data_budget = MarketDataBudget(limit=95)


# ── TTL Logic ───────────────────────────────────────────────────────────

def _ttl_for_bar_size(bar_size: str) -> int:
    """Return cache TTL in seconds based on bar size."""
    bs = bar_size.lower()
    if "day" in bs:
        return 3600     # 1 hour — daily bars don't change intraday
    if "4" in bs and "hour" in bs:
        return 600      # 10 minutes
    if "hour" in bs:
        return 300      # 5 minutes
    if "min" in bs:
        return 120      # 2 minutes
    return 300


# ── Helper Functions (callers use these) ────────────────────────────────

def _contract_key(contract) -> str:
    """Generate a cache key for a contract."""
    return (
        f"{getattr(contract, 'symbol', '')}|"
        f"{getattr(contract, 'exchange', '')}|"
        f"{getattr(contract, 'currency', '')}|"
        f"{type(contract).__name__}|"
        f"{getattr(contract, 'lastTradeDateOrContractMonth', '')}"
    )


def throttled_qualify_contracts(ib, contract):
    """qualifyContracts with caching and rate limiting."""
    key = _contract_key(contract)
    cached = _contract_cache.get_qualified(key)
    if cached is not None:
        # Copy cached contract IDs back to caller's contract
        for attr in ("conId", "exchange", "primaryExchange", "currency",
                      "localSymbol", "tradingClass", "lastTradeDateOrContractMonth",
                      "multiplier"):
            val = getattr(cached, attr, None)
            if val is not None:
                try:
                    setattr(contract, attr, val)
                except (AttributeError, TypeError):
                    pass
        return contract

    _global_limiter.acquire()
    ib.qualifyContracts(contract)

    # Cache the result
    _contract_cache.put_qualified(key, contract)
    # Also cache with the now-populated key (conId might have changed)
    new_key = _contract_key(contract)
    if new_key != key:
        _contract_cache.put_qualified(new_key, contract)

    return contract


def throttled_req_historical_data(
    ib,
    contract,
    duration: str,
    bar_size: str,
    what_to_show: str = "MIDPOINT",
    use_rth: bool = False,
):
    """reqHistoricalData with caching, historical data throttle, and rate limiting."""
    cache_key = (
        getattr(contract, "symbol", ""),
        getattr(contract, "exchange", ""),
        duration,
        bar_size,
        what_to_show,
    )

    cached = _hist_cache.get(cache_key)
    if cached is not None:
        print(f"[RateLimiter] Cache hit: {contract.symbol} ({duration}, {bar_size})")
        return cached

    _global_limiter.acquire()
    _hist_throttle.acquire()

    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=2,
    )

    if bars:
        ttl = _ttl_for_bar_size(bar_size)
        _hist_cache.put(cache_key, bars, ttl_seconds=ttl)

    return bars


def throttled_req_contract_details(ib, contract) -> list:
    """reqContractDetails with caching and rate limiting."""
    key = f"details|{getattr(contract, 'symbol', '')}|{getattr(contract, 'exchange', '')}"
    cached = _contract_cache.get_details(key)
    if cached is not None:
        return cached

    _global_limiter.acquire()
    details = ib.reqContractDetails(contract)
    if details:
        _contract_cache.put_details(key, details)
    return details


def throttled_req_tickers(ib, *contracts):
    """reqTickers with rate limiting (no caching — prices change constantly)."""
    _global_limiter.acquire()
    return ib.reqTickers(*contracts)
