"""令牌桶限流 + 指数退避重试。

飞书各接口限频不一（建空间 10/min、上传 5QPS、导入 100/min、docx 3QPS…），
故限流器按「每接口一个桶」使用；退避处理 429/限频错误码与临时故障。
"""
from __future__ import annotations

import random
import threading
import time
from typing import Callable, Iterable, TypeVar

T = TypeVar("T")


class TokenBucket:
    """线程安全令牌桶。rate=每秒补充令牌数，capacity=桶容量（突发）。"""

    def __init__(self, rate: float, capacity: float | None = None):
        self.rate = rate
        self.capacity = capacity if capacity is not None else max(1.0, rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        """阻塞直到可取到令牌。"""
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._last) * self.rate
                )
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self.rate
            time.sleep(wait)


# 按「接口名 -> 每秒速率」维护限流桶。数值取文档限额并留余量。
_DEFAULT_RATES = {
    "wiki_space_create": 10 / 60,   # 10/min
    "wiki_node": 100 / 60,          # 100/min
    "drive_upload": 5.0,            # 5 QPS
    "drive_folder": 5.0,            # 5 QPS
    "import_task": 100 / 60,        # 100/min
    "docx_block": 3.0,              # 3 QPS
    "permission": 5.0,
    "default": 5.0,
}


class RateLimiterRegistry:
    def __init__(self, rates: dict[str, float] | None = None):
        rates = rates or _DEFAULT_RATES
        self._buckets = {name: TokenBucket(r) for name, r in rates.items()}

    def acquire(self, name: str) -> None:
        self._buckets.get(name, self._buckets["default"]).acquire()


class RetryableError(Exception):
    """标记「可重试」的错误（429 / 限频码 / 5xx / 网络抖动）。"""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def with_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.3,
) -> T:
    """指数退避重试。仅对 RetryableError 重试；尊重 retry_after。"""
    attempt = 0
    while True:
        try:
            return fn()
        except RetryableError as e:
            attempt += 1
            if attempt >= max_attempts:
                raise
            if e.retry_after is not None:
                delay = e.retry_after
            else:
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                delay += random.uniform(0, jitter * delay)
            time.sleep(delay)
