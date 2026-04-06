"""Batch and parallel execution helpers for multiple assets."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def gather_limited(
    coros: list[Awaitable[T]],
    limit: int = 8,
) -> list[T | BaseException]:
    """Run awaitables with a simple semaphore (parallel cap)."""
    sem = asyncio.Semaphore(max(1, limit))

    async def _wrap(c: Awaitable[T]) -> T:
        async with sem:
            return await c

    return await asyncio.gather(*[_wrap(c) for c in coros], return_exceptions=True)


async def map_parallel(
    items: list[str],
    fn: Callable[[str], Awaitable[T]],
    max_workers: int = 8,
) -> dict[str, T | BaseException]:
    """Apply async fn to each item in parallel (capped)."""
    coros = [fn(x) for x in items]
    results = await gather_limited(coros, limit=max_workers)
    return {items[i]: results[i] for i in range(len(items))}
