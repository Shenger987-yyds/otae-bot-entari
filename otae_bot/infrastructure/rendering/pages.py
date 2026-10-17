"""Render the pages of a multi-page card concurrently."""

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

_T = TypeVar("_T")


async def render_pages(factories: Iterable[Callable[[], Awaitable[_T]]]) -> tuple[_T, ...]:
    """Run every page factory at once and return the results in page order.

    The first page to fail cancels the others and its own exception propagates as is
    (not wrapped in an ExceptionGroup), so callers' ``is_height_limit_error`` retries
    see the same ``RuntimeError`` a serial loop would raise. No extra limit is needed:
    screenshots wait for browser render slots and downloads go through the asset broker.
    """
    tasks: list[asyncio.Future] = []
    try:
        for factory in factories:
            tasks.append(asyncio.ensure_future(factory()))
        return tuple(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        raise
