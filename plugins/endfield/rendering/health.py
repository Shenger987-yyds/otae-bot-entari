"""Per-render asset health; no URL or account data is logged or persisted."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RenderHealth:
    missing: set[str] = field(default_factory=set)
    # Time spent preparing assets in this render; parallel batches add up.
    asset_seconds: float = 0.0
    # Asset fetches that went to the network, how many of them were 304
    # revalidations, and how many were still running at the soft deadline.
    network: int = 0
    revalidated: int = 0
    refill: int = 0

    @property
    def complete(self) -> bool:
        return not self.missing


_health: ContextVar[RenderHealth | None] = ContextVar(
    "endfield_render_health", default=None
)


@contextmanager
def track_render_health() -> Iterator[RenderHealth]:
    health = RenderHealth()
    token = _health.set(health)
    try:
        yield health
    finally:
        _health.reset(token)


def record_assets(requested: Iterable[str], resolved: Iterable[str]) -> None:
    health = _health.get()
    if health is not None:
        ready = set(resolved)
        health.missing.update(url for url in requested if url and url not in ready)
        health.missing.difference_update(ready)


def record_asset_seconds(seconds: float) -> None:
    health = _health.get()
    if health is not None:
        health.asset_seconds += max(0.0, seconds)


def record_batch_stats(stats: Any) -> None:
    """Add one ``fetch_many_resilient`` batch's network counts (None: a test double)."""
    health = _health.get()
    if health is not None and stats is not None:
        health.network += stats.network
        health.revalidated += stats.revalidated
        health.refill += stats.refill


def record_fallback_success(candidates: Iterable[str]) -> None:
    health = _health.get()
    if health is not None:
        health.missing.difference_update(candidates)
