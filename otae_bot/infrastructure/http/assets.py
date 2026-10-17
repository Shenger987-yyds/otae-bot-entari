"""AssetBroker：素材下载的网络队列、软/硬截止续传与预取。

这里不发请求：每个下载任务（job）仍调 ``client.fetch_bytes``，磁盘读写、304、按主机熔断与
代理沿用素材通道。broker 只决定谁先下、等多久、没等到的怎么办：

- 一批素材先分流（内存 / 新鲜磁盘副本），剩下的每个 URL 一个 job；同一 URL 同时只跑一个 job，
  后来的批次直接等它。交互批次加入一个预取 job 时把它抬到交互优先级。
- 网络槽位按（优先级，到达顺序）发放：交互 > 续传 > 预取；总并发之内再按主机限流。
  已经在传输的请求不会被打断。
- 软截止（整卡预算）到点只是不再等：没下完的 job 降为续传继续跑，完成后照常写内存与磁盘，
  下次查询就是热的（不补发完整图）。硬截止（默认 180 s）才取消。调用方被取消也只是不再等。
- 预取只写磁盘、不进内存，按自然日限流量；走素材代理的主机不预取。
- 交互批次用到的公共图片记进热集，重启后在后台把过期的那部分重新校验 / 下载回磁盘。

日志只有计数，不含 URL 与账号标识。
"""

from __future__ import annotations

import asyncio
import itertools
import json
import os
import sqlite3
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from loguru import logger

from . import client as http
from .asset_policy import (
    asset_settings,
    host_concurrency,
    proxied_host,
    rewrite_asset_url,
)

# 进程关闭时最多等这么久让在途的交互与续传下载落盘，之后取消。
CLOSE_WAIT_SECONDS = 10.0
# 热集与预取流量的状态文件在改动后这么久写一次盘（关闭时也写）。
STATE_SAVE_DELAY_SECONDS = 60.0
HOTSET_FILE = "asset-hotset.json"
PREFETCH_BUDGET_FILE = "asset-prefetch-budget.json"
HOTSET_MAX_ENTRIES = 4096
HOTSET_WINDOW_SECONDS = 24 * 3600.0
PREFETCH_MAX_BYTES = 24 * 1024 * 1024
# Same freshness window as a query's fetch (client.DEFAULT_CACHE_TTL_SECONDS).
PREFETCH_TTL_SECONDS = 600.0
# 流量按北京时间的自然日计（中国不用夏令时，固定 +8 即可）。
_BUDGET_TZ = timezone(timedelta(hours=8))


class Priority(IntEnum):
    INTERACTIVE = 0
    REFILL = 1
    BACKGROUND = 2


class PrefetchSkipped(Exception):
    """A queued prefetch reached the front after the daily budget ran out; nothing was sent."""


@dataclass(frozen=True, slots=True)
class BatchStats:
    requested: int = 0
    memory: int = 0
    disk: int = 0
    # Went to the network; ``revalidated`` of them came back 304.
    network: int = 0
    revalidated: int = 0
    network_seconds: float = 0.0
    soft_hit: bool = False
    # Downloads still running after the soft deadline, continued as refills.
    refill: int = 0


class BatchResult(tuple):
    """``(resources, failures)`` as before; ``stats`` rides along without changing the shape."""

    stats: BatchStats

    def __new__(cls, resources, failures, stats: BatchStats):
        result = super().__new__(cls, (resources, failures))
        result.stats = stats
        return result


@dataclass(eq=False, slots=True)
class _Refill:
    """The downloads one batch stopped waiting for; logged once all of them end."""

    namespace: str
    log_prefix: str
    started: float
    total: int
    remaining: int
    bytes: int = 0
    failed: int = 0
    abandoned: int = 0
    reason: str = ""


@dataclass(eq=False, slots=True)
class _Job:
    key: tuple[str, str, str]
    url: str
    namespace: str
    headers: Mapping[str, str] | None
    timeout_seconds: float | None
    ttl_seconds: float
    max_bytes: int
    disk_aliases: tuple[str, ...]
    attempts: int
    base_delay_seconds: float
    source: str
    # Prefetch jobs only fill the disk; query jobs also publish to the memory pool.
    memory: bool = True
    # Interactive batches waiting for this job right now.
    waiters: int = 0
    # Priority when nobody waits: REFILL once a batch stopped waiting for it.
    floor: Priority = Priority.BACKGROUND
    task: asyncio.Task | None = None
    timer: asyncio.TimerHandle | None = None
    ticket: _Ticket | None = None
    refills: list[_Refill] = field(default_factory=list)
    downloaded: int = 0
    cancel_reason: str = ""

    @property
    def priority(self) -> Priority:
        return Priority.INTERACTIVE if self.waiters else self.floor

    @property
    def done(self) -> bool:
        return self.task is not None and self.task.done()


@dataclass(eq=False, slots=True)
class _Ticket:
    host: str
    job: _Job | None
    priority: Priority
    seq: int
    future: asyncio.Future


_current_job: ContextVar[_Job | None] = ContextVar("asset_broker_job", default=None)


def record_download(size: int) -> None:
    """Count body bytes a broker job received (200 only; a 304 sends none).

    Prefetch bytes are charged right away, before the slot goes to the next
    queued prefetch, so at most one image overshoots the daily budget.
    """
    job = _current_job.get()
    if job is not None:
        job.downloaded += size
        if job.source != "query":
            broker.budget.charge(size)
            broker._schedule_save()


class _Gate:
    """Hands out request slots by (priority, arrival) within a total and a per-host limit."""

    def __init__(self, total: int, host_limit: Callable[[str], int], admit: Callable[[_Job], bool]):
        self.total = max(1, total)
        self._host_limit = host_limit
        self._admit = admit
        self._running: dict[str, int] = {}
        self._queues: dict[str, tuple[OrderedDict[int, _Ticket], ...]] = {}
        self._seq = itertools.count()

    @property
    def active(self) -> int:
        return sum(self._running.values())

    @asynccontextmanager
    async def slot(self, host: str, job: _Job | None) -> AsyncIterator[None]:
        await self._acquire(host, job)
        try:
            yield
        finally:
            self._release(host)

    async def _acquire(self, host: str, job: _Job | None) -> None:
        priority = Priority.INTERACTIVE if job is None else job.priority
        ticket = _Ticket(host, job, priority, next(self._seq), asyncio.get_running_loop().create_future())
        self._queue(host)[priority][ticket.seq] = ticket
        if job is not None:
            job.ticket = ticket
        self._dispatch()
        try:
            await ticket.future
        except BaseException:
            future = ticket.future
            if future.done() and not future.cancelled() and future.exception() is None:
                self._release(host)  # granted, but the caller is gone
            else:
                self._queue(host)[ticket.priority].pop(ticket.seq, None)
            raise
        finally:
            if job is not None and job.ticket is ticket:
                job.ticket = None

    def _release(self, host: str) -> None:
        self._running[host] -= 1
        if not self._running[host]:
            del self._running[host]
        self._dispatch()

    def _queue(self, host: str) -> tuple[OrderedDict[int, _Ticket], ...]:
        queues = self._queues.get(host)
        if queues is None:
            queues = self._queues[host] = tuple(OrderedDict() for _ in Priority)
        return queues

    def _dispatch(self) -> None:
        while self.active < self.total:
            best: _Ticket | None = None
            for host, queues in self._queues.items():
                if self._running.get(host, 0) >= self._host_limit(host):
                    continue
                head = next((next(iter(queue.values())) for queue in queues if queue), None)
                if head is not None and (
                    best is None or (head.priority, head.seq) < (best.priority, best.seq)
                ):
                    best = head
            if best is None:
                return
            del self._queues[best.host][best.priority][best.seq]
            if best.future.done():
                continue  # its waiter was cancelled
            if best.job is not None and not self._admit(best.job):
                best.future.set_exception(PrefetchSkipped())
                continue
            self._running[best.host] = self._running.get(best.host, 0) + 1
            best.future.set_result(None)

    def reprioritize(self, job: _Job) -> None:
        ticket = job.ticket
        if ticket is None or ticket.future.done() or ticket.priority == job.priority:
            return
        queues = self._queue(ticket.host)
        queues[ticket.priority].pop(ticket.seq, None)
        ticket.priority = job.priority
        ticket.seq = next(self._seq)
        queues[ticket.priority][ticket.seq] = ticket
        self._dispatch()

    def snapshot(self) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
        waiting = {priority.name.lower(): 0 for priority in Priority}
        hosts: dict[str, dict[str, int]] = {}
        for host in sorted(set(self._queues) | set(self._running)):
            queued = sum(len(queue) for queue in self._queues.get(host, ()))
            for priority, queue in zip(Priority, self._queues.get(host, ())):
                waiting[priority.name.lower()] += len(queue)
            if queued or self._running.get(host):
                hosts[host] = {"active": self._running.get(host, 0), "waiting": queued}
        return waiting, hosts


@dataclass(slots=True)
class _HotEntry:
    namespace: str
    aliases: tuple[str, ...]
    used_at: float
    size: int


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        logger.warning(f"[assets] state file unreadable, starting empty: {path.name} {type(exc).__name__}")
        return None


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)


class HotSet:
    """Public image URLs interactive queries used recently (most recent last)."""

    def __init__(self, path: Path, *, max_entries: int = HOTSET_MAX_ENTRIES):
        self.path = path
        self.max_entries = max_entries
        self.dirty = False
        self._entries: OrderedDict[str, _HotEntry] | None = None

    def _load(self) -> OrderedDict[str, _HotEntry]:
        if self._entries is None:
            entries: OrderedDict[str, _HotEntry] = OrderedDict()
            payload = _read_json(self.path)
            rows = payload.get("entries") if isinstance(payload, dict) else None
            for row in rows if isinstance(rows, list) else ():
                try:
                    entries[str(row["url"])] = _HotEntry(
                        str(row["namespace"]),
                        tuple(str(alias) for alias in row.get("aliases") or ()),
                        float(row["used_at"]),
                        int(row["size"]),
                    )
                except (KeyError, TypeError, ValueError, AttributeError):
                    continue
            self._entries = entries
        return self._entries

    def __len__(self) -> int:
        return len(self._load())

    def note(self, namespace: str, sizes: Mapping[str, int], aliases: Sequence[str] = ()) -> None:
        if not sizes:
            return
        entries = self._load()
        now = time.time()
        for url, size in sizes.items():
            entries.pop(url, None)
            entries[url] = _HotEntry(namespace, tuple(aliases), now, int(size))
        while len(entries) > self.max_entries:
            entries.popitem(last=False)
        self.dirty = True

    def startup_groups(
        self, *, now: float | None = None, max_size: int | None = None
    ) -> dict[tuple[str, tuple[str, ...]], list[str]]:
        """URLs used in the last 24 h by (namespace, aliases), smallest first; large images skipped."""
        cutoff = (time.time() if now is None else now) - HOTSET_WINDOW_SECONDS
        limit = http.ASSET_LARGE_ENTRY_BYTES if max_size is None else max_size
        recent = sorted(
            (entry.size, url, entry)
            for url, entry in self._load().items()
            if entry.used_at >= cutoff and entry.size <= limit
        )
        groups: dict[tuple[str, tuple[str, ...]], list[str]] = {}
        for _size, url, entry in recent:
            groups.setdefault((entry.namespace, entry.aliases), []).append(url)
        return groups

    def clear(self) -> None:
        if self._load():
            self._entries = OrderedDict()
            self.dirty = True

    def dump(self) -> dict[str, Any]:
        self.dirty = False
        return {
            "schema": 1,
            "entries": [
                {
                    "url": url,
                    "namespace": entry.namespace,
                    "aliases": list(entry.aliases),
                    "used_at": round(entry.used_at, 3),
                    "size": entry.size,
                }
                for url, entry in self._load().items()
            ],
        }


class PrefetchBudget:
    """Bytes prefetch downloaded today (Beijing time); survives restarts."""

    def __init__(self, path: Path, *, clock: Callable[[], datetime] | None = None):
        self.path = path
        self.dirty = False
        self._clock = clock or (lambda: datetime.now(_BUDGET_TZ))
        self._day = ""
        self._bytes = 0
        self._loaded = False
        self._logged_day = ""

    @property
    def limit(self) -> int:
        return asset_settings().prefetch_daily_mb * 1024 * 1024

    def _today(self) -> str:
        return self._clock().date().isoformat()

    @property
    def day(self) -> str:
        """Today; a new day starts from zero (a stale file needs no rewrite)."""
        if not self._loaded:
            self._loaded = True
            payload = _read_json(self.path)
            if isinstance(payload, dict):
                try:
                    self._day, self._bytes = str(payload["day"]), max(0, int(payload["bytes"]))
                except (KeyError, TypeError, ValueError):
                    pass
        today = self._today()
        if self._day != today:
            self._day, self._bytes = today, 0
        return today

    @property
    def used(self) -> int:
        _ = self.day  # rolls over to a new day first
        return self._bytes

    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def exhausted(self) -> bool:
        return self.remaining() <= 0

    def charge(self, size: int) -> None:
        if size <= 0:
            return
        day = self.day
        self._bytes += size
        self.dirty = True
        if self._bytes >= self.limit and self._logged_day != day:
            self._logged_day = day
            logger.warning(
                f"[assets] prefetch budget exhausted day={day} bytes={self._bytes} limit={self.limit}"
            )

    def dump(self) -> dict[str, Any]:
        self.dirty = False
        return {"day": self.day, "bytes": self._bytes}


class AssetBroker:
    def __init__(self, state_dir: Path | None = None):
        # None: next to the public image cache (looked up when first needed).
        self._state_dir = state_dir
        self._hotset: HotSet | None = None
        self._budget: PrefetchBudget | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._gate: _Gate | None = None
        self._jobs: dict[tuple[str, str, str], _Job] = {}
        self._spawned: set[asyncio.Task] = set()
        self._save_handle: asyncio.TimerHandle | None = None

    # ------------------------------------------------------------ state

    def _state_path(self, name: str) -> Path:
        return (self._state_dir or http.public_images.path.parent) / name

    @property
    def hotset(self) -> HotSet:
        if self._hotset is None:
            self._hotset = HotSet(self._state_path(HOTSET_FILE))
        return self._hotset

    @property
    def budget(self) -> PrefetchBudget:
        if self._budget is None:
            self._budget = PrefetchBudget(self._state_path(PREFETCH_BUDGET_FILE))
        return self._budget

    def _bind(self) -> _Gate:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            # A different event loop: work owned by the previous one cannot be awaited here.
            self._loop = loop
            self._gate = None
            self._jobs = {}
            self._spawned = set()
            self._save_handle = None
        if self._gate is None:
            settings = asset_settings()
            self._gate = _Gate(
                settings.concurrency, partial(host_concurrency, settings=settings), self._admit
            )
        return self._gate

    def _schedule_save(self) -> None:
        if self._save_handle is None and self._loop is not None:
            self._save_handle = self._loop.call_later(STATE_SAVE_DELAY_SECONDS, self._save_soon)

    def _save_soon(self) -> None:
        self._save_handle = None
        self._spawn(self._save_state())

    async def _save_state(self) -> None:
        writes = [
            (state.path, state.dump())
            for state in (self._hotset, self._budget)
            if state is not None and state.dirty
        ]
        for path, payload in writes:
            try:
                await asyncio.to_thread(_write_json, path, payload)
            except OSError as exc:
                logger.warning(f"[assets] state file not saved: {path.name} {type(exc).__name__}")

    def _spawn(self, coroutine) -> None:
        task = asyncio.get_running_loop().create_task(coroutine)
        self._spawned.add(task)
        task.add_done_callback(self._spawned.discard)

    # ------------------------------------------------------------ requests

    def slot(self, host: str):
        """One asset request to ``host``; the job (and its priority) comes from the task context."""
        return self._bind().slot(host, _current_job.get())

    def _reprioritize(self, job: _Job) -> None:
        if self._gate is not None:  # None only after close()
            self._gate.reprioritize(job)

    def _admit(self, job: _Job) -> bool:
        # A pure prefetch that reaches the front after today's budget ran out is dropped.
        return not (job.waiters == 0 and job.floor is Priority.BACKGROUND and self.budget.exhausted())

    def _key(self, url: str, namespace: str, headers: Mapping[str, str] | None) -> tuple[str, str, str]:
        return (namespace, rewrite_asset_url(url), http._header_fingerprint(headers))

    def _start(self, job: _Job) -> _Job:
        loop = asyncio.get_running_loop()
        self._jobs[job.key] = job
        job.task = loop.create_task(self._run(job))
        job.task.add_done_callback(partial(self._finished, job))
        job.timer = loop.call_later(asset_settings().hard_deadline, self._expire, job)
        return job

    async def _run(self, job: _Job) -> http.HttpResource:
        _current_job.set(job)
        attempt = 0
        while True:
            try:
                return await http.fetch_bytes(
                    job.url,
                    namespace=job.namespace,
                    headers=job.headers,
                    timeout_seconds=job.timeout_seconds,
                    ttl_seconds=job.ttl_seconds,
                    max_bytes=job.max_bytes,
                    asset=True,
                    disk_aliases=job.disk_aliases,
                    memory=job.memory,
                    coalesce=False,
                )
            except PrefetchSkipped:
                raise
            except Exception as exc:
                give_up, _reason = http.classify_failure(exc)
                attempt += 1
                if give_up or attempt >= job.attempts:
                    raise
                await asyncio.sleep(job.base_delay_seconds * 2 ** (attempt - 1))

    def _expire(self, job: _Job) -> None:
        if not job.done:
            job.cancel_reason = "hard_deadline"
            job.task.cancel()

    def _finished(self, job: _Job, task: asyncio.Task) -> None:
        if self._jobs.get(job.key) is job:
            del self._jobs[job.key]
        if job.timer is not None:
            job.timer.cancel()
        error = None if task.cancelled() else task.exception()  # retrieved: no orphan warning
        for refill in job.refills:
            refill.remaining -= 1
            refill.bytes += job.downloaded
            if task.cancelled():
                refill.abandoned += 1
                refill.reason = refill.reason or job.cancel_reason or "cancelled"
            elif error is not None:
                refill.failed += 1
            if not refill.remaining:
                self._log_refill(refill)

    def _log_refill(self, refill: _Refill) -> None:
        if refill.abandoned:
            logger.warning(
                f"{refill.log_prefix} refill abandoned namespace={refill.namespace} "
                f"url_count={refill.abandoned} of={refill.total} reason={refill.reason}"
            )
            return
        logger.info(
            f"{refill.log_prefix} refill done namespace={refill.namespace} url_count={refill.total} "
            f"seconds={time.perf_counter() - refill.started:.3f} bytes={refill.bytes} failed={refill.failed}"
        )

    def _join(self, url: str, **options: Any) -> _Job:
        """The job for ``url`` with one more interactive waiter (started when there is none)."""
        key = self._key(url, options["namespace"], options["headers"])
        job = self._jobs.get(key)
        if job is None or job.done:
            job = self._start(_Job(key, url, source="query", **options))
            job.waiters += 1
            return job
        job.waiters += 1
        if job.waiters == 1:
            self._reprioritize(job)  # a refill or prefetch is first in line again
        if job.floor is Priority.BACKGROUND:
            # A prefetch picked up by a query gets the full hard deadline from now on.
            job.timer.cancel()
            job.timer = asyncio.get_running_loop().call_later(
                asset_settings().hard_deadline, self._expire, job
            )
        return job

    async def fetch_batch(
        self,
        urls: Iterable[str],
        *,
        namespace: str,
        headers: Mapping[str, str] | None,
        timeout_seconds: float | None,
        ttl_seconds: float,
        max_bytes: int,
        attempts: int,
        base_delay_seconds: float,
        deadline: float | None,
        disk_aliases: Sequence[str] = (),
        log_prefix: str = "[assets]",
    ) -> BatchResult:
        """Serve ``urls`` from memory, disk, then the network queue, until the soft ``deadline``.

        ``deadline`` is a ``time.monotonic()`` value (None waits for everything).
        Downloads still running then keep going as refills; their failures read
        ``deadline`` and an older disk copy is used where one exists.
        """
        self._bind()
        unique_urls = tuple(dict.fromkeys(str(url) for url in urls if url))
        resolved, memory_hits, disk_hits = await http._cached_batch(
            unique_urls,
            namespace=namespace,
            headers=headers,
            ttl_seconds=ttl_seconds,
            max_bytes=max_bytes,
            disk_aliases=disk_aliases,
        )
        aliases = tuple(dict.fromkeys(disk_aliases))
        options = {
            "namespace": namespace,
            "headers": headers,
            "timeout_seconds": timeout_seconds,
            "ttl_seconds": ttl_seconds,
            "max_bytes": max_bytes,
            "disk_aliases": aliases,
            "attempts": max(1, attempts),
            "base_delay_seconds": base_delay_seconds,
        }
        jobs = {url: self._join(url, **options) for url in unique_urls if url not in resolved}
        network_started = time.perf_counter()
        late: list[_Job] = []
        try:
            tasks = {job.task for job in jobs.values()}
            if tasks:
                timeout = None
                if deadline is not None:
                    timeout = max(http.DEADLINE_GRACE_SECONDS, deadline - time.monotonic())
                await asyncio.wait(tasks, timeout=timeout)
        finally:
            # Leaving early (deadline or a cancelled caller) never cancels the downloads.
            for job in jobs.values():
                job.waiters -= 1
            late = list({id(job): job for job in jobs.values() if not job.done}.values())
            if late:
                refill = _Refill(namespace, log_prefix, time.perf_counter(), len(late), len(late))
                for job in late:
                    job.refills.append(refill)
                    if job.floor is Priority.BACKGROUND:
                        job.floor = Priority.REFILL
                    self._reprioritize(job)
        network_seconds = time.perf_counter() - network_started if jobs else 0.0
        failures: dict[str, str] = {}
        revalidated = 0
        for url, job in jobs.items():
            task = job.task
            if not task.done() or task.cancelled():
                failures[url] = "deadline"
                continue
            if (error := task.exception()) is not None:
                failures[url] = http.classify_failure(error)[1]
                continue
            resource = task.result()
            if len(resource.content) > max_bytes:
                failures[url] = "too_large"
                continue
            resolved[url] = resource
            revalidated += resource.revalidated
        if failures:
            # 到点没取到或重新校验失败：有旧磁盘副本的先用旧图，不画空白。
            # 没跑完的下载在后台续传，拿到新图后更新缓存，下次渲染就是新的。
            try:
                recovered = await http._stale_copies(
                    list(failures),
                    namespace=namespace,
                    headers=headers,
                    ttl_seconds=ttl_seconds,
                    max_bytes=max_bytes,
                    disk_aliases=disk_aliases,
                )
            except (OSError, sqlite3.Error, httpx.HTTPError) as exc:
                logger.debug(f"{log_prefix} stale lookup failed: {type(exc).__name__}")
                recovered = {}
            for url, resource in recovered.items():
                resolved[url] = resource
                failures.pop(url, None)
        stale = sum(1 for resource in resolved.values() if resource.stale)
        if failures or stale:
            summary = ", ".join(
                f"{url.rsplit('/', 1)[-1][:12]}={failures[url]}" for url in list(failures)[:6]
            )
            log = logger.warning if failures else logger.info
            log(
                f"{log_prefix} fetch {'incomplete' if failures else 'used stale copies'} "
                f"namespace={namespace} requested={len(unique_urls)} resolved={len(resolved)} "
                f"failed={len(failures)} stale={stale}{' deadline=hit' if late else ''}"
                f"{f' detail={summary}' if summary else ''}"
            )
        stats = BatchStats(
            requested=len(unique_urls),
            memory=memory_hits,
            disk=disk_hits,
            network=len(jobs),
            revalidated=revalidated,
            network_seconds=network_seconds,
            soft_hit=bool(late),
            refill=len(late),
        )
        if unique_urls:
            logger.info(
                f"{log_prefix} fetch batch namespace={namespace} requested={stats.requested} "
                f"mem={stats.memory} disk={stats.disk} net={stats.network} "
                f"net_304={stats.revalidated} net_seconds={stats.network_seconds:.3f} "
                f"soft_hit={'true' if stats.soft_hit else 'false'} refill_queued={stats.refill}"
            )
        self._note_hot(namespace, headers, aliases, resolved)
        return BatchResult({url: resolved.get(url) for url in unique_urls}, failures, stats)

    def _note_hot(
        self,
        namespace: str,
        headers: Mapping[str, str] | None,
        aliases: tuple[str, ...],
        resolved: Mapping[str, http.HttpResource],
    ) -> None:
        if headers or http.public_images.budget <= 0:
            return
        sizes = {
            url: len(resource.content)
            for url, resource in resolved.items()
            if not resource.stale
            and http.public_image_request(rewrite_asset_url(url), namespace, None, None, "bytes")
        }
        if sizes:
            self.hotset.note(namespace, sizes, aliases)
            self._schedule_save()

    # ------------------------------------------------------------ prefetch

    async def prefetch(
        self,
        urls: Iterable[str],
        *,
        namespace: str,
        source: str,
        disk_aliases: Sequence[str] = (),
        ttl_seconds: float = PREFETCH_TTL_SECONDS,
        max_bytes: int = PREFETCH_MAX_BYTES,
    ) -> int:
        """Queue background downloads into the disk cache; returns how many were queued.

        Skipped: URLs memory or a fresh disk row already serves, ones already in
        flight, hosts behind the asset proxy, anything the disk cache would not
        keep, and everything once today's budget or the queue limit is reached.
        """
        settings = asset_settings()
        unique_urls = tuple(dict.fromkeys(str(url) for url in urls if url))
        if not unique_urls or not settings.prefetch:
            return 0
        self._bind()
        aliases = tuple(dict.fromkeys(disk_aliases))
        skipped = {"fresh": 0, "proxy": 0, "budget": 0, "ineligible": 0, "dropped": 0}
        queued = 0
        active = sum(1 for job in self._jobs.values() if not job.done)
        for url in unique_urls:
            key = self._key(url, namespace, None)
            if proxied_host(urlsplit(key[1]).hostname or "", settings):
                skipped["proxy"] += 1
                continue
            existing = self._jobs.get(key)
            if existing is not None and not existing.done:
                skipped["fresh"] += 1
                continue
            try:
                fresh = await http._asset_disk_fresh(
                    url, namespace=namespace, ttl_seconds=ttl_seconds, disk_aliases=aliases
                )
            except (OSError, sqlite3.Error):
                fresh = None
            if fresh is None:
                skipped["ineligible"] += 1
                continue
            if fresh:
                skipped["fresh"] += 1
                continue
            if self.budget.exhausted():
                skipped["budget"] += 1
                continue
            if active >= settings.queue_limit:
                skipped["dropped"] += 1
                continue
            if key in self._jobs and not self._jobs[key].done:
                skipped["fresh"] += 1  # queued by a concurrent caller while we checked
                continue
            self._start(
                _Job(
                    key,
                    url,
                    namespace=namespace,
                    headers=None,
                    timeout_seconds=None,
                    ttl_seconds=ttl_seconds,
                    max_bytes=max_bytes,
                    disk_aliases=aliases,
                    attempts=1,  # a miss is retried by the next query, not here
                    base_delay_seconds=0.0,
                    source=source,
                    memory=False,
                )
            )
            queued += 1
            active += 1
        logger.info(
            f"[assets] prefetch source={source} requested={len(unique_urls)} "
            f"skipped_fresh={skipped['fresh']} skipped_proxy={skipped['proxy']} "
            f"skipped_budget={skipped['budget']} skipped_ineligible={skipped['ineligible']} "
            f"dropped={skipped['dropped']} queued={queued}"
        )
        return queued

    def prefetch_soon(self, urls: Iterable[str], **options: Any) -> None:
        """``prefetch`` in the background: a command handler never waits for the cache checks."""
        urls = tuple(urls)
        if not urls or not asset_settings().prefetch:
            return
        self._bind()
        self._spawn(self._prefetch_quietly(urls, options))

    async def _prefetch_quietly(self, urls: tuple[str, ...], options: dict[str, Any]) -> None:
        try:
            await self.prefetch(urls, **options)
        except Exception as exc:  # noqa: BLE001 - prefetch is best effort
            logger.debug(f"[assets] prefetch failed: {type(exc).__name__}")

    async def prefetch_hotset(self) -> int:
        """Warm the disk with what queries used in the last 24 h (restart warm-up)."""
        hotset = self.hotset
        await asyncio.to_thread(hotset._load)
        queued = 0
        for (namespace, aliases), urls in hotset.startup_groups().items():
            queued += await self.prefetch(
                urls, namespace=namespace, source="startup", disk_aliases=aliases
            )
        return queued

    async def drain(self) -> None:
        """Wait until every queued and running download has ended (scripts and tests)."""
        while tasks := [job.task for job in self._jobs.values() if not job.done]:
            await asyncio.gather(*tasks, return_exceptions=True)

    def cancel_background(self) -> int:
        """Cancel pure prefetch jobs (``/ef dev clear``); refills keep going."""
        cancelled = 0
        for job in list(self._jobs.values()):
            if not job.done and job.priority is Priority.BACKGROUND:
                job.cancel_reason = "cleared"
                job.task.cancel()
                cancelled += 1
        return cancelled

    # ------------------------------------------------------------ status and shutdown

    def snapshot(self) -> dict[str, Any]:
        jobs = [job for job in self._jobs.values() if not job.done]
        waiting, hosts = self._gate.snapshot() if self._gate is not None else ({}, {})
        budget = self.budget
        return {
            "inflight": self._gate.active if self._gate is not None else 0,
            "jobs": len(jobs),
            "waiting": {name: waiting.get(name, 0) for name in ("interactive", "refill", "background")},
            "refill": sum(1 for job in jobs if job.priority is Priority.REFILL),
            "background": sum(1 for job in jobs if job.priority is Priority.BACKGROUND),
            "hosts": hosts,
            "prefetch": {
                "day": budget.day,
                "bytes": budget.used,
                "limit_bytes": budget.limit,
                "remaining_bytes": budget.remaining(),
            },
            "hotset": len(self.hotset),
        }

    async def close(self, timeout: float | None = None) -> None:
        """Let interactive downloads and refills finish (bounded), then cancel the rest."""
        timeout = CLOSE_WAIT_SECONDS if timeout is None else timeout
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and self._loop is loop:
            if self._save_handle is not None:
                self._save_handle.cancel()
            spawned = list(self._spawned)
            for task in spawned:
                task.cancel()
            jobs = [job for job in self._jobs.values() if not job.done]
            for job in jobs:
                if job.priority is Priority.BACKGROUND:
                    job.cancel_reason = "shutdown"
                    job.task.cancel()
            pending = [job.task for job in jobs if not job.done]
            if pending and timeout > 0:
                await asyncio.wait(pending, timeout=timeout)
            for job in jobs:
                if not job.done:
                    job.cancel_reason = "shutdown"
                    job.task.cancel()
            await asyncio.gather(*spawned, *(job.task for job in jobs), return_exceptions=True)
            await self._save_state()
        self._loop = None
        self._gate = None
        self._jobs = {}
        self._spawned = set()
        self._save_handle = None
        # Reloaded on next use: the image cache (and so the state directory) may move.
        self._hotset = None
        self._budget = None


broker = AssetBroker()
