from __future__ import annotations

import asyncio
import unittest

from otae_bot.infrastructure.cache import AsyncTTLCache


class CacheLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def cache(self):
        cache = AsyncTTLCache(ttl_seconds=60, max_bytes=1024, sizeof=len)
        self.addAsyncCleanup(cache.close)
        return cache

    async def test_post_clear_reader_does_not_join_old_work(self):
        cache = self.cache()
        started, release = asyncio.Event(), asyncio.Event()

        async def old():
            started.set()
            await release.wait()
            return b"old"

        async def new():
            return b"new"

        first = asyncio.create_task(cache.get_or_create("key", old))
        await started.wait()
        await cache.clear()
        self.assertEqual(await cache.get_or_create("key", new), b"new")
        release.set()
        self.assertEqual(await first, b"old")
        self.assertEqual(await cache.get_or_create("key", old), b"new")

    async def test_scoped_clear_does_not_discard_other_flights(self):
        cache = self.cache()
        started, release = asyncio.Event(), asyncio.Event()

        async def factory():
            started.set()
            await release.wait()
            return b"value"

        task = asyncio.create_task(cache.get_or_create("weapon", factory))
        await started.wait()
        await cache.clear(lambda key: key == "operator")
        release.set()
        await task
        self.assertEqual((await cache.stats()).entries, 1)

    async def test_cancelled_owner_does_not_remove_shared_work(self):
        cache = self.cache()
        started, release = asyncio.Event(), asyncio.Event()
        calls = 0

        async def factory():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return b"value"

        owner = asyncio.create_task(cache.get_or_create("key", factory))
        await started.wait()
        owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner
        follower = asyncio.create_task(cache.get_or_create("key", factory))
        await asyncio.sleep(0)
        release.set()
        self.assertEqual(await follower, b"value")
        self.assertEqual(calls, 1)
        self.assertEqual((await cache.stats()).entries, 1)

    async def test_no_waiters_still_publish_and_retrieve_failures(self):
        cache = self.cache()
        started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def factory():
            started.set()
            await release.wait()
            finished.set()
            raise ValueError("failed")

        owner = asyncio.create_task(cache.get_or_create("key", factory))
        await started.wait()
        owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner
        release.set()
        await finished.wait()
        await asyncio.sleep(0)
        self.assertEqual((await cache.stats()).entries, 0)
        self.assertEqual((await cache.stats()).failures, 1)
        self.assertFalse(cache._tasks)

    async def test_close_drains_detached_work(self):
        cache = self.cache()
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def factory():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        owner = asyncio.create_task(cache.get_or_create("key", factory))
        await started.wait()
        await cache.clear()
        await cache.close()
        self.assertTrue(cancelled.is_set())
        with self.assertRaises(asyncio.CancelledError):
            await owner
        self.assertFalse(cache._tasks)

    async def test_metric_events_distinguish_direct_hits_and_waiting(self):
        events = []
        cache = AsyncTTLCache(
            ttl_seconds=60,
            max_bytes=1024,
            sizeof=len,
            on_event=lambda key, event, value: events.append((key, event, value)),
        )
        self.addAsyncCleanup(cache.close)

        async def factory():
            await asyncio.sleep(0)
            return b"value"

        await asyncio.gather(*(cache.get_or_create("key", factory) for _ in range(4)))
        await cache.get_or_create("key", factory)
        stats = await cache.stats()
        self.assertEqual((stats.direct_hits, stats.coalesced, stats.misses), (1, 3, 1))
        self.assertGreater(stats.fill_seconds, 0)
        self.assertGreater(stats.wait_seconds, 0)
        self.assertEqual(sum(event == "coalesced" for _, event, _ in events), 3)

    async def test_expiration_and_capacity_metrics_are_separate(self):
        now = [0.0]
        cache = AsyncTTLCache(
            ttl_seconds=5, max_bytes=4, sizeof=len, clock=lambda: now[0]
        )
        self.addAsyncCleanup(cache.close)

        async def factory():
            return b"abc"

        await cache.get_or_create("a", factory)
        await cache.get_or_create("b", factory)
        self.assertEqual((await cache.stats()).capacity_evictions, 1)
        now[0] = 6
        stats = await cache.stats()
        self.assertEqual((stats.expirations, stats.evictions), (1, 2))

    async def test_peek_counts_a_hit_without_extending_or_filling(self):
        now = [0.0]
        cache = AsyncTTLCache(ttl_seconds=5, max_bytes=1024, sizeof=len, clock=lambda: now[0])
        self.addAsyncCleanup(cache.close)
        self.assertIsNone(cache.peek("key"))
        self.assertEqual((await cache.stats()).misses, 0)  # a peek miss is not a fill
        started, release = asyncio.Event(), asyncio.Event()

        async def factory():
            started.set()
            await release.wait()
            return b"value"

        task = asyncio.create_task(cache.get_or_create("key", factory))
        await started.wait()
        self.assertIsNone(cache.peek("key"))  # in flight: nothing stored yet
        release.set()
        await task
        self.assertEqual(cache.peek("key"), b"value")
        self.assertEqual((await cache.stats()).direct_hits, 1)
        now[0] = 4.9
        self.assertEqual(cache.peek("key"), b"value")
        now[0] = 5.0  # a peek never renews the entry
        self.assertIsNone(cache.peek("key"))

    async def test_put_follows_the_admission_rules_of_a_fill(self):
        events = []
        cache = AsyncTTLCache(
            ttl_seconds=60,
            max_bytes=10,
            sizeof=len,
            max_entry_bytes=4,
            may_bypass=lambda value: not value.startswith(b"keep"),
            on_event=lambda key, event, value: events.append((key, event)),
            ttl_for_value=lambda value: 0 if value == b"no" else float("inf"),
        )
        self.addAsyncCleanup(cache.close)
        self.assertTrue(cache.put("small", b"abc"))
        self.assertFalse(cache.put("large", b"abcdef"))  # over the per-entry cap: bypassed
        self.assertTrue(cache.put("kept", b"keep-me"))  # cap applies only where may_bypass agrees
        self.assertFalse(cache.put("expired", b"no"))
        self.assertFalse(cache.put("huge", b"keep" * 3))  # over the pool budget: oversized
        self.assertEqual(cache.peek("small"), b"abc")
        self.assertIsNone(cache.peek("large"))
        self.assertEqual(cache.peek("kept"), b"keep-me")
        stats = await cache.stats()
        self.assertEqual((stats.entries, stats.bytes, stats.oversized), (2, 10, 1))
        self.assertIn(("large", "bypassed"), events)
        self.assertTrue(cache.put("next", b"xyz"))  # byte budget evicts the oldest
        self.assertIsNone(cache.peek("small"))
        self.assertEqual((await cache.stats()).capacity_evictions, 1)

    async def test_large_fill_is_shared_but_not_stored(self):
        events = []
        cache = AsyncTTLCache(
            ttl_seconds=60,
            max_bytes=1024,
            sizeof=len,
            max_entry_bytes=4,
            on_event=lambda key, event, value: events.append(event),
        )
        self.addAsyncCleanup(cache.close)
        calls = 0

        async def factory():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return b"large-value"

        results = await asyncio.gather(*(cache.get_or_create("key", factory) for _ in range(3)))
        self.assertEqual(results, [b"large-value"] * 3)
        self.assertEqual(calls, 1)  # concurrent callers still coalesce
        self.assertEqual(events.count("bypassed"), 1)
        self.assertEqual((await cache.stats()).entries, 0)
        await cache.get_or_create("key", factory)
        self.assertEqual(calls, 2)

    async def test_put_does_not_disturb_an_inflight_fill(self):
        cache = self.cache()
        started, release = asyncio.Event(), asyncio.Event()

        async def factory():
            started.set()
            await release.wait()
            return b"filled"

        task = asyncio.create_task(cache.get_or_create("key", factory))
        await started.wait()
        self.assertTrue(cache.put("key", b"put"))
        self.assertEqual(cache.peek("key"), b"put")
        follower = asyncio.create_task(cache.get_or_create("key", factory))
        self.assertEqual(await follower, b"put")  # a stored value wins over joining the flight
        release.set()
        self.assertEqual(await task, b"filled")
        self.assertEqual(cache.peek("key"), b"filled")  # the fill finished last
