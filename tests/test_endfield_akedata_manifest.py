"""AKEData manifest：60 秒内直接用、60–600 秒先给旧值并后台单飞刷新、刷新失败保留旧值、换版本清表。

不访问网络：``akedata._get`` 换成按调用计数的假实现。
"""

from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from plugins.endfield.providers import akedata


def manifest(latest: str, shared: str = "r1") -> dict:
    return {
        "latest": latest,
        "sharedRevision": shared,
        "versions": [{"id": latest, "tableCfgPath": f"public/{latest.split('@')[0]}/1-1/TableCfg"}],
    }


class ManifestStaleWhileRevalidateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        akedata.clear_i18n_process_warm()
        self.addCleanup(akedata.clear_i18n_process_warm)
        self.current: dict | Exception = manifest("1.5.3@1-1")
        self.gate: asyncio.Event | None = None
        self.paths: list[tuple[str, float | None]] = []

        async def fake_get(path, *, ttl_seconds=None, **_kwargs):
            self.paths.append((path, ttl_seconds))
            if self.gate is not None:
                await self.gate.wait()
            if isinstance(self.current, Exception):
                raise self.current
            return self.current

        self.get = patch.object(akedata, "_get", side_effect=fake_get)
        self.get.start()
        self.addCleanup(self.get.stop)
        self.clear = patch.object(akedata, "clear_http_cache", AsyncMock(return_value=0))
        self.clear_mock = self.clear.start()
        self.addCleanup(self.clear.stop)

    def age(self, seconds: float) -> None:
        akedata._manifest.fetched_at = time.monotonic() - seconds

    async def settle_background(self) -> None:
        task = akedata._manifest.task
        if task is not None:
            await asyncio.wait({task})

    async def test_fresh_manifest_needs_no_request(self):
        first = await akedata.fetch_akedata_manifest()
        second = await akedata.fetch_akedata_manifest()
        self.assertIs(first, second)
        self.assertEqual(len(self.paths), 1)
        path, ttl = self.paths[0]
        self.assertTrue(path.startswith("/manifest.json?t="))  # still busts CDN caches
        self.assertEqual(ttl, 0)  # memory caching is the state above, not the API pool

    async def test_concurrent_cold_callers_share_one_request(self):
        self.gate = asyncio.Event()
        callers = [asyncio.create_task(akedata.fetch_akedata_manifest()) for _ in range(5)]
        await asyncio.sleep(0)
        self.gate.set()
        results = await asyncio.gather(*callers)
        self.assertEqual(len(self.paths), 1)
        self.assertTrue(all(result is results[0] for result in results))

    async def test_stale_manifest_is_served_while_one_background_refresh_runs(self):
        old = await akedata.fetch_akedata_manifest()
        self.age(120)
        self.current = manifest("1.5.3@1-1", "r1")
        self.gate = asyncio.Event()
        served = await asyncio.wait_for(
            asyncio.gather(*(akedata.fetch_akedata_manifest() for _ in range(5))), timeout=1
        )
        self.assertTrue(all(result is old for result in served))  # no caller waited
        self.assertEqual(len(self.paths), 2)  # one background refresh, not five
        self.gate.set()
        await self.settle_background()
        refreshed = await akedata.fetch_akedata_manifest()
        self.assertIsNot(refreshed, old)
        self.assertEqual(len(self.paths), 2)
        self.clear_mock.assert_not_awaited()  # same build: nothing to drop

    async def test_long_unrefreshed_manifest_waits_for_the_network(self):
        await akedata.fetch_akedata_manifest()
        self.age(akedata.MANIFEST_STALE_LIMIT_SECONDS + 1)
        self.current = manifest("1.5.4@2-2")
        self.assertEqual((await akedata.fetch_akedata_manifest())["latest"], "1.5.4@2-2")

    async def test_failed_refresh_keeps_the_last_manifest_and_warns_once(self):
        old = await akedata.fetch_akedata_manifest()
        self.current = RuntimeError("cdn down")
        self.age(akedata.MANIFEST_STALE_LIMIT_SECONDS + 1)
        with patch.object(akedata, "logger") as log:
            self.assertIs(await akedata.fetch_akedata_manifest(), old)
            requests = len(self.paths)
            # Back inside the stale window with a retry delay: callers do not wait or re-ask.
            self.assertIs(await akedata.fetch_akedata_manifest(), old)
            self.assertEqual(len(self.paths), requests)
            akedata._manifest.retry_at = 0.0
            self.assertIs(await akedata.fetch_akedata_manifest(), old)
            await self.settle_background()
            self.assertEqual(len(self.paths), requests + 1)
        self.assertEqual(log.warning.call_count, 1)  # rate limited
        self.assertIn("keeping latest=1.5.3@1-1", log.warning.call_args.args[0])

    async def test_first_fetch_failure_and_malformed_manifest_raise(self):
        self.current = RuntimeError("cdn down")
        with self.assertRaisesRegex(RuntimeError, "cdn down"):
            await akedata.fetch_akedata_manifest()
        self.current = {"versions": []}
        with self.assertRaisesRegex(RuntimeError, "结构异常"):
            await akedata.fetch_akedata_manifest()

    async def test_changed_build_drops_cached_tables_and_the_i18n_warm_flag(self):
        await akedata.fetch_akedata_manifest()
        akedata.remember_i18n_loaded("/public/1.5.3/1-1/TableCfg/I18nTextTable_CN.json")
        for changed in (manifest("1.5.3@1-1", "r2"), manifest("1.5.4@2-2", "r2")):
            akedata.remember_i18n_loaded("/public/1.5.3/1-1/TableCfg/I18nTextTable_CN.json")
            self.clear_mock.reset_mock()
            self.current = changed
            self.age(akedata.MANIFEST_STALE_LIMIT_SECONDS + 1)
            with patch.object(akedata, "logger") as log:
                self.assertIs(await akedata.fetch_akedata_manifest(), changed)
            self.clear_mock.assert_awaited_once_with("akedata", include_disk=False)
            self.assertFalse(akedata.i18n_process_warm())
            self.assertIn("manifest changed", log.info.call_args.args[0])
            # The new manifest stays cached; the change is handled once.
            self.assertIs(await akedata.fetch_akedata_manifest(), changed)

    async def test_reset_forgets_the_manifest(self):
        await akedata.fetch_akedata_manifest()
        akedata.reset_manifest_state()
        await akedata.fetch_akedata_manifest()
        self.assertEqual(len(self.paths), 2)
        akedata.clear_i18n_process_warm()  # /ef dev clear goes through this one
        await akedata.fetch_akedata_manifest()
        self.assertEqual(len(self.paths), 3)


if __name__ == "__main__":
    unittest.main()
