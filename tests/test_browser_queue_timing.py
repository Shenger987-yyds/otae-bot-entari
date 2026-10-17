"""浏览器线程排队计时：只换掉同步截图函数，不启动 Chromium。"""

from __future__ import annotations

import asyncio
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from otae_bot.infrastructure.rendering import browser


class ScreenshotQueueTimingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fake-playwright")
        self.addCleanup(self.executor.shutdown, wait=True)
        patcher = patch.object(browser, "_get_browser_executor", return_value=self.executor)
        patcher.start()
        self.addCleanup(patcher.stop)

        def slow_screenshot(*_args):
            time.sleep(0.2)
            return b"png"

        def quick_evaluate(*_args):
            return {"ok": True}

        for name, fake in (
            ("_screenshot_web_element_sync", slow_screenshot),
            ("_evaluate_web_page_sync", quick_evaluate),
        ):
            patcher = patch.object(browser, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_queue_wait_grows_behind_an_earlier_screenshot(self):
        async def timed_call(delay: float):
            await asyncio.sleep(delay)
            with browser.screenshot_timing() as timing:
                result = await browser.screenshot_web_element("file:///card.html")
            return result, timing

        with patch.object(browser, "logger") as log:
            (first, first_timing), (second, second_timing) = await asyncio.gather(
                timed_call(0), timed_call(0.02)
            )
        self.assertEqual((first, second), (b"png", b"png"))
        self.assertLess(first_timing.queue_wait, 0.1)
        self.assertGreater(second_timing.queue_wait, 0.1)  # waited for the first one's 0.2 s
        self.assertGreaterEqual(second_timing.run, 0.15)
        self.assertEqual((first_timing.calls, second_timing.calls), (1, 1))
        log.warning.assert_not_called()  # below the 1 s alarm

    async def test_scopes_add_up_and_long_waits_are_reported(self):
        with patch.object(browser, "QUEUE_WAIT_WARNING_SECONDS", 0.05), patch.object(browser, "logger") as log:
            blocker = asyncio.create_task(browser.screenshot_web_element("file:///a.html"))
            await asyncio.sleep(0.02)
            with browser.screenshot_timing() as timing:
                await browser.screenshot_web_element("file:///b.html")
                self.assertEqual(await browser.evaluate_web_page("file:///b.html", "1"), {"ok": True})
            await blocker
        self.assertEqual(timing.calls, 2)
        self.assertGreater(timing.queue_wait, 0.1)
        log.warning.assert_called_once()
        self.assertIn("queue_wait=", log.warning.call_args.args[0])

    async def test_calls_outside_a_scope_are_not_recorded(self):
        with browser.screenshot_timing() as timing:
            pass
        await browser.screenshot_web_element("file:///c.html")
        self.assertEqual((timing.queue_wait, timing.run, timing.calls), (0.0, 0.0, 0))


if __name__ == "__main__":
    unittest.main()
