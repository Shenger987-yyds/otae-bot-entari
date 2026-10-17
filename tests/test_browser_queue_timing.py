"""渲染槽位排队计时：只换掉页面上的截图工作，不启动 Chromium。"""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from otae_bot.infrastructure.rendering import browser
from tests.support.browser import FakeBrowserHarness


async def slow_screenshot(*_args):
    await asyncio.sleep(0.2)
    return b"png"


async def quick_evaluate(*_args):
    return {"ok": True}


class ScreenshotQueueTimingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await FakeBrowserHarness(
            self, concurrency=1, screenshot=slow_screenshot, evaluate=quick_evaluate
        ).start()

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
