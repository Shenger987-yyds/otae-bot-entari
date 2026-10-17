"""The RSSHub race collects every backup fetch, so none is left unretrieved."""

from __future__ import annotations

import asyncio
import gc

import pytest

from plugins.bilibilibot.api import rsshub
from plugins.bilibilibot.api.session import BiliAPIError


def _race(fetchers, *, cancel_after=None):
    """Run first_item over named fetchers; returns (result or exception, unretrieved task errors)."""
    unretrieved = []

    async def scenario():
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: unretrieved.append(context.get("message", "")))

        async def fetch(base_url, route, *, deadline=None):
            return await fetchers[base_url]()

        race = rsshub.first_item(fetch, list(fetchers), "/bilibili/user/video/1")
        try:
            outcome = await (asyncio.wait_for(race, cancel_after) if cancel_after else race)
        except Exception as exc:  # noqa: BLE001 - Returned for the assertions.
            # Drop the frames, which would keep the race's tasks alive past gc.collect().
            exc.__traceback__ = exc.__cause__ = exc.__context__ = None
            outcome = exc
        # Let any straggler finish, then collect it as the loop would eventually.
        for _ in range(10):
            await asyncio.sleep(0)
        gc.collect()
        return outcome

    return asyncio.run(scenario()), unretrieved


def test_a_backup_failing_after_the_winner_is_collected():
    cancelled = []

    async def winner():
        return {"base_url": "https://a", "item": {"title": "ok"}}

    async def stubborn_backup():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(True)
            # Cleanup that fails while being cancelled used to be logged as
            # "Task exception was never retrieved".
            raise BiliAPIError("https://b: connection reset during cancel")

    async def failing_backup():
        raise BiliAPIError("https://c: HTTP 503")

    outcome, unretrieved = _race({"https://a": winner, "https://b": stubborn_backup, "https://c": failing_backup})
    assert outcome == {"title": "ok"}
    assert cancelled == [True]
    assert unretrieved == []


def test_all_backups_failing_reports_them_and_leaves_nothing_behind():
    async def broken(name):
        raise BiliAPIError(f"{name}: HTTP 503")

    outcome, unretrieved = _race({name: (lambda name=name: broken(name)) for name in ("https://a", "https://b")})
    assert isinstance(outcome, BiliAPIError)
    assert "all RSSHub instances unavailable" in str(outcome)
    assert unretrieved == []


def test_a_cancelled_race_still_collects_its_fetches():
    finished = []

    async def slow():
        try:
            await asyncio.sleep(10)
        finally:
            finished.append(True)

    async def fails_on_cancel():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise BiliAPIError("https://b: failed while cancelled")

    outcome, unretrieved = _race({"https://a": slow, "https://b": fails_on_cancel}, cancel_after=0.01)
    assert isinstance(outcome, TimeoutError)
    assert finished == [True]
    assert unretrieved == []


def test_no_instances_is_reported_without_starting_tasks():
    outcome, unretrieved = _race({})
    assert isinstance(outcome, BiliAPIError)
    assert unretrieved == []


# --- 一页多条 ---------------------------------------------------------------

_RSS_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>终末地 的 B站动态</title>
<item><title>第一条</title><link>https://t.bilibili.com/1</link>
<description>&lt;img src="https://i0.hdslb.com/1.jpg"/&gt;正文一</description>
<pubDate>Tue, 14 Oct 2025 08:00:00 GMT</pubDate></item>
<item><title>第二条</title><link>https://t.bilibili.com/2</link>
<description>正文二</description>
<pubDate>Tue, 14 Oct 2025 09:00:00 GMT</pubDate></item>
<item><title>第三条</title><link>https://t.bilibili.com/3</link>
<description>正文三</description>
<pubDate>Tue, 14 Oct 2025 10:00:00 GMT</pubDate></item>
</channel></rss>"""


def test_parse_items_reads_a_bounded_page():
    items = rsshub.parse_items(_RSS_PAGE, limit=2)
    assert [item["link"] for item in items] == [
        "https://t.bilibili.com/1",
        "https://t.bilibili.com/2",
    ]
    assert items[0]["cover_url"] == "https://i0.hdslb.com/1.jpg"
    assert items[0]["author"] == "终末地"
    assert items[1]["cover_url"] == ""


def test_parse_items_rejects_an_empty_feed():
    with pytest.raises(BiliAPIError, match="contains no item"):
        rsshub.parse_items("<rss><channel></channel></rss>")


def test_parse_items_handles_atom_entries():
    page = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>终末地 的 B站动态</title>
<entry><title>甲</title><link href="https://t.bilibili.com/9"/><summary>正文甲</summary>
<updated>2025-10-14T08:00:00Z</updated></entry></feed>"""
    items = rsshub.parse_items(page)
    assert items[0]["link"] == "https://t.bilibili.com/9"
    assert items[0]["description"] == "正文甲"
