"""官号动态的 RSS 详情回查链路：RSS 只给 ID，正文回查官方 detail。

覆盖三件事：detail 的结构校验、从 RSS 链接提取可验证的 ID、以及 space 接口
失败时整页回查而不是退化成一条卡片。
"""

from __future__ import annotations

import asyncio

import pytest

from plugins.bilibilibot.api import space
from plugins.bilibilibot.api.session import BiliAPIError
from plugins.bilibilibot.official import ENDFIELD_OFFICIAL_UID


def _opus_item(item_id: str = "1", *, mid: str = ENDFIELD_OFFICIAL_UID, text: str = "正文") -> dict:
    return {
        "id_str": item_id,
        "type": "DYNAMIC_TYPE_WORD",
        "modules": {
            "module_author": {"mid": mid, "pub_ts": 1700000000},
            "module_dynamic": {
                "desc": None,
                "major": {
                    "type": "MAJOR_TYPE_OPUS",
                    "opus": {
                        "summary": {"text": text},
                        "jump_url": "//www.bilibili.com/opus/1",
                    },
                },
            },
        },
    }


class FakeApi:
    """只实现 space 模块用到的门面方法。"""

    def __init__(self, *, feed=None, detail=None, hints=None):
        self.feed = feed
        self.detail = detail or {}
        self.hints = hints or []
        self.detail_calls = []
        self.rsshub_calls = []

    async def _within(self, label, deadline, factory):
        return await factory()

    async def _get_json_with_risk_retry(self, url, label, params=None):
        if self.feed is None:
            raise BiliAPIError("space api down")
        return self.feed

    async def rsshub_items(self, route, *, deadline=None):
        self.rsshub_calls.append(route)
        return list(self.hints)

    async def official_dynamic_detail(self, item_id, *, deadline=None):
        self.detail_calls.append(item_id)
        return self.detail[item_id]

    def _rss_card_to_dynamic_item(self, card, uid):
        return space.rss_card_to_dynamic_item(card, uid)

    async def dynamic_items(self, uid, *, deadline=None):
        return await space.dynamic_items(self, uid, deadline=deadline)


# --- 结构校验 ---------------------------------------------------------------


def test_opus_without_summary_is_rejected():
    item = _opus_item()
    item["modules"]["module_dynamic"]["major"]["opus"] = {}
    with pytest.raises(BiliAPIError, match="Opus 缺少正文摘要"):
        space.validate_official_dynamic(item)


def test_legacy_draw_without_desc_is_rejected():
    item = {
        "id_str": "1",
        "modules": {
            "module_author": {"mid": ENDFIELD_OFFICIAL_UID},
            "module_dynamic": {"desc": None, "major": {"type": "MAJOR_TYPE_DRAW"}},
        },
    }
    with pytest.raises(BiliAPIError, match="缺少 Opus 正文"):
        space.validate_official_dynamic(item)


def test_forward_without_original_is_rejected():
    item = _opus_item()
    item["type"] = "DYNAMIC_TYPE_FORWARD"
    with pytest.raises(BiliAPIError, match="缺少原动态"):
        space.validate_official_dynamic(item)


def test_forward_checks_the_original_too():
    item = _opus_item()
    item["type"] = "DYNAMIC_TYPE_FORWARD"
    item["orig"] = _opus_item("2")
    item["orig"]["modules"]["module_dynamic"] = None
    with pytest.raises(BiliAPIError, match="缺少正文结构"):
        space.validate_official_dynamic(item)


def test_valid_opus_passes():
    space.validate_official_dynamic(_opus_item())


# --- RSS 链接提取与详情回查 --------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


def test_rss_links_become_deduplicated_ids():
    api = FakeApi(
        hints=[
            {"link": "https://t.bilibili.com/100"},
            {"link": "https://www.bilibili.com/opus/200"},
            {"link": "https://t.bilibili.com/100"},
        ],
        detail={"100": _opus_item("100"), "200": _opus_item("200")},
    )
    items = _run(space.official_rss_dynamics(api))
    assert [item["id_str"] for item in items] == ["100", "200"]
    assert api.detail_calls == ["100", "200"]
    assert api.rsshub_calls == [space.DYNAMIC_ROUTE.format(uid=ENDFIELD_OFFICIAL_UID)]


def test_unverifiable_rss_link_is_rejected():
    api = FakeApi(hints=[{"link": "https://example.com/100"}])
    with pytest.raises(BiliAPIError, match="可验证的动态链接"):
        _run(space.official_rss_dynamics(api))


def test_non_numeric_rss_id_is_rejected():
    api = FakeApi(hints=[{"link": "https://t.bilibili.com/abc"}])
    with pytest.raises(BiliAPIError, match="动态 ID 无效"):
        _run(space.official_rss_dynamics(api))


def test_empty_rss_page_is_rejected():
    with pytest.raises(BiliAPIError, match="RSS 未提供动态"):
        _run(space.official_rss_dynamics(FakeApi(hints=[])))


def test_one_failed_detail_fails_the_whole_page():
    """部分成功不能推进游标，否则没拿到的那条会被永久跳过。"""

    api = FakeApi(
        hints=[{"link": "https://t.bilibili.com/100"}, {"link": "https://t.bilibili.com/200"}],
        detail={"100": _opus_item("100")},
    )

    async def detail(item_id, *, deadline=None):
        api.detail_calls.append(item_id)
        if item_id == "200":
            raise BiliAPIError("detail 200 down")
        return api.detail[item_id]

    api.official_dynamic_detail = detail
    with pytest.raises(BiliAPIError, match="detail 200 down"):
        _run(space.official_rss_dynamics(api))


# --- space 失败时的整体回退 --------------------------------------------------


def test_official_feed_failure_falls_back_to_rss_details():
    api = FakeApi(
        hints=[{"link": "https://t.bilibili.com/100"}],
        detail={"100": _opus_item("100")},
    )
    items = _run(space.dynamic_items(api, ENDFIELD_OFFICIAL_UID))
    assert [item["id_str"] for item in items] == ["100"]


def test_official_feed_failure_reports_both_sides():
    api = FakeApi(hints=[])
    with pytest.raises(BiliAPIError, match="终末地官号动态采集失败") as excinfo:
        _run(space.dynamic_items(api, ENDFIELD_OFFICIAL_UID))
    assert "space api down" in str(excinfo.value)


def test_other_accounts_still_degrade_to_a_single_card():
    api = FakeApi()

    async def latest_dynamic(uid, reason, *, deadline=None):
        from plugins.bilibilibot.models import BiliCard, KIND_DYNAMIC

        return BiliCard(KIND_DYNAMIC, "标题", uid=uid, item_id="9")

    api._rsshub_latest_dynamic = latest_dynamic
    items = _run(space.dynamic_items(api, "12345"))
    assert items[0]["id_str"] == "9"


def test_official_feed_items_are_validated():
    api = FakeApi(feed={"data": {"items": [{"modules": {}}]}})
    with pytest.raises(BiliAPIError, match="缺少正文结构"):
        _run(space.dynamic_items(api, ENDFIELD_OFFICIAL_UID))


def test_latest_dynamic_ignores_a_pinned_older_post():
    api = FakeApi(
        feed={
            "data": {
                "items": [
                    _opus_item("pinned"),
                    _opus_item("newest"),
                ]
            }
        }
    )
    api.feed["data"]["items"][0]["modules"]["module_author"]["pub_ts"] = 1600000000
    api.feed["data"]["items"][1]["modules"]["module_author"]["pub_ts"] = 1700000000
    card = _run(space.latest_dynamic(api, ENDFIELD_OFFICIAL_UID))
    assert card.item_id == "newest"
