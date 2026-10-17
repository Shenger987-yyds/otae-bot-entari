"""终末地接入 AssetBroker：card_detail 后预取、持有率快照后台写、清缓存与状态行。"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock, patch

from otae_bot.infrastructure.http.client import HttpResource
from plugins.endfield import handlers
from plugins.endfield.account import draw as account_draw
from plugins.endfield.account import prefetch
from plugins.endfield.account.detail.draw import _account_detail_icon_urls
from plugins.endfield.account.detail.service import build_account_detail_view
from plugins.endfield.cold_start import (
    ACCOUNT_UI_ASSET_NAMESPACE,
    GACHA_IMAGE_NAMESPACE,
    REMOTE_ASSET_NAMESPACE,
    shared_image_namespaces,
)
from plugins.endfield.providers.assets import sprite_png

HYCDN = "https://bbs.hycdn.cn/image/{}.png"


def _detail() -> dict:
    def equip(name: str) -> dict:
        return {"equipData": {"name": name, "iconUrl": HYCDN.format(name)}}

    return {
        "base": {"name": "测试管理员", "avatarUrl": HYCDN.format("avatar")},
        "chars": [
            {
                "charData": {
                    "id": "chr_a",
                    "name": "甲",
                    "profession": {"key": "profession_guard"},
                    "property": {"key": "char_property_cryst"},
                    "avatarSqUrl": HYCDN.format("a-square"),
                    "avatarRtUrl": HYCDN.format("a-rect"),
                    "skills": [{"id": "s1", "iconUrl": HYCDN.format("skill-1")}, {"id": "s2", "iconUrl": ""}],
                },
                "weapon": {"weaponData": {"name": "剑", "iconUrl": HYCDN.format("weapon")}},
                "bodyEquip": equip("body"),
                "secondAccessory": equip("accessory"),
                "tacticalItem": {"tacticalItemData": {"name": "饮料", "iconUrl": HYCDN.format("tactical")}},
            },
            {
                "charData": {
                    "id": "chr_b",
                    "name": "乙",
                    "profession": {"key": "profession_caster"},
                    "property": {"key": "char_property_fire"},
                    "avatarSqUrl": HYCDN.format("b-square"),
                    "skills": [{"id": "s1", "iconUrl": HYCDN.format("skill-1")}],
                },
            },
            "not-a-character",
        ],
    }


class DetailAssetUrlTests(unittest.TestCase):
    def test_urls_match_the_detail_card_plus_class_and_element_icons(self):
        detail = _detail()
        card = set(_account_detail_icon_urls(build_account_detail_view(detail, uid="x")))
        semantic = {
            sprite_png("charprofessionicon", "icon_profession_0_s"),
            sprite_png("elementicon", "icon_charattrtype_cold"),
            sprite_png("charprofessionicon", "icon_profession_5_s"),
            sprite_png("elementicon", "icon_charattrtype_fire"),
        }
        urls = prefetch.detail_asset_urls(detail)
        self.assertEqual(len(urls), len(set(urls)))
        self.assertEqual(set(urls), card | semantic)
        self.assertNotIn(HYCDN.format("a-rect"), urls)  # the detail card never draws it

    def test_empty_or_odd_payloads_give_no_urls(self):
        self.assertEqual(prefetch.detail_asset_urls({}), ())
        self.assertEqual(prefetch.detail_asset_urls({"chars": None, "base": "x"}), ())

    def test_prefetch_goes_to_the_detail_namespace_with_its_aliases(self):
        with patch.object(prefetch.broker, "prefetch_soon") as soon:
            prefetch.prefetch_detail_assets(_detail(), source="card_detail")
        options = soon.call_args.kwargs
        self.assertEqual(soon.call_args.args[0], prefetch.detail_asset_urls(_detail()))
        self.assertEqual(
            (options["namespace"], options["source"], options["disk_aliases"]),
            (REMOTE_ASSET_NAMESPACE, "card_detail", (GACHA_IMAGE_NAMESPACE, ACCOUNT_UI_ASSET_NAMESPACE)),
        )

    def test_three_image_namespaces_borrow_each_other(self):
        for namespace in (REMOTE_ASSET_NAMESPACE, GACHA_IMAGE_NAMESPACE, ACCOUNT_UI_ASSET_NAMESPACE):
            aliases = shared_image_namespaces(namespace)
            self.assertEqual(len(aliases), 2)
            self.assertNotIn(namespace, aliases)


class BrokerWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_clearing_icons_forgets_the_hot_set_and_queued_prefetches(self):
        broker = Mock()
        with patch.object(handlers, "asset_broker", broker), patch.object(
            handlers, "clear_http_cache", AsyncMock(return_value=0)
        ):
            await handlers._clear_endfield_caches("icon")
            broker.hotset.clear.assert_called_once_with()
            broker.cancel_background.assert_called_once_with()
            broker.reset_mock()
            await handlers._clear_endfield_caches("operator")
            broker.hotset.clear.assert_not_called()

    async def test_status_shows_the_asset_queue(self):
        diagnostics = {
            "pools": {
                "assets": {"bytes": 0, "budget_bytes": 64 << 20},
                "i18n": {"bytes": 0, "budget_bytes": 64 << 20},
            },
            "namespaces": {},
            "broker": {
                "inflight": 3,
                "waiting": {"interactive": 1, "refill": 2, "background": 40},
                "refill": 5,
                "prefetch": {"bytes": 12 << 20, "remaining_bytes": 288 << 20},
            },
        }
        with patch.object(handlers, "get_http_cache_diagnostics", AsyncMock(return_value=diagnostics)):
            lines = await handlers._cache_status_lines()
        self.assertIn(
            "素材队列: 在途 3 / 排队 交互 1 续传 2 后台 40 / 续传中 5 / 今日预取 12.0 MiB / 剩余 288.0 MiB",
            lines,
        )
        self.assertTrue(lines[-1].startswith("缓存策略"))

    async def test_account_overview_borrows_other_namespaces_and_reports_network_counts(self):
        from otae_bot.infrastructure.http.assets import BatchResult, BatchStats
        from plugins.endfield.rendering.health import track_render_health

        url = "https://bbs.hycdn.cn/image/overview.png"

        async def fake_fetch(urls, **kwargs):
            self.assertEqual(kwargs["disk_aliases"], shared_image_namespaces(ACCOUNT_UI_ASSET_NAMESPACE))
            return BatchResult(
                {url: HttpResource(b"img", "image/png", 200, url)},
                {},
                BatchStats(requested=1, network=1, revalidated=1, refill=0),
            )

        with track_render_health() as health, patch.object(account_draw, "fetch_many_resilient", fake_fetch):
            prepared = await account_draw._prepare_assets([url], inline=True)
        self.assertTrue(prepared.urls[url].startswith("data:image/png;base64,"))
        self.assertEqual((health.network, health.revalidated, health.refill), (1, 1, 0))


if __name__ == "__main__":
    unittest.main()
