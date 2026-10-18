from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from plugins.endfield.catalog.models import TermStyleView
from plugins.endfield.encyclopedia.draw import _rich
from plugins.endfield.rendering.cards import highlight_terms
from plugins.endfield.richtext import (
    iter_rich_tags,
    rich_tag_at,
    stray_rich_tag_len,
)

# 祀 P2 原文：外层 <@ba.vup> 里嵌了两个 <#…> 词条，线上就是这条把标记印到了卡上。
NESTED = (
    "施放连携技<@ba.key>成竹在胸</>时，攻击力<@ba.vup>+[8%+3%×"
    "<#ba.consume>消耗</>或<#ba.absorb>吸收</>的最高附着层数]</>，持续10秒，无法叠加。"
)


class RichTagParsingTest(unittest.TestCase):
    def test_nested_tag_closes_by_depth(self):
        tag = rich_tag_at(NESTED, NESTED.index("<@ba.vup>"))
        self.assertIsNotNone(tag)
        kind, tag_id, inner, end = tag
        self.assertEqual((kind, tag_id), ("@", "ba.vup"))
        self.assertIn("<#ba.consume>消耗</>", inner)
        self.assertIn("<#ba.absorb>吸收</>", inner)
        self.assertEqual(NESTED[end - 3 : end], "</>")

    def test_unclosed_tag_is_not_a_tag(self):
        self.assertIsNone(rich_tag_at("<#ba.consume>消耗", 0))
        self.assertEqual(
            stray_rich_tag_len("<#ba.consume>消耗", 0), len("<#ba.consume>")
        )

    def test_iter_rich_tags_includes_nested_entries(self):
        entries = list(iter_rich_tags(NESTED))
        ids = [tag_id for _, tag_id, _ in entries]
        self.assertIn("ba.vup", ids)
        self.assertIn("ba.consume", ids)
        self.assertIn("ba.absorb", ids)
        outer = next(inner for _, tag_id, inner in entries if tag_id == "ba.vup")
        self.assertIn("<#ba.consume>消耗</>", outer)


class OperatorCardRichTextTest(unittest.TestCase):
    def test_nested_tags_render_inner_terms(self):
        html = highlight_terms(NESTED, {}, {})
        self.assertNotIn("&lt;", html)
        self.assertNotIn("&gt;", html)
        self.assertIn('<span class="term">消耗</span>', html)
        self.assertIn('<span class="term">吸收</span>', html)
        self.assertIn('<span class="vup">', html)

    def test_nested_term_uses_its_own_style(self):
        styles = {"ba.consume": TermStyleView("ba.consume", "#ff8e59", "")}
        html = highlight_terms(NESTED, styles, {})
        self.assertIn("--term-color: #ff8e59", html)

    def test_unclosed_tag_never_leaks(self):
        html = highlight_terms("攻击<#ba.consume>消耗", {}, {})
        self.assertNotIn("&lt;", html)
        self.assertIn("消耗", html)

    def test_stray_close_never_leaks(self):
        html = highlight_terms("攻击力<@ba.vup>+8%</></>", {}, {})
        self.assertNotIn("&lt;/", html)
        self.assertIn("攻击力", html)


class EncyclopediaRichTextTest(unittest.TestCase):
    def test_nested_tags_render_inner_strong(self):
        html = _rich(NESTED, {})
        self.assertNotIn("&lt;", html)
        self.assertIn("消耗", html)
        self.assertIn("<strong", html)

    def test_unclosed_tag_never_leaks(self):
        html = _rich("攻击<#ba.consume>消耗", {})
        self.assertNotIn("&lt;", html)
        self.assertIn("消耗", html)


if __name__ == "__main__":
    unittest.main()
