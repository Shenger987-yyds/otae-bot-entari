"""词条数值表：异常反应的固定配置，按异常等级取四个值。

数值出处：《新乘区导论（伤害篇）》「源石技艺强度」一节的配置表
（AKE 研究文档 `public/CH/research/藕粉选修三 新乘区导论.md`，本仓库留档于
`docs/reference/ake-research/新乘区导论-伤害篇.md`），与《终末地数据机制导论》
§2.1/§2.2 的倍率公式逐条吻合：

- 法术异常初始伤害 = 80% × (1 + 异常等级) → 160/240/320/400%
- 燃烧持续伤害 = 12% × (1 + 异常等级) → 24/36/48/60%
- 碎甲伤害 = 50% × (1 + 异常等级) → 100/150/200/250%
- 碎冰 = 120% × (1 + 异常等级)、猛击 = 150% × (1 + 异常等级)

这些数值不在 TableCfg 里（游戏侧是引擎固定配置，不是表数据），所以只能落成
常量；不要改成从 `snapshot.tables(...)` 读。`catalog/views/constants.py` 的
`LOADOUT_STATUS_LEVELS`（速算用）与这里重叠的导电/碎甲/腐蚀三项必须一致，
由 `tests/test_endfield_encyclopedia.py` 的一致性用例守着。
"""

from __future__ import annotations

from .models import TermValueRow


def _row(label: str, values: tuple[str, ...], calc: str = "") -> TermValueRow:
    return TermValueRow(label=label, values=values, calc=calc)


def _pct(*values: int) -> tuple[str, ...]:
    return tuple(f"{value}%" for value in values)


def _seconds(*values: int) -> tuple[str, ...]:
    return tuple(f"{value} 秒" for value in values)


_ANOMALY_NOTE = (
    "异常等级 = 触发时消耗的破防 / 法术附着层数（最高 4 级）"
    "　·　出处《新乘区导论（伤害篇）》源石技艺强度·配置表"
)
_BURST_NOTE = (
    "四档相同：任意法术爆发固定 160%，不随异常等级变化"
    "　·　等级系数区 = 1 +（触发者等级 − 1）/ 196"
    "　·　出处同上"
)
_COMBO_NOTE = "四档按当前连击层数读取，单层增伤 × 层数　·　出处同上"

# 「异常初始伤害倍率」是导电/腐蚀/燃烧/冻结共用的初段伤害配置。
_INITIAL = _row("异常初始伤害倍率", _pct(160, 240, 320, 400), "伤害")

TERM_VALUES: dict[str, tuple[TermValueRow, ...]] = {
    "ba.conduct": (
        _row("导电法术伤害提高", _pct(12, 16, 20, 24), "减益效果"),
        _row("导电持续时间", _seconds(12, 18, 24, 30)),
        _INITIAL,
    ),
    "ba.corrupt": (
        _row("腐蚀初始减抗", ("-3.6", "-4.8", "-6.0", "-7.2"), "减益效果"),
        _row("腐蚀每跳减抗", ("-0.84", "-1.12", "-1.4", "-1.68"), "减益效果"),
        _row("腐蚀减抗上限", ("-12", "-16", "-20", "-24"), "减益效果"),
        _row("腐蚀持续时间", ("15 秒（固定）",)),
    ),
    "ba.burning": (
        _row("燃烧每跳伤害", _pct(24, 36, 48, 60), "伤害"),
        _row("燃烧持续时间", ("10 秒（固定）",)),
        _INITIAL,
    ),
    "ba.frozen": (
        _row("冻结持续时间", _seconds(6, 7, 8, 9)),
        _INITIAL,
    ),
    "ba.crystbreak": (_row("碎冰倍率", _pct(240, 360, 480, 600), "伤害"),),
    "ba.crush": (_row("猛击伤害", _pct(300, 450, 600, 750), "伤害"),),
    "ba.fracture": (
        _row("碎甲物理伤害提高", _pct(12, 16, 20, 24), "减益效果"),
        _row("碎甲伤害", _pct(100, 150, 200, 250), "伤害"),
        _row("碎甲持续时间", _seconds(12, 18, 24, 30)),
    ),
    "ba.airborne": (
        _row("击飞伤害", ("120%（固定）",), "伤害"),
        _row("击飞失衡", ("10（固定）",), "失衡"),
    ),
    "ba.knockdown": (_row("倒地伤害", ("120%（固定）",), "伤害"),),
    "ba.combo": (_row("连击增伤", _pct(20, 15, 13.33, 12.5)),),
}

_BURST_ROW = (_row("法术爆发伤害倍率", ("160%（固定）",), "伤害"),)
for _burst in (
    "ba.spellburst",
    "ba.fireburst",
    "ba.pulseburst",
    "ba.crystburst",
    "ba.naturalburst",
):
    TERM_VALUES[_burst] = _BURST_ROW

TERM_VALUE_NOTES: dict[str, str] = {
    "ba.spellburst": _BURST_NOTE,
    "ba.fireburst": _BURST_NOTE,
    "ba.pulseburst": _BURST_NOTE,
    "ba.crystburst": _BURST_NOTE,
    "ba.naturalburst": _BURST_NOTE,
    "ba.combo": _COMBO_NOTE,
}


def term_values(term_id: str) -> tuple[tuple[TermValueRow, ...], str]:
    """返回（数值行, 口径说明）；没有确认数值的词条返回空，卡片不画数值区。"""
    rows = TERM_VALUES.get(term_id, ())
    if not rows:
        return (), ""
    return rows, TERM_VALUE_NOTES.get(term_id, _ANOMALY_NOTE)
