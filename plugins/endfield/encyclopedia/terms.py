"""词条说明卡：族名、颜色、companion、来源反查。

来源映射按 `snapshot.revision` 做模块级缓存，单趟扫完，不在 `build_view` 里
每张卡重扫一遍技能表。映射不进 `index`：`/ef 物品` 不为词条买单。
"""

from __future__ import annotations

import re

from ..catalog.views.common import (
    _build_term_styles,
    _clean_fz_rich_text,
    _rich_text_visual,
)
from ..providers.assets import static_sprite_url
from ..providers.repository import AkeDataIncomplete, AkeSnapshot, localize
from . import akedata, classify
from .models import EncyclopediaIndex, IndexEntry, TermSourceView, TermView
from .term_values import term_values

TAG_RE = re.compile(r"<([@#])(ba\.[A-Za-z0-9_.-]+)>")
TERM_PREFIX = "ba."
# 每条最多画 20 个「名字 + 技能名 + 类型」，其余写剩余条数。
# 12 会切掉导电（16 条）的「触发」组，20 只压 4 个超长词条（ba.key 78 条等）。
SOURCE_LIMIT = 20

# 一条引用对词条的作用方式，数字越小越强：同一技能多次命中同一词条时只留最强的那条
# （弧光战技「疾风迅雷」既要求处于导电又消耗导电，记成「消耗」）。
_ROLE_RANK = {"apply": 0, "enhance": 1, "consume": 2, "trigger": 3, "mention": 4}
_CLAUSE_BREAKS = "，。；、\n"
_TRIGGER_MARKERS = ("处于", "进入", "被", "触发")

_SOURCES: dict[str, dict[str, tuple[tuple[str, str, str, str], ...]]] = {}
_SOURCE_ORDER: list[str] = []


async def build_term_index(snapshot: AkeSnapshot) -> EncyclopediaIndex:
    rows = await akedata.hyperlinks(snapshot)
    texts = await akedata.texts(snapshot)
    entries: list[IndexEntry] = []
    for term_id, row in rows.items():
        if not isinstance(row, dict) or not str(term_id).startswith(TERM_PREFIX):
            continue
        name = akedata.text_of(row.get("name"), texts)
        if not name:
            continue
        family_id, family = classify.term_family(str(row.get("richTextId") or ""))
        entries.append(
            IndexEntry(
                kind="term",
                key=str(term_id),
                display_name=name,
                extra_names=(str(term_id),),
                listed=classify.term_listed(name),
                summary=_summary(row, texts),
                group=family_id,
                icon_url=static_sprite_url(row.get("iconPath")),
                subtitle=family,
            )
        )
    entries.sort(key=lambda entry: entry.display_name)
    return EncyclopediaIndex(
        kind="term",
        revision=snapshot.revision,
        entries=tuple(entries),
        types=_present_families(entries),
    )


async def build_term_view(snapshot: AkeSnapshot, term_id: str) -> TermView:
    rows = await akedata.hyperlinks(snapshot)
    row = rows.get(str(term_id))
    if not isinstance(row, dict):
        raise AkeDataIncomplete(f"AKE HyperlinkTextTable is missing {term_id}")
    texts = await akedata.texts(snapshot)
    styles = await akedata.rich_text_styles(snapshot)
    rich_text_id = str(row.get("richTextId") or "")
    color, style_icon = _rich_text_visual(
        styles.get(rich_text_id) or styles.get(str(term_id)) or {}
    )
    family_id, family = classify.term_family(rich_text_id)
    description = akedata.text_of(row.get("desc"), texts)
    related, related_names = _related_named(rows, texts, str(term_id), description)
    sources, total, mentions = await _view_sources(snapshot, str(term_id))
    values, values_note = term_values(str(term_id))
    return TermView(
        term_id=str(term_id),
        name=akedata.text_of(row.get("name"), texts),
        family_id=family_id,
        family=family,
        color=color,
        icon_url=static_sprite_url(row.get("iconPath") or style_icon),
        summary=_clean_fz_rich_text(description),
        related=related,
        related_names=related_names,
        sources=sources,
        source_total=total,
        mention_total=mentions,
        values=values,
        values_note=values_note,
        revision=snapshot.revision,
    )


async def term_styles_for(snapshot: AkeSnapshot) -> dict:
    """`_build_term_styles` 吃 AkeData 形状的两张表；不用 `_build_fz_term_styles`。"""
    hyperlinks, styles = await snapshot.tables(
        "HyperlinkTextTable", "RichTextStyleTable"
    )
    return _build_term_styles(
        {"hyperlinkTextTable": hyperlinks, "richTextStyleTable": styles}
    )


async def term_sources(
    snapshot: AkeSnapshot,
) -> dict[str, tuple[tuple[str, str, str, str], ...]]:
    """按 revision 缓存的反向映射 `term_id → ((名字, 技能名, 角色, 类型), …)`。"""
    revision = snapshot.revision
    cached = _SOURCES.get(revision)
    if cached is not None:
        return cached
    built = await _build_sources(snapshot)
    _SOURCES[revision] = built
    _touch(revision)
    return built


def clear_caches() -> None:
    _SOURCES.clear()
    _SOURCE_ORDER.clear()


async def _build_sources(
    snapshot: AkeSnapshot,
) -> dict[str, tuple[tuple[str, str, str, str], ...]]:
    patches, texts = await snapshot.tables("SkillPatchTable", "I18nTextTable_CN")
    growth, items, weapons = await snapshot.tables(
        "CharGrowthTable", "ItemTable", "WeaponBasicTable"
    )
    suits = await _optional_table(snapshot, "EquipSuitTable")
    talents = await _optional_table(snapshot, "PotentialTalentEffectTable")
    # term_id → {(名字, 技能名, 类型): (角色, 类型)}；同一技能多句命中时留最强的角色。
    # 类型要进键：天赋 / 潜能的技能名是空的，只按 (名字, 技能名) 去重会把
    # 「佩丽卡 · 潜能 1/3/4」压成一条。
    collected: dict[str, dict[tuple[str, str, str], tuple[str, str]]] = {}

    def record(
        term_id: str, name: str, skill: str, role: str, kind: str = ""
    ) -> None:
        if not name:
            return
        slot = collected.setdefault(term_id, {})
        key = (name, skill, kind)
        current = slot.get(key)
        if current is None or _ROLE_RANK[role] < _ROLE_RANK[current[0]]:
            slot[key] = (role, kind)

    # 干员侧：技能卡文案取自 skillGroupMap 的 group desc；同一组的 skill id 的
    # SkillPatchDataBundle[].description 一并认（两处都可能带标记）。
    for operator_id, row in growth.items():
        if not isinstance(row, dict):
            continue
        operator_name = _text(row.get("name"), texts) or str(operator_id)
        for group_id, group in (row.get("skillGroupMap") or {}).items():
            if not isinstance(group, dict):
                continue
            skill_name = _text(group.get("name"), texts)
            kind = _skill_kind(str(group_id))
            bodies = [_text(group.get("desc"), texts)]
            for skill_id in group.get("skillIdList") or ():
                bodies.extend(_patch_texts(patches, skill_id, texts))
            for body in bodies:
                for term_id, role in _tag_roles(body).items():
                    record(term_id, operator_name, skill_name, role, kind)

    # 武器侧：WeaponBasicTable.weaponSkillList → SkillPatchTable 的 description。
    for weapon_id, row in weapons.items():
        if not isinstance(row, dict):
            continue
        item = items.get(str(weapon_id))
        weapon_name = (
            _text(item.get("name"), texts) if isinstance(item, dict) else ""
        ) or str(weapon_id)
        for skill_id in row.get("weaponSkillList") or ():
            for body in _patch_texts(patches, skill_id, texts):
                skill_name = _patch_skill_name(patches, skill_id, texts)
                for term_id, role in _tag_roles(body).items():
                    record(term_id, weapon_name, skill_name, role, "武器")

    # 装备套组：EquipSuitTable.list[] → skillID 的套组被动文案。
    for suit_id, row in suits.items():
        if not isinstance(row, dict):
            continue
        for entry in row.get("list") or ():
            if not isinstance(entry, dict):
                continue
            suit_name = _text(entry.get("suitName"), texts) or str(suit_id)
            count = entry.get("equipCnt")
            label = f"{count} 件套" if count else ""
            for body in _patch_texts(patches, entry.get("skillID"), texts):
                for term_id, role in _tag_roles(body).items():
                    record(term_id, suit_name, label, role, "装备")

    # 天赋 / 潜能：PotentialTalentEffectTable 的行 id 自带归属。
    for talent_id, row in talents.items():
        if not isinstance(row, dict):
            continue
        body = _text(row.get("desc"), texts)
        if not body:
            continue
        owner, kind = _talent_owner(str(talent_id), growth, texts)
        for term_id, role in _tag_roles(body).items():
            record(term_id, owner, "", role, kind)

    return {
        term_id: tuple(
            (name, skill, role, kind)
            for (name, skill, kind), (role, _) in rows.items()
        )
        for term_id, rows in collected.items()
    }


async def _view_sources(
    snapshot: AkeSnapshot, term_id: str
) -> tuple[tuple[TermSourceView, ...], int, int]:
    """返回（要画的来源, 直接来源总数, 仅被提及的条数）。

    只「提及」的引用不进来源区：这类句子既没施加也没增强该词条，列出来只会
    让人误以为找到了出处（碎甲被条件句当成来源就是这个毛病）。
    """
    rows = (await term_sources(snapshot)).get(term_id, ())
    direct = sorted(
        (row for row in rows if row[2] != "mention"),
        key=lambda row: _ROLE_RANK[row[2]],
    )
    views = tuple(
        TermSourceView(name=name, skill=skill, role=role, kind=kind)
        for name, skill, role, kind in direct[:SOURCE_LIMIT]
    )
    mentions = sum(1 for row in rows if row[2] == "mention")
    return views, len(direct), mentions


def _tag_role(text: str, start: int) -> str:
    """判定标记在句中扮演的角色。

    先看整句（增强往往写在标记之后：「…的导电使…提升至原本的 1.33 倍」），再看
    标记前的一小句（施加 / 消耗紧贴标记：「…并强制施加<#ba.conduct>导电</>」）。
    整句含「可以发动」的一律算触发，免得「当…或碎甲消耗破防层数后可以发动」
    这类条件句被读成消耗。
    """
    end = text.find("。", start)
    sentence = text[start : end if end > 0 else len(text)]
    head = _clause_before(text, start)
    # 标记本身只匹配到开标签 `<#ba.x>`，标签字之后才是「时 / 后」。
    close = text.find("</>", start)
    label_end = close + 3 if close >= 0 else start
    # 条件句的「时 / 后」可能隔着「或<#ba.x>另一词条</>」，看到本句断句为止。
    window = text[label_end : label_end + 24]
    cut = min(
        (index for index in (window.find(sep) for sep in _CLAUSE_BREAKS) if index >= 0),
        default=len(window),
    )
    condition = "时" in window[:cut] or "后" in window[:cut]
    if "提升至" in sentence or "伤害提高" in sentence:
        return "enhance"
    if "持续时间" in sentence and "<@ba.vup>" in sentence:
        return "enhance"
    # 「施加X后 / 施加X时」是条件句，不是这个技能在施加：装备套组、武器被动、
    # 潜能里常见「装备者施加导电时，全队获得…」，说的是施加之后给增伤。
    if condition:
        return "consume" if "消耗" in head else "trigger"
    if "施加" in head or "强制造成" in head:
        return "apply"
    if "消耗" in head:
        return "consume"
    if "可以发动" in sentence:
        return "trigger"
    if any(marker in head for marker in _TRIGGER_MARKERS):
        return "trigger"
    return "mention"


def _clause_before(text: str, start: int) -> str:
    head = text[max(0, start - 40) : start]
    cut = max((head.rfind(sep) for sep in _CLAUSE_BREAKS), default=-1)
    return head[cut + 1 :] if cut >= 0 else head


def _tag_roles(text: str) -> dict[str, str]:
    """一张文案里出现的词条 → 最强角色。"""
    roles: dict[str, str] = {}
    for match in TAG_RE.finditer(text):
        term_id = match.group(2)
        role = _tag_role(text, match.start())
        known = roles.get(term_id)
        if known is None or _ROLE_RANK[role] < _ROLE_RANK[known]:
            roles[term_id] = role
    return roles


def _skill_kind(group_id: str) -> str:
    for suffix, name in (
        ("_ComboSkill", "连携技"),
        ("_NormalSkill", "战技"),
        ("_UltimateSkill", "终结技"),
        ("_NormalAttack", "普攻"),
    ):
        if group_id.endswith(suffix):
            return name
    return ""


def _patch_texts(patches: dict, skill_id: object, texts: dict) -> tuple[str, ...]:
    row = patches.get(str(skill_id))
    if not isinstance(row, dict):
        return ()
    bodies = []
    for bundle in row.get("SkillPatchDataBundle") or ():
        if isinstance(bundle, dict):
            body = _text(bundle.get("description"), texts)
            if body:
                bodies.append(body)
    return tuple(bodies)


def _patch_skill_name(patches: dict, skill_id: object, texts: dict) -> str:
    row = patches.get(str(skill_id))
    if not isinstance(row, dict):
        return ""
    for bundle in row.get("SkillPatchDataBundle") or ():
        if isinstance(bundle, dict):
            name = _text(bundle.get("skillName"), texts)
            if name:
                return name
    return ""


def _talent_owner(talent_id: str, growth: dict, texts: dict) -> tuple[str, str]:
    """`chr_0004_pelica_potential_4` → ("佩丽卡", "潜能 4")。"""
    match = re.match(
        r"^(?P<owner>.+?)_(?P<kind>talent|potential)_(?P<rest>[0-9_]+)$", talent_id
    )
    if match is None:
        return "", ""
    owner_id = match.group("owner")
    row = growth.get(owner_id)
    owner = (_text(row.get("name"), texts) if isinstance(row, dict) else "") or owner_id
    rest = match.group("rest")
    if match.group("kind") == "potential":
        return owner, "潜能 " + rest
    return owner, "天赋 " + rest.replace("_", "-")


async def _optional_table(snapshot: AkeSnapshot, name: str) -> dict:
    """装备套组 / 天赋表在旧快照里可能缺席：缺了只是少一类来源，别让整张卡挂掉。"""
    try:
        return await snapshot.table(name)
    except AkeDataIncomplete:
        return {}


def _related(rows: dict, term_id: str, description: str) -> tuple[str, ...]:
    related: list[str] = []
    for _, tag_id in TAG_RE.findall(description):
        if tag_id != term_id and tag_id not in related:
            related.append(tag_id)
    # companion 不在正文里也要出现。
    existing = {str(key) for key in rows}
    for candidate in existing:
        if candidate == term_id:
            continue
        if classify.term_companion(candidate, existing) == term_id and candidate not in related:
            related.append(candidate)
    return tuple(related)


def _related_named(
    rows: dict, texts: dict, term_id: str, description: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """关联词条：返回（id, 中文名）两组一一对应的元组。

    名字先查词条名，再退到族名（`ba.phy` 这类只存在于样式表）；两处都没有的纯样式
    id（如 `ba.pd`）直接丢掉——卡片上写一行 `ba.pd` 对用户没有意义。
    """
    ids: list[str] = []
    names: list[str] = []
    for related_id in _related(rows, term_id, description):
        row = rows.get(related_id)
        name = akedata.text_of(row.get("name"), texts) if isinstance(row, dict) else ""
        if not name:
            _, family = classify.term_family(related_id)
            name = family if family != related_id else ""
        if not name:
            continue
        ids.append(related_id)
        names.append(name)
    return tuple(ids), tuple(names)


def _summary(row: dict, texts: dict) -> str:
    clean = _clean_fz_rich_text(_text(row.get("desc"), texts))
    return clean[:42]


def _text(value: object, texts: dict) -> str:
    return localize(value, texts) if not isinstance(value, str) else value


def _touch(revision: str) -> None:
    if revision in _SOURCE_ORDER:
        _SOURCE_ORDER.remove(revision)
    _SOURCE_ORDER.append(revision)
    while len(_SOURCE_ORDER) > 2:
        stale = _SOURCE_ORDER.pop(0)
        _SOURCES.pop(stale, None)


def _present_families(entries: list[IndexEntry]) -> tuple[tuple[str, str], ...]:
    ordered = list(classify.TERM_FAMILY_NAMES)
    seen: list[str] = []
    for entry in entries:
        if entry.group not in seen:
            seen.append(entry.group)
    seen.sort(key=lambda key: ordered.index(key) if key in ordered else len(ordered))
    return tuple((key, classify.term_family(key)[1]) for key in seen)
