"""终末地富文本标记 ``<@ba.x>…</>`` 与 ``<#ba.x>…</>`` 的配对解析。

游戏文案允许嵌套：祀的 P2 就是
``<@ba.vup>+[…×<#ba.consume>消耗</>或<#ba.absorb>吸收</>…]</>``。
按最近一个 ``</>`` 收口的写法会把内层开标签当成外层正文，内层标签因此
失去闭合、被当纯文本打印出来，外层闭合也变成孤立标记。这里按深度配对，
并把配不上的残缺标记单独标出来，交给渲染层吞掉。
"""

from __future__ import annotations

import re
from collections.abc import Iterator

# 与历史实现保持一致：标签 id 只允许字母数字与 . _ -
_OPEN_RE = re.compile(r"<([@#])([A-Za-z0-9_.-]+)>")
CLOSE = "</>"


def rich_tag_at(text: str, index: int) -> tuple[str, str, str, int] | None:
    """``index`` 处的富文本标签 → ``(kind, tag_id, inner, end_index)``。

    ``index`` 不是开标签、或到结尾都没有配对 ``</>`` 时返回 None。``inner``
    保留嵌套标记原文，交给调用方递归渲染。
    """
    match = _OPEN_RE.match(text, index)
    if not match:
        return None
    depth = 1
    cursor = match.end()
    while cursor < len(text):
        nested = _OPEN_RE.match(text, cursor)
        if nested:
            depth += 1
            cursor = nested.end()
            continue
        if text.startswith(CLOSE, cursor):
            depth -= 1
            if depth == 0:
                return (
                    match.group(1),
                    match.group(2),
                    text[match.end() : cursor],
                    cursor + len(CLOSE),
                )
            cursor += len(CLOSE)
            continue
        cursor += 1
    return None


def stray_rich_tag_len(text: str, index: int) -> int:
    """``index`` 处是配不上的残缺标记时返回其长度，否则 0。

    只该在 ``rich_tag_at`` 返回 None 之后调用：孤立 ``</>`` 或没有闭合的
    开标签，渲染层据此跳过标记本身，避免把 ``<#ba.consume>`` 印到卡上。
    """
    if text.startswith(CLOSE, index):
        return len(CLOSE)
    match = _OPEN_RE.match(text, index)
    return len(match.group(0)) if match else 0


def iter_rich_tags(text: str) -> Iterator[tuple[str, str, str]]:
    """按出现顺序产出 ``(kind, tag_id, inner)``，嵌套内层也会产出。"""
    index = 0
    while index < len(text):
        tag = rich_tag_at(text, index)
        if tag is None:
            index += 1
            continue
        kind, tag_id, inner, end_index = tag
        yield kind, tag_id, inner
        yield from iter_rich_tags(inner)
        index = end_index
