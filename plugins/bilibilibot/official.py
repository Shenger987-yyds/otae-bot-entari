"""终末地官方 B 站账号的采集约定。

官号的内容形态和普通 UP 主不一样：正文在 Opus 结构里、置顶旧动态会排在
最前、抽奖混在资讯里。这些约定集中在这里，避免散落在 api 与 poller 各处。
"""

from __future__ import annotations

# 终末地官号：动态订阅对它放宽条数限制，并按 Opus 结构校验正文。
ENDFIELD_OFFICIAL_UID = "1265652806"
