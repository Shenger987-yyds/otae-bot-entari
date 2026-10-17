"""Prefetch the public images an account card will need, as soon as card/detail returns.

The URLs come straight from the detail payload (same fields the detail view
reads), so the download overlaps with the name tables and view building. The
broker only fills the disk cache, skips hosts behind the asset proxy and keeps
to the daily budget; nothing here waits for it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from otae_bot.infrastructure.http.assets import broker

from ..cold_start import REMOTE_ASSET_NAMESPACE, shared_image_namespaces
from .detail.service import EQUIP_SLOTS
from .draw import _character_semantic_icon_urls
from .i18n import localized_text


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> tuple[Any, ...]:
    return tuple(value) if isinstance(value, (list, tuple)) else ()


def detail_asset_urls(detail: Mapping[str, Any]) -> tuple[str, ...]:
    """Remote images of the account detail card, plus each operator's AKE class/element icon."""
    urls: list[str] = [localized_text(_mapping(detail.get("base")).get("avatarUrl"))]
    for entry in _sequence(detail.get("chars")):
        character = _mapping(entry)
        data = _mapping(character.get("charData"))
        urls.append(localized_text(data.get("avatarSqUrl")))
        tactical = _mapping(_mapping(character.get("tacticalItem")).get("tacticalItemData"))
        urls.append(localized_text(tactical.get("iconUrl")))
        urls.extend(localized_text(_mapping(skill).get("iconUrl")) for skill in _sequence(data.get("skills")))
        weapon = _mapping(_mapping(character.get("weapon")).get("weaponData"))
        urls.append(localized_text(weapon.get("iconUrl")))
        for key, _label in EQUIP_SLOTS:
            urls.append(localized_text(_mapping(_mapping(character.get(key)).get("equipData")).get("iconUrl")))
        urls.extend(_character_semantic_icon_urls(dict(character)))
    return tuple(dict.fromkeys(url for url in urls if url.startswith(("https://", "http://"))))


def prefetch_detail_assets(detail: Mapping[str, Any], *, source: str) -> None:
    """Queue the detail's public images for a background disk prefetch (never waits)."""
    broker.prefetch_soon(
        detail_asset_urls(detail),
        namespace=REMOTE_ASSET_NAMESPACE,
        source=source,
        disk_aliases=shared_image_namespaces(REMOTE_ASSET_NAMESPACE),
    )
