from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Decoration:
    id: str
    name: str
    emoji: str
    price: int = 50
    shop: bool = True


STORY_DECORATION_ROLE_NAME = "Прошедший порог"
STORY_DECORATION_ID = "passed_threshold"


DECORATIONS: tuple[Decoration, ...] = (
    Decoration(id="ashen_mark", name="Пепельный знак", emoji="🔥"),
    Decoration(id="blood_mark", name="Знак крови", emoji="🩸"),
    Decoration(id="moon_shard", name="Лунный осколок", emoji="🌙"),
    Decoration(id="void_seal", name="Печать пустоты", emoji="⬛"),
    Decoration(id="withered_wreath", name="Увядший венец", emoji="🥀"),
    Decoration(id="abyss_eye", name="Око бездны", emoji="👁️"),
    Decoration(id="fate_thread", name="Нить судьбы", emoji="🕸️"),
    Decoration(id="dusk_brand", name="Клеймо сумрака", emoji="🌫️"),
    Decoration(id="broken_crown", name="Сломанная корона", emoji="👑"),
    Decoration(id="ash_whisper", name="Шепот праха", emoji="✨"),
    Decoration(
        id=STORY_DECORATION_ID,
        name="Прошедший порог",
        emoji="🚪",
        price=0,
        shop=False,
    ),
)


DECORATIONS_BY_ID: dict[str, Decoration] = {d.id: d for d in DECORATIONS}
SHOP_DECORATIONS: tuple[Decoration, ...] = tuple(d for d in DECORATIONS if d.shop)
