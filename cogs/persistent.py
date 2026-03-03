# cogs/persistent.py
from __future__ import annotations

import importlib
import logging
from typing import List

import discord
from discord.ext import commands

log = logging.getLogger("void")


def _safe_import(name: str):
    try:
        return importlib.import_module(name)
    except Exception as e:
        log.warning("Persistent: не смог импортировать %s: %s", name, e)
        return None


def _collect_views(bot: commands.Bot) -> List[discord.ui.View]:
    """
    Собираем persistent-view из когов.
    Каждый cog-файл может (и должен) иметь функцию:
      def get_persistent_views(bot) -> list[discord.ui.View]
    """
    views: List[discord.ui.View] = []

    for mod_name in (
        "cogs.shop",
        "cogs.coin",
        "cogs.duel",
        "cogs.void_info",
        # позже добавим:
        "cogs.story",
        "cogs.tendril",
    ):
        mod = _safe_import(mod_name)
        if not mod:
            continue

        getter = getattr(mod, "get_persistent_views", None)
        if not getter:
            continue

        try:
            got = getter(bot)
            if got:
                views.extend(got)
        except Exception as e:
            log.exception("Persistent: ошибка в get_persistent_views() у %s: %s", mod_name, e)

    return views


class PersistentCog(commands.Cog):
    """
    Этот ког при старте регистрирует все persistent views.
    После этого кнопки на уже опубликованных сообщениях НЕ умирают при перезапуске.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._registered = False

    async def cog_load(self) -> None:
        # cog_load вызывается при загрузке extension.
        if self._registered:
            return
        self._registered = True

        views = _collect_views(self.bot)
        if not views:
            log.info("Persistent: views не найдены (пока нечего регистрировать).")
            return

        for v in views:
            # ВАЖНО: persistent view должен быть timeout=None
            if v.timeout is not None:
                log.warning("Persistent: view %s НЕ timeout=None, пропускаю.", type(v).__name__)
                continue

            try:
                self.bot.add_view(v)  # <— ключевая строка
                log.info("Persistent: зарегистрирован view %s", type(v).__name__)
            except Exception as e:
                log.exception("Persistent: не смог зарегистрировать %s: %s", type(v).__name__, e)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(PersistentCog(bot))