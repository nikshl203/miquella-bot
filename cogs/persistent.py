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
        log.warning("Persistent: import failed for %s: %s", name, e)
        return None


def _collect_views(bot: commands.Bot) -> List[discord.ui.View]:
    """Collect persistent views from cogs via get_persistent_views(bot)."""
    views: List[discord.ui.View] = []

    for mod_name in (
        "cogs.shop",
        "cogs.coin",
        "cogs.duel",
        "cogs.void_info",
        "cogs.echo_posts",
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
            log.exception("Persistent: get_persistent_views() failed for %s: %s", mod_name, e)

    return views


class PersistentCog(commands.Cog):
    """Register all timeout=None views so component buttons survive restarts."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._registered = False

    async def cog_load(self) -> None:
        if self._registered:
            return
        self._registered = True

        views = _collect_views(self.bot)
        if not views:
            log.info("Persistent: no views to register.")
            return

        for v in views:
            if v.timeout is not None:
                log.warning("Persistent: skip %s (timeout is not None)", type(v).__name__)
                continue

            try:
                self.bot.add_view(v)
                log.info("Persistent: registered view %s", type(v).__name__)
            except Exception as e:
                log.exception("Persistent: failed to register %s: %s", type(v).__name__, e)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(PersistentCog(bot))
