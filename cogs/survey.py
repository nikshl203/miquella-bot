from __future__ import annotations

import asyncio
from typing import Any, List

import discord
from discord.ext import commands


def _emoji_key(e: Any) -> str:
    if isinstance(e, discord.PartialEmoji):
        return f"{e.name}:{e.id}" if e.id else (e.name or "")
    if isinstance(e, discord.Emoji):
        return f"{e.name}:{e.id}"
    return str(e)


def _parse_custom_emoji(key: str) -> discord.PartialEmoji:
    if ":" in key:
        name, sid = key.split(":", 1)
        if sid.isdigit():
            return discord.PartialEmoji(name=name, id=int(sid))
    return discord.PartialEmoji(name=key)


def _mapping_for_message(cfg: dict, message_id: int) -> tuple[dict[str, int], bool]:
    age_id = int(cfg["survey"]["age_message_id"])
    reg_id = int(cfg["survey"]["region_message_id"])
    games_id = int(cfg["survey"]["games_message_id"])

    if int(message_id) == age_id:
        return {k: int(v) for k, v in cfg["survey_age"].items()}, True
    if int(message_id) == reg_id:
        return {k: int(v) for k, v in cfg["survey_region"].items()}, True
    if int(message_id) == games_id:
        return {k: int(v) for k, v in cfg["survey_games"].items()}, False
    return {}, False


class SurveyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="setup_survey")
    async def setup_survey(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        cfg = self.bot.cfg  # type: ignore
        g = ctx.guild
        if not g:
            return

        survey_ch = g.get_channel(int(cfg["channels"]["survey"]))
        if not isinstance(survey_ch, discord.TextChannel):
            await ctx.send("❌ Канал анкеты не найден.")
            return

        async def add_reacts(msg_id: int, keys: List[str]) -> None:
            msg = await survey_ch.fetch_message(msg_id)
            for k in keys:
                try:
                    await msg.add_reaction(_parse_custom_emoji(k))
                    await asyncio.sleep(0.15)
                except Exception:
                    pass

        await add_reacts(int(cfg["survey"]["age_message_id"]), list(cfg["survey_age"].keys()))
        await add_reacts(int(cfg["survey"]["region_message_id"]), list(cfg["survey_region"].keys()))
        await add_reacts(int(cfg["survey"]["games_message_id"]), list(cfg["survey_games"].keys()))
        await ctx.send("✅ Реакции на анкете выставлены.")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        cfg = self.bot.cfg  # type: ignore
        if payload.guild_id != int(cfg["guild_id"]):
            return
        if self.bot.user and payload.user_id == self.bot.user.id:
            return

        mapping, exclusive = _mapping_for_message(cfg, payload.message_id)
        if not mapping:
            return

        ek = _emoji_key(payload.emoji)
        role_id = mapping.get(ek)
        if not role_id:
            return

        g = self.bot.get_guild(int(cfg["guild_id"]))
        if not g:
            return
        member = g.get_member(payload.user_id)
        if not member:
            return

        channel = g.get_channel(payload.channel_id)
        if exclusive and isinstance(channel, discord.TextChannel):
            try:
                msg = await channel.fetch_message(payload.message_id)
                for react in msg.reactions:
                    rk = _emoji_key(react.emoji)
                    if rk != ek and rk in mapping:
                        try:
                            await msg.remove_reaction(react.emoji, member)
                        except Exception:
                            pass
                        other_role = g.get_role(mapping[rk])
                        if other_role and other_role in member.roles:
                            try:
                                await member.remove_roles(other_role, reason="exclusive survey")
                            except Exception:
                                pass
            except Exception:
                pass

        role = g.get_role(role_id)
        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason="survey")
            except Exception:
                pass

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        cfg = self.bot.cfg  # type: ignore
        if payload.guild_id != int(cfg["guild_id"]):
            return
        if self.bot.user and payload.user_id == self.bot.user.id:
            return

        mapping, _ = _mapping_for_message(cfg, payload.message_id)
        if not mapping:
            return

        ek = _emoji_key(payload.emoji)
        role_id = mapping.get(ek)
        if not role_id:
            return

        g = self.bot.get_guild(int(cfg["guild_id"]))
        if not g:
            return

        member = g.get_member(payload.user_id)
        if not member:
            try:
                member = await g.fetch_member(payload.user_id)
            except Exception:
                return

        role = g.get_role(role_id)
        if role and role in member.roles:
            try:
                await member.remove_roles(role, reason="survey reaction removed")
            except Exception:
                pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(SurveyCog(bot))
