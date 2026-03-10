from __future__ import annotations

import asyncio
import logging
from typing import Any, List

import discord
from discord.ext import commands


log = logging.getLogger("void.survey")


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


def _all_survey_emoji_keys(cfg: dict) -> list[str]:
    keys: list[str] = []
    for section in ("survey_age", "survey_region", "survey_games"):
        for key in (cfg.get(section) or {}).keys():
            key_s = str(key)
            if key_s not in keys:
                keys.append(key_s)
    return keys


def _emoji_id_from_key(key: str) -> int:
    parts = str(key or "").split(":", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


def _format_perm_state(ok: bool) -> str:
    return "да" if ok else "нет"


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
                except Exception as exc:
                    log.warning("survey add_reaction failed message_id=%s emoji=%s: %s", msg_id, k, exc)

        await add_reacts(int(cfg["survey"]["age_message_id"]), list(cfg["survey_age"].keys()))
        await add_reacts(int(cfg["survey"]["region_message_id"]), list(cfg["survey_region"].keys()))
        await add_reacts(int(cfg["survey"]["games_message_id"]), list(cfg["survey_games"].keys()))
        await ctx.send("✅ Реакции на анкете выставлены.")

    @commands.command(name="survey_audit")
    async def survey_audit(self, ctx: commands.Context, member: discord.Member | None = None) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return

        cfg = self.bot.cfg  # type: ignore
        guild = ctx.guild
        if guild is None:
            return

        survey_ch = guild.get_channel(int(cfg["channels"]["survey"]))
        if not isinstance(survey_ch, discord.TextChannel):
            await ctx.send("❌ Канал анкеты не найден.")
            return

        everyone = guild.default_role
        everyone_perms = survey_ch.permissions_for(everyone)
        me = guild.me
        bot_perms = survey_ch.permissions_for(me) if me else None
        target = member or ctx.author
        target_perms = survey_ch.permissions_for(target)

        lines = [
            f"Канал: {survey_ch.mention}",
            "",
            "@everyone:",
            f"- Видеть канал: {_format_perm_state(everyone_perms.view_channel)}",
            f"- Читать историю сообщений: {_format_perm_state(everyone_perms.read_message_history)}",
            f"- Добавлять реакции: {_format_perm_state(everyone_perms.add_reactions)}",
            "",
            f"Участник: {target.mention}",
            f"- Видеть канал: {_format_perm_state(target_perms.view_channel)}",
            f"- Читать историю сообщений: {_format_perm_state(target_perms.read_message_history)}",
            f"- Добавлять реакции: {_format_perm_state(target_perms.add_reactions)}",
        ]
        if bot_perms is not None:
            lines.extend(
                [
                    "",
                    "Бот:",
                    f"- Видеть канал: {_format_perm_state(bot_perms.view_channel)}",
                    f"- Читать историю сообщений: {_format_perm_state(bot_perms.read_message_history)}",
                    f"- Добавлять реакции: {_format_perm_state(bot_perms.add_reactions)}",
                    f"- Управлять сообщениями: {_format_perm_state(bot_perms.manage_messages)}",
                ]
            )

        missing_emoji: list[str] = []
        restricted_emoji: list[str] = []
        for key in _all_survey_emoji_keys(cfg):
            emoji_id = _emoji_id_from_key(key)
            if emoji_id <= 0:
                continue
            emoji = guild.get_emoji(emoji_id)
            if emoji is None:
                missing_emoji.append(key)
                continue
            if emoji.roles:
                role_names = ", ".join(role.name for role in emoji.roles)
                restricted_emoji.append(f"{emoji} -> {role_names}")

        lines.append("")
        lines.append("Эмодзи анкеты:")
        if missing_emoji:
            lines.append("- Не найдены: " + "; ".join(missing_emoji))
        else:
            lines.append("- Все эмодзи найдены.")
        if restricted_emoji:
            lines.append("- Ограничены ролями:")
            for item in restricted_emoji:
                lines.append(f"  {item}")
        else:
            lines.append("- Ограничений по ролям на эмодзи не найдено.")

        denied_role_lines: list[str] = []
        for role in sorted(target.roles, key=lambda r: r.position, reverse=True):
            overwrite = survey_ch.overwrites_for(role)
            denied: list[str] = []
            if overwrite.view_channel is False:
                denied.append("Видеть канал")
            if overwrite.read_message_history is False:
                denied.append("Читать историю сообщений")
            if overwrite.add_reactions is False:
                denied.append("Добавлять реакции")
            if denied:
                denied_role_lines.append(f"- {role.name}: {', '.join(denied)}")

        lines.append("")
        lines.append("Для участников должны быть включены:")
        lines.append("- Видеть канал")
        lines.append("- Читать историю сообщений")
        lines.append("- Добавлять реакции")
        lines.append("- Если эмодзи с другого сервера: Использовать внешние эмодзи")

        if denied_role_lines:
            lines.append("")
            lines.append("Ролевые запреты для выбранного участника:")
            lines.extend(denied_role_lines)

        await ctx.send("\n".join(lines))

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
            try:
                member = await g.fetch_member(payload.user_id)
            except Exception:
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
