# cogs/level_roles.py
from __future__ import annotations

import logging
from typing import Any, List, Tuple

import discord
from discord.ext import commands

log = logging.getLogger("void")


def _digits(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_rules(bot: commands.Bot) -> List[Tuple[int, int]]:
    """
    Returns sorted list of (min_level, role_id)
    """
    cfg = getattr(bot, "cfg", {}) or {}
    rules = cfg.get("level_roles")

    # fallback (если вдруг забыли в конфиг добавить)
    if not rules:
        return [
            (3, 1473309391015903366),
            (7, 1473309592971644969),
            (15, 1473309658486673571),
        ]

    out: List[Tuple[int, int]] = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        ml = _digits(r.get("min_level", 0), 0)
        rid = _digits(r.get("role_id", 0), 0)
        if ml > 0 and rid > 0:
            out.append((ml, rid))

    out.sort(key=lambda x: x[0])
    return out


class LevelRolesCog(commands.Cog):
    """
    Автоматически выдаёт/снимает роли по уровню.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _desired_role_ids(self, level: int) -> set[int]:
        rules = _load_rules(self.bot)
        lvl = int(level)
        return {rid for (ml, rid) in rules if lvl >= ml}

    async def sync_member(self, member: discord.Member, level: int, *, reason: str = "") -> None:
        """
        Приводит роли участника в соответствие с его уровнем.
        """
        desired = self._desired_role_ids(level)
        rules = _load_rules(self.bot)
        rule_ids = {rid for _, rid in rules}

        guild = member.guild
        to_add: list[discord.Role] = []
        to_remove: list[discord.Role] = []

        member_role_ids = {r.id for r in member.roles}

        for rid in rule_ids:
            role = guild.get_role(rid)
            if role is None:
                continue

            has = rid in member_role_ids
            should = rid in desired

            if should and not has:
                to_add.append(role)
            elif (not should) and has:
                to_remove.append(role)

        # ничего менять не надо
        if not to_add and not to_remove:
            return

        # ВАЖНО: если у бота нет прав/иерархии — тут будет Forbidden
        why = reason or f"LevelRoles: level={level}"
        try:
            if to_add:
                await member.add_roles(*to_add, reason=why)
            if to_remove:
                await member.remove_roles(*to_remove, reason=why)
        except discord.Forbidden:
            log.warning("No permission/hierarchy to edit roles for member=%s", member.id)
        except discord.HTTPException as e:
            log.warning("HTTPException while editing roles: %s", e)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        # При входе: выдать роли по текущему уровню из БД
        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return

        await repo.ensure_user(member.id)
        u = await repo.get_user(member.id)
        level = int(u.get("level", 1))
        await self.sync_member(member, level, reason="Member joined")

    @commands.command(name="sync_roles")
    async def sync_roles(self, ctx: commands.Context, member: commands.MemberConverter | None = None) -> None:
        """
        Админ-команда: пересчитать роли по уровню.
        !sync_roles @user
        """
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return

        target = member or ctx.author
        if not isinstance(target, discord.Member):
            await ctx.reply("❌ Не удалось определить участника.")
            return

        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return

        await repo.ensure_user(target.id)
        u = await repo.get_user(target.id)
        level = int(u.get("level", 1))
        await self.sync_member(target, level, reason=f"Manual sync by {ctx.author.id}")
        await ctx.reply(f"✅ Роли синхронизированы для {target.mention} (уровень {level})", mention_author=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(LevelRolesCog(bot))
