from __future__ import annotations

import discord
from discord.ext import commands


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="me")
    async def me(self, ctx: commands.Context) -> None:
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(ctx.author.id)
        u = await repo.get_user(ctx.author.id)
        await ctx.reply(
            f"{ctx.author.mention}\nуровень: **{u['level']}**\nруны: **{u['runes']}**\nxp: **{u['xp']}**",
            mention_author=False,
        )

    @commands.command(name="set_runes")
    async def set_runes(self, ctx: commands.Context, member: commands.MemberConverter, runes: int) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(member.id)
        await repo.set_user_fields(member.id, runes=int(runes))
        await ctx.send(f"✅ {member.mention} руны = {runes}")

    @commands.command(name="set_level")
    async def set_level(self, ctx: commands.Context, member: commands.MemberConverter, level: int) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(member.id)
        act = self.bot.get_cog("ActivityCog")
        if act is not None:
            try:
                level = max(1, min(int(level), int(getattr(act, "max_level", int(level)))))
            except Exception:
                level = int(level)

        await repo.set_user_fields(member.id, level=int(level), xp=0)

        # синхронизируем роли сразу
        lr = self.bot.get_cog("LevelRolesCog")
        if lr is not None and isinstance(member, discord.Member):
            try:
                await lr.sync_member(member, int(level), reason="Admin set_level")
            except Exception:
                pass

        await ctx.send(f"✅ {member.mention} уровень = {level}")


    @commands.command(name="set_xp")
    async def set_xp(self, ctx: commands.Context, member: commands.MemberConverter, xp: int) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(member.id)
        u = await repo.get_user(member.id)
        level = int(u.get("level", 1))

        act = self.bot.get_cog("ActivityCog")
        xp_i = max(0, int(xp))
        if act is not None:
            try:
                need = int(act._need_xp(level))  # type: ignore
                xp_i = min(xp_i, max(0, need - 1))
            except Exception:
                pass

        await repo.set_user_fields(member.id, xp=int(xp_i))
        await ctx.send(f"✅ {member.mention} xp = {xp_i} (ур.{level})")

    @commands.command(name="add_xp")
    async def add_xp(self, ctx: commands.Context, member: commands.MemberConverter, add_xp: int) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(member.id)
        u = await repo.get_user(member.id)

        act = self.bot.get_cog("ActivityCog")
        if act is None:
            # fallback: просто прибавим xp без левел-апа
            new_xp = max(0, int(u.get("xp", 0)) + int(add_xp))
            await repo.set_user_fields(member.id, xp=int(new_xp))
            await ctx.send(f"✅ {member.mention} xp += {add_xp} → {new_xp}")
            return

        try:
            new_level, new_xp = await act._apply_xp_and_level(repo, member.id, u, int(add_xp))  # type: ignore
        except Exception:
            new_level = int(u.get("level", 1))
            new_xp = max(0, int(u.get("xp", 0)) + int(add_xp))

        await repo.set_user_fields(member.id, level=int(new_level), xp=int(new_xp))

        # синк ролей по уровню (если есть)
        lr = self.bot.get_cog("LevelRolesCog")
        if lr is not None and isinstance(member, discord.Member):
            try:
                await lr.sync_member(member, int(new_level), reason="Admin add_xp")
            except Exception:
                pass

        await ctx.send(f"✅ {member.mention} xp += {add_xp} → xp={new_xp}, ур.{new_level}")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminCog(bot))