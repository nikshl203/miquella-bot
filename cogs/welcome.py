from __future__ import annotations

import random
from io import BytesIO
from pathlib import Path
from typing import Dict

import discord
from discord.ext import commands
from PIL import Image, ImageDraw, ImageOps

ROOT = Path(__file__).resolve().parents[1]


def _compose_welcome(template_path: Path, avatar_bytes: bytes, circle: Dict[str, int]) -> bytes:
    base = Image.open(template_path).convert("RGBA")
    avatar = Image.open(BytesIO(avatar_bytes)).convert("RGBA")

    x, y, r = int(circle["x"]), int(circle["y"]), int(circle["r"])
    size = r * 2
    avatar = ImageOps.fit(avatar, (size, size), method=Image.Resampling.LANCZOS)

    mask = Image.new("L", (size, size), 0)
    d = ImageDraw.Draw(mask)
    d.ellipse((0, 0, size - 1, size - 1), fill=255)

    base.paste(avatar, (x - r, y - r), mask)

    out = BytesIO()
    base.save(out, format="PNG")
    return out.getvalue()


class WelcomeCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _send_welcome(self, channel: discord.TextChannel, member: discord.Member) -> None:
        cfg = self.bot.cfg  # type: ignore
        w = cfg.get("welcome", {})
        texts = w.get("texts", [])
        greeting = random.choice(texts) if texts else "Ты вошёл. Пустота заметила."

        rules_id = int(cfg["channels"]["rules"])
        survey_id = int(cfg["channels"]["survey"])
        footer = (
            f"\n\n📜 Прочитай <#{rules_id}>."
            f"\n📝 Отметь себя в <#{survey_id}>."
            f"\nИ да — Пустота слышит смех лучше, чем молитвы."
        )

        template_path = ROOT / w.get("template_path", "assets/welcome_template.png")
        circle = w.get("avatar_circle", {"x": 356, "y": 684, "r": 320})

        if template_path.exists():
            try:
                avatar = await member.display_avatar.read()
                img = _compose_welcome(template_path, avatar, circle)
                await channel.send(
                    content=f"{member.mention}\n{greeting}{footer}",
                    file=discord.File(BytesIO(img), filename="welcome.png"),
                    allowed_mentions=discord.AllowedMentions(users=True),
                )
                return
            except Exception:
                pass

        await channel.send(f"{member.mention}\n{greeting}{footer}", allowed_mentions=discord.AllowedMentions(users=True))

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        cfg = self.bot.cfg  # type: ignore
        if member.guild.id != int(cfg["guild_id"]):
            return

        ch = member.guild.get_channel(int(cfg["channels"]["welcome"]))
        if isinstance(ch, discord.TextChannel):
            await self._send_welcome(ch, member)

    @commands.command(name="welcome_test")
    async def welcome_test(self, ctx: commands.Context, member: discord.Member | None = None) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        member = member or ctx.author  # type: ignore
        cfg = self.bot.cfg  # type: ignore
        ch = ctx.guild.get_channel(int(cfg["channels"]["welcome"])) if ctx.guild else None
        if isinstance(ch, discord.TextChannel):
            await self._send_welcome(ch, member)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WelcomeCog(bot))