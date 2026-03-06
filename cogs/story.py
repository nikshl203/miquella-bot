from __future__ import annotations

from pathlib import Path

import discord
from discord.ext import commands

from ._interactions import GuardedView, safe_send
from .story_ch1 import get_persistent_views as get_ch1_persistent_views
from .story_ch1 import send_ch1_scene_1
from .story_shared import ROOT, cfg as _cfg, get_or_create_story_thread as _get_or_create_story_thread, repo as _repo


class OrderHallView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Глава 1 — «Порог» (ур. 5)",
        style=discord.ButtonStyle.secondary,
        custom_id="orderhall:story:s1:ch1",
        row=0,
    )
    async def ch1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await orderhall_issue_code(self.bot, interaction, chapter_id=1)

    @discord.ui.button(
        label="Перепройти текущую главу",
        style=discord.ButtonStyle.primary,
        custom_id="orderhall:story:replay_current",
        row=1,
    )
    async def replay(self, interaction: discord.Interaction, _: discord.ui.Button):
        await orderhall_replay_current(self.bot, interaction)


def get_persistent_views(bot: commands.Bot):
    return [OrderHallView(bot), *get_ch1_persistent_views(bot)]


async def _dm(member: discord.Member, content: str) -> bool:
    try:
        await member.send(content)
        return True
    except Exception:
        return False


async def orderhall_issue_code(bot: commands.Bot, interaction: discord.Interaction, chapter_id: int) -> None:
    assert interaction.user and isinstance(interaction.user, discord.Member)
    member: discord.Member = interaction.user

    cfg = _cfg(bot)
    repo = _repo(bot)

    u = await repo.get_user(member.id)
    lvl = int(u["level"])

    chapters = cfg.get("story", {}).get("chapters", [])
    ch_cfg = next((c for c in chapters if int(c.get("id", 0)) == chapter_id), None)
    if not ch_cfg:
        await safe_send(interaction, "Глава не настроена в config.json.", ephemeral=True)
        return

    req_lvl = int(ch_cfg.get("required_level", 0))
    title = str(ch_cfg.get("title", f"Глава {chapter_id}"))

    if lvl < req_lvl:
        await safe_send(interaction, "🕯️ Тебе ещё рано. Порог не узнаёт твой шаг.", ephemeral=True)
        await _dm(member, f"🕯️ **{title}**\nПока рано: нужен уровень **{req_lvl}**.")
        return

    st = await repo.get_story(member.id)
    completed = int(st["chapter_completed"])
    if completed >= chapter_id:
        await safe_send(interaction, "🕯️ Эта дверь уже открыта. Проверь тред истории.", ephemeral=True)
        await _dm(member, f"🕯️ **{title}**\nТы уже открыл эту дверь. Проверь свой тред истории.")
        return

    if chapter_id > 1:
        prev = next((c for c in chapters if int(c.get("id", 0)) == chapter_id - 1), None)
        prev_role = int(prev.get("reward_role_id", 0)) if prev else 0
        if prev_role and member.guild:
            role = member.guild.get_role(prev_role)
            if role and role not in member.roles:
                await safe_send(interaction, "🕯️ Тебя не признают без печати прошлой главы.", ephemeral=True)
                await _dm(member, f"🕯️ **{title}**\nНужна печать прошлой главы (роль) + уровень.")
                return

    kind = str(ch_cfg.get("code_kind", f"story:s1:ch{chapter_id}"))
    code = await repo.get_or_create_code(member.id, kind=kind, length=10)

    msg = (
        f"🕯️ **{title}**\n"
        "В Зале орденов тебе выдали ключ.\n\n"
        f"Ключ (уникальный): **`{code}`**\n\n"
        "Вставь ключ в канал **#дверь-тайн** — и откроется сюжетный замок.\n"
        "Если ты потеряешь это сообщение — нажми кнопку главы ещё раз, и ключ придёт снова."
    )

    ok = await _dm(member, msg)
    if not ok:
        await safe_send(
            interaction,
            "Не смог отправить ЛС. Открой личные сообщения от участников сервера и нажми ещё раз.",
            ephemeral=True,
        )
        return

    await safe_send(interaction, "🕯️ Ключ отправлен тебе в ЛС.", ephemeral=True)


async def orderhall_replay_current(bot: commands.Bot, interaction: discord.Interaction) -> None:
    assert interaction.user and isinstance(interaction.user, discord.Member)
    member: discord.Member = interaction.user
    repo = _repo(bot)
    cfg = _cfg(bot)

    st = await repo.get_story(member.id)
    completed = int(st["chapter_completed"])
    if completed <= 0:
        await safe_send(interaction, "🕯️ Тебе нечего перепроходить.", ephemeral=True)
        return

    current = completed
    chapters = cfg.get("story", {}).get("chapters", [])
    ch_cfg = next((c for c in chapters if int(c.get("id", 0)) == current), None)
    if not ch_cfg:
        await safe_send(interaction, "Глава для перепрохождения не настроена.", ephemeral=True)
        return

    reward_role_id = int(ch_cfg.get("reward_role_id", 0))
    if reward_role_id and member.guild:
        role = member.guild.get_role(reward_role_id)
        if role and role in member.roles:
            try:
                await member.remove_roles(role, reason="Перепрохождение главы: роль временно снята")
            except Exception:
                pass

    kind = str(ch_cfg.get("code_kind", f"story:s1:ch{current}"))
    code = await repo.get_or_create_code(member.id, kind=kind, length=10)

    ok = await _dm(
        member,
        f"🕯️ **Перепрохождение**\n"
        f"Текущая глава: **{ch_cfg.get('title', '')}**\n\n"
        f"Ключ: **`{code}`**\n"
        "Вставь его в **#дверь-тайн**.\n"
        "До успешного прохождения печать (роль) снята.",
    )
    if not ok:
        await safe_send(interaction, "Не смог отправить ЛС (закрыты личные).", ephemeral=True)
        return

    await safe_send(interaction, "🕯️ Инструкция на перепрохождение отправлена в ЛС.", ephemeral=True)


async def _open_story_lock(bot: commands.Bot, member: discord.Member, kind: str) -> None:
    repo = _repo(bot)
    thread = await _get_or_create_story_thread(bot, member)

    if kind == "story:s1:ch1":
        await repo.set_story_fields(member.id, attempt_chapter=1, attempt_json="")
        await thread.send("🕯️ **Дверь приняла ключ.** Открывается замок главы 1…")
        await send_ch1_scene_1(bot, thread, member)
        return

    if kind == "secret:archival_seam":
        cfg = _cfg(bot)
        audio_path = (
            cfg.get("story", {})
            .get("secret_audio", {})
            .get("archival_seam", "assets/audio/archival_seam.mp3")
        )
        p = Path(audio_path)
        if not p.is_absolute():
            p = ROOT / p
        if p.exists():
            try:
                await thread.send(file=discord.File(str(p), filename=p.name))
            except Exception:
                pass
        return


class StoryCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="order_panel")
    @commands.has_permissions(administrator=True)
    async def order_panel(self, ctx: commands.Context):
        cfg = _cfg(self.bot)
        hall_id = cfg["channels"].get("order_hall")
        if not hall_id:
            await ctx.reply("В config.json не задан channels.order_hall")
            return
        ch = self.bot.get_channel(int(hall_id))
        if not isinstance(ch, discord.TextChannel):
            await ctx.reply("order_hall должен быть текстовым каналом.")
            return

        await ch.send(
            "🕯️ **Зал орденов**\n"
            "Выбери главу. Ключ придёт тебе в ЛС. Ключ вводится в **#дверь-тайн**.",
            view=OrderHallView(self.bot),
        )
        await ctx.reply("Панель зала орденов опубликована.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if not message.guild:
            return

        cfg = _cfg(self.bot)
        door_id = cfg["channels"].get("door_secrets")
        if not door_id:
            return
        if message.channel.id != int(door_id):
            return

        content = (message.content or "").strip().upper()
        if len(content) != 10:
            return

        repo = _repo(self.bot)
        ok, kind, status = await repo.consume_code(message.author.id, content)

        try:
            await message.delete()
        except Exception:
            pass

        member = message.author
        if not isinstance(member, discord.Member):
            return

        if not ok:
            if status == "used":
                await message.channel.send(f"{member.mention} 🕯️ Ты уже открыл эту дверь. Проверь тред истории.", delete_after=10)
            elif status == "чужой":
                await message.channel.send(f"{member.mention} 🕯️ Не лезь в чужую историю.", delete_after=10)
            else:
                await message.channel.send(f"{member.mention} 🕯️ Печать не узнаёт этот ключ.", delete_after=10)
            return

        await message.channel.send(f"{member.mention} 🕯️ Ключ принят. Замок открыт.", delete_after=10)
        await _open_story_lock(self.bot, member, kind)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(StoryCog(bot))
