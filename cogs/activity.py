# cogs/activity.py
from __future__ import annotations

import asyncio
import datetime as _dt

from time_utils import msk_day_key
from economy_utils import economy_per_day
import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, Tuple

import discord
from discord.ext import commands, tasks

log = logging.getLogger("void")


def _day_key() -> str:
    return msk_day_key()


def _digits(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


class ActivityCog(commands.Cog):
    """
    Начисление рун/XP за:
      - голосовые каналы (по минутам, с накоплением секунд)
      - сообщения (кулдаун, бонус за медиа/гиф-ссылки)

    Дефолты:
      - войс: 2+ человек = 100%, 1 человек = 25%, AFK=0%
      - войс лимит примерно за 8 часов при 2+ людях
      - чат: минимум 10 символов (без пробелов), кулдаун 10 минут
      - медиа/гиф-ссылки дают больше
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        cfg = getattr(bot, "cfg", {}) or {}
        act: Dict[str, Any] = cfg.get("activity", {}) or {}

        voice = act.get("voice", {}) or {}
        chat = act.get("chat", {}) or {}
        levels = act.get("levels", {}) or {}

        # ---- voice settings ----
        self.voice_share: float = float(voice.get("share", 0.80))
        self.voice_target_hours: float = float(voice.get("target_hours_for_cap", 8))
        self.voice_solo_multiplier: float = float(voice.get("solo_multiplier", 0.25))
        self.voice_xp_per_rune: int = _digits(voice.get("xp_per_rune", 12), 12)
        self.afk_channel_id: int = _digits(voice.get("afk_channel_id", 0), 0)

        # ---- chat settings ----
        self.chat_share: float = float(chat.get("share", 0.20))
        self.chat_cooldown_seconds: int = _digits(chat.get("cooldown_seconds", 60), 600)
        self.chat_min_chars_no_space: int = _digits(chat.get("min_chars_no_space", 10), 10)

        base_reward = chat.get("base_reward", {}) or {}
        media_reward = chat.get("media_reward", {}) or {}
        self.chat_base_runes: int = _digits(base_reward.get("runes", 1), 1)
        self.chat_base_xp: int = _digits(base_reward.get("xp", 12), 12)
        self.chat_media_runes: int = _digits(media_reward.get("runes", 2), 2)
        self.chat_media_xp: int = _digits(media_reward.get("xp", 18), 18)

        self.gif_domains = set((chat.get("gif_domains") or ["tenor.com", "giphy.com"]))
        self.gif_domains = {str(x).lower() for x in self.gif_domains if x}

        # ---- levels ----
        self.max_level: int = _digits(levels.get("max_level", 50), 50)
        xp_formula = levels.get("xp_formula", {}) or {}
        self.xp_need_base: int = _digits(xp_formula.get("base", 250), 250)
        self.xp_need_per_level: int = _digits(xp_formula.get("per_level", 50), 50)

        # guild scope (если бот окажется на нескольких серверах)
        self.guild_id: int = _digits(cfg.get("guild_id", 0), 0)

        # per-user lock to avoid race (voice loop vs chat listener)
        self._locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

        self._voice_loop.start()

    async def cog_unload(self) -> None:
        self._voice_loop.cancel()

    # ----------------- helpers -----------------

    def _need_xp(self, level: int) -> int:
        # need_xp(level) = base + per_level * level
        return max(1, self.xp_need_base + self.xp_need_per_level * int(level))

    def _economy_per_day(self, level: int) -> int:
        cfg = getattr(self.bot, "cfg", {}) or {}
        return economy_per_day(cfg, level)

    def _calc_caps(self, level: int) -> Tuple[int, int, int, int, int]:
        """
        Returns:
          per_day, voice_runes_cap, chat_runes_cap, voice_xp_cap, chat_xp_cap
        """
        per_day = self._economy_per_day(level)
        voice_cap = int(round(per_day * self.voice_share))
        voice_cap = max(0, min(per_day, voice_cap))
        chat_cap = max(0, per_day - voice_cap)

        voice_xp_cap = voice_cap * self.voice_xp_per_rune
        chat_xp_cap = chat_cap * max(self.chat_base_xp, self.chat_media_xp)

        return per_day, voice_cap, chat_cap, voice_xp_cap, chat_xp_cap

    def _voice_seconds_per_rune(self, voice_cap: int) -> int:
        # чтобы выбить voice_cap примерно за voice_target_hours при 2+ людях
        cap = max(1, int(voice_cap))
        sec = int(round(self.voice_target_hours * 3600.0 / cap))
        return max(1, sec)

    def _has_media(self, message: discord.Message) -> bool:
        if message.attachments:
            return True
        stickers = getattr(message, "stickers", None)
        if stickers:
            try:
                if len(stickers) > 0:
                    return True
            except Exception:
                pass
        txt = (message.content or "").lower()
        return any(d in txt for d in self.gif_domains)

    def _text_len_no_space(self, message: discord.Message) -> int:
        txt = message.content or ""
        txt = re.sub(r"\s+", "", txt, flags=re.UNICODE)
        return len(txt)

    async def _reset_day_if_needed(self, repo: Any, user_id: int, u: Dict[str, Any]) -> Dict[str, Any]:
        day = _day_key()
        if u.get("day_key", "") != day:
            await repo.set_user_fields(
                user_id,
                day_key=day,
                voice_runes_today=0,
                chat_runes_today=0,
                voice_xp_today=0,
                chat_xp_today=0,
            )
            u["day_key"] = day
            u["voice_runes_today"] = 0
            u["chat_runes_today"] = 0
            u["voice_xp_today"] = 0
            u["chat_xp_today"] = 0
        return u

    def _lock(self, user_id: int) -> asyncio.Lock:
        return self._locks[int(user_id)]

    async def _apply_xp_and_level(self, repo: Any, user_id: int, u: Dict[str, Any], add_xp: int) -> Tuple[int, int]:
        level = int(u.get("level", 1))
        xp = int(u.get("xp", 0)) + int(add_xp)
        if xp < 0:
            xp = 0

        while level < self.max_level:
            need = self._need_xp(level)
            if xp < need:
                break
            xp -= need
            level += 1

        return level, xp

    # ----------------- VOICE LOOP -----------------

    @tasks.loop(seconds=60)
    async def _voice_loop(self) -> None:
        if self.guild_id:
            guilds = [g for g in self.bot.guilds if g.id == self.guild_id]
        else:
            guilds = list(self.bot.guilds)

        now = int(time.time())

        for g in guilds:
            chans = list(getattr(g, "voice_channels", [])) + list(getattr(g, "stage_channels", []))
            for ch in chans:
                if self.afk_channel_id and int(ch.id) == int(self.afk_channel_id):
                    continue

                members = [m for m in getattr(ch, "members", []) if m and not m.bot]
                if not members:
                    continue

                nonbot_count = len(members)
                weight = 1.0 if nonbot_count >= 2 else float(self.voice_solo_multiplier)
                add_sec = int(round(60 * weight))
                if add_sec <= 0:
                    continue

                for m in members:
                    try:
                        await self._handle_voice_member(m, add_sec, now)
                    except Exception:
                        log.exception("voice award error: guild=%s channel=%s member=%s", g.id, ch.id, m.id)

    @_voice_loop.before_loop
    async def _before_voice_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def _handle_voice_member(self, member: discord.Member, add_sec: int, now_ts: int) -> None:
        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return

        uid = int(member.id)
        async with self._lock(uid):
            await repo.ensure_user(uid)
            u = await repo.get_user(uid)
            u = await self._reset_day_if_needed(repo, uid, u)

            level = int(u.get("level", 1))
            per_day, voice_cap, _, voice_xp_cap, _ = self._calc_caps(level)
            if per_day <= 0 or voice_cap <= 0:
                return

            sec_per_rune = self._voice_seconds_per_rune(voice_cap)

            voice_runes_today = int(u.get("voice_runes_today", 0))
            voice_xp_today = int(u.get("voice_xp_today", 0))
            bank = int(u.get("voice_sec_bank", 0)) + int(add_sec)

            # если лимит выбит — не накапливаем бесконечно
            if voice_runes_today >= voice_cap and voice_xp_today >= voice_xp_cap:
                bank = min(bank, max(0, sec_per_rune - 1))
                await repo.set_user_fields(uid, voice_sec_bank=bank)
                return

            available_runes = max(0, voice_cap - voice_runes_today)
            can_award = bank // sec_per_rune
            award_runes = min(available_runes, int(can_award))
            bank -= award_runes * sec_per_rune

            award_xp = 0
            if award_runes > 0:
                available_xp = max(0, voice_xp_cap - voice_xp_today)
                award_xp = min(available_xp, award_runes * self.voice_xp_per_rune)

            if award_runes == 0 and award_xp == 0:
                await repo.set_user_fields(uid, voice_sec_bank=bank)
                return

            new_level, new_xp = await self._apply_xp_and_level(repo, uid, u, award_xp)

            if award_runes > 0:
                await repo.add_runes(uid, int(award_runes))

            await repo.set_user_fields(
                uid,
                xp=int(new_xp),
                level=int(new_level),
                voice_runes_today=voice_runes_today + int(award_runes),
                voice_xp_today=voice_xp_today + int(award_xp),
                voice_sec_bank=int(bank),
                last_voice_award_ts=int(now_ts),
            )

            # ✅ синк ролей по уровню (если ког подключен)
            lr = self.bot.get_cog("LevelRolesCog")
            if lr is not None:
                try:
                    await lr.sync_member(member, int(new_level), reason="Voice activity level sync")
                except Exception:
                    pass

    # ----------------- CHAT LISTENER -----------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        if not message.guild:
            return
        if self.guild_id and int(message.guild.id) != int(self.guild_id):
            return
        if (message.content or "").startswith("!"):
            return

        has_media = self._has_media(message)
        text_ok = self._text_len_no_space(message) >= self.chat_min_chars_no_space
        if not (text_ok or has_media):
            return

        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return

        uid = int(message.author.id)
        now = int(time.time())

        async with self._lock(uid):
            await repo.ensure_user(uid)
            u = await repo.get_user(uid)
            u = await self._reset_day_if_needed(repo, uid, u)

            last = int(u.get("last_chat_award_ts", 0))
            if now - last < self.chat_cooldown_seconds:
                return

            level = int(u.get("level", 1))
            per_day, _, chat_cap, _, chat_xp_cap = self._calc_caps(level)
            if per_day <= 0:
                return

            chat_runes_today = int(u.get("chat_runes_today", 0))
            chat_xp_today = int(u.get("chat_xp_today", 0))

            if has_media:
                want_runes, want_xp = self.chat_media_runes, self.chat_media_xp
            else:
                want_runes, want_xp = self.chat_base_runes, self.chat_base_xp

            add_runes = min(max(0, chat_cap - chat_runes_today), int(want_runes))
            add_xp = min(max(0, chat_xp_cap - chat_xp_today), int(want_xp))

            if add_runes <= 0 and add_xp <= 0:
                return

            new_level, new_xp = await self._apply_xp_and_level(repo, uid, u, add_xp)

            if add_runes > 0:
                await repo.add_runes(uid, int(add_runes))

            await repo.set_user_fields(
                uid,
                xp=int(new_xp),
                level=int(new_level),
                chat_runes_today=chat_runes_today + int(add_runes),
                chat_xp_today=chat_xp_today + int(add_xp),
                last_chat_award_ts=int(now),
            )

            # ✅ синк ролей по уровню (если ког подключен)
            if isinstance(message.author, discord.Member):
                lr = self.bot.get_cog("LevelRolesCog")
                if lr is not None:
                    try:
                        await lr.sync_member(message.author, int(new_level), reason="Chat activity level sync")
                    except Exception:
                        pass

    # ----------------- USER COMMANDS -----------------

    @commands.command(name="income")
    async def income(self, ctx: commands.Context) -> None:
        """Показывает дневные лимиты и прогресс по активности."""
        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return

        uid = int(ctx.author.id)
        async with self._lock(uid):
            await repo.ensure_user(uid)
            u = await repo.get_user(uid)
            u = await self._reset_day_if_needed(repo, uid, u)

        level = int(u.get("level", 1))
        xp = int(u.get("xp", 0))
        need = self._need_xp(level) if level < self.max_level else 0

        per_day, voice_cap, chat_cap, voice_xp_cap, chat_xp_cap = self._calc_caps(level)
        sec_per_rune = self._voice_seconds_per_rune(voice_cap) if voice_cap > 0 else 0

        voice_r_today = int(u.get("voice_runes_today", 0))
        chat_r_today = int(u.get("chat_runes_today", 0))
        voice_x_today = int(u.get("voice_xp_today", 0))
        chat_x_today = int(u.get("chat_xp_today", 0))
        bank = int(u.get("voice_sec_bank", 0))

        prog = ""
        if sec_per_rune > 0:
            pct = int((min(bank, sec_per_rune) / sec_per_rune) * 100)
            prog = f"{pct}% до следующей руны (войс)"

        e = discord.Embed(title="🕯️ Доход и прогресс", color=0x2B2D31)
        e.add_field(name="Уровень", value=f"**{level}** / {self.max_level}", inline=True)
        if level < self.max_level:
            e.add_field(name="XP", value=f"**{xp}** / {need}", inline=True)
        else:
            e.add_field(name="XP", value=f"**{xp}** (уровень макс.)", inline=True)

        e.add_field(name="Руны в день (итого)", value=f"**{per_day}**", inline=True)
        e.add_field(name="Войс (руны)", value=f"**{voice_r_today} / {voice_cap}**", inline=True)
        e.add_field(name="Чат (руны)", value=f"**{chat_r_today} / {chat_cap}**", inline=True)
        e.add_field(name="Войс (XP)", value=f"**{voice_x_today} / {voice_xp_cap}**", inline=True)
        e.add_field(name="Чат (XP)", value=f"**{chat_x_today} / {chat_xp_cap}**", inline=True)

        if sec_per_rune > 0:
            mins = round(sec_per_rune / 60, 1)
            mins_solo = round((sec_per_rune / max(0.01, self.voice_solo_multiplier)) / 60, 1)
            e.add_field(
                name="Войс: скорость",
                value=f"≈ **{mins} мин/руна** (2+)\n≈ **{mins_solo} мин/руна** (соло)",
                inline=False,
            )
        if prog:
            e.set_footer(text=prog)

        e.add_field(
            name="Чат: условия",
            value=(
                f"Кулдаун: **{self.chat_cooldown_seconds//60} мин**\n"
                f"Минимум: **{self.chat_min_chars_no_space}** символов (без пробелов)\n"
                f"Бонус за медиа/гиф: **{self.chat_media_runes} руны / {self.chat_media_xp} XP**"
            ),
            inline=False,
        )

        await ctx.reply(embed=e, mention_author=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ActivityCog(bot))