# =========================
# FILE: cogs/void_info.py
# =========================
from __future__ import annotations

from time_utils import msk_day_key
from typing import Any, Optional, Tuple

import discord
from discord.ext import commands

from ._interactions import GuardedView, safe_send
from .decorations import (
    DECORATIONS_BY_ID,
    SHOP_DECORATIONS,
    STORY_DECORATION_ID,
    STORY_DECORATION_ROLE_NAME,
    Decoration,
)

ORDER_LABELS: dict[str, str] = {
    "rher": "Орден Rher",
    "sylvian": "Орден Sylvian",
    "gro": "Орден Gro-Goroth",
}
ORDER_EMOJI: dict[str, str] = {
    "rher": "🌙",
    "sylvian": "🌿",
    "gro": "🔥",
}


def _is_admin(bot: commands.Bot, user_id: int) -> bool:
    return bot.is_admin(user_id)  # type: ignore


def _day_key() -> str:
    return msk_day_key()


def _digits(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


async def _safe_defer(interaction: discord.Interaction, *, ephemeral: bool) -> bool:
    """
    Пытаемся быстро подтвердить interaction.
    Возвращает True если подтверждение прошло, False если Discord уже "протух".
    """
    try:
        if interaction.response.is_done():
            return True
        await interaction.response.defer(ephemeral=ephemeral, thinking=False)
        return True
    except (discord.NotFound, discord.HTTPException):
        # NotFound = Unknown interaction (10062)
        return False


async def _fetch_user_row(bot: commands.Bot, user_id: int) -> dict[str, Any]:
    repo = bot.repo  # type: ignore
    await repo.ensure_user(user_id)
    u = await repo.get_user(user_id)
    return u


async def _reset_day_if_needed(bot: commands.Bot, user_id: int, u: dict[str, Any]) -> dict[str, Any]:
    repo = bot.repo  # type: ignore
    day = _day_key()
    if (u.get("day_key", "") or "") != day:
        await repo.set_user_fields(
            user_id,
            day_key=day,
            voice_runes_today=0,
            chat_runes_today=0,
            voice_xp_today=0,
            chat_xp_today=0,
            last_voice_award_ts=0,
            last_chat_award_ts=0,
        )
        u["day_key"] = day
        u["voice_runes_today"] = 0
        u["chat_runes_today"] = 0
        u["voice_xp_today"] = 0
        u["chat_xp_today"] = 0
        u["last_voice_award_ts"] = 0
        u["last_chat_award_ts"] = 0
    return u


def _economy_per_day(bot: commands.Bot, level: int) -> int:
    cfg = bot.cfg  # type: ignore
    economy = (cfg.get("economy", {}) or {}).get("rune_income_per_day", []) or []
    lvl = int(level)
    for band in economy:
        try:
            f = int(band.get("from", 0))
            t = int(band.get("to", 0))
            p = int(band.get("per_day", 0))
        except Exception:
            continue
        if f <= lvl <= t:
            return max(0, p)
    return 0


def _activity_cfg(bot: commands.Bot) -> dict[str, Any]:
    cfg = bot.cfg  # type: ignore
    return (cfg.get("activity", {}) or {})


def _need_xp(bot: commands.Bot, level: int) -> int:
    act = _activity_cfg(bot)
    levels = act.get("levels", {}) or {}
    xp_formula = levels.get("xp_formula", {}) or {}
    base = _digits(xp_formula.get("base", 250), 250)
    per_level = _digits(xp_formula.get("per_level", 50), 50)
    return max(1, base + per_level * int(level))


def _max_level(bot: commands.Bot) -> int:
    act = _activity_cfg(bot)
    levels = act.get("levels", {}) or {}
    return _digits(levels.get("max_level", 50), 50)


def _calc_caps(bot: commands.Bot, level: int) -> Tuple[int, int, int, int, int]:
    """
    Returns:
      per_day, voice_runes_cap, chat_runes_cap, voice_xp_cap, chat_xp_cap
    """
    act = _activity_cfg(bot)
    voice = act.get("voice", {}) or {}
    chat = act.get("chat", {}) or {}

    voice_share = float(voice.get("share", 0.80))
    chat_share = float(chat.get("share", 0.20))

    # per_day по вашей экономике
    per_day = _economy_per_day(bot, level)

    # деление 80/20 (округление голосом, остаток в чат)
    voice_cap = int(round(per_day * voice_share))
    voice_cap = max(0, min(per_day, voice_cap))
    chat_cap = max(0, per_day - voice_cap)

    # XP caps
    xp_per_rune = _digits(voice.get("xp_per_rune", 12), 12)
    base_reward = chat.get("base_reward", {}) or {}
    media_reward = chat.get("media_reward", {}) or {}
    chat_base_xp = _digits(base_reward.get("xp", 12), 12)
    chat_media_xp = _digits(media_reward.get("xp", 18), 18)

    voice_xp_cap = voice_cap * xp_per_rune
    chat_xp_cap = chat_cap * max(chat_base_xp, chat_media_xp)

    return per_day, voice_cap, chat_cap, voice_xp_cap, chat_xp_cap


def _voice_params(bot: commands.Bot) -> Tuple[int, float, float]:
    act = _activity_cfg(bot)
    voice = act.get("voice", {}) or {}
    afk_id = _digits(voice.get("afk_channel_id", 0), 0)
    target_hours = float(voice.get("target_hours_for_cap", 8))
    solo_mult = float(voice.get("solo_multiplier", 0.25))
    return afk_id, target_hours, solo_mult


def _voice_sec_per_rune(bot: commands.Bot, voice_cap: int) -> int:
    _, target_hours, _ = _voice_params(bot)
    cap = max(1, int(voice_cap))
    sec = int(round(target_hours * 3600.0 / cap))
    return max(1, sec)


def _chat_params(bot: commands.Bot) -> Tuple[int, int, int, int, int]:
    act = _activity_cfg(bot)
    chat = act.get("chat", {}) or {}
    cooldown = _digits(chat.get("cooldown_seconds", 600), 600)
    min_chars = _digits(chat.get("min_chars_no_space", 10), 10)
    base_reward = chat.get("base_reward", {}) or {}
    media_reward = chat.get("media_reward", {}) or {}
    base_runes = _digits(base_reward.get("runes", 1), 1)
    base_xp = _digits(base_reward.get("xp", 12), 12)
    media_runes = _digits(media_reward.get("runes", 2), 2)
    media_xp = _digits(media_reward.get("xp", 18), 18)
    return cooldown, min_chars, base_runes, base_xp, media_runes, media_xp


async def _fetch_top10_by_runes(bot: commands.Bot, guild: Optional[discord.Guild]) -> list[tuple[int, int, int]]:
    """
    Returns: [(user_id, runes, level), ...] только для тех, кто реально есть на сервере.
    """
    repo = bot.repo  # type: ignore

    cur = await repo.conn.execute(
        "SELECT user_id, runes, level FROM users ORDER BY runes DESC, level DESC LIMIT 50"
    )
    rows = await cur.fetchall()
    await cur.close()

    out: list[tuple[int, int, int]] = []
    for r in rows:
        uid = int(r[0])
        runes = int(r[1])
        lvl = int(r[2])

        if guild is None:
            out.append((uid, runes, lvl))
        else:
            m = guild.get_member(uid)
            if m is None:
                continue
            out.append((uid, runes, lvl))

        if len(out) >= 10:
            break

    return out


async def _build_order_influence_embed(bot: commands.Bot, guild: Optional[discord.Guild]) -> discord.Embed:
    repo = bot.repo  # type: ignore
    raw = await repo.order_all_influence()

    rows: list[tuple[str, int]] = []
    for oid in ("rher", "sylvian", "gro"):
        rows.append((oid, int(raw.get(oid, 0))))
    rows.sort(key=lambda x: x[1], reverse=True)

    lines: list[str] = []
    for i, (oid, influence) in enumerate(rows, start=1):
        label = ORDER_LABELS.get(oid, oid)
        emoji = ORDER_EMOJI.get(oid, "✦")
        herald_text = "—"
        try:
            herald = await repo.order_get_herald(oid)
            herald_id = int(herald.get("user_id", 0) or 0)
            if herald_id > 0:
                member = guild.get_member(herald_id) if guild else None
                herald_text = member.mention if member else f"`{herald_id}`"
        except Exception:
            pass

        lines.append(f"**{i}.** {emoji} {label} — **{influence}** влияния")
        lines.append(f"Глашатай: {herald_text}")

    emb = discord.Embed(
        title="⚔️ Равновесие Орденов — топ по влиянию",
        description="\n".join(lines) if lines else "Пока нет данных по влиянию орденов.",
    )
    emb.set_footer(text="Кнопка обновляет влияние и текущих глашатаев.")
    return emb


def _leaderboard_scope_ids(guild: Optional[discord.Guild]) -> set[int] | None:
    if guild is None:
        return None
    return {int(m.id) for m in guild.members}


def _story_reward_role_id(bot: commands.Bot) -> int:
    cfg = bot.cfg  # type: ignore
    chapters = ((cfg.get("story", {}) or {}).get("chapters", []) or [])
    for ch in chapters:
        if _digits(ch.get("id", 0), 0) != 1:
            continue
        role_id = _digits(ch.get("reward_role_id", 0), 0)
        if role_id > 0:
            return role_id
    return 0


def _has_story_decoration_access(bot: commands.Bot, member: discord.Member) -> bool:
    role_id = _story_reward_role_id(bot)
    if role_id > 0 and member.get_role(role_id):
        return True
    target = STORY_DECORATION_ROLE_NAME.casefold()
    for role in member.roles:
        if str(role.name).strip().casefold() == target:
            return True
    return False


async def _available_decorations(bot: commands.Bot, member: discord.Member) -> list[Decoration]:
    repo = bot.repo  # type: ignore
    owned = await repo.purchases_by_kind(member.id, "decoration")

    out: list[Decoration] = [d for d in SHOP_DECORATIONS if d.id in owned]
    story_dec = DECORATIONS_BY_ID.get(STORY_DECORATION_ID)
    if story_dec and _has_story_decoration_access(bot, member):
        out.append(story_dec)

    return out


def _resolve_active_decoration(
    bot: commands.Bot,
    member: Optional[discord.Member],
    decoration_id: str,
) -> Optional[Decoration]:
    d = DECORATIONS_BY_ID.get(str(decoration_id or ""))
    if not d:
        return None

    if d.id == STORY_DECORATION_ID:
        if member is None:
            return None
        if not _has_story_decoration_access(bot, member):
            return None

    return d


def _voice_mode_now(bot: commands.Bot, member: discord.Member) -> str:
    afk_id, _, solo_mult = _voice_params(bot)

    vs = getattr(member, "voice", None)
    if vs is None or vs.channel is None:
        return "не в войсе"

    ch = vs.channel
    try:
        ch_id = int(ch.id)
    except Exception:
        ch_id = 0

    if afk_id and ch_id == afk_id:
        return "AFK-канал (0%)"

    members = [m for m in getattr(ch, "members", []) if m and not m.bot]
    n = len(members)
    if n >= 2:
        return "2+ в канале (100%)"
    return f"соло ({int(round(solo_mult*100))}%)"


class DecorationPickSelect(discord.ui.Select):
    def __init__(self, bot: commands.Bot, owner_id: int, options: list[Decoration], current_id: str):
        self.bot = bot
        self.owner_id = int(owner_id)
        self.allowed_ids: set[str] = {"none"}

        select_options: list[discord.SelectOption] = [
            discord.SelectOption(
                label="Без украшения",
                value="none",
                description="Скрыть строку Печать в летописи",
                default=(not current_id),
            )
        ]

        for d in options:
            self.allowed_ids.add(d.id)
            select_options.append(
                discord.SelectOption(
                    label=d.name[:100],
                    value=d.id,
                    emoji=d.emoji,
                    default=(d.id == current_id),
                )
            )

        super().__init__(
            placeholder="Выбери активное украшение",
            min_values=1,
            max_values=1,
            options=select_options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await safe_send(interaction, "Это меню не для тебя.", ephemeral=True)
            return

        selected = str(self.values[0] if self.values else "none")
        if selected not in self.allowed_ids:
            await safe_send(interaction, "Украшение недоступно.", ephemeral=True)
            return

        repo = self.bot.repo  # type: ignore
        if selected == "none":
            await repo.set_active_decoration(self.owner_id, None)
            text = "✅ Активное украшение снято."
        else:
            await repo.set_active_decoration(self.owner_id, selected)
            d = DECORATIONS_BY_ID.get(selected)
            if d:
                text = f"✅ Активно: {d.emoji} {d.name}."
            else:
                text = "✅ Украшение обновлено."

        try:
            await interaction.response.edit_message(content=text, embed=None, view=None)
        except Exception:
            await safe_send(interaction, text, ephemeral=True)


class DecorationPickView(GuardedView):
    def __init__(self, bot: commands.Bot, owner_id: int, options: list[Decoration], current_id: str):
        super().__init__(timeout=120)
        self.owner_id = int(owner_id)
        self.add_item(DecorationPickSelect(bot, owner_id, options, current_id))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await safe_send(interaction, "Это меню не для тебя.", ephemeral=True)
            return False
        return True


class VoidInfoView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)  # persistent
        self.bot = bot

    @discord.ui.button(
        label="Проверить себя",
        style=discord.ButtonStyle.primary,
        custom_id="void:check_me",
    )
    async def check_me(self, interaction: discord.Interaction, _: discord.ui.Button):
        ok = await _safe_defer(interaction, ephemeral=True)
        if not ok:
            return

        repo = self.bot.repo  # type: ignore
        uid = int(interaction.user.id)

        u = await _fetch_user_row(self.bot, uid)
        u = await _reset_day_if_needed(self.bot, uid, u)

        level = int(u.get("level", 1))
        runes = int(u.get("runes", 0))
        xp = int(u.get("xp", 0))
        max_lvl = _max_level(self.bot)

        per_day, voice_cap, chat_cap, voice_xp_cap, chat_xp_cap = _calc_caps(self.bot, level)
        need = _need_xp(self.bot, level) if level < max_lvl else 0

        voice_r_today = int(u.get("voice_runes_today", 0))
        chat_r_today = int(u.get("chat_runes_today", 0))
        voice_x_today = int(u.get("voice_xp_today", 0))
        chat_x_today = int(u.get("chat_xp_today", 0))
        bank = int(u.get("voice_sec_bank", 0))

        # скорость/процент до руны
        sec_per_rune = _voice_sec_per_rune(self.bot, voice_cap) if voice_cap > 0 else 0
        prog = ""
        if sec_per_rune > 0:
            pct = int((min(bank, sec_per_rune) / sec_per_rune) * 100)
            prog = f"{pct}% до следующей руны (войс)"
            mins = round(sec_per_rune / 60, 1)
            _, _, solo_mult = _voice_params(self.bot)
            mins_solo = round((sec_per_rune / max(0.01, solo_mult)) / 60, 1)
        else:
            mins = 0.0
            mins_solo = 0.0

        cooldown, min_chars, base_runes, base_xp, media_runes, media_xp = _chat_params(self.bot)

        emb = discord.Embed(title="🕳️ Твой след в Пустоте", color=0x2B2D31)
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None and interaction.guild:
            member = interaction.guild.get_member(uid)

        active_id = await repo.get_active_decoration(uid)
        active_dec = _resolve_active_decoration(self.bot, member, active_id)
        if active_dec:
            emb.description = f"{active_dec.emoji} {active_dec.name}"

        # верхняя строка как в income
        emb.add_field(name="Уровень", value=f"**{level}** / {max_lvl}", inline=True)
        if level < max_lvl:
            emb.add_field(name="XP", value=f"**{xp}** / {need}", inline=True)
        else:
            emb.add_field(name="XP", value=f"**{xp}** (макс.)", inline=True)
        emb.add_field(name="Руны в день (итого)", value=f"**{per_day}**", inline=True)

        # лимиты
        emb.add_field(name="Войс (руны)", value=f"**{voice_r_today} / {voice_cap}**", inline=True)
        emb.add_field(name="Чат (руны)", value=f"**{chat_r_today} / {chat_cap}**", inline=True)
        emb.add_field(name="Войс (XP)", value=f"**{voice_x_today} / {voice_xp_cap}**", inline=True)
        emb.add_field(name="Чат (XP)", value=f"**{chat_x_today} / {chat_xp_cap}**", inline=True)

        # скорость войса
        if sec_per_rune > 0:
            emb.add_field(
                name="Войс: скорость",
                value=f"≈ **{mins} мин/руна** (2+)\n≈ **{mins_solo} мин/руна** (соло)",
                inline=False,
            )

        # режим войса сейчас (чтобы не гадать)
        if isinstance(interaction.user, discord.Member):
            mode = _voice_mode_now(self.bot, interaction.user)
            emb.add_field(name="Войс: режим сейчас", value=f"**{mode}**", inline=False)

        # чат условия
        emb.add_field(
            name="Чат: условия",
            value=(
                f"Кулдаун: **{cooldown//60} мин**\n"
                f"Минимум: **{min_chars}** символов (без пробелов)\n"
                f"Обычное: **{base_runes} руна / {base_xp} XP**\n"
                f"Медиа/гиф: **{media_runes} руны / {media_xp} XP**"
            ),
            inline=False,
        )

        if prog:
            emb.set_footer(text=prog)
        else:
            emb.set_footer(text="Пустота видит цифры. Но не видит смысла.")

        try:
            await interaction.followup.send(embed=emb, ephemeral=True)
        except (discord.NotFound, discord.HTTPException):
            pass

    @discord.ui.button(
        label="Выбрать украшение",
        style=discord.ButtonStyle.secondary,
        custom_id="void:pick_decoration",
    )
    async def pick_decoration(self, interaction: discord.Interaction, _: discord.ui.Button):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None and interaction.guild:
            member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await safe_send(interaction, "Открой выбор украшения внутри сервера.", ephemeral=True)
            return

        repo = self.bot.repo  # type: ignore
        current_id = await repo.get_active_decoration(member.id)
        options = await _available_decorations(self.bot, member)

        if current_id and _resolve_active_decoration(self.bot, member, current_id) is None:
            await repo.set_active_decoration(member.id, None)
            current_id = ""

        view = DecorationPickView(self.bot, member.id, options, current_id)
        if options:
            desc = f"Доступно украшений: **{len(options)}**.\nВыбери одно активное или отключи украшение."
        else:
            desc = "У тебя пока нет доступных украшений. Купи их в магазине Пустоты."

        emb = discord.Embed(title="✨ Выбор украшения", description=desc)
        await safe_send(interaction, embed=emb, view=view, ephemeral=True)

    @discord.ui.button(
        label="Моё место в летописи",
        style=discord.ButtonStyle.secondary,
        custom_id="void:my_rank",
    )
    async def my_rank(self, interaction: discord.Interaction, _: discord.ui.Button):
        ok = await _safe_defer(interaction, ephemeral=True)
        if not ok:
            return

        repo = self.bot.repo  # type: ignore
        uid = int(interaction.user.id)
        scope_ids = _leaderboard_scope_ids(interaction.guild)
        info = await repo.get_runes_rank_info(uid, scope_user_ids=scope_ids)

        rank = int(info.get("rank", 0) or 0)
        runes = int(info.get("current_runes", 0) or 0)

        if rank <= 0:
            text = (
                "Ты пока не попал в Летопись Пустоты.\n"
                f"У тебя {runes} рун."
            )
        elif rank == 1:
            text = (
                "Ты находишься на 1 месте в Летописи Пустоты.\n"
                f"У тебя {runes} рун.\n"
                "Ты возглавляешь летопись."
            )
        else:
            need = int(info.get("runes_needed", 0) or 0)
            text = (
                f"Ты находишься на {rank} месте в Летописи Пустоты.\n"
                f"У тебя {runes} рун.\n"
                f"До {rank - 1} места осталось {need} рун."
            )

        await safe_send(interaction, text, ephemeral=True)


class LeaderboardView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)  # persistent
        self.bot = bot

    @discord.ui.button(
        label="Обновить летопись",
        style=discord.ButtonStyle.secondary,
        custom_id="void:refresh_leaderboard",
    )
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        ok = await _safe_defer(interaction, ephemeral=True)
        if not ok:
            return

        rows = await _fetch_top10_by_runes(self.bot, interaction.guild)
        repo = self.bot.repo  # type: ignore

        lines: list[str] = []
        for i, (uid, runes, lvl) in enumerate(rows, start=1):
            member = interaction.guild.get_member(uid) if interaction.guild else None
            name = member.mention if member else f"`{uid}`"
            lines.append(f"**{i}.** {name} — **{runes}** рун | ур.{lvl}")
            active_id = await repo.get_active_decoration(uid)
            active_dec = _resolve_active_decoration(self.bot, member, active_id)
            if active_dec:
                lines.append(f"{active_dec.emoji} {active_dec.name}")

        emb = discord.Embed(
            title="🏛️ Летопись Пустоты — топ 10 по рунам",
            description="\n".join(lines) if lines else "Пусто. Ни одной души в списках.",
        )
        emb.set_footer(text="Нажми кнопку — и строки перепишутся.")

        try:
            await interaction.message.edit(embed=emb, view=self)
        except Exception:
            pass

        try:
            await interaction.followup.send("✅ Летопись обновлена.", ephemeral=True)
        except Exception:
            pass


class OrdersInfluenceView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)  # persistent
        self.bot = bot

    @discord.ui.button(
        label="Обновить влияние орденов",
        style=discord.ButtonStyle.secondary,
        custom_id="void:refresh_order_influence",
    )
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        ok = await _safe_defer(interaction, ephemeral=True)
        if not ok:
            return

        emb = await _build_order_influence_embed(self.bot, interaction.guild)
        try:
            await interaction.message.edit(embed=emb, view=self)
        except Exception:
            pass

        try:
            await interaction.followup.send("✅ Влияние орденов обновлено.", ephemeral=True)
        except Exception:
            pass


def get_persistent_views(bot: commands.Bot):
    return [VoidInfoView(bot), LeaderboardView(bot), OrdersInfluenceView(bot)]



class VoidInfoCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.info_view: Optional[VoidInfoView] = None
        self.lb_view: Optional[LeaderboardView] = None
        self.order_inf_view: Optional[OrdersInfluenceView] = None

    async def cog_load(self) -> None:
        self.info_view = VoidInfoView(self.bot)
        self.lb_view = LeaderboardView(self.bot)
        self.order_inf_view = OrdersInfluenceView(self.bot)

    @commands.command(name="post_void_panels")
    async def post_void_panels(self, ctx: commands.Context) -> None:
        if not _is_admin(self.bot, ctx.author.id):
            return

        cfg = self.bot.cfg  # type: ignore
        ch_id = int(cfg["channels"]["void_info"])
        ch = ctx.guild.get_channel(ch_id) if ctx.guild else None
        if not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Канал информации пустоты не найден.")
            return

        assert self.info_view is not None
        assert self.lb_view is not None
        assert self.order_inf_view is not None

        emb1 = discord.Embed(
            title="🕳️ Информация Пустоты",
            description="Здесь не спрашивают — здесь проверяют.\nНажми кнопку и увидишь свой след.",
        )
        m1 = await ch.send(embed=emb1, view=self.info_view)
        try:
            await m1.pin()
        except Exception:
            pass

        rows = await _fetch_top10_by_runes(self.bot, ctx.guild)
        repo = self.bot.repo  # type: ignore
        lines: list[str] = []
        for i, (uid, runes, lvl) in enumerate(rows, start=1):
            member = ctx.guild.get_member(uid) if ctx.guild else None
            name = member.mention if member else f"`{uid}`"
            lines.append(f"**{i}.** {name} — **{runes}** рун | ур.{lvl}")
            active_id = await repo.get_active_decoration(uid)
            active_dec = _resolve_active_decoration(self.bot, member, active_id)
            if active_dec:
                lines.append(f"{active_dec.emoji} {active_dec.name}")

        emb2 = discord.Embed(
            title="🏛️ Летопись Пустоты — топ 10 по рунам",
            description="\n".join(lines) if lines else "Пусто. Ни одной души в списках.",
        )
        emb2.set_footer(text="Кнопка переписывает строки прямо в этом сообщении.")
        m2 = await ch.send(embed=emb2, view=self.lb_view)
        try:
            await m2.pin()
        except Exception:
            pass

        emb3 = await _build_order_influence_embed(self.bot, ctx.guild)
        m3 = await ch.send(embed=emb3, view=self.order_inf_view)
        try:
            await m3.pin()
        except Exception:
            pass

        await ctx.send("✅ Панели Пустоты опубликованы и закреплены.")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(VoidInfoCog(bot))
