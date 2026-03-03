# cogs/duel.py
from __future__ import annotations

import asyncio
import math
import time
from io import BytesIO
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import discord
from discord.ext import commands
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "assets" / "duel"

BANNER_PATH = ASSETS / "duel_banner.png"
ARENA_PATH = ASSETS / "arena.png"
CM_PATH = ASSETS / "cm.png"
HOOK_PATH = ASSETS / "hook.png"


def ceil_int(x: float) -> int:
    return int(math.ceil(x))


def stake_options_for_level(level: int) -> List[int]:
    """
    С 15 по 20: 30,40,50,60
    каждые +5 уровней: +10 ко всем
    """
    if level < 15:
        return []
    step = (level - 15) // 5  # 0,1,2...
    base = 30 + step * 10
    return [base, base + 10, base + 20, base + 30]


def can_play_duel(level: int) -> bool:
    return level >= 15


# Коэффы по стороне (победитель)
COEF_PUDGE = 0.9
COEF_CM = 1.4

# Координаты клеток (3 ряда × 3 колонки). Колонка: 0,1,2. Ряд: 0=верх,1=середина,2=низ.
CELL_CENTERS = [
    [(520, 230), (520, 520), (520, 820)],   # col 0
    [(770, 230), (770, 520), (770, 820)],   # col 1
    [(1030, 230), (1030, 520), (1030, 820)] # col 2
]


def render_arena(cm_pos: Tuple[int, int], hook_pos: Tuple[int, int]) -> BytesIO:
    """Render arena into memory (safe for parallel matches)."""
    arena = Image.open(ARENA_PATH).convert("RGBA")
    cm = Image.open(CM_PATH).convert("RGBA")
    hook = Image.open(HOOK_PATH).convert("RGBA")

    cm_size = (180, 180)
    hook_size = (180, 180)
    cm = cm.resize(cm_size, Image.LANCZOS)
    hook = hook.resize(hook_size, Image.LANCZOS)

    def paste_icon(img: Image.Image, icon: Image.Image, col: int, row: int):
        cx, cy = CELL_CENTERS[col][row]
        x = int(cx - icon.size[0] / 2)
        y = int(cy - icon.size[1] / 2)
        img.alpha_composite(icon, (x, y))

    paste_icon(arena, cm, cm_pos[0], cm_pos[1])
    paste_icon(arena, hook, hook_pos[0], hook_pos[1])

    bio = BytesIO()
    arena.save(bio, format="PNG")
    bio.seek(0)
    return bio


@dataclass
class Bet:
    user_id: int
    side: str  # "pudge" / "cm"
    stake: int


@dataclass
class DuelMatch:
    channel_id: int
    created_ts: int = field(default_factory=lambda: int(time.time()))
    starter_id: int = 0

    pudge_id: Optional[int] = None
    cm_id: Optional[int] = None

    stake: int = 0
    phase: str = "lobby"  # lobby -> stake_pick -> stake_confirm -> round -> finished/cancelled

    lobby_message_id: Optional[int] = None
    status_message_id: Optional[int] = None

    round_index: int = 0  # 0..2
    cm_row: Optional[int] = None
    hook_row: Optional[int] = None

    bets: Dict[int, Bet] = field(default_factory=dict)

    task_timeout: Optional[asyncio.Task] = None
    task_turn_timeout: Optional[asyncio.Task] = None


def get_persistent_views(bot: commands.Bot):
    return [StartDuelView()]


class DuelCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.matches: Dict[int, DuelMatch] = {}

        # ✅ persistent view для кнопки "Играть" на закреплённой панели
        # persistent view регистрируется через cogs/persistent.py

    async def _get_level(self, user_id: int) -> int:
        repo = getattr(self.bot, "repo", None)
        if repo is None:
            return 1
        u = await repo.get_user(user_id)
        return int(u.get("level", 1))

    def _is_admin(self, user_id: int) -> bool:
        cfg = getattr(self.bot, "cfg", {})  # type: ignore
        return int(cfg.get("admin_user_id", 0)) == int(user_id)

    async def _safe_delete(self, msg: Optional[discord.Message]) -> None:
        if not msg:
            return
        try:
            await msg.delete()
        except Exception:
            pass

    async def _send_results(self, guild: discord.Guild, text: str) -> None:
        cfg = getattr(self.bot, "cfg", {})  # type: ignore
        ch_id = int(cfg.get("channels", {}).get("duel_results", 0))
        if not ch_id:
            return
        ch = guild.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            return
        await ch.send(text)

    # ---------- admin: post panel ----------
    @commands.command(name="post_duel_panels")
    async def post_duel_panels(self, ctx: commands.Context) -> None:
        if not self._is_admin(ctx.author.id):
            return

        cfg = getattr(self.bot, "cfg", {})  # type: ignore
        channels = cfg.get("channels", {})
        keys = ["duel_1", "duel_2"]

        for key in keys:
            ch_id = int(channels.get(key, 0))
            ch = ctx.guild.get_channel(ch_id) if ctx.guild else None
            if not isinstance(ch, discord.TextChannel):
                continue

            embed = discord.Embed(
                title="🎣 Алтарь Дуэли — Пудж vs ЦМ",
                description=(
                    "Нажми **Играть**, чтобы открыть матч.\n"
                    "Один матч — один канал.\n"
                    "Ставки доступны с **15 уровня**."
                ),
            )

            file = None
            if BANNER_PATH.exists():
                file = discord.File(BANNER_PATH.as_posix(), filename="duel_banner.png")
                embed.set_image(url="attachment://duel_banner.png")

            # ✅ view без channel_id, потому что она persistent и берёт channel из interaction.channel
            msg = await ch.send(embed=embed, view=StartDuelView(self), file=file)
            try:
                await msg.pin()
            except Exception:
                pass

        await ctx.send("✅ Панели дуэли опубликованы и закреплены.")

    # ---------- match flow ----------
    async def start_lobby(self, channel: discord.TextChannel, starter: discord.Member) -> None:
        if channel.id in self.matches and self.matches[channel.id].phase not in ("finished", "cancelled"):
            await channel.send("⚠️ В этом канале уже идёт матч.", delete_after=6)
            return

        match = DuelMatch(channel_id=channel.id, starter_id=starter.id)
        self.matches[channel.id] = match

        embed = discord.Embed(
            title="⚔️ Матч открыт",
            description=(
                "Выбери сторону:\n"
                "🎣 **Пудж** — выигрывает, если угадает линию ЦМ.\n"
                "❄️ **ЦМ** — выигрывает, если проживёт 3 шага.\n\n"
                "⏳ У вас **60 сек** выбрать стороны. Иначе матч исчезнет.\n"
                "💰 Ставки будут предложены после выбора сторон."
            ),
        )
        embed.add_field(name="Пудж", value="—", inline=True)
        embed.add_field(name="ЦМ", value="—", inline=True)

        view = LobbyView(self, match)
        msg = await channel.send(embed=embed, view=view)
        match.lobby_message_id = msg.id

        async def lobby_timeout():
            await asyncio.sleep(60)
            m = self.matches.get(channel.id)
            if not m or m.phase != "lobby":
                return
            if not m.pudge_id or not m.cm_id:
                m.phase = "cancelled"
                await self._safe_delete(msg)
                await channel.send("⛓️ Матч рассыпался. Никто не взял обе стороны вовремя.", delete_after=8)
                self.matches.pop(channel.id, None)

        match.task_timeout = asyncio.create_task(lobby_timeout())

    async def on_sides_chosen(self, channel: discord.TextChannel, match: DuelMatch, lobby_msg: discord.Message) -> None:
        if match.task_timeout:
            match.task_timeout.cancel()
            match.task_timeout = None

        pudge_lvl = await self._get_level(match.pudge_id)  # type: ignore
        cm_lvl = await self._get_level(match.cm_id)  # type: ignore

        if pudge_lvl < cm_lvl:
            proposer_id = match.pudge_id
        elif cm_lvl < pudge_lvl:
            proposer_id = match.cm_id
        else:
            proposer_id = match.starter_id

        match.phase = "stake_pick"

        proposer = channel.guild.get_member(proposer_id) if channel.guild else None
        if proposer is None:
            await channel.send("⚠️ Не удалось определить предлагающего ставку.", delete_after=8)
            match.phase = "cancelled"
            await self._safe_delete(lobby_msg)
            self.matches.pop(channel.id, None)
            return

        # ✅ чтобы не захламляло: лобби-объявление удалим чуть позже
        try:
            await lobby_msg.edit(view=None)
        except Exception:
            pass
        asyncio.create_task(self._delete_later(lobby_msg, 10))

        await channel.send(
            f"💰 Ставка определяется. Предлагает: {proposer.mention}. ⏳ У него 30 сек.",
            delete_after=12,
        )

        view = ProposerPickButton(self, match, proposer_id=proposer_id)
        await channel.send("Нажми кнопку ниже, чтобы выбрать ставку:", view=view, delete_after=35)

        async def stake_pick_timeout():
            await asyncio.sleep(35)
            m = self.matches.get(channel.id)
            if not m or m.phase != "stake_pick":
                return
            m.phase = "cancelled"
            await channel.send("⛓️ Матч рассыпался. Ставка не была предложена.", delete_after=8)
            self.matches.pop(channel.id, None)

        match.task_timeout = asyncio.create_task(stake_pick_timeout())

    async def proposer_set_stake(self, interaction: discord.Interaction, match: DuelMatch, stake: int) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            return

        if match.task_timeout:
            match.task_timeout.cancel()
            match.task_timeout = None

        match.stake = int(stake)
        match.phase = "stake_confirm"

        other_id = match.cm_id if interaction.user.id == match.pudge_id else match.pudge_id
        other = channel.guild.get_member(other_id) if channel.guild and other_id else None
        if other is None:
            match.phase = "cancelled"
            await channel.send("⚠️ Второй игрок пропал. Матч отменён.", delete_after=8)
            self.matches.pop(channel.id, None)
            return

        await channel.send(
            f"💰 Ставка предложена: **{match.stake} рун**. Подтверждает: {other.mention} (30 сек).",
            delete_after=12,
        )

        view = ConfirmStakeView(self, match, confirmer_id=other.id)
        await channel.send("Подтверди или откажись:", view=view, delete_after=35)

        async def confirm_timeout():
            await asyncio.sleep(35)
            m = self.matches.get(channel.id)
            if not m or m.phase != "stake_confirm":
                return
            m.phase = "cancelled"
            await channel.send("⛓️ Матч рассыпался. Ставка не подтверждена.", delete_after=8)
            self.matches.pop(channel.id, None)

        match.task_timeout = asyncio.create_task(confirm_timeout())

    async def confirm_stake(self, channel: discord.TextChannel, match: DuelMatch, accepted: bool) -> None:
        if match.task_timeout:
            match.task_timeout.cancel()
            match.task_timeout = None

        if not accepted:
            match.phase = "cancelled"
            await channel.send("⛓️ Ставка отклонена. Матч отменён.", delete_after=8)
            self.matches.pop(channel.id, None)
            return

        repo = getattr(self.bot, "repo", None)
        if repo is None:
            match.phase = "cancelled"
            await channel.send("⚠️ База не подключена, матч отменён.", delete_after=8)
            self.matches.pop(channel.id, None)
            return

        # ✅ если ставка 0 — просто играем без списаний
        if match.stake > 0:
            ok1 = await repo.spend_runes(match.pudge_id, match.stake)  # type: ignore
            ok2 = await repo.spend_runes(match.cm_id, match.stake)  # type: ignore
            if not (ok1 and ok2):
                if ok1 and not ok2:
                    await repo.add_runes(match.pudge_id, match.stake)  # type: ignore
                if ok2 and not ok1:
                    await repo.add_runes(match.cm_id, match.stake)  # type: ignore
                match.phase = "cancelled"
                await channel.send("⛓️ Руны не сошлись. Матч отменён.", delete_after=8)
                self.matches.pop(channel.id, None)
                return

        match.phase = "round"
        match.round_index = 0

        await channel.send(
            "🎣 Дуэль началась.\n"
            "⏳ На ход у каждого **30 сек**. Если не выберешь — автоматическое поражение.\n"
            "Если оба промолчат — матч отменится.",
            delete_after=12,
        )
        await self.post_round_prompt(channel, match)

    async def post_round_prompt(self, channel: discord.TextChannel, match: DuelMatch) -> None:
        match.cm_row = None
        match.hook_row = None

        r = match.round_index + 1
        embed = discord.Embed(
            title=f"Раунд {r}/3",
            description="Выберите линию: **Верх / Центр / Низ**.",
        )
        embed.add_field(name="Пудж", value="ждёт…", inline=True)
        embed.add_field(name="ЦМ", value="ждёт…", inline=True)

        view = TurnPickView(self, match)
        status = await channel.send(embed=embed, view=view)
        match.status_message_id = status.id

        async def turn_timeout():
            await asyncio.sleep(30)
            m = self.matches.get(channel.id)
            if not m or m.phase != "round" or m.status_message_id != status.id:
                return

            if m.cm_row is None and m.hook_row is None:
                m.phase = "cancelled"
                await self._safe_delete(status)
                await channel.send("⛓️ Никто не сделал ход. Матч отменён.", delete_after=8)
                self.matches.pop(channel.id, None)
                return

            if m.hook_row is None:
                await self.finish_match(channel, m, winner="cm", reason="Пудж промолчал.")
                await self._safe_delete(status)
                return

            if m.cm_row is None:
                await self.finish_match(channel, m, winner="pudge", reason="ЦМ промолчала.")
                await self._safe_delete(status)
                return

        match.task_turn_timeout = asyncio.create_task(turn_timeout())

    async def on_pick(self, channel: discord.TextChannel, match: DuelMatch, side: str, row: int) -> None:
        if side == "pudge":
            match.hook_row = row
        else:
            match.cm_row = row

        # если оба выбрали — резолв
        if match.hook_row is None or match.cm_row is None:
            return

        if match.task_turn_timeout:
            match.task_turn_timeout.cancel()
            match.task_turn_timeout = None

        # удаляем сообщение выбора хода (кнопки)
        status_msg = None
        if match.status_message_id:
            try:
                status_msg = await channel.fetch_message(match.status_message_id)
            except Exception:
                status_msg = None
        await self._safe_delete(status_msg)

        col = match.round_index
        hook_pos = (col, match.hook_row)
        cm_pos = (col, match.cm_row)

        img_path = render_arena(cm_pos=cm_pos, hook_pos=hook_pos)

        # ✅ картинки арены тоже не копим: auto-delete
        if match.hook_row == match.cm_row:
            await channel.send(
                f"🎣 **ПОПАДАНИЕ!** Пудж угадал линию на шаге {match.round_index + 1}.",
                file=discord.File(img_path, filename="arena.png"),
                delete_after=20,
            )
            await self.finish_match(channel, match, winner="pudge", reason="Совпала линия.")
            return

        await channel.send(
            f"❄️ ЦМ ускользнула на шаге {match.round_index + 1}.",
            file=discord.File(img_path, filename="arena.png"),
            delete_after=20,
        )

        match.round_index += 1
        if match.round_index >= 3:
            await self.finish_match(channel, match, winner="cm", reason="ЦМ пережила три шага.")
            return

        await self.post_round_prompt(channel, match)

    async def finish_match(self, channel: discord.TextChannel, match: DuelMatch, winner: str, reason: str) -> None:
        if match.phase in ("finished", "cancelled"):
            return
        match.phase = "finished"

        repo = getattr(self.bot, "repo", None)
        if repo is None:
            await channel.send("⚠️ База не подключена, не могу выдать руны.", delete_after=8)
            self.matches.pop(channel.id, None)
            return

        pudge_id = match.pudge_id
        cm_id = match.cm_id
        if not pudge_id or not cm_id:
            await channel.send("⚠️ Участники исчезли.", delete_after=8)
            self.matches.pop(channel.id, None)
            return

        if winner == "pudge":
            winner_id = pudge_id
            loser_id = cm_id
            coef = COEF_PUDGE
            win_side_name = "Пудж"
        else:
            winner_id = cm_id
            loser_id = pudge_id
            coef = COEF_CM
            win_side_name = "ЦМ"

        # ✅ выплата победителю (если ставка 0 — выплата 0)
        payout = ceil_int(match.stake * coef) if match.stake > 0 else 0
        if payout > 0:
            await repo.add_runes(winner_id, payout)

        total_bets = 0
        winners_bets = 0
        for b in list(match.bets.values()):
            total_bets += 1
            if (winner == "pudge" and b.side == "pudge") or (winner == "cm" and b.side == "cm"):
                winners_bets += 1
                await repo.add_runes(b.user_id, ceil_int(b.stake * coef))

        winner_m = channel.guild.get_member(winner_id)
        loser_m = channel.guild.get_member(loser_id)

        # ✅ красивая финальная карточка с баннером (и сама исчезнет)
        fin_embed = discord.Embed(
            title="🏁 Дуэль завершена",
            description=f"Победа: **{win_side_name}**\nПричина: *{reason}*\nВыплата победителю: **+{payout} рун**.",
        )
        file = None
        if BANNER_PATH.exists():
            file = discord.File(BANNER_PATH.as_posix(), filename="duel_banner.png")
            fin_embed.set_image(url="attachment://duel_banner.png")

        try:
            await channel.send(embed=fin_embed, file=file, delete_after=25)
        except Exception:
            await channel.send(
                f"🏁 **Матч завершён** — победа: **{win_side_name}**.\nПричина: *{reason}*\nВыплата: +{payout} рун.",
                delete_after=18,
            )

        # лог в результаты (без картинки)
        text = (
            f"🏹 **Дуэль — результат**\n"
            f"Победитель: {winner_m.mention if winner_m else winner_id}\n"
            f"Проигравший: {loser_m.mention if loser_m else loser_id}\n"
            f"Ставка: {match.stake}\n"
            f"Выплата: +{payout}\n"
            f"Ставочники: {winners_bets}/{total_bets} выиграли"
        )
        await self._send_results(channel.guild, text)

        # чистим лобби если осталось
        if match.lobby_message_id:
            try:
                m = await channel.fetch_message(match.lobby_message_id)
                await self._safe_delete(m)
            except Exception:
                pass

        self.matches.pop(channel.id, None)

    async def _delete_later(self, msg: discord.Message, seconds: int) -> None:
        await asyncio.sleep(seconds)
        await self._safe_delete(msg)

    # ---------- betting ----------
    async def open_bet_panel(self, interaction: discord.Interaction, match: DuelMatch) -> None:
        user = interaction.user
        if not isinstance(user, discord.Member):
            return
        lvl = await self._get_level(user.id)
        if not can_play_duel(lvl):
            await interaction.response.send_message("⛓️ Тебе ещё рано для ставок дуэли (нужно 15+).", ephemeral=True)
            return

        opts = stake_options_for_level(lvl)
        if not opts:
            await interaction.response.send_message("⛓️ Тебе ещё рано.", ephemeral=True)
            return

        view = BettorStakeView(self, match, user_id=user.id, options=opts)
        await interaction.response.send_message(
            "💰 Ставочник: выбери сторону и ставку.\n"
            "Если матч отменится — ставка вернётся.\n"
            "Если проиграешь — ставка сгорит.",
            view=view,
            ephemeral=True,
        )


# ---------------- Views ----------------

class StartDuelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Играть", style=discord.ButtonStyle.primary, custom_id="duel:start")
    async def play(self, interaction: discord.Interaction, _: discord.ui.Button):
        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("⚠️ Канал не найден.", ephemeral=True)
            return

        cog = interaction.client.get_cog("DuelCog")  # type: ignore
        if not cog:
            await interaction.response.send_message("⚠️ Ког дуэли не загружен.", ephemeral=True)
            return

        await interaction.response.send_message("✅ Матч открыт.", ephemeral=True)
        await cog.start_lobby(ch, interaction.user)  # type: ignore


class LobbyView(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch):
        super().__init__(timeout=70)
        self.cog = cog
        self.match = match

    async def _refresh_embed(self, msg: discord.Message):
        e = msg.embeds[0] if msg.embeds else discord.Embed(title="⚔️ Матч открыт")
        pudge = f"<@{self.match.pudge_id}>" if self.match.pudge_id else "—"
        cm = f"<@{self.match.cm_id}>" if self.match.cm_id else "—"
        if e.fields and len(e.fields) >= 2:
            e.set_field_at(0, name="Пудж", value=pudge, inline=True)
            e.set_field_at(1, name="ЦМ", value=cm, inline=True)
        else:
            e.clear_fields()
            e.add_field(name="Пудж", value=pudge, inline=True)
            e.add_field(name="ЦМ", value=cm, inline=True)
        await msg.edit(embed=e, view=self)

    def _side_taken(self, side: str) -> bool:
        return (self.match.pudge_id is not None) if side == "pudge" else (self.match.cm_id is not None)

    def _both_taken(self) -> bool:
        return self.match.pudge_id is not None and self.match.cm_id is not None

    @discord.ui.button(label="🎣 Пудж", style=discord.ButtonStyle.secondary)
    async def take_pudge(self, interaction: discord.Interaction, _: discord.ui.Button):
        lvl = await self.cog._get_level(interaction.user.id)
        if not can_play_duel(lvl) and not self.cog._is_admin(interaction.user.id):
            await interaction.response.send_message("⛓️ Тебе ещё рано для дуэли (15+).", ephemeral=True)
            return
        if self._side_taken("pudge"):
            await interaction.response.send_message("⚠️ Пудж уже выбран.", ephemeral=True)
            return
        if self.match.cm_id == interaction.user.id:
            await interaction.response.send_message("⚠️ Ты уже ЦМ.", ephemeral=True)
            return

        self.match.pudge_id = interaction.user.id
        await interaction.response.send_message("✅ Ты стал Пуджом.", ephemeral=True, delete_after=10)
        msg = interaction.message
        if isinstance(msg, discord.Message):
            await self._refresh_embed(msg)
            if self._both_taken():
                ch = interaction.channel
                if isinstance(ch, discord.TextChannel):
                    await self.cog.on_sides_chosen(ch, self.match, msg)

    @discord.ui.button(label="❄️ ЦМ", style=discord.ButtonStyle.secondary)
    async def take_cm(self, interaction: discord.Interaction, _: discord.ui.Button):
        lvl = await self.cog._get_level(interaction.user.id)
        if not can_play_duel(lvl) and not self.cog._is_admin(interaction.user.id):
            await interaction.response.send_message("⛓️ Тебе ещё рано для дуэли (15+).", ephemeral=True)
            return
        if self._side_taken("cm"):
            await interaction.response.send_message("⚠️ ЦМ уже выбрана.", ephemeral=True)
            return
        if self.match.pudge_id == interaction.user.id:
            await interaction.response.send_message("⚠️ Ты уже Пудж.", ephemeral=True)
            return

        self.match.cm_id = interaction.user.id
        await interaction.response.send_message("✅ Ты стал ЦМ.", ephemeral=True, delete_after=10)
        msg = interaction.message
        if isinstance(msg, discord.Message):
            await self._refresh_embed(msg)
            if self._both_taken():
                ch = interaction.channel
                if isinstance(ch, discord.TextChannel):
                    await self.cog.on_sides_chosen(ch, self.match, msg)

    @discord.ui.button(label="💰 Ставки", style=discord.ButtonStyle.primary)
    async def bets(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.match.pudge_id is None or self.match.cm_id is None:
            await interaction.response.send_message("⏳ Сначала выберите обе стороны.", ephemeral=True)
            return
        await self.cog.open_bet_panel(interaction, self.match)

    @discord.ui.button(label="Отменить", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id not in (self.match.pudge_id, self.match.cm_id, self.match.starter_id) and not self.cog._is_admin(interaction.user.id):
            await interaction.response.send_message("⚠️ Отменить может только инициатор или игроки.", ephemeral=True)
            return
        self.match.phase = "cancelled"
        await interaction.response.send_message("⛓️ Матч отменён.", ephemeral=True)
        try:
            await interaction.message.delete()
        except Exception:
            pass
        self.cog.matches.pop(self.match.channel_id, None)


class ProposerPickButton(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch, proposer_id: int):
        super().__init__(timeout=35)
        self.cog = cog
        self.match = match
        self.proposer_id = proposer_id

    @discord.ui.button(label="Выбрать ставку", style=discord.ButtonStyle.primary)
    async def pick(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.proposer_id:
            await interaction.response.send_message("⚠️ Ставку выбирает другой игрок.", ephemeral=True)
            return

        lvl = await self.cog._get_level(interaction.user.id)
        opts = stake_options_for_level(lvl)
        if not opts:
            await interaction.response.send_message("⛓️ Тебе ещё рано для ставок дуэли.", ephemeral=True)
            return

        view = ProposerStakeView(self.cog, self.match, opts)
        await interaction.response.send_message("Выбери ставку (30 сек):", view=view, ephemeral=True)


class ProposerStakeView(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch, options: List[int]):
        super().__init__(timeout=30)
        self.cog = cog
        self.match = match
        for v in options:
            self.add_item(ProposerStakeButton(cog, match, v))
        self.add_item(ProposerStakeButton(cog, match, 0, label="Не ставлю"))


class ProposerStakeButton(discord.ui.Button):
    def __init__(self, cog: DuelCog, match: DuelMatch, stake: int, label: Optional[str] = None):
        self.cog = cog
        self.match = match
        txt = label if label else f"{stake}"
        super().__init__(label=txt, style=discord.ButtonStyle.secondary)
        self._stake = stake

    async def callback(self, interaction: discord.Interaction):
        if self.match.phase != "stake_pick":
            await interaction.response.send_message("⚠️ Уже не время выбирать ставку.", ephemeral=True)
            return

        await interaction.response.send_message(f"✅ Ставка предложена: {self._stake}.", ephemeral=True, delete_after=10)
        await self.cog.proposer_set_stake(interaction, self.match, self._stake)


class ConfirmStakeView(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch, confirmer_id: int):
        super().__init__(timeout=35)
        self.cog = cog
        self.match = match
        self.confirmer_id = confirmer_id

    @discord.ui.button(label="✅ Принять", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.confirmer_id:
            await interaction.response.send_message("⚠️ Подтверждает другой игрок.", ephemeral=True)
            return
        await interaction.response.send_message("✅ Принято.", ephemeral=True, delete_after=10)
        ch = interaction.channel
        if isinstance(ch, discord.TextChannel):
            await self.cog.confirm_stake(ch, self.match, True)

    @discord.ui.button(label="❌ Отказаться", style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.confirmer_id:
            await interaction.response.send_message("⚠️ Подтверждает другой игрок.", ephemeral=True)
            return
        await interaction.response.send_message("⛓️ Отклонено.", ephemeral=True, delete_after=10)
        ch = interaction.channel
        if isinstance(ch, discord.TextChannel):
            await self.cog.confirm_stake(ch, self.match, False)


class BettorStakeView(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch, user_id: int, options: List[int]):
        super().__init__(timeout=30)
        self.cog = cog
        self.match = match
        self.user_id = user_id
        self.side: Optional[str] = None

        self.add_item(BettorSideButton("Ставлю на Пуджа", "pudge"))
        self.add_item(BettorSideButton("Ставлю на ЦМ", "cm"))
        for v in options[:4]:
            self.add_item(BettorStakePickButton(cog, match, user_id, v))
        self.add_item(BettorCancelButton())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class BettorSideButton(discord.ui.Button):
    def __init__(self, label: str, side: str):
        super().__init__(label=label, style=discord.ButtonStyle.secondary)
        self.side = side

    async def callback(self, interaction: discord.Interaction):
        view: BettorStakeView = self.view  # type: ignore
        view.side = self.side
        await interaction.response.send_message(f"✅ Сторона выбрана. Теперь ставка.", ephemeral=True, delete_after=10)


class BettorStakePickButton(discord.ui.Button):
    def __init__(self, cog: DuelCog, match: DuelMatch, user_id: int, stake: int):
        super().__init__(label=str(stake), style=discord.ButtonStyle.primary)
        self.cog = cog
        self.match = match
        self.user_id = user_id
        self.stake = stake

    async def callback(self, interaction: discord.Interaction):
        view: BettorStakeView = self.view  # type: ignore
        if view.side not in ("pudge", "cm"):
            await interaction.response.send_message("⚠️ Сначала выбери сторону.", ephemeral=True)
            return

        repo = getattr(self.cog.bot, "repo", None)
        if repo is None:
            await interaction.response.send_message("⚠️ База не подключена.", ephemeral=True)
            return

        ok = await repo.spend_runes(self.user_id, self.stake)
        if not ok:
            await interaction.response.send_message("⛓️ У тебя не хватает рун для этой ставки.", ephemeral=True)
            return

        self.match.bets[self.user_id] = Bet(user_id=self.user_id, side=view.side, stake=self.stake)
        await interaction.response.send_message(f"✅ Ставка принята.", ephemeral=True, delete_after=10)


class BettorCancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Отменить участие", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message("Ок. Ты не участвуешь.", ephemeral=True, delete_after=10)


class TurnPickView(discord.ui.View):
    def __init__(self, cog: DuelCog, match: DuelMatch):
        super().__init__(timeout=35)
        self.cog = cog
        self.match = match
        self.add_item(TurnPickButton("Верх", 0))
        self.add_item(TurnPickButton("Центр", 1))
        self.add_item(TurnPickButton("Низ", 2))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id in (self.match.pudge_id, self.match.cm_id)


class TurnPickButton(discord.ui.Button):
    def __init__(self, label: str, row: int):
        super().__init__(label=label, style=discord.ButtonStyle.primary)
        self.row = row

    async def callback(self, interaction: discord.Interaction):
        view: TurnPickView = self.view  # type: ignore
        match = view.match
        cog = view.cog
        ch = interaction.channel

        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("⚠️ Канал не найден.", ephemeral=True)
            return

        if interaction.user.id == match.pudge_id:
            side = "pudge"
            if match.hook_row is not None:
                await interaction.response.send_message("⚠️ Ты уже выбрал ход.", ephemeral=True)
                return
        else:
            side = "cm"
            if match.cm_row is not None:
                await interaction.response.send_message("⚠️ Ты уже выбрала ход.", ephemeral=True)
                return

        await interaction.response.send_message("✅ Ход принят.", ephemeral=True, delete_after=10)
        await cog.on_pick(ch, match, side=side, row=self.row)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(DuelCog(bot))