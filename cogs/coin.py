\
# cogs/coin.py
from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

import discord
from discord.ext import commands

# --- paths ---
ROOT = Path(__file__).resolve().parents[1]
COIN_DIR = ROOT / "assets" / "coin"
SPIN_GIF = COIN_DIR / "coin_spin.gif"
HEADS_GIF = COIN_DIR / "coin_heads.gif"
TAILS_GIF = COIN_DIR / "coin_tails.gif"

# --- difficulties (keep your balance) ---
@dataclass(frozen=True)
class Diff:
    key: str
    title: str
    win_chance: float
    unlock_level: int
    cd_seconds: int
    mult: float
    bets: Tuple[int, ...]


DIFFS = [
    Diff("easy", "Лёгкая", 0.60, 3, 20 * 60, 0.50, (5, 10, 20, 30)),
    Diff("mid", "Средняя", 0.50, 7, 25 * 60, 0.80, (30, 40, 50, 60)),
    Diff("hard", "Сложная", 0.40, 15, 30 * 60, 1.10, (60, 75, 90, 100)),
    Diff("cursed", "Проклятая", 0.35, 10, 24 * 60 * 60, 0.0, ()),
]

DAILY_PROFIT_CAP = {"easy": 30, "mid": 140, "hard": 250}
FEE_RATE = 0.05  # 5% (ceil), min 1 if profit > 0

SIDE_NAME = {"heads": "Орёл", "tails": "Решка"}


def msk_day_key() -> str:
    # MSK = UTC+3, no DST
    dt = datetime.now(timezone.utc) + timedelta(hours=3)
    return dt.strftime("%Y-%m-%d")


def fee_from_profit(profit: int) -> int:
    if profit <= 0:
        return 0
    return max(1, int(math.ceil(profit * FEE_RATE)))


def round_up_int(x: float) -> int:
    i = int(x)
    return i if x == x.__class__(i) else i + 1


def lore_no_runes() -> str:
    return random.choice(
        [
            "Пустота заглянула в твой кошель… и не услышала звона.",
            "Рун не хватает. Монета улыбается: «Пока не сегодня».",
            "Твои руны слишком тихие. Монета не слушает шёпот бедняка.",
            "Ты тянешься к ставке — а пальцы скользят по пустоте.",
            "Сейчас ты беднее, чем твоя надежда.",
        ]
    )


def lore_too_early() -> str:
    return random.choice(
        [
            "Тебе ещё рано. Монета не смотрит на тех, кто не дорос до риска.",
            "Пока нет. Пустота держит эту дверь закрытой.",
            "Ты чувствуешь холод — это уровень не пускает дальше.",
            "Монета молчит. Твой уровень ещё не звучит достаточно громко.",
        ]
    )


def lore_cursed_cd() -> str:
    return random.choice(
        [
            "Ты уже испытал проклятую удачу. Пустота помнит. Вернись завтра.",
            "Проклятая монета не любит повторений. 24 часа тишины.",
            "Её шёпот ещё не остыл. Подожди сутки.",
        ]
    )


def pre_spin_phrase() -> str:
    return random.choice(
        [
            "Держи ладонь ровно. Падение монеты всегда неровное.",
            "Сейчас ты почувствуешь надежду. Она будет недолгой.",
            "Монета уже решила. Но ты всё равно сделаешь вид, что выбираешь.",
            "Твой выбор важен ровно настолько, насколько Пустота позволяет.",
            "Сделай вдох. На выдохе обычно проигрывают.",
            "Не смотри слишком пристально — удача любит чужие глаза.",
            "Шанс тоньше волоса. Но именно им тебя и режут.",
            "Пусть руны не дрожат. Это дрожишь ты.",
            "Стук монеты громче твоих обещаний.",
            "Пустота улыбается. Не тебе.",
        ]
    )


class CoinCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # persistent "Играть"
        self.start_view = StartView(self)
        bot.add_view(self.start_view)

    @property
    def cfg(self) -> dict:
        return getattr(self.bot, "cfg", {})  # type: ignore

    @property
    def repo(self):
        return getattr(self.bot, "repo", None)  # type: ignore

    def is_admin(self, user_id: int) -> bool:
        return int(self.cfg.get("admin_user_id", 0)) == int(user_id)

    async def get_level(self, user_id: int) -> int:
        u = await self.repo.get_user(user_id)
        return int(u.get("level", 1))

    async def cd_key(self, diff_key: str) -> str:
        return f"coin:{diff_key}"

    async def daily_progress(self, user_id: int) -> Dict[str, Tuple[int, int]]:
        day = msk_day_key()
        out: Dict[str, Tuple[int, int]] = {}
        dp_get = getattr(self.repo, "dp_get", None)
        for k, cap in DAILY_PROFIT_CAP.items():
            have = 0
            if callable(dp_get):
                try:
                    have = int(await dp_get(user_id, day, f"coin:{k}"))
                except Exception:
                    have = 0
            out[k] = (have, cap)
        return out

    async def can_play(self, user_id: int, diff: Diff) -> Tuple[bool, str]:
        lvl = await self.get_level(user_id)
        if (not self.is_admin(user_id)) and lvl < diff.unlock_level:
            return False, lore_too_early()

        until = await self.repo.cd_get(user_id, await self.cd_key(diff.key))
        now = int(time.time())
        if until > now:
            if diff.key == "cursed":
                return False, lore_cursed_cd()
            left = max(1, (until - now) // 60)
            return False, f"Пустота просит паузу. Вернись через ~{left} мин."

        cap = DAILY_PROFIT_CAP.get(diff.key)
        if cap is not None and not self.is_admin(user_id):
            day = msk_day_key()
            dp_get = getattr(self.repo, "dp_get", None)
            have = 0
            if callable(dp_get):
                try:
                    have = int(await dp_get(user_id, day, f"coin:{diff.key}"))
                except Exception:
                    have = 0
            if have >= cap:
                msg = (
                    f"Печать Пустоты на сегодня закрыта: **{have}/{cap}** чистой удачи.\n"
                    "Вернись завтра (по МСК) или выбери другую сложность."
                )
                return False, msg

        return True, ""

    async def set_cd(self, user_id: int, diff: Diff) -> None:
        await self.repo.cd_set(user_id, await self.cd_key(diff.key), diff.cd_seconds)

    async def post_public_result(
        self,
        interaction: discord.Interaction,
        diff: Diff,
        bet: int,
        side: str,
        landed: str,
        win: bool,
        reward: int,
    ) -> None:
        guild = interaction.guild
        if not guild:
            return
        ch_id = int(self.cfg.get("channels", {}).get("coin_results", 0))
        ch = guild.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            return

        embed = discord.Embed(
            title=f"🪙 Монетка — {'ПОБЕДА' if win else 'ПОРАЖЕНИЕ'}",
            description=f"Игрок: {interaction.user.mention}",
        )
        embed.add_field(name="Сложность", value=f"{diff.title} ({int(diff.win_chance*100)}%)", inline=True)
        embed.add_field(name="Сторона", value=SIDE_NAME[side], inline=True)
        embed.add_field(name="Выпало", value=SIDE_NAME[landed], inline=True)
        embed.add_field(name="Ставка", value=str(bet if diff.key != "cursed" else 0), inline=True)

        if diff.key == "cursed":
            embed.add_field(name="Итог", value=f"+{reward}", inline=True)
        else:
            if win:
                profit = int(reward)
                fee = fee_from_profit(profit)
                net = max(0, profit - fee)
                embed.add_field(name="Итог", value=f"+{net}", inline=True)
            else:
                embed.add_field(name="Итог", value="+0", inline=True)

        await ch.send(embed=embed)

    async def begin(self, interaction: discord.Interaction) -> None:
        # Avoid Unknown interaction
        try:
            await interaction.response.defer(ephemeral=True, thinking=False)
        except Exception:
            pass

        prog = await self.daily_progress(interaction.user.id)
        view = DifficultyView(self, interaction.user.id, prog)
        try:
            await interaction.followup.send(
                content="**Выбор судьбы открыт.**",
                embed=view.make_embed(),
                view=view,
                ephemeral=True,
            )
        except discord.NotFound:
            return

    async def go_spin_and_resolve(self, interaction: discord.Interaction, diff: Diff, bet: int, side: str) -> None:
        user_id = interaction.user.id

        # charge bet (except cursed)
        if diff.key != "cursed":
            ok = await self.repo.spend_runes(user_id, bet)
            if not ok:
                await interaction.response.send_message(lore_no_runes(), ephemeral=True)
                return

        await self.set_cd(user_id, diff)

        win = random.random() < diff.win_chance
        landed = side if win else ("tails" if side == "heads" else "heads")

        # reward calculation (profit-only; bet is returned on win)
        if diff.key == "cursed":
            # reward = daily income from cfg if win, else 0
            lvl = await self.get_level(user_id)
            reward = _daily_income_from_cfg(self.cfg, lvl) if win else 0
            if not win:
                # apply curse if tendril cog supports it
                tendril = self.bot.get_cog("TendrilCog")
                if tendril and interaction.guild and isinstance(interaction.user, discord.Member):
                    try:
                        await tendril.apply_curse(interaction.user)
                    except Exception:
                        pass
        else:
            reward = round_up_int(bet * diff.mult) if win else 0

        # Show spin
        try:
            await interaction.response.defer(ephemeral=True, thinking=False)
        except Exception:
            pass

        spin_embed = discord.Embed(title="Монета вращается…", description=pre_spin_phrase())
        spin_files = []
        if SPIN_GIF.exists():
            spin_files = [discord.File(SPIN_GIF, filename="coin_spin.gif")]
            spin_embed.set_image(url="attachment://coin_spin.gif")

        try:
            await interaction.edit_original_response(content="", embed=spin_embed, view=None, attachments=[])
            if spin_files:
                await interaction.edit_original_response(embed=spin_embed, attachments=spin_files)
        except Exception:
            try:
                await interaction.followup.send(embed=spin_embed, files=spin_files, ephemeral=True)
            except Exception:
                return

        await asyncio.sleep(2)

        # Final embed
        result_embed = discord.Embed(title=f"🪙 Монетка — {'ПОБЕДА' if win else 'ПОРАЖЕНИЕ'}")
        result_embed.add_field(name="Сложность", value=f"{diff.title} ({int(diff.win_chance*100)}%)", inline=True)
        result_embed.add_field(name="Ставка", value=str(bet if diff.key != "cursed" else 0), inline=True)
        result_embed.add_field(name="Твоя сторона", value=SIDE_NAME[side], inline=True)
        result_embed.add_field(name="Выпало", value=SIDE_NAME[landed], inline=True)

        final_files = []
        fin_path = HEADS_GIF if landed == "heads" else TAILS_GIF
        if fin_path.exists():
            fname = fin_path.name
            final_files = [discord.File(fin_path, filename=fname)]
            result_embed.set_image(url=f"attachment://{fname}")

        if diff.key == "cursed":
            result_embed.add_field(name="Награда", value=f"+{reward} рун", inline=True)
        else:
            profit = int(reward) if win else 0
            fee = fee_from_profit(profit) if win else 0
            net = max(0, profit - fee)
            result_embed.add_field(name="Прибыль", value=f"+{profit}", inline=True)
            result_embed.add_field(name="Дань Бездне", value=f"-{fee}", inline=True)
            result_embed.add_field(name="Чистая прибыль", value=f"+{net}", inline=True)

            cap = DAILY_PROFIT_CAP.get(diff.key)
            if cap is not None:
                have = 0
                dp_get = getattr(self.repo, "dp_get", None)
                if callable(dp_get):
                    try:
                        have = int(await dp_get(user_id, msk_day_key(), f"coin:{diff.key}"))
                    except Exception:
                        have = 0
                result_embed.add_field(name="Лимит прибыли (сегодня)", value=f"{have + (net if win else 0)}/{cap}", inline=True)

        # Apply rewards
        if win and reward > 0:
            if diff.key == "cursed":
                await self.repo.add_runes(user_id, int(reward))
            else:
                profit = int(reward)
                fee = fee_from_profit(profit)
                net = max(0, profit - fee)
                # bet is returned + net profit
                await self.repo.add_runes(user_id, int(bet) + int(net))
                dp_add = getattr(self.repo, "dp_add", None)
                if callable(dp_add) and net > 0:
                    try:
                        await dp_add(user_id, msk_day_key(), f"coin:{diff.key}", int(net))
                    except Exception:
                        pass

        # Send final
        try:
            await interaction.edit_original_response(content="", embed=result_embed, view=None, attachments=[])
            if final_files:
                await interaction.edit_original_response(embed=result_embed, attachments=final_files)
        except Exception:
            try:
                await interaction.followup.send(embed=result_embed, files=final_files, ephemeral=True)
            except Exception:
                return

        await self.post_public_result(interaction, diff, bet, side, landed, win, reward)

        # auto-delete ephemeral after 30s
        await asyncio.sleep(30)
        try:
            await interaction.delete_original_response()
        except Exception:
            pass

    @commands.command(name="post_coin_panels")
    async def post_coin_panels(self, ctx: commands.Context) -> None:
        if not self.is_admin(ctx.author.id):
            return

        for key in ("coin_1", "coin_2", "coin_3"):
            ch_id = int(self.cfg.get("channels", {}).get(key, 0))
            ch = ctx.guild.get_channel(ch_id) if ctx.guild else None
            if not isinstance(ch, discord.TextChannel):
                await ctx.send(f"❌ Канал {key} не найден.")
                continue

            embed = discord.Embed(
                title="🪙 Алтарь Монетки",
                description=(
                    "Нажми **Играть**. Дальше кнопки будут работать только для того, кто рискнул.\n"
                    "Монета крутится — как и твоя удача."
                ),
            )

            if SPIN_GIF.exists():
                file = discord.File(SPIN_GIF, filename="coin_spin.gif")
                embed.set_image(url="attachment://coin_spin.gif")
                msg = await ch.send(embed=embed, view=self.start_view, file=file)
            else:
                msg = await ch.send(embed=embed, view=self.start_view)

            try:
                await msg.pin()
            except Exception:
                pass

        await ctx.send("✅ Панели монетки опубликованы и закреплены.")


# --- helper used above (kept compatible with your cfg) ---
def _daily_income_from_cfg(cfg: dict, level: int) -> int:
    econ = cfg.get("economy", {})
    table = econ.get("rune_income_per_day", [])
    for row in table:
        if int(row.get("from", 0)) <= level <= int(row.get("to", 10**9)):
            return int(row.get("per_day", 30))
    return 30


# ---------------- Views ----------------
class StartView(discord.ui.View):
    def __init__(self, cog: CoinCog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Играть", style=discord.ButtonStyle.primary, custom_id="coin:start")
    async def play(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.begin(interaction)


class DifficultyView(discord.ui.View):
    def __init__(self, cog: CoinCog, user_id: int, progress: Dict[str, Tuple[int, int]]):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.progress = progress

        for d in DIFFS:
            label = f"{d.title} ({int(d.win_chance*100)}%) • ур.{d.unlock_level}+"
            self.add_item(DiffButton(d, label))

        self.add_item(CancelButton())

    def make_embed(self) -> discord.Embed:
        e = discord.Embed(
            title="Выбор сложности",
            description="Выбери сложность. (Все видны всегда, но Пустота пускает не всех.)",
        )

        e.add_field(
            name="Проклятая",
            value="Бесплатная. Выигрыш = дневной доход по уровню.\nКД: 24 часа. При поражении — статус.",
            inline=False,
        )

        easy_have, easy_cap = self.progress.get("easy", (0, DAILY_PROFIT_CAP["easy"]))
        mid_have, mid_cap = self.progress.get("mid", (0, DAILY_PROFIT_CAP["mid"]))
        hard_have, hard_cap = self.progress.get("hard", (0, DAILY_PROFIT_CAP["hard"]))

        e.add_field(
            name="Печать Пустоты — твой прогресс (по МСК)",
            value=(
                "Лёгкая: **{}/{}**  |  Средняя: **{}/{}**  |  Сложная: **{}/{}**\n"
                "Дань Бездне: **5%** с прибыли (округление вверх)."
            ).format(easy_have, easy_cap, mid_have, mid_cap, hard_have, hard_cap),
            inline=False,
        )
        return e

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Это не твой бросок.", ephemeral=True)
            return False
        return True


class DiffButton(discord.ui.Button):
    def __init__(self, diff: Diff, label: str):
        super().__init__(label=label[:80], style=discord.ButtonStyle.secondary)
        self.diff = diff

    async def callback(self, interaction: discord.Interaction):
        view: DifficultyView = self.view  # type: ignore
        ok, why = await view.cog.can_play(interaction.user.id, self.diff)
        if not ok:
            await interaction.response.send_message(why, ephemeral=True)
            return

        if self.diff.key == "cursed":
            sv = SideView(view.cog, interaction.user.id, self.diff, bet=0)
            await interaction.response.edit_message(embed=sv.make_embed(), view=sv)
            return

        bv = BetView(view.cog, interaction.user.id, self.diff)
        await interaction.response.edit_message(embed=bv.make_embed(), view=bv)


class BetView(discord.ui.View):
    def __init__(self, cog: CoinCog, user_id: int, diff: Diff):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.diff = diff

        for b in diff.bets:
            win_amt = round_up_int(b * diff.mult)
            self.add_item(BetButton(b, win_amt))

        self.add_item(BackButton())
        self.add_item(CancelButton())

    def make_embed(self) -> discord.Embed:
        return discord.Embed(
            title=f"Ставка: {self.diff.title} ({int(self.diff.win_chance*100)}%)",
            description="Выбери ставку. Рядом — сколько получишь при победе (до дани).",
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Это не твой бросок.", ephemeral=True)
            return False
        return True


class BetButton(discord.ui.Button):
    def __init__(self, bet: int, win_amt: int):
        super().__init__(label=f"{bet} (+{win_amt})", style=discord.ButtonStyle.primary)
        self.bet = bet

    async def callback(self, interaction: discord.Interaction):
        view: BetView = self.view  # type: ignore
        u = await view.cog.repo.get_user(interaction.user.id)
        if int(u.get("runes", 0)) < self.bet and not view.cog.is_admin(interaction.user.id):
            await interaction.response.send_message(lore_no_runes(), ephemeral=True)
            return

        sv = SideView(view.cog, interaction.user.id, view.diff, bet=self.bet)
        await interaction.response.edit_message(embed=sv.make_embed(), view=sv)


class SideView(discord.ui.View):
    def __init__(self, cog: CoinCog, user_id: int, diff: Diff, bet: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.diff = diff
        self.bet = bet

        self.add_item(SideButton("heads"))
        self.add_item(SideButton("tails"))
        self.add_item(BackButton())
        self.add_item(CancelButton())

    def make_embed(self) -> discord.Embed:
        if self.diff.key == "cursed":
            return discord.Embed(
                title=f"Проклятая ({int(self.diff.win_chance*100)}%) — выбери сторону",
                description="Цена: 0. Победа даст дневной доход по уровню.\nПоражение — статус.",
            )
        return discord.Embed(
            title="Выбор стороны",
            description=f"Ставка: **{self.bet}**. Выбери сторону.",
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Это не твой бросок.", ephemeral=True)
            return False
        return True


class SideButton(discord.ui.Button):
    def __init__(self, side: str):
        super().__init__(
            label=SIDE_NAME[side],
            style=discord.ButtonStyle.success if side == "heads" else discord.ButtonStyle.danger,
        )
        self.side = side

    async def callback(self, interaction: discord.Interaction):
        view: SideView = self.view  # type: ignore
        ok, why = await view.cog.can_play(interaction.user.id, view.diff)
        if not ok:
            await interaction.response.send_message(why, ephemeral=True)
            return
        await view.cog.go_spin_and_resolve(interaction, view.diff, view.bet, self.side)


class BackButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Назад", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        cog: CoinCog = view.cog  # type: ignore
        try:
            await interaction.response.defer(ephemeral=True, thinking=False)
        except Exception:
            pass
        prog = await cog.daily_progress(interaction.user.id)
        dv = DifficultyView(cog, interaction.user.id, prog)
        await interaction.edit_original_response(embed=dv.make_embed(), view=dv)


class CancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Отменить", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Отменено.", embed=None, view=None)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CoinCog(bot))
