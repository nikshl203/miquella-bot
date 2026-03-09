from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from ._interactions import GuardedView, safe_defer_ephemeral, safe_send

log = logging.getLogger("void.orders")

ORDER_UNLOCK_LEVEL = 15
ELECTION_COOLDOWN_SECONDS = 48 * 60 * 60
ELECTION_COLLECT_SECONDS = 24 * 60 * 60
ELECTION_VOTE_SECONDS = 24 * 60 * 60
WAR_COOLDOWN_SECONDS = 24 * 60 * 60
WAR_STAGE_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class OrderSpec:
    id: str
    label: str
    role_id: int
    council_channel_id: int
    war_channel_id: int
    archive_channel_id: int
    join_button_label: str
    lore: str


ORDERS: dict[str, OrderSpec] = {
    "rher": OrderSpec(
        id="rher",
        label="Орден Rher",
        role_id=1473310429211267214,
        council_channel_id=1473328783833694300,
        war_channel_id=1473328809712422952,
        archive_channel_id=1473328865060458506,
        join_button_label="Вступить в Орден Rher",
        lore=(
            "Орден луны, тайных знаков и беспокойных откровений. Те, кто идут за Rher, чтут "
            "скрытое, наблюдают за чужими тенями и ищут истину там, где другим мерещится "
            "лишь безумие. Их путь — это шепот, ночное видение и знание, пришедшее из тревожного сна."
        ),
    ),
    "sylvian": OrderSpec(
        id="sylvian",
        label="Орден Sylvian",
        role_id=1473310627375091786,
        council_channel_id=1473328928033603595,
        war_channel_id=1473328966227202151,
        archive_channel_id=1473329001945628692,
        join_button_label="Вступить в Орден Sylvian",
        lore=(
            "Орден плоти, близости и живой связи между существами. Последователи Sylvian чтут "
            "единение, красоту, рождение нового и силу, что возникает там, где плоть и воля "
            "переплетаются в единый узор. Их путь — это жизнь, притяжение и преображение."
        ),
    ),
    "gro": OrderSpec(
        id="gro",
        label="Орден Gro-Goroth",
        role_id=1473310909949804605,
        council_channel_id=1473329142362673234,
        war_channel_id=1473329199283441850,
        archive_channel_id=1473329230321418270,
        join_button_label="Вступить в Орден Gro-Goroth",
        lore=(
            "Орден разрушения, жертвы и безжалостной силы. Верные Gro-Goroth принимают боль как цену "
            "перемен, а бой — как язык, которым мир напоминает о своей истинной сути. Их путь — "
            "это кровь, огонь и торжество мощи над слабостью."
        ),
    ),
}

ORDER_IDS = tuple(ORDERS.keys())

ORDER_PANEL_TEXT = (
    "✦ **Зал Орденов**\n"
    "Здесь не дают имени — здесь выбирают путь.\n"
    "Лишь достигшие **15 уровня** могут вступить под своды одного из трёх орденов.\n"
    "Выбор окончателен на текущем этапе. Внимательно вслушайся в зов и вступи туда, чьи истины тебе ближе всего."
)
ORDER_JOIN_DENY_LEVEL = (
    "Ты ещё не достиг порога, за которым ордена начинают слышать твой голос.\n"
    "Вернись, когда твой уровень достигнет 15 — и тогда Зал Орденов распахнёт перед тобой двери."
)
ORDER_JOIN_OK = (
    "Твой выбор услышан. Отныне ты идёшь под знаменем выбранного ордена.\n"
    "Пусть его воля станет твоим долгом, а его победы — твоей летописью."
)
ELECTION_COOLDOWN_DENY = (
    "Чертоги ещё помнят эхо прошлых выборов. Новый глашатай не может быть призван так скоро.\n"
    "Вернитесь, когда рассеется след последнего голосования."
)
ELECTION_PROMPT = (
    "Начались выборы глашатая войны.\n"
    "Готов ли ты выйти вперёд и говорить от имени ордена, когда придёт час зова?"
)
ELECTION_START_VOTE = (
    "✦ **Выборы глашатая войны начались**\n"
    "Ниже приведены имена тех, кто решился взять на себя право говорить от имени ордена.\n"
    "Отдайте голос тому, кому доверили бы первое письмо войны."
)
ELECTION_ONE_CANDIDATE = (
    "Лишь один воин осмелился принять зов. Спор не нужен — голос ордена отныне принадлежит ему.\n"
    "Новый глашатай войны назначен."
)
ELECTION_NO_CANDIDATES = (
    "Ни один участник ордена не решился принять на себя знамя глашатая.\n"
    "Выборы завершены без результата. Прежний голос ордена сохраняется."
)
ELECTION_FINISHED = (
    "Голоса подсчитаны. Чертоги услышали решение ордена.\n"
    "Новый глашатай войны избран и отныне будет говорить первым, когда придёт час бросить вызов."
)

WAR_ATTACKER_STARTED = (
    "✦ **Письмо войны составлено**\n"
    "Орден готовится воззвать к противнику. Но прежде свои же воины должны явить готовность.\n"
    "Соберите нужное число бойцов, и письмо будет отправлено."
)
WAR_DEFENDER_STARTED = (
    "✦ **Вам объявлена война**\n"
    "Чужой орден воззвал к вашему имени и требует ответа в назначенный час.\n"
    "Соберите воинов, если примете зов, или останьтесь в стороне, если не желаете выходить на поле."
)
WAR_READY_OK = "Твой ответ принят. Имя твоё вписано в список тех, кто готов выйти на зов войны."
WAR_WAIT_OK = "Твой ответ принят. Ты отступаешь от этого зова, и орден увидит твой выбор."
ARCHIVE_ATTACKER_FAIL = (
    "Вызов угас ещё до того, как достиг чужих стен. Орден не собрал достаточно воинов в назначенный срок.\n"
    "Война отменена. Орден теряет 1 очко влияния."
)
ARCHIVE_DEFENDER_FAIL = (
    "Письмо войны было услышано, но ответ не воплотился в строй воинов.\n"
    "Орден не собрал достаточно бойцов в назначенный срок. Война отменена. Несобравшая сторона теряет 1 очко влияния."
)
ARCHIVE_WAR_CONFIRMED = (
    "Два ордена собрали воинов и закрепили свой ответ в срок.\n"
    "Война состоится. Известие отправлено Микелле, и поле для состязания будет подготовлено."
)
ADMIN_WAR_CONFIRMED = (
    "Обе стороны собрали нужное число участников. Состязание можно организовывать.\n"
    "Ниже приложены параметры войны и списки желающих участвовать."
)


def _now() -> int:
    return int(time.time())


def _fmt_ts_rel(ts: int) -> str:
    return f"<t:{int(ts)}:R>"


def _fmt_ts_full(ts: int) -> str:
    return f"<t:{int(ts)}:f>"


def _clean_order_id(raw: str) -> str:
    s = str(raw or "").strip().lower()
    if s in ORDER_IDS:
        return s
    if "syl" in s:
        return "sylvian"
    if "gro" in s:
        return "gro"
    if "rher" in s:
        return "rher"
    return ""


def _order_name(order_id: str) -> str:
    spec = ORDERS.get(str(order_id).strip().lower())
    return spec.label if spec else str(order_id)


def _mention_user(uid: int) -> str:
    return f"<@{int(uid)}>"


def _uniq_keep_order(items: list[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for x in items:
        i = int(x)
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


class JoinOrderButton(discord.ui.Button):
    def __init__(self, order_id: str):
        spec = ORDERS[order_id]
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=spec.join_button_label,
            custom_id=f"orders:join:{order_id}",
            row=0,
        )
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_join_order(interaction, self.order_id)


class JoinOrderView(GuardedView):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        for order_id in ORDER_IDS:
            self.add_item(JoinOrderButton(order_id))


class HeraldPickButton(discord.ui.Button):
    def __init__(self, order_id: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Выбрать глашатая",
            custom_id=f"orders:herald:start:{order_id}",
            row=0,
        )
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_start_herald_election(interaction, self.order_id)


class DeclareWarButton(discord.ui.Button):
    def __init__(self, order_id: str):
        super().__init__(
            style=discord.ButtonStyle.danger,
            label="Объявить войну",
            custom_id=f"orders:war:declare:{order_id}",
            row=0,
        )
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_open_war_modal(interaction, self.order_id)


class OrderWarControlView(GuardedView):
    def __init__(self, order_id: str) -> None:
        super().__init__(timeout=None)
        self.order_id = order_id
        self.add_item(HeraldPickButton(order_id))
        self.add_item(DeclareWarButton(order_id))


class ElectionRespondButton(discord.ui.Button):
    def __init__(self, order_id: str):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Ответить на зов глашатая",
            custom_id=f"orders:election:respond:{order_id}",
            row=0,
        )
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_open_candidate_choice(interaction, self.order_id)


class ElectionRespondView(GuardedView):
    def __init__(self, order_id: str) -> None:
        super().__init__(timeout=None)
        self.order_id = order_id
        self.add_item(ElectionRespondButton(order_id))


class VotePromptButton(discord.ui.Button):
    def __init__(self, order_id: str):
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Отдать голос",
            custom_id=f"orders:election:vote:{order_id}",
            row=0,
        )
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_open_vote(interaction, self.order_id)


class VotePromptView(GuardedView):
    def __init__(self, order_id: str) -> None:
        super().__init__(timeout=None)
        self.order_id = order_id
        self.add_item(VotePromptButton(order_id))


class WarReadyButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Принять зов войны",
            custom_id="orders:war:ready",
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_war_choice(interaction, ready=True)


class WarWaitButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Пережду войну",
            custom_id="orders:war:wait",
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional[OrdersCog] = interaction.client.get_cog("OrdersCog")  # type: ignore[name-defined]
        if cog:
            await cog.handle_war_choice(interaction, ready=False)


class WarDecisionView(GuardedView):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(WarReadyButton())
        self.add_item(WarWaitButton())


class CandidateDecisionView(GuardedView):
    def __init__(self, cog: "OrdersCog", order_id: str):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.order_id = order_id

    @discord.ui.button(label="Участвовать за звание глашатая", style=discord.ButtonStyle.success)
    async def join_candidate(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handle_candidate_decision(interaction, self.order_id, willing=True)

    @discord.ui.button(label="Отказаться", style=discord.ButtonStyle.secondary)
    async def decline_candidate(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handle_candidate_decision(interaction, self.order_id, willing=False)


class VoteSelect(discord.ui.Select):
    def __init__(self, cog: "OrdersCog", order_id: str, options: list[discord.SelectOption]):
        super().__init__(
            custom_id=f"orders:election:vote_select:{order_id}",
            placeholder="Выбери кандидата",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction) -> None:
        val = self.values[0] if self.values else ""
        try:
            cand_id = int(val)
        except Exception:
            await safe_send(interaction, "Не удалось прочитать выбор кандидата.", ephemeral=True)
            return
        await self.cog.handle_vote(interaction, self.order_id, cand_id)


class VoteSelectView(GuardedView):
    def __init__(self, cog: "OrdersCog", order_id: str, options: list[discord.SelectOption]):
        super().__init__(timeout=5 * 60)
        self.add_item(VoteSelect(cog, order_id, options))


class DeclareWarModal(discord.ui.Modal, title="Объявить войну"):
    target_order = discord.ui.TextInput(
        label="Орден-цель (rher / sylvian / gro)",
        placeholder="Например: sylvian",
        max_length=20,
        required=True,
    )
    game_name = discord.ui.TextInput(
        label="Игра",
        placeholder="Например: Dead by Daylight",
        max_length=80,
        required=True,
    )
    needed_count = discord.ui.TextInput(
        label="Сколько участников требуется",
        placeholder="Например: 3",
        max_length=3,
        required=True,
    )
    match_note = discord.ui.TextInput(
        label="Когда пройдет состязание",
        placeholder="Например: 15 марта, 21:00",
        max_length=120,
        required=True,
    )

    def __init__(self, cog: "OrdersCog", attacker_order_id: str):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.attacker_order_id = attacker_order_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_submit_war_modal(
            interaction,
            attacker_order_id=self.attacker_order_id,
            target_raw=str(self.target_order),
            game_name=str(self.game_name),
            needed_raw=str(self.needed_count),
            match_note=str(self.match_note),
        )


class OrdersCog(commands.Cog, name="OrdersCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.join_view = JoinOrderView()
        self.war_panel_views: dict[str, OrderWarControlView] = {
            oid: OrderWarControlView(oid) for oid in ORDER_IDS
        }
        self.respond_views: dict[str, ElectionRespondView] = {
            oid: ElectionRespondView(oid) for oid in ORDER_IDS
        }
        self.vote_views: dict[str, VotePromptView] = {
            oid: VotePromptView(oid) for oid in ORDER_IDS
        }
        self.war_decision_view = WarDecisionView()
        self._tick_lock = False

    @property
    def repo(self):
        return getattr(self.bot, "repo", None)

    @property
    def cfg(self) -> dict[str, Any]:
        return getattr(self.bot, "cfg", {}) or {}

    async def cog_load(self) -> None:
        if not self.heartbeat.is_running():
            self.heartbeat.start()

    async def cog_unload(self) -> None:
        if self.heartbeat.is_running():
            self.heartbeat.cancel()

    def _is_admin(self, user_id: int) -> bool:
        try:
            return bool(self.bot.is_admin(int(user_id)))  # type: ignore[attr-defined]
        except Exception:
            return False

    def _guild_id(self) -> int:
        try:
            return int(self.cfg.get("guild_id", 0))
        except Exception:
            return 0

    def _guild(self) -> Optional[discord.Guild]:
        gid = self._guild_id()
        if gid <= 0:
            return None
        return self.bot.get_guild(gid)

    def _admin_panel_channel_id(self) -> int:
        ch = self.cfg.get("channels", {}) or {}
        try:
            return int(ch.get("admin_panel", 0))
        except Exception:
            return 0

    def _hall_channel_id(self) -> int:
        ch = self.cfg.get("channels", {}) or {}
        try:
            return int(ch.get("order_hall", 0))
        except Exception:
            return 0

    async def _send_archive(self, order_id: str, text: str) -> None:
        spec = ORDERS.get(order_id)
        if not spec:
            return
        ch = self.bot.get_channel(spec.archive_channel_id)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(text)
            except Exception:
                log.exception("archive send failed order=%s", order_id)

    async def _send_admin(self, text: str) -> None:
        ch_id = self._admin_panel_channel_id()
        if ch_id <= 0:
            return
        ch = self.bot.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(text)
            except Exception:
                log.exception("admin panel send failed")

    async def _member_ids_for_order(self, guild: discord.Guild, order_id: str) -> list[int]:
        if not self.repo:
            return []
        all_ids = await self.repo.order_member_ids_from_db(order_id)
        out: list[int] = []
        for uid in all_ids:
            m = guild.get_member(int(uid))
            if not m:
                continue
            if m.bot or self._is_admin(m.id):
                continue
            out.append(int(uid))
        return _uniq_keep_order(out)

    async def _member_name(self, guild: Optional[discord.Guild], user_id: int) -> str:
        if guild:
            m = guild.get_member(int(user_id))
            if m:
                return m.display_name
        user = self.bot.get_user(int(user_id))
        if user:
            return user.display_name
        return str(user_id)

    async def _get_order_role(self, guild: discord.Guild, order_id: str) -> Optional[discord.Role]:
        spec = ORDERS.get(order_id)
        if not spec:
            return None
        return guild.get_role(int(spec.role_id))

    async def _sync_member_order_roles(self, member: discord.Member, chosen_order_id: str) -> None:
        chosen_role = await self._get_order_role(member.guild, chosen_order_id)
        if not chosen_role:
            return
        remove_roles: list[discord.Role] = []
        for oid, spec in ORDERS.items():
            if oid == chosen_order_id:
                continue
            role = member.guild.get_role(int(spec.role_id))
            if role and role in member.roles:
                remove_roles.append(role)
        try:
            if remove_roles:
                await member.remove_roles(*remove_roles, reason="Смена состояния ордена при вступлении")
            if chosen_role not in member.roles:
                await member.add_roles(chosen_role, reason="Вступление в орден")
        except Exception:
            log.exception("Failed to sync order roles member=%s order=%s", member.id, chosen_order_id)

    def _build_order_panel_embed(self) -> discord.Embed:
        emb = discord.Embed(
            title="Выбор ордена",
            description=ORDER_PANEL_TEXT,
            color=discord.Color.dark_theme(),
        )
        for oid in ORDER_IDS:
            spec = ORDERS[oid]
            emb.add_field(name=spec.label, value=spec.lore, inline=False)
        return emb

    def _build_order_war_embed(self, order_id: str) -> discord.Embed:
        spec = ORDERS[order_id]
        emb = discord.Embed(
            title=f"{spec.label} — письма войны",
            description=(
                "Выбери действие:\n"
                "• `Выбрать глашатая` — запустить выборы внутри ордена.\n"
                "• `Объявить войну` — доступно только действующему глашатаю."
            ),
            color=discord.Color.dark_red(),
        )
        return emb

    @commands.command(name="orders_panel")
    @commands.has_permissions(administrator=True)
    async def orders_panel(self, ctx: commands.Context) -> None:
        hall = self.bot.get_channel(self._hall_channel_id())
        if not isinstance(hall, discord.TextChannel):
            await ctx.reply("Не найден канал `channels.order_hall`.")
            return
        await hall.send(embed=self._build_order_panel_embed(), view=self.join_view)
        await ctx.reply("Панель выбора ордена опубликована.")

    @commands.command(name="orders_war_panels")
    @commands.has_permissions(administrator=True)
    async def orders_war_panels(self, ctx: commands.Context) -> None:
        sent = 0
        for oid, spec in ORDERS.items():
            ch = self.bot.get_channel(spec.war_channel_id)
            if not isinstance(ch, discord.TextChannel):
                continue
            await ch.send(embed=self._build_order_war_embed(oid), view=self.war_panel_views[oid])
            sent += 1
        await ctx.reply(f"Панели войны опубликованы: {sent}.")

    @commands.command(name="war_result")
    @commands.has_permissions(administrator=True)
    async def war_result(self, ctx: commands.Context, war_id: int, winner_order: str) -> None:
        if not self.repo:
            await ctx.reply("БД недоступна.")
            return
        oid = _clean_order_id(winner_order)
        if not oid:
            await ctx.reply("Орден победителя: rher / sylvian / gro.")
            return
        war = await self.repo.order_get_war(int(war_id))
        if not war:
            await ctx.reply("Война не найдена.")
            return
        await self.repo.order_add_influence(oid, 2)
        await self.repo.order_finish_war(int(war_id), stage="completed", result_order_id=oid)

        text = (
            "✦ **Итог войны зафиксирован**\n"
            f"Война #{war_id}: {_order_name(war['attacker_order_id'])} vs {_order_name(war['defender_order_id'])}\n"
            f"Победитель: {_order_name(oid)} (+2 влияния)."
        )
        await self._send_archive(war["attacker_order_id"], text)
        await self._send_archive(war["defender_order_id"], text)
        await self._send_admin(text)
        await ctx.reply("Результат войны зафиксирован.")

    async def handle_join_order(self, interaction: discord.Interaction, order_id: str) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система орденов недоступна.", ephemeral=True)
            return
        member = interaction.user
        await safe_defer_ephemeral(interaction)

        user = await self.repo.get_user(member.id)
        lvl = int(user.get("level", 1))
        if lvl < ORDER_UNLOCK_LEVEL:
            await safe_send(interaction, ORDER_JOIN_DENY_LEVEL, ephemeral=True)
            return

        current = await self.repo.order_get_user(member.id)
        if current and current != order_id:
            await safe_send(
                interaction,
                f"Ты уже связан с {_order_name(current)}. На текущем этапе смена ордена недоступна.",
                ephemeral=True,
            )
            return

        ok = await self.repo.order_join(member.id, order_id)
        if not ok:
            await safe_send(interaction, "Не удалось вступить в орден.", ephemeral=True)
            return

        await self._sync_member_order_roles(member, order_id)
        await safe_send(interaction, ORDER_JOIN_OK, ephemeral=True)

    async def handle_start_herald_election(self, interaction: discord.Interaction, order_id: str) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        await safe_defer_ephemeral(interaction)

        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Только участник своего ордена может начать выборы.", ephemeral=True)
            return

        state = await self.repo.order_get_election(order_id)
        if state and str(state.get("stage") or "") in {"collect", "vote"}:
            await safe_send(interaction, "Выборы уже идут.", ephemeral=True)
            return

        now = _now()
        last_finished = int(state.get("last_finished_ts", 0)) if state else 0
        if last_finished > 0 and now < last_finished + ELECTION_COOLDOWN_SECONDS:
            await safe_send(interaction, ELECTION_COOLDOWN_DENY, ephemeral=True)
            return

        await self.repo.order_election_reset_runtime(order_id, stage="collect", last_finished_ts=last_finished)
        collect_deadline = now + ELECTION_COLLECT_SECONDS
        await self.repo.order_upsert_election(
            order_id,
            stage="collect",
            started_ts=now,
            collect_deadline_ts=collect_deadline,
            vote_deadline_ts=0,
            collect_message_id=0,
            vote_message_id=0,
        )

        spec = ORDERS[order_id]
        war_ch = self.bot.get_channel(spec.war_channel_id)
        if isinstance(war_ch, discord.TextChannel):
            role = war_ch.guild.get_role(spec.role_id)
            ping = role.mention if role else spec.label
            msg = await war_ch.send(
                f"{ping}\n{ELECTION_PROMPT}\nДо завершения этапа: {_fmt_ts_rel(collect_deadline)}",
                view=self.respond_views[order_id],
            )
            await self.repo.order_upsert_election(order_id, collect_message_id=int(msg.id))

        await safe_send(interaction, "Выборы глашатая запущены.", ephemeral=True)

    async def handle_open_candidate_choice(self, interaction: discord.Interaction, order_id: str) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        if interaction.user.bot or self._is_admin(interaction.user.id):
            await safe_send(interaction, "Бот и администратор не участвуют в этих выборах.", ephemeral=True)
            return
        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "collect":
            await safe_send(interaction, "Этап сбора кандидатов уже завершён.", ephemeral=True)
            return
        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Это выборы другого ордена.", ephemeral=True)
            return
        view = CandidateDecisionView(self, order_id)
        await safe_send(interaction, ELECTION_PROMPT, ephemeral=True, view=view)

    async def handle_candidate_decision(
        self,
        interaction: discord.Interaction,
        order_id: str,
        *,
        willing: bool,
    ) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        if interaction.user.bot or self._is_admin(interaction.user.id):
            await safe_send(interaction, "Бот и администратор не участвуют в этих выборах.", ephemeral=True)
            return
        await safe_defer_ephemeral(interaction)
        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "collect":
            await safe_send(interaction, "Этап уже закрыт.", ephemeral=True)
            return
        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Это выборы другого ордена.", ephemeral=True)
            return
        await self.repo.order_election_set_candidate(order_id, interaction.user.id, willing)
        await safe_send(
            interaction,
            "Твой ответ записан." if willing else "Записано: ты отказываешься от участия.",
            ephemeral=True,
        )
        await self._refresh_collect_message(order_id)
        await self._maybe_finalize_collect(order_id)

    async def _collect_candidates_filtered(self, guild: discord.Guild, order_id: str) -> list[dict[str, Any]]:
        if not self.repo:
            return []
        eligible = set(await self._member_ids_for_order(guild, order_id))
        rows = await self.repo.order_election_candidates(order_id)
        out: list[dict[str, Any]] = []
        for r in rows:
            uid = int(r.get("user_id", 0))
            if uid in eligible:
                out.append(r)
        return out

    async def _refresh_collect_message(self, order_id: str) -> None:
        if not self.repo:
            return
        state = await self.repo.order_get_election(order_id)
        if not state:
            return
        msg_id = int(state.get("collect_message_id", 0))
        if msg_id <= 0:
            return
        spec = ORDERS[order_id]
        ch = self.bot.get_channel(spec.war_channel_id)
        guild = self._guild()
        if not isinstance(ch, discord.TextChannel) or not guild:
            return
        try:
            msg = await ch.fetch_message(msg_id)
        except Exception:
            return
        rows = await self._collect_candidates_filtered(guild, order_id)
        candidates = [int(r["user_id"]) for r in rows if int(r.get("willing", 0)) == 1]
        declined = [int(r["user_id"]) for r in rows if int(r.get("willing", 0)) == 0 and int(r.get("responded_ts", 0)) > 0]
        eligible = await self._member_ids_for_order(guild, order_id)
        responded = [int(r["user_id"]) for r in rows if int(r.get("responded_ts", 0)) > 0]

        lines = [
            ELECTION_PROMPT,
            f"До завершения этапа: {_fmt_ts_rel(int(state.get('collect_deadline_ts', 0)))}",
            "",
            f"Ответили: **{len(set(responded))}/{len(eligible)}**",
            f"Кандидаты: **{len(candidates)}**",
            f"Отказались: **{len(declined)}**",
        ]
        try:
            await msg.edit(content="\n".join(lines), view=self.respond_views[order_id])
        except Exception:
            log.exception("Failed to refresh collect message order=%s", order_id)

    async def _maybe_finalize_collect(self, order_id: str, *, force: bool = False) -> None:
        if not self.repo:
            return
        guild = self._guild()
        if not guild:
            return
        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "collect":
            return
        now = _now()
        deadline = int(state.get("collect_deadline_ts", 0))
        rows = await self._collect_candidates_filtered(guild, order_id)
        eligible_ids = await self._member_ids_for_order(guild, order_id)
        responded_ids = {int(r["user_id"]) for r in rows if int(r.get("responded_ts", 0)) > 0}

        done_early = len(eligible_ids) > 0 and len(responded_ids) >= len(eligible_ids)
        if not force and not done_early and now < deadline:
            return

        candidates = [
            r for r in rows if int(r.get("willing", 0)) == 1
        ]
        candidates.sort(key=lambda x: int(x.get("applied_ts", 0)))
        await self._refresh_collect_message(order_id)

        war_ch = self.bot.get_channel(ORDERS[order_id].war_channel_id)
        if len(candidates) == 0:
            await self.repo.order_election_reset_runtime(order_id, stage="", last_finished_ts=now)
            if isinstance(war_ch, discord.TextChannel):
                await war_ch.send(ELECTION_NO_CANDIDATES)
            await self._send_archive(order_id, ELECTION_NO_CANDIDATES)
            return

        if len(candidates) == 1:
            winner_id = int(candidates[0]["user_id"])
            await self.repo.order_set_herald(order_id, winner_id)
            await self.repo.order_election_reset_runtime(order_id, stage="", last_finished_ts=now)
            text = f"{ELECTION_ONE_CANDIDATE}\nНовый глашатай: {_mention_user(winner_id)}"
            if isinstance(war_ch, discord.TextChannel):
                await war_ch.send(text)
            await self._send_archive(order_id, text)
            return

        vote_deadline = now + ELECTION_VOTE_SECONDS
        await self.repo.order_upsert_election(
            order_id,
            stage="vote",
            vote_deadline_ts=vote_deadline,
            collect_deadline_ts=0,
        )
        if isinstance(war_ch, discord.TextChannel):
            names: list[str] = []
            for r in candidates:
                uid = int(r["user_id"])
                names.append(f"• {_mention_user(uid)}")
            msg = await war_ch.send(
                f"{ELECTION_START_VOTE}\n\nКандидаты:\n" + "\n".join(names) +
                f"\n\nДо завершения: {_fmt_ts_rel(vote_deadline)}",
                view=self.vote_views[order_id],
            )
            await self.repo.order_upsert_election(order_id, vote_message_id=int(msg.id))

    async def handle_open_vote(self, interaction: discord.Interaction, order_id: str) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        if interaction.user.bot or self._is_admin(interaction.user.id):
            await safe_send(interaction, "Бот и администратор не голосуют на выборах ордена.", ephemeral=True)
            return
        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "vote":
            await safe_send(interaction, "Голосование сейчас неактивно.", ephemeral=True)
            return
        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Это голосование другого ордена.", ephemeral=True)
            return

        guild = self._guild()
        if not guild:
            await safe_send(interaction, "Гильдия недоступна.", ephemeral=True)
            return
        rows = await self._collect_candidates_filtered(guild, order_id)
        candidates = [r for r in rows if int(r.get("willing", 0)) == 1]
        candidates.sort(key=lambda x: int(x.get("applied_ts", 0)))
        if not candidates:
            await safe_send(interaction, "Список кандидатов пуст.", ephemeral=True)
            return

        opts: list[discord.SelectOption] = []
        for r in candidates:
            uid = int(r["user_id"])
            name = await self._member_name(guild, uid)
            opts.append(discord.SelectOption(label=name[:100], value=str(uid)))
        await safe_send(
            interaction,
            "Выбери кандидата. Голос можно изменить до дедлайна.",
            ephemeral=True,
            view=VoteSelectView(self, order_id, opts),
        )

    async def handle_vote(self, interaction: discord.Interaction, order_id: str, candidate_id: int) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        if interaction.user.bot or self._is_admin(interaction.user.id):
            await safe_send(interaction, "Бот и администратор не голосуют на выборах ордена.", ephemeral=True)
            return
        await safe_defer_ephemeral(interaction)

        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "vote":
            await safe_send(interaction, "Голосование закрыто.", ephemeral=True)
            return
        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Это голосование другого ордена.", ephemeral=True)
            return

        guild = self._guild()
        if not guild:
            await safe_send(interaction, "Гильдия недоступна.", ephemeral=True)
            return
        rows = await self._collect_candidates_filtered(guild, order_id)
        candidates = {int(r["user_id"]) for r in rows if int(r.get("willing", 0)) == 1}
        if int(candidate_id) not in candidates:
            await safe_send(interaction, "Кандидат больше не доступен.", ephemeral=True)
            return

        await self.repo.order_election_set_vote(order_id, interaction.user.id, int(candidate_id))
        await safe_send(interaction, "Голос принят.", ephemeral=True)
        await self._refresh_vote_message(order_id)
        await self._maybe_finalize_vote(order_id)

    async def _refresh_vote_message(self, order_id: str) -> None:
        if not self.repo:
            return
        state = await self.repo.order_get_election(order_id)
        if not state:
            return
        msg_id = int(state.get("vote_message_id", 0))
        if msg_id <= 0:
            return
        spec = ORDERS[order_id]
        ch = self.bot.get_channel(spec.war_channel_id)
        guild = self._guild()
        if not isinstance(ch, discord.TextChannel) or not guild:
            return
        try:
            msg = await ch.fetch_message(msg_id)
        except Exception:
            return

        candidate_rows = await self._collect_candidates_filtered(guild, order_id)
        candidate_rows = [r for r in candidate_rows if int(r.get("willing", 0)) == 1]
        candidate_rows.sort(key=lambda x: int(x.get("applied_ts", 0)))
        votes = await self.repo.order_election_votes(order_id)
        eligible = set(await self._member_ids_for_order(guild, order_id))
        voter_ids = {int(v["voter_user_id"]) for v in votes if int(v["voter_user_id"]) in eligible}
        counts: dict[int, int] = {}
        for v in votes:
            voter = int(v["voter_user_id"])
            cand = int(v["candidate_user_id"])
            if voter not in eligible:
                continue
            counts[cand] = counts.get(cand, 0) + 1

        lines = [ELECTION_START_VOTE, ""]
        lines.append(f"До завершения: {_fmt_ts_rel(int(state.get('vote_deadline_ts', 0)))}")
        lines.append(f"Проголосовали: **{len(voter_ids)}/{len(eligible)}**")
        lines.append("")
        lines.append("Кандидаты:")
        for r in candidate_rows:
            uid = int(r["user_id"])
            votes_count = counts.get(uid, 0)
            lines.append(f"• {_mention_user(uid)} — {votes_count} голос(ов)")
        try:
            await msg.edit(content="\n".join(lines), view=self.vote_views[order_id])
        except Exception:
            log.exception("Failed to refresh vote message order=%s", order_id)

    async def _maybe_finalize_vote(self, order_id: str, *, force: bool = False) -> None:
        if not self.repo:
            return
        state = await self.repo.order_get_election(order_id)
        if not state or str(state.get("stage") or "") != "vote":
            return
        guild = self._guild()
        if not guild:
            return
        now = _now()
        deadline = int(state.get("vote_deadline_ts", 0))

        eligible = set(await self._member_ids_for_order(guild, order_id))
        votes = await self.repo.order_election_votes(order_id)
        voted = {int(v["voter_user_id"]) for v in votes if int(v["voter_user_id"]) in eligible}
        done_early = len(eligible) > 0 and len(voted) >= len(eligible)
        if not force and not done_early and now < deadline:
            return

        candidates = await self._collect_candidates_filtered(guild, order_id)
        candidates = [r for r in candidates if int(r.get("willing", 0)) == 1]
        if not candidates:
            await self.repo.order_election_reset_runtime(order_id, stage="", last_finished_ts=now)
            await self._send_archive(order_id, ELECTION_NO_CANDIDATES)
            return
        candidates.sort(key=lambda x: int(x.get("applied_ts", 0)))
        order_by_apply = [int(r["user_id"]) for r in candidates]

        counts: dict[int, int] = {uid: 0 for uid in order_by_apply}
        for v in votes:
            voter = int(v["voter_user_id"])
            cand = int(v["candidate_user_id"])
            if voter not in eligible or cand not in counts:
                continue
            counts[cand] = counts.get(cand, 0) + 1

        winner = order_by_apply[0]
        winner_votes = counts.get(winner, 0)
        for uid in order_by_apply[1:]:
            v = counts.get(uid, 0)
            if v > winner_votes:
                winner = uid
                winner_votes = v

        await self.repo.order_set_herald(order_id, winner)
        await self.repo.order_election_reset_runtime(order_id, stage="", last_finished_ts=now)
        text = f"{ELECTION_FINISHED}\nНовый глашатай: {_mention_user(winner)}"
        ch = self.bot.get_channel(ORDERS[order_id].war_channel_id)
        if isinstance(ch, discord.TextChannel):
            await ch.send(text)
        await self._send_archive(order_id, text)

    async def handle_open_war_modal(self, interaction: discord.Interaction, order_id: str) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != order_id:
            await safe_send(interaction, "Только участник своего ордена может делать это действие.", ephemeral=True)
            return
        herald = await self.repo.order_get_herald(order_id)
        if int(herald.get("user_id", 0)) != int(interaction.user.id):
            await safe_send(interaction, "Только действующий глашатай может объявить войну.", ephemeral=True)
            return
        modal = DeclareWarModal(self, order_id)
        await interaction.response.send_modal(modal)

    async def handle_submit_war_modal(
        self,
        interaction: discord.Interaction,
        *,
        attacker_order_id: str,
        target_raw: str,
        game_name: str,
        needed_raw: str,
        match_note: str,
    ) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        await safe_defer_ephemeral(interaction)
        guild = self._guild()
        if not guild:
            await safe_send(interaction, "Гильдия недоступна.", ephemeral=True)
            return

        defender_order_id = _clean_order_id(target_raw)
        if not defender_order_id or defender_order_id == attacker_order_id:
            await safe_send(interaction, "Укажи корректный орден-цель (rher/sylvian/gro, не свой).", ephemeral=True)
            return
        try:
            needed_count = int(str(needed_raw).strip())
        except Exception:
            needed_count = 0
        if needed_count <= 0:
            await safe_send(interaction, "Количество участников должно быть положительным числом.", ephemeral=True)
            return

        if await self.repo.order_has_active_war_for(attacker_order_id):
            await safe_send(interaction, "Твой орден уже участвует в активной войне или предложении войны.", ephemeral=True)
            return
        if await self.repo.order_has_active_war_for(defender_order_id):
            await safe_send(interaction, "Целевой орден уже участвует в активной войне или предложении войны.", ephemeral=True)
            return

        last_attack_ts = await self.repo.order_last_war_attack_ts(attacker_order_id)
        now = _now()
        if last_attack_ts > 0 and now < last_attack_ts + WAR_COOLDOWN_SECONDS:
            await safe_send(interaction, "Твой орден может объявлять войны не чаще одного раза в 24 часа.", ephemeral=True)
            return

        attacker_members = await self._member_ids_for_order(guild, attacker_order_id)
        defender_members = await self._member_ids_for_order(guild, defender_order_id)
        if needed_count > len(attacker_members):
            await safe_send(interaction, "Размер войны больше реального состава твоего ордена.", ephemeral=True)
            return
        if needed_count > len(defender_members):
            await safe_send(interaction, "Размер войны больше реального состава целевого ордена.", ephemeral=True)
            return

        war_id = await self.repo.order_create_war(
            attacker_order_id=attacker_order_id,
            defender_order_id=defender_order_id,
            game_name=game_name.strip(),
            needed_count=needed_count,
            match_note=match_note.strip(),
            started_by_user_id=interaction.user.id,
            stage="collect_attacker",
            attacker_deadline_ts=now + WAR_STAGE_SECONDS,
        )
        await self._post_attacker_war_messages(war_id)
        await safe_send(interaction, "Война объявлена. Запущен сбор атакующей стороны.", ephemeral=True)

    async def _post_attacker_war_messages(self, war_id: int) -> None:
        if not self.repo:
            return
        war = await self.repo.order_get_war(war_id)
        if not war:
            return
        spec = ORDERS[war["attacker_order_id"]]
        ch = self.bot.get_channel(spec.war_channel_id)
        if not isinstance(ch, discord.TextChannel):
            return

        letter = await ch.send(
            f"{WAR_ATTACKER_STARTED}\n\n"
            f"Противник: {_order_name(war['defender_order_id'])}\n"
            f"Игра: {war['game_name']}\n"
            f"Дата и время: {war['match_note']}\n"
            f"Требуется бойцов: {war['needed_count']}"
        )
        status = await ch.send(
            await self._render_war_status_text(war, side_order_id=war["attacker_order_id"]),
            view=self.war_decision_view,
        )
        await self.repo.order_update_war(
            war_id,
            attacker_letter_message_id=int(letter.id),
            attacker_status_message_id=int(status.id),
        )

    async def _post_defender_war_messages(self, war_id: int) -> None:
        if not self.repo:
            return
        war = await self.repo.order_get_war(war_id)
        if not war:
            return
        spec = ORDERS[war["defender_order_id"]]
        ch = self.bot.get_channel(spec.war_channel_id)
        if not isinstance(ch, discord.TextChannel):
            return

        letter = await ch.send(
            f"{WAR_DEFENDER_STARTED}\n\n"
            f"Атакующий орден: {_order_name(war['attacker_order_id'])}\n"
            f"Игра: {war['game_name']}\n"
            f"Дата и время: {war['match_note']}\n"
            f"Требуется бойцов: {war['needed_count']}"
        )
        status = await ch.send(
            await self._render_war_status_text(war, side_order_id=war["defender_order_id"]),
            view=self.war_decision_view,
        )
        await self.repo.order_update_war(
            war_id,
            defender_letter_message_id=int(letter.id),
            defender_status_message_id=int(status.id),
        )

    async def _find_war_by_status_message(self, message_id: int) -> tuple[Optional[dict[str, Any]], str]:
        if not self.repo:
            return None, ""
        wars = await self.repo.order_active_wars()
        mid = int(message_id)
        for war in wars:
            if int(war.get("attacker_status_message_id", 0)) == mid:
                return war, str(war["attacker_order_id"])
            if int(war.get("defender_status_message_id", 0)) == mid:
                return war, str(war["defender_order_id"])
        return None, ""

    async def handle_war_choice(self, interaction: discord.Interaction, *, ready: bool) -> None:
        if not self.repo or not isinstance(interaction.user, discord.Member) or not interaction.message:
            await safe_send(interaction, "Система недоступна.", ephemeral=True)
            return
        await safe_defer_ephemeral(interaction)
        war, side_order_id = await self._find_war_by_status_message(interaction.message.id)
        if not war:
            await safe_send(interaction, "Эта война уже не активна.", ephemeral=True)
            return

        user_order = await self.repo.order_get_user(interaction.user.id)
        if user_order != side_order_id:
            await safe_send(interaction, "Ты не входишь в состав этого ордена для текущего этапа.", ephemeral=True)
            return
        if self._is_admin(interaction.user.id):
            await safe_send(interaction, "Администратор не участвует в боевых списках.", ephemeral=True)
            return

        await self.repo.order_set_war_response(int(war["war_id"]), side_order_id, interaction.user.id, ready)
        await safe_send(interaction, WAR_READY_OK if ready else WAR_WAIT_OK, ephemeral=True)
        await self._refresh_war_status_messages(int(war["war_id"]))
        await self._maybe_advance_war(int(war["war_id"]))

    async def _war_responses(self, war_id: int, order_id: str) -> tuple[list[int], list[int]]:
        if not self.repo:
            return [], []
        rows = await self.repo.order_get_war_responses(war_id, order_id)
        ready_ids: list[int] = []
        wait_ids: list[int] = []
        for r in rows:
            uid = int(r.get("user_id", 0))
            if int(r.get("ready", 0)) == 1:
                ready_ids.append(uid)
            else:
                wait_ids.append(uid)
        return _uniq_keep_order(ready_ids), _uniq_keep_order(wait_ids)

    async def _war_responses_filtered(self, war_id: int, order_id: str) -> tuple[list[int], list[int]]:
        guild = self._guild()
        if not guild:
            return await self._war_responses(war_id, order_id)
        eligible = set(await self._member_ids_for_order(guild, order_id))
        ready_ids, wait_ids = await self._war_responses(war_id, order_id)
        return (
            [x for x in ready_ids if x in eligible],
            [x for x in wait_ids if x in eligible],
        )

    async def _render_war_status_text(self, war: dict[str, Any], *, side_order_id: str) -> str:
        guild = self._guild()
        war_id = int(war["war_id"])
        ready_ids, wait_ids = await self._war_responses_filtered(war_id, side_order_id)
        needed = int(war["needed_count"])
        if str(war["stage"]) == "collect_attacker":
            deadline = int(war["attacker_deadline_ts"])
            enemy = war["defender_order_id"]
            title = f"✦ Сбор воинов { _order_name(side_order_id) }"
        else:
            deadline = int(war["defender_deadline_ts"])
            enemy = war["attacker_order_id"]
            title = f"✦ Сбор воинов { _order_name(side_order_id) }"

        lines = [
            title,
            f"Противник: {_order_name(enemy)}",
            f"Игра: {war['game_name']}",
            f"Дата и время: {war['match_note']}",
            f"Требуется бойцов: {needed}",
            f"До окончания сбора: {_fmt_ts_rel(deadline)}",
            "",
            "Готовы к войне:",
        ]
        if ready_ids:
            for i, uid in enumerate(ready_ids, start=1):
                lines.append(f"{i}. {_mention_user(uid)}")
        else:
            lines.append("—")
        lines.append("")
        lines.append("Отказались:")
        if wait_ids:
            for i, uid in enumerate(wait_ids, start=1):
                lines.append(f"{i}. {_mention_user(uid)}")
        else:
            lines.append("—")
        if guild:
            pass
        return "\n".join(lines)

    async def _refresh_war_status_messages(self, war_id: int) -> None:
        if not self.repo:
            return
        war = await self.repo.order_get_war(war_id)
        if not war:
            return
        stage = str(war.get("stage") or "")
        if stage not in {"collect_attacker", "collect_defender"}:
            return

        a_spec = ORDERS[war["attacker_order_id"]]
        d_spec = ORDERS[war["defender_order_id"]]
        a_ch = self.bot.get_channel(a_spec.war_channel_id)
        d_ch = self.bot.get_channel(d_spec.war_channel_id)

        async def _edit(ch: Any, msg_id: int, side_order: str) -> None:
            if not isinstance(ch, discord.TextChannel) or int(msg_id) <= 0:
                return
            try:
                msg = await ch.fetch_message(int(msg_id))
            except Exception:
                return
            try:
                await msg.edit(
                    content=await self._render_war_status_text(war, side_order_id=side_order),
                    view=self.war_decision_view,
                )
            except Exception:
                log.exception("Failed to edit war status war=%s side=%s", war_id, side_order)

        await _edit(a_ch, int(war.get("attacker_status_message_id", 0)), war["attacker_order_id"])
        await _edit(d_ch, int(war.get("defender_status_message_id", 0)), war["defender_order_id"])

    async def _disable_war_status_messages(self, war: dict[str, Any], final_note: str) -> None:
        a_spec = ORDERS[war["attacker_order_id"]]
        d_spec = ORDERS[war["defender_order_id"]]
        pairs = (
            (self.bot.get_channel(a_spec.war_channel_id), int(war.get("attacker_status_message_id", 0))),
            (self.bot.get_channel(d_spec.war_channel_id), int(war.get("defender_status_message_id", 0))),
        )
        for ch, msg_id in pairs:
            if not isinstance(ch, discord.TextChannel) or msg_id <= 0:
                continue
            try:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(content=(msg.content + "\n\n" + final_note).strip(), view=None)
            except Exception:
                pass

    async def _maybe_advance_war(self, war_id: int) -> None:
        if not self.repo:
            return
        war = await self.repo.order_get_war(war_id)
        if not war:
            return
        stage = str(war.get("stage") or "")
        needed = int(war.get("needed_count", 0))
        now = _now()

        if stage == "collect_attacker":
            ready_ids, _ = await self._war_responses_filtered(war_id, war["attacker_order_id"])
            if len(ready_ids) >= needed:
                await self.repo.order_update_war(
                    war_id,
                    stage="collect_defender",
                    defender_deadline_ts=now + WAR_STAGE_SECONDS,
                )
                await self._post_defender_war_messages(war_id)
                await self._refresh_war_status_messages(war_id)
                return
            if now >= int(war.get("attacker_deadline_ts", 0)):
                await self.repo.order_add_influence(war["attacker_order_id"], -1)
                await self.repo.order_finish_war(
                    war_id,
                    stage="cancelled",
                    cancel_reason="attacker_no_team",
                )
                text = ARCHIVE_ATTACKER_FAIL + f"\nВойна #{war_id}."
                await self._send_archive(war["attacker_order_id"], text)
                await self._send_archive(war["defender_order_id"], text)
                await self._send_admin(text)
                await self._disable_war_status_messages(war, "Сбор закрыт. Война отменена.")
                return

        if stage == "collect_defender":
            ready_ids, _ = await self._war_responses_filtered(war_id, war["defender_order_id"])
            if len(ready_ids) >= needed:
                await self.repo.order_finish_war(
                    war_id,
                    stage="confirmed",
                    cancel_reason="",
                    result_order_id="",
                )
                war = await self.repo.order_get_war(war_id) or war
                await self._disable_war_status_messages(war, "Сбор закрыт. Война подтверждена.")
                await self._announce_war_confirmed(war)
                return
            if now >= int(war.get("defender_deadline_ts", 0)):
                await self.repo.order_add_influence(war["defender_order_id"], -1)
                await self.repo.order_finish_war(
                    war_id,
                    stage="cancelled",
                    cancel_reason="defender_no_team",
                )
                text = ARCHIVE_DEFENDER_FAIL + f"\nВойна #{war_id}."
                await self._send_archive(war["attacker_order_id"], text)
                await self._send_archive(war["defender_order_id"], text)
                await self._send_admin(text)
                await self._disable_war_status_messages(war, "Сбор закрыт. Война отменена.")

    async def _announce_war_confirmed(self, war: dict[str, Any]) -> None:
        war_id = int(war["war_id"])
        attacker_ready, _ = await self._war_responses_filtered(war_id, war["attacker_order_id"])
        defender_ready, _ = await self._war_responses_filtered(war_id, war["defender_order_id"])
        details = [
            ARCHIVE_WAR_CONFIRMED,
            "",
            f"Война #{war_id}",
            f"Атакующий: {_order_name(war['attacker_order_id'])}",
            f"Защитник: {_order_name(war['defender_order_id'])}",
            f"Игра: {war['game_name']}",
            f"Дата и время: {war['match_note']}",
            f"Требуется бойцов: {war['needed_count']}",
            "",
            "Готовы (атакующий): " + (", ".join(_mention_user(x) for x in attacker_ready) or "—"),
            "Готовы (защитник): " + (", ".join(_mention_user(x) for x in defender_ready) or "—"),
        ]
        text = "\n".join(details)
        await self._send_archive(war["attacker_order_id"], text)
        await self._send_archive(war["defender_order_id"], text)

        admin_lines = [
            ADMIN_WAR_CONFIRMED,
            "",
            f"Война #{war_id}",
            f"{_order_name(war['attacker_order_id'])} vs {_order_name(war['defender_order_id'])}",
            f"Игра: {war['game_name']}",
            f"Дата и время: {war['match_note']}",
            f"Требуется бойцов: {war['needed_count']}",
            "Атакующие: " + (", ".join(_mention_user(x) for x in attacker_ready) or "—"),
            "Защитники: " + (", ".join(_mention_user(x) for x in defender_ready) or "—"),
        ]
        await self._send_admin("\n".join(admin_lines))

    @tasks.loop(seconds=20)
    async def heartbeat(self) -> None:
        if self._tick_lock:
            return
        if not self.bot.is_ready():
            return
        if not self.repo:
            return
        self._tick_lock = True
        try:
            for oid in ORDER_IDS:
                await self._maybe_finalize_collect(oid)
                await self._maybe_finalize_vote(oid)
            wars = await self.repo.order_active_wars()
            for war in wars:
                await self._refresh_war_status_messages(int(war["war_id"]))
                await self._maybe_advance_war(int(war["war_id"]))
        except Exception:
            log.exception("orders heartbeat failed")
        finally:
            self._tick_lock = False

    @heartbeat.before_loop
    async def heartbeat_before_loop(self) -> None:
        await self.bot.wait_until_ready()


def get_persistent_views(bot: commands.Bot) -> list[discord.ui.View]:
    # timeout=None views that must survive restarts
    views: list[discord.ui.View] = [JoinOrderView(), WarDecisionView()]
    for oid in ORDER_IDS:
        views.append(OrderWarControlView(oid))
        views.append(ElectionRespondView(oid))
        views.append(VotePromptView(oid))
    return views


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(OrdersCog(bot))
