# cogs/tendril.py
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from ._interactions import GuardedView, safe_defer_ephemeral, safe_defer_update, safe_edit_message, safe_send


def _digits(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _now() -> int:
    return int(time.time())


def _fmt_left(sec: int) -> str:
    sec = max(0, int(sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    if h > 0:
        return f"{h}ч {m}м"
    return f"{m}м"


def _is_online_or_in_voice(m: discord.Member) -> bool:
    # online если не offline, или если в голосовом (даже при невидимке в войсе)
    try:
        if m.voice and m.voice.channel is not None:
            return True
    except Exception:
        pass
    try:
        return m.status != discord.Status.offline
    except Exception:
        return False


@dataclass(frozen=True)
class TendrilCfg:
    # уровни
    cursed_coin_unlock: int = 10
    cast_unlock: int = 15
    min_target_level: int = 10

    # экономика
    attack_cost: int = 70
    shield_cost: int = 100
    min_target_runes: int = 70

    # тайминги
    tick_seconds: int = 10 * 60
    announce_seconds: int = 30 * 60
    duration_seconds: int = 6 * 60 * 60
    cast_cd_seconds: int = 24 * 60 * 60
    immunity_seconds: int = 24 * 60 * 60
    shield_seconds: int = 72 * 60 * 60

    # попытки снятия
    attempts: int = 3
    remove_attempt_cd_seconds: int = 10 * 60  # кд между попытками снятия

    # сколько рун ест за тик: берем дневной доход жертвы и множим на 5%
    # per_tick = clamp(round(per_day * 0.05), 1..3)
    per_day_pct: float = 0.05
    per_tick_min: int = 1
    per_tick_max: int = 3


ATTACK_FIRST_BITE = 30
CURSE_FIRST_BITE = 10
CURSE_MAX_TOTAL = 60


LORE_INFECT_CURSE = [
    "🕯️ Тишина дрогнула… {v} помечен проклятой монетой. Отросток вцепился в кошель.",
    "🕳️ Монета упала не той стороной — и {v} услышал шёпот. Отросток проснулся.",
    "🩸 На ребре монеты выступила кровь. {v} отмечен. Отросток ищет руны.",
    "🌑 Пустота улыбается {v}. Проклятие породило отросток.",
    "🕯️ Долг шевельнулся в кармане {v}. Отросток тянется к рунам.",
    "🕳️ Невидимая нить затянулась вокруг {v}. Отросток начал жатву.",
    "🌫️ Холодный шёпот коснулся {v}: «Отдай». Отросток проснулся.",
    "🕯️ На пальцах {v} осталась сажа от монеты. Отросток вцепился в добычу.",
    "🕳️ {v} проиграл не игру — договор. Отросток пришёл за оплатой.",
    "🌑 Сторона тьмы выпала для {v}. Отросток считает руны чужими.",
]

LORE_INFECT_ATTACK = [
    "🕯️ Печать треснула — {v} найден. Отросток пошёл по следу ритуала. (рука за нитями: {a})",
    "🕳️ Ритуал завершён. Отросток обнял {v}. (источник: {a})",
    "🌑 Нити сомкнулись на {v}. Отросток начал высасывать руны. (ритуал: {a})",
    "🩸 На алтаре погасла свеча — и {v} заражён. (рука: {a})",
    "🕯️ Кто-то прошептал имя {v}. Отросток услышал. (ритуал: {a})",
    "🕳️ Гвоздь печати вбит. Отросток держит {v}. (источник: {a})",
    "🌫️ В воздухе пахнет железом — {v} отмечен. (ритуал: {a})",
    "🌑 Отросток нашёл {v} и начал счёт монет. (ритуал: {a})",
    "🕯️ Тень протянула руку к {v}. Отросток закрепился. (источник: {a})",
    "🩸 Ритуальная нить накинута на {v}. Отросток начал жатву. (рука: {a})",
]

LORE_ANNOUNCE = [
    "🕯️ Отросток пожирает руны у {v}. Съедено уже **{n}**.",
    "🌑 Слышен хруст монет… {v} теряет руны. Всего: **{n}**.",
    "🕳️ Пустота шепчет о {v}: «Ещё». Украдено/сожрано: **{n}**.",
    "🌫️ Кошель {v} становится легче. Отросток уже забрал **{n}**.",
    "🩸 На рунах {v} — следы зубов. Отросток забрал **{n}**.",
    "🕯️ {v} платит по счёту. Отросток забрал **{n}**.",
    "🌑 Ритуальная жадность не спит: {v}, потери — **{n}**.",
    "🕳️ Отросток тянет руны из {v}. Итого: **{n}**.",
    "🌫️ Шёпоты считают монеты {v}: **{n}** уже исчезли.",
    "🩸 Монеты у {v} дрожат. Отросток отнял **{n}**.",
]

LORE_EXPIRE = [
    "🕯️ Отросток на {v} иссох и рассыпался пеплом.",
    "🌑 Пустота отпустила {v}: отросток погиб.",
    "🕳️ Нить оборвалась. Отросток слетел с {v}.",
    "🌫️ Шёпот стих — отросток отступил от {v}.",
    "🩸 Ритуальная язва закрылась: {v} свободен.",
    "🕯️ Отросток потерял хватку и упал с {v}.",
]

LORE_RIP_SHIELD = [
    "🛡️ Щит сорвал отросток с {v}. Пустота отступила.",
    "🛡️ {v} поднял щит — отросток разорван.",
    "🛡️ Свет щита прожёг нить. Отросток отпал от {v}.",
    "🛡️ Печать щита сработала: отросток на {v} уничтожен.",
]


class TendrilPanelView(GuardedView):
    def __init__(self, *, attack_cost: int = TendrilCfg.attack_cost, shield_cost: int = TendrilCfg.shield_cost):
        super().__init__(timeout=None)
        self.cast_btn.label = f"Наслать отросток ({int(attack_cost)} рун)"
        self.buy_shield_btn.label = f"Купить щит 72ч ({int(shield_cost)})"

    @discord.ui.button(label="Статус", style=discord.ButtonStyle.secondary, custom_id="tendril:status")
    async def status_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        cog = interaction.client.get_cog("TendrilCog")  # type: ignore
        if cog:
            await cog.ui_status(interaction)

    @discord.ui.button(label="Снять отросток", style=discord.ButtonStyle.primary, custom_id="tendril:remove")
    async def remove_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        cog = interaction.client.get_cog("TendrilCog")  # type: ignore
        if cog:
            await cog.ui_remove_start(interaction)

    @discord.ui.button(label="Наслать отросток", style=discord.ButtonStyle.danger, custom_id="tendril:cast")
    async def cast_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        cog = interaction.client.get_cog("TendrilCog")  # type: ignore
        if cog:
            await cog.ui_cast_start(interaction)

    @discord.ui.button(label="Купить щит (72ч)", style=discord.ButtonStyle.success, custom_id="tendril:buy_shield")
    async def buy_shield_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        cog = interaction.client.get_cog("TendrilCog")  # type: ignore
        if cog:
            await cog.ui_buy_shield(interaction)

    @discord.ui.button(label="Статус щита", style=discord.ButtonStyle.secondary, custom_id="tendril:shield_status")
    async def shield_status_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        cog = interaction.client.get_cog("TendrilCog")  # type: ignore
        if cog:
            await cog.ui_shield_status(interaction)


class CastSelect(discord.ui.Select):
    def __init__(self, cog: "TendrilCog", caster_id: int, options: list[discord.SelectOption], note: str):
        super().__init__(
            placeholder="Выбери цель для ритуала…",
            min_values=1,
            max_values=1,
            options=options[:25],
            custom_id="tendril:cast_select",
        )
        self.cog = cog
        self.caster_id = caster_id
        self.note = note

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.caster_id:
            await safe_send(interaction, "Это не твой ритуал.", ephemeral=True)
            return
        target_id = int(self.values[0])
        await self.cog.ui_cast_finish(interaction, target_id)


class CastView(GuardedView):
    def __init__(self, cog: "TendrilCog", caster_id: int, options: list[discord.SelectOption], note: str):
        super().__init__(timeout=120)
        self.add_item(CastSelect(cog, caster_id, options, note))

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        await safe_edit_message(interaction, content="Ритуал отменён.", embed=None, view=None)


@dataclass(frozen=True)
class Seal:
    sid: int
    aura: str
    material: str
    idx: int  # 1..6

    @property
    def parity(self) -> str:
        return "odd" if (self.idx % 2 == 1) else "even"


_AURA_POOL = [
    ("❄️", "ХОЛОД"),
    ("🩸", "КРОВЬ"),
    ("🔥", "ПЕПЕЛ"),
    ("🌑", "ТЬМА"),
    ("🕯️", "СВЕТ"),
    ("🌫️", "ТУМАН"),
]

_MATERIAL_POOL = [
    ("🦴", "КОСТЬ"),
    ("⛓️", "ЖЕЛЕЗО"),
    ("🪙", "ЗОЛОТО"),
    ("🧪", "СТЕКЛО"),
    ("🪵", "ДРЕВО"),
]


def _mk_seals() -> list[Seal]:
    # генерируем 6 печатей с повторяющимися аурами/материалами, но уникальными парами aura+material для 3-й попытки
    # (если не получилось — перегенерируем)
    for _ in range(40):
        auras = [random.choice(_AURA_POOL)[1] for _ in range(6)]
        mats = [random.choice(_MATERIAL_POOL)[1] for _ in range(6)]
        pairs = list(zip(auras, mats))
        if len(set(pairs)) == 6:
            return [Seal(sid=i, aura=auras[i], material=mats[i], idx=i + 1) for i in range(6)]
    # fallback: жёстко делаем уникально
    a = [x[1] for x in _AURA_POOL]
    m = [x[1] for x in _MATERIAL_POOL]
    random.shuffle(a)
    random.shuffle(m)
    return [Seal(sid=i, aura=a[i % len(a)], material=m[i % len(m)], idx=i + 1) for i in range(6)]


def _seal_label(s: Seal) -> str:
    aura_emoji = next((e for e, n in _AURA_POOL if n == s.aura), "🕯️")
    mat_emoji = next((e for e, n in _MATERIAL_POOL if n == s.material), "⛓️")
    # коротко и читаемо: "1) ❄️ ХОЛОД • 🦴 КОСТЬ"
    return f"{s.idx}) {aura_emoji} {s.aura} • {mat_emoji} {s.material}"


def _choose_puzzle(seals: list[Seal], correct: Seal, stage: int) -> tuple[str, set[int]]:
    """
    stage: 3 => 1-я попытка (нужно 4 кандидата)
           2 => 2-я попытка (нужно 2 кандидата)
           1 => 3-я попытка (нужно 1 кандидат, без рандома)
    Возвращает (текст подсказки, множество sid-кандидатов).
    """
    want = 4 if stage == 3 else 2 if stage == 2 else 1

    # заранее подготовим списки
    odds = {s.sid for s in seals if s.parity == "odd"}
    evens = {s.sid for s in seals if s.parity == "even"}
    by_aura: dict[str, set[int]] = {}
    by_mat: dict[str, set[int]] = {}
    for s in seals:
        by_aura.setdefault(s.aura, set()).add(s.sid)
        by_mat.setdefault(s.material, set()).add(s.sid)

    def cand_from(rule: set[int], clue: str) -> Optional[tuple[str, set[int]]]:
        if correct.sid not in rule:
            return None
        if len(rule) != want:
            return None
        return clue, set(rule)

    # Третья попытка: детерминированно — уникальная пара aura+material
    if want == 1:
        clue = (
            f"Третья попытка. Шёпот больше не прячется.\n"
            f"Сердце узла — печать с аурой **{correct.aura}** и материей **{correct.material}**."
        )
        return clue, {correct.sid}

    # Список шаблонов подсказок (для 1-й и 2-й попыток)
    templates: list[tuple[str, set[int]]] = []

    # 1) Паритет + аура: (odd OR aura==X)
    for aura, sids in by_aura.items():
        rule = odds | sids
        templates.append((
            f"Шёпот даёт знак:\n"
            f"**Либо** печать стоит на *нечётной* позиции, **либо** её аура — **{aura}**.",
            rule
        ))
        rule2 = evens | sids
        templates.append((
            f"Шёпот даёт знак:\n"
            f"**Либо** печать стоит на *чётной* позиции, **либо** её аура — **{aura}**.",
            rule2
        ))

    # 2) Материя исключение: (material != X)
    for mat, sids in by_mat.items():
        rule = {s.sid for s in seals if s.material != mat}
        templates.append((
            f"Шёпот скребёт по зубам:\n"
            f"Сердце узла **не** спрятано в материи **{mat}**.",
            rule
        ))

    # 3) Аура принадлежит множеству из двух
    aura_keys = list(by_aura.keys())
    random.shuffle(aura_keys)
    for i in range(min(8, len(aura_keys))):
        for j in range(i + 1, min(8, len(aura_keys))):
            a1, a2 = aura_keys[i], aura_keys[j]
            rule = by_aura[a1] | by_aura[a2]
            templates.append((
                f"Шёпот путает следы:\n"
                f"Аура сердца узла — **{a1}** или **{a2}**.",
                rule
            ))

    # 4) Материя принадлежит множеству из двух
    mat_keys = list(by_mat.keys())
    random.shuffle(mat_keys)
    for i in range(min(6, len(mat_keys))):
        for j in range(i + 1, min(6, len(mat_keys))):
            m1, m2 = mat_keys[i], mat_keys[j]
            rule = by_mat[m1] | by_mat[m2]
            templates.append((
                f"Шёпот глухо звенит:\n"
                f"Материя узла — **{m1}** или **{m2}**.",
                rule
            ))

    random.shuffle(templates)

    # пытаемся подобрать шаблон, который даст ровно want кандидатов
    for clue, rule in templates:
        res = cand_from(rule, clue)
        if res:
            return res

    # если не нашли — делаем гарантированный набор кандидатов вокруг правильной
    # stage 3 => 4 кандидата; stage 2 => 2 кандидата
    others = [s.sid for s in seals if s.sid != correct.sid]
    random.shuffle(others)
    picked = {correct.sid} | set(others[: want - 1])
    clue = (
        "Шёпот срывается на смех:\n"
        "Сердце узла прячется **среди этих печатей** — выбери верную."
    )
    return clue, picked


class RemovePickView(GuardedView):
    """
    Новая мини-игра: 1-я попытка => 4 кандидата (нужна удача)
                     2-я попытка => 2 кандидата (удачи меньше)
                     3-я попытка => 1 кандидат (без рандома)
    """
    def __init__(self, cog: "TendrilCog", victim_id: int, correct_sid: int, seals: list[Seal], candidates: set[int]):
        super().__init__(timeout=120)
        self.cog = cog
        self.victim_id = victim_id
        self.correct_sid = correct_sid
        self.seals = seals
        self.candidates = candidates

        for s in seals:
            self.add_item(RemovePickButton(sid=s.sid, label=_seal_label(s)))

    async def handle_pick(self, interaction: discord.Interaction, picked_sid: int) -> None:
        if interaction.user.id != self.victim_id:
            await safe_send(interaction, "Это не твой обряд.", ephemeral=True)
            return
        await self.cog.ui_remove_pick(interaction, picked_sid, self.correct_sid)


class RemovePickButton(discord.ui.Button):
    def __init__(self, sid: int, label: str):
        super().__init__(label=label[:80], style=discord.ButtonStyle.primary)
        self.sid = sid

    async def callback(self, interaction: discord.Interaction) -> None:
        view: RemovePickView = self.view  # type: ignore
        await view.handle_pick(interaction, self.sid)


class TendrilCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        root_cfg = getattr(self.bot, "cfg", {}) or {}
        tcfg = root_cfg.get("tendril", {}) or {}
        self.cfg = TendrilCfg(
            attack_cost=_digits(tcfg.get("attack_cost", TendrilCfg.attack_cost), TendrilCfg.attack_cost),
            shield_cost=_digits(tcfg.get("shield_cost", TendrilCfg.shield_cost), TendrilCfg.shield_cost),
        )
        self._loop.start()

    def _repo(self):
        return getattr(self.bot, "repo", None)  # type: ignore

    def _guild_id(self) -> int:
        cfg = getattr(self.bot, "cfg", {}) or {}
        return _digits(cfg.get("guild_id", 0), 0)

    def _channels(self) -> dict[str, int]:
        cfg = getattr(self.bot, "cfg", {}) or {}
        return {k: int(v) for k, v in (cfg.get("channels", {}) or {}).items()}

    def _economy_per_day(self, level: int) -> int:
        cfg = getattr(self.bot, "cfg", {}) or {}
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

    def _announce_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        chs = self._channels()
        ch_id = chs.get("general_chat") or chs.get("coin_results") or chs.get("tendril")
        if not ch_id:
            return None
        ch = guild.get_channel(int(ch_id))
        return ch if isinstance(ch, discord.TextChannel) else None

    def _tendril_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        ch_id = self._channels().get("tendril")
        if not ch_id:
            return None
        ch = guild.get_channel(int(ch_id))
        return ch if isinstance(ch, discord.TextChannel) else None
    async def _db_get(self, victim_id: int) -> Optional[dict[str, Any]]:
        repo = self._repo()
        if not repo or not repo.conn:
            return None

        # кешируем список колонок (один раз за запуск бота)
        cols: set[str] | None = getattr(self, "_tendrils_cols_cache", None)
        if cols is None:
            curc = await repo.conn.execute("PRAGMA table_info(tendrils)")
            rowsc = await curc.fetchall()
            await curc.close()
            cols = {str(r[1]) for r in rowsc}
            setattr(self, "_tendrils_cols_cache", cols)

        ordered = [
            "victim_id",
            "source",
            "attacker_id",
            "started_ts",
            "expires_ts",
            "last_tick_ts",
            "last_announce_ts",
            "stolen_total",
            "attempts_left",
            # legacy:
            "last_steal_ts",
            "last_notify_ts",
            "active",
        ]
        use_cols = [c for c in ordered if c in cols]
        col_list = ", ".join(use_cols)

        cur = await repo.conn.execute(f"SELECT {col_list} FROM tendrils WHERE victim_id=?", (int(victim_id),))
        r = await cur.fetchone()
        await cur.close()
        if not r:
            return None

        row = {use_cols[i]: r[i] for i in range(len(use_cols))}
        now = _now()

        started_ts = int(row.get("started_ts") or 0)
        if started_ts <= 0:
            started_ts = now

        expires_ts = row.get("expires_ts", None)
        if expires_ts is None or int(expires_ts or 0) <= 0:
            expires_ts = started_ts + int(self.cfg.duration_seconds)

        last_tick_ts = row.get("last_tick_ts", None)
        if last_tick_ts is None or int(last_tick_ts or 0) <= 0:
            last_tick_ts = row.get("last_steal_ts", None)
        if last_tick_ts is None or int(last_tick_ts or 0) <= 0:
            last_tick_ts = started_ts

        last_announce_ts = row.get("last_announce_ts", None)
        if last_announce_ts is None or int(last_announce_ts or 0) <= 0:
            last_announce_ts = row.get("last_notify_ts", None)
        if last_announce_ts is None or int(last_announce_ts or 0) <= 0:
            last_announce_ts = started_ts

        stolen_total = int(row.get("stolen_total") or 0)
        attempts_left = int(row.get("attempts_left") or int(self.cfg.attempts))

        return {
            "victim_id": int(row.get("victim_id") or victim_id),
            "source": str(row.get("source") or "attack"),
            "attacker_id": int(row.get("attacker_id") or 0),
            "started_ts": int(started_ts),
            "expires_ts": int(expires_ts),
            "last_tick_ts": int(last_tick_ts),
            "last_announce_ts": int(last_announce_ts),
            "stolen_total": int(stolen_total),
            "attempts_left": int(attempts_left),
        }




    async def _db_insert(self, victim_id: int, source: str, attacker_id: int) -> None:
        """Вставка/обновление записи отростка.

        Важно: в ранней схеме таблицы tendrils были колонки last_steal_ts/last_notify_ts/active
        с NOT NULL. В новой схеме мы используем expires_ts/last_tick_ts/last_announce_ts и т.д.

        Чтобы не ломать серверные данные и поддержать обе схемы, собираем INSERT динамически
        по реальным колонкам таблицы (PRAGMA table_info).
        """
        repo = self._repo()
        assert repo and repo.conn

        # кешируем список колонок (один раз за запуск бота)
        cols: set[str] | None = getattr(self, "_tendrils_cols_cache", None)
        if cols is None:
            cur = await repo.conn.execute("PRAGMA table_info(tendrils)")
            rows = await cur.fetchall()
            await cur.close()
            cols = {str(r[1]) for r in rows}  # r[1] = column name
            setattr(self, "_tendrils_cols_cache", cols)

        now = _now()

        # значения по умолчанию для обеих схем
        values: dict[str, Any] = {
            "victim_id": int(victim_id),
            "source": str(source),
            "attacker_id": int(attacker_id),
            "started_ts": now,
            "expires_ts": now + int(self.cfg.duration_seconds),
            "last_tick_ts": now,
            "last_announce_ts": now,
            "stolen_total": 0,
            "attempts_left": int(self.cfg.attempts),

            # legacy schema (могут быть NOT NULL без default)
            "last_steal_ts": now,
            "last_notify_ts": now,
            "active": 1,
        }

        ordered = [
            "victim_id",
            "source",
            "attacker_id",
            "started_ts",
            "expires_ts",
            "last_tick_ts",
            "last_announce_ts",
            "stolen_total",
            "attempts_left",
            "last_steal_ts",
            "last_notify_ts",
            "active",
        ]
        use_cols = [c for c in ordered if c in cols]
        placeholders = ",".join(["?"] * len(use_cols))
        col_list = ", ".join(use_cols)

        await repo.conn.execute(
            f"INSERT OR REPLACE INTO tendrils({col_list}) VALUES ({placeholders})",
            tuple(values[c] for c in use_cols),
        )
        await repo.conn.commit()

    async def _db_update(self, victim_id: int, **fields: Any) -> None:
        repo = self._repo()
        if not repo or not repo.conn or not fields:
            return
        keys = list(fields.keys())
        vals = [fields[k] for k in keys]
        set_clause = ", ".join(f"{k}=?" for k in keys)
        await repo.conn.execute(f"UPDATE tendrils SET {set_clause} WHERE victim_id=?", (*vals, int(victim_id)))
        await repo.conn.commit()

    async def _db_delete(self, victim_id: int) -> None:
        repo = self._repo()
        if not repo or not repo.conn:
            return
        await repo.conn.execute("DELETE FROM tendrils WHERE victim_id=?", (int(victim_id),))
        await repo.conn.commit()

    async def apply_curse(self, member: discord.Member) -> tuple[bool, str]:
        return await self._apply(member, source="curse", attacker=None)

    async def apply_attack(self, victim: discord.Member, attacker: discord.Member) -> tuple[bool, str]:
        return await self._apply(victim, source="attack", attacker=attacker)

    async def _apply(self, victim: discord.Member, source: str, attacker: Optional[discord.Member]) -> tuple[bool, str]:
        repo = self._repo()
        if not repo:
            return False, "Repo не готов."

        existing = await self._db_get(victim.id)
        if existing:
            return False, "На цели уже висит отросток."

        # иммунитет 24ч только если отросток наслали игроком (чтобы не блокировать проклятую монетку)
        if source == "attack":
            await repo.cd_set(victim.id, "tendril:immunity", int(self.cfg.immunity_seconds))

        attacker_id = attacker.id if attacker else 0
        u_v = await repo.get_user(victim.id)
        v_runes = int(u_v.get("runes", 0))

        # Для проклятия от монетки: если у цели уже 0 рун — отросток не стартует.
        if source == "curse" and v_runes <= 0:
            return False, "У цели нет рун: проклятие не за что цеплять."

        first_bite = ATTACK_FIRST_BITE if source == "attack" else CURSE_FIRST_BITE
        first_take = 0
        if v_runes > 0 and first_bite > 0:
            first_take = min(int(first_bite), int(v_runes))
            if first_take > 0:
                await repo.set_user_fields(victim.id, runes=max(0, int(v_runes) - int(first_take)))
                if source == "attack" and attacker_id:
                    u_a = await repo.get_user(attacker_id)
                    await repo.set_user_fields(attacker_id, runes=int(u_a.get("runes", 0)) + int(first_take))

        # Для проклятия: если после первого укуса руны 0 — отросток сразу отпадает.
        v_left_after_bite = max(0, int(v_runes) - int(first_take))
        if source == "curse" and v_left_after_bite <= 0:
            ch_now = self._announce_channel(victim.guild)
            if ch_now:
                await ch_now.send(f"🌫️ {victim.mention} обнулён первым укусом проклятия. Отросток отпал сразу.")
            return True, "CURSE_DRIED_ON_BITE"

        await self._db_insert(victim.id, source=source, attacker_id=attacker_id)
        if first_take > 0:
            await self._db_update(victim.id, stolen_total=int(first_take))

        # публичное объявление
        ch = self._announce_channel(victim.guild)
        if ch:
            if source == "attack":
                msg = random.choice(LORE_INFECT_ATTACK).format(
                    v=victim.mention,
                    a=(attacker.mention if attacker else "неизвестный"),
                )
            else:
                msg = random.choice(LORE_INFECT_CURSE).format(v=victim.mention)
            if first_take > 0:
                msg += f"\nПервый укус: **-{first_take}** рун."
            await ch.send(msg)

        return True, "OK"

    def _calc_tick_runes(self, victim_level: int) -> int:
        per_day = self._economy_per_day(victim_level)
        raw = round(per_day * float(self.cfg.per_day_pct))
        return max(int(self.cfg.per_tick_min), min(int(self.cfg.per_tick_max), int(raw) if raw > 0 else 1))

    # ---------------- loops ----------------

    @tasks.loop(seconds=60)
    async def _loop(self) -> None:
        repo = self._repo()
        if not repo or not repo.conn:
            return

        gid = self._guild_id()
        guilds = [g for g in self.bot.guilds if (not gid) or g.id == gid]
        if not guilds:
            return

        guild = guilds[0]
        ch_announce = self._announce_channel(guild)

        now = _now()

        cur = await repo.conn.execute(
            "SELECT victim_id, source, attacker_id, started_ts, expires_ts, last_tick_ts, last_announce_ts, stolen_total, attempts_left FROM tendrils"
        )
        rows = await cur.fetchall()
        await cur.close()

        for r in rows:
            victim_id = int(r[0])
            source = str(r[1])
            attacker_id = int(r[2])
            expires_ts = int(r[4])
            last_tick_ts = int(r[5])
            last_announce_ts = int(r[6])
            stolen_total = int(r[7])
            attempts_left = int(r[8])

            # истёк — снимаем
            if now >= expires_ts:
                await self._db_delete(victim_id)
                if ch_announce:
                    m = guild.get_member(victim_id)
                    if m:
                        await ch_announce.send(random.choice(LORE_EXPIRE).format(v=m.mention))
                continue

            # тики (каждые 10 минут)
            due = (now - last_tick_ts) // int(self.cfg.tick_seconds)
            if due > 0:
                remove_curse_now = False
                # берем жертву
                victim = guild.get_member(victim_id)
                if victim:
                    u_v = await repo.get_user(victim_id)
                    v_level = int(u_v.get("level", 1))
                    tick_take = self._calc_tick_runes(v_level)

                    u_runes = int(u_v.get("runes", 0))
                    total_take = 0
                    victim_take = 0

                    # считаем по-тиково, чтобы корректно отрабатывать лимиты/истощение
                    for _ in range(int(due)):
                        if source == "curse":
                            # Проклятие от монетки: максимум 60 за 6 часов и автоснятие при 0 рунах.
                            if u_runes <= 0:
                                remove_curse_now = True
                                break
                            remain_cap = max(0, int(CURSE_MAX_TOTAL) - int(stolen_total + total_take))
                            if remain_cap <= 0:
                                break
                            take = min(int(tick_take), int(u_runes), int(remain_cap))
                            if take <= 0:
                                break
                            u_runes -= int(take)
                            victim_take += int(take)
                            total_take += int(take)
                            if u_runes <= 0:
                                remove_curse_now = True
                                break
                        else:
                            # Ритуал игрока: если руны жертвы кончились, тики продолжают
                            # начислять атакеру добычу из Пустоты (жертва не уходит в минус).
                            if u_runes > 0:
                                take = min(int(tick_take), int(u_runes))
                                if take > 0:
                                    u_runes -= int(take)
                                    victim_take += int(take)
                                    total_take += int(take)
                            else:
                                total_take += int(tick_take)

                    if victim_take > 0:
                        # списать с жертвы (жертва никогда не уходит в минус)
                        await repo.set_user_fields(victim_id, runes=max(0, int(u_runes)))

                    # если attack — отдать атакеру всю добычу (включая Пустоту при нуле у жертвы)
                    if total_take > 0 and source == "attack" and attacker_id:
                        u_a = await repo.get_user(attacker_id)
                        await repo.set_user_fields(attacker_id, runes=int(u_a.get("runes", 0)) + int(total_take))

                    if total_take > 0:
                        stolen_total += int(total_take)

                # обновим last_tick_ts (сдвигаем на количество тиков)
                last_tick_ts = last_tick_ts + int(due) * int(self.cfg.tick_seconds)
                if source == "curse" and remove_curse_now:
                    await self._db_delete(victim_id)
                    if ch_announce:
                        m = guild.get_member(victim_id)
                        if m:
                            await ch_announce.send(f"🌫️ У {m.mention} руны закончились. Проклятый отросток распался.")
                    continue
                else:
                    await self._db_update(
                        victim_id,
                        last_tick_ts=int(last_tick_ts),
                        stolen_total=int(stolen_total),
                        attempts_left=int(attempts_left),
                    )

            # объявления каждые 30 минут
            if ch_announce and (now - last_announce_ts) >= int(self.cfg.announce_seconds):
                m = guild.get_member(victim_id)
                if m:
                    msg = random.choice(LORE_ANNOUNCE).format(v=m.mention, n=stolen_total)
                    if source == "attack" and attacker_id:
                        a = guild.get_member(attacker_id)
                        if a:
                            msg += f" (добыча уходит к {a.mention})"
                    await ch_announce.send(msg)

                last_announce_ts = now
                await self._db_update(victim_id, last_announce_ts=int(last_announce_ts))

    @_loop.before_loop
    async def _before_loop(self) -> None:
        await self.bot.wait_until_ready()

    # ---------------- UI (ephemeral) ----------------

    async def ui_status(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        repo = self._repo()
        if not repo:
            return

        if not interaction.guild:
            await safe_send(interaction, "Только на сервере.", ephemeral=True)
            return

        u = await repo.get_user(interaction.user.id)
        lvl = int(u.get("level", 1))
        runes = int(u.get("runes", 0))

        t = await self._db_get(interaction.user.id)
        now = _now()

        shield_until = await repo.cd_get(interaction.user.id, "tendril:shield")
        immune_until = await repo.cd_get(interaction.user.id, "tendril:immunity")
        cast_until = await repo.cd_get(interaction.user.id, "tendril:cast")

        e = discord.Embed(title="🌿 Статус отростка", color=0x2B2D31)
        e.add_field(name="Ты", value=f"ур.{lvl} | руны: **{runes}**", inline=False)

        if t:
            left = t["expires_ts"] - now
            src = "Проклятие монеты" if t["source"] == "curse" else "Ритуал"
            e.add_field(name="Отросток", value=f"**АКТИВЕН** ({src})", inline=True)
            e.add_field(name="Украл/сожрал", value=f"**{t['stolen_total']}** рун", inline=True)
            e.add_field(name="Осталось жить", value=f"**{_fmt_left(left)}**", inline=True)
            e.add_field(name="Попытки снять", value=f"**{t['attempts_left']}**", inline=True)
            if t["source"] == "attack" and t["attacker_id"]:
                a = interaction.guild.get_member(int(t["attacker_id"]))
                e.add_field(name="Кому уходит добыча", value=(a.mention if a else f"`{t['attacker_id']}`"), inline=True)
        else:
            e.add_field(name="Отросток", value="нет активного", inline=False)

        if shield_until > now:
            e.add_field(name="Щит", value=f"активен ещё **{_fmt_left(shield_until - now)}**", inline=False)
        else:
            e.add_field(name="Щит", value="нет", inline=False)

        if immune_until > now:
            e.add_field(name="Иммунитет (24ч)", value=f"ещё **{_fmt_left(immune_until - now)}**", inline=True)
        if cast_until > now:
            e.add_field(name="КД ритуала", value=f"ещё **{_fmt_left(cast_until - now)}**", inline=True)

        await safe_send(interaction, embed=e, ephemeral=True)

    async def ui_shield_status(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        repo = self._repo()
        if not repo:
            return
        now = _now()
        until = await repo.cd_get(interaction.user.id, "tendril:shield")
        if until > now:
            await safe_send(interaction, f"🛡️ Щит активен ещё **{_fmt_left(until - now)}**.", ephemeral=True)
        else:
            await safe_send(interaction, "🛡️ Щита нет.", ephemeral=True)

    async def ui_buy_shield(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        repo = self._repo()
        if not repo:
            return
        if not interaction.guild:
            await safe_send(interaction, "Только на сервере.", ephemeral=True)
            return

        u = await repo.get_user(interaction.user.id)
        lvl = int(u.get("level", 1))
        if lvl < self.cfg.min_target_level:
            await safe_send(interaction, f"Щит доступен с ур.{self.cfg.min_target_level}+.", ephemeral=True)
            return

        now = _now()
        until = await repo.cd_get(interaction.user.id, "tendril:shield")
        if until > now:
            await safe_send(interaction, f"Щит уже активен ещё **{_fmt_left(until - now)}**.", ephemeral=True)
            return

        ok = await repo.spend_runes(interaction.user.id, self.cfg.shield_cost)
        if not ok:
            await safe_send(interaction, f"Не хватает рун. Нужно **{self.cfg.shield_cost}**.", ephemeral=True)
            return

        # если на тебе был отросток — сорвать
        t = await self._db_get(interaction.user.id)
        if t:
            await self._db_delete(interaction.user.id)
            ch = self._announce_channel(interaction.guild)
            if ch:
                await ch.send(random.choice(LORE_RIP_SHIELD).format(v=interaction.user.mention))

        await repo.cd_set(interaction.user.id, "tendril:shield", self.cfg.shield_seconds)
        await safe_send(interaction, 
            f"🛡️ Щит куплен: **72 часа** защиты.\nЦена: **{self.cfg.shield_cost}** рун.",
            ephemeral=True,
        )

    async def ui_remove_start(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        repo = self._repo()
        if not repo:
            return
        if not interaction.guild:
            await safe_send(interaction, "Только на сервере.", ephemeral=True)
            return

        t = await self._db_get(interaction.user.id)
        if not t:
            await safe_send(interaction, "На тебе нет отростка.", ephemeral=True)
            return

        now = _now()
        cd_until = await repo.cd_get(interaction.user.id, "tendril:remove_cd")
        if cd_until > now:
            await safe_send(interaction, 
                f"Подожди **{_fmt_left(cd_until - now)}** перед следующей попыткой.",
                ephemeral=True,
            )
            return

        attempts_left = int(t.get("attempts_left", 0))
        if attempts_left <= 0:
            left = int(t["expires_ts"]) - now
            await safe_send(interaction, 
                f"Попытки кончились. Отросток иссохнет через **{_fmt_left(left)}** (или купи щит — он срывает сразу).",
                ephemeral=True,
            )
            return

        # --- новая мини-игра: "Разрезать узел" ---
        # 1-я попытка: 4 кандидата (нужна удача)
        # 2-я попытка: 2 кандидата
        # 3-я попытка: 1 кандидат (без рандома)
        stage = attempts_left  # 3/2/1
        seals = _mk_seals()
        correct = random.choice(seals)
        clue, candidates = _choose_puzzle(seals, correct, stage=stage)

        title = "🌿 Снятие отростка: Узел"
        if stage == 3:
            flavor = "Первая попытка. Шёпот нарочно неполный — придётся положиться на удачу."
        elif stage == 2:
            flavor = "Вторая попытка. Нить слабее — удачи нужно меньше."
        else:
            flavor = "Третья попытка. Шёпот раскрывает истину — здесь решает только ты."

        e = discord.Embed(
            title=title,
            description=(
                f"{flavor}\n\n"
                f"{clue}\n\n"
                f"Кандидатов по подсказке: **{len(candidates)}**\n"
                f"Попыток осталось: **{attempts_left}**\n"
                f"Выбери печать, чтобы разрезать узел."
            ),
            color=0x2B2D31,
        )

        await safe_send(interaction, 
            embed=e,
            view=RemovePickView(self, interaction.user.id, correct.sid, seals, candidates),
            ephemeral=True,
        )

    async def ui_remove_pick(self, interaction: discord.Interaction, picked_sid: int, correct_sid: int) -> None:
        await safe_defer_update(interaction)
        repo = self._repo()
        if not repo:
            return
        if not interaction.guild:
            return

        now = _now()
        t = await self._db_get(interaction.user.id)
        if not t:
            await safe_edit_message(interaction, content="Отросток уже исчез.", embed=None, view=None)
            return

        if int(picked_sid) == int(correct_sid):
            await self._db_delete(interaction.user.id)
            await safe_edit_message(interaction, content="✅ Узел разрезан. Отросток снят. Шёпот стих.", embed=None, view=None)
            return

        # ошибка — минус попытка
        left = max(0, int(t.get("attempts_left", 0)) - 1)
        await self._db_update(interaction.user.id, attempts_left=int(left))
        await repo.cd_set(interaction.user.id, "tendril:remove_cd", int(self.cfg.remove_attempt_cd_seconds))

        if left <= 0:
            await safe_edit_message(interaction, 
                content="❌ Узел не поддался. Попытки кончились (ждёшь до конца 6 часов или покупаешь щит — он срывает сразу).",
                embed=None,
                view=None,
            )
        else:
            hint = "Нажми «Снять отросток» ещё раз — шёпот станет яснее."
            if left == 1:
                hint = "Нажми «Снять отросток» ещё раз — третья попытка без рандома."
            await safe_edit_message(interaction, 
                content=f"❌ Неверная печать. Попыток осталось: **{left}**.\n{hint}",
                embed=None,
                view=None,
            )

    async def ui_cast_start(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        repo = self._repo()
        if not repo:
            return
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Только на сервере.", ephemeral=True)
            return

        caster: discord.Member = interaction.user
        u = await repo.get_user(caster.id)
        lvl = int(u.get("level", 1))
        if lvl < self.cfg.cast_unlock:
            await safe_send(interaction, f"Ритуал доступен с ур.{self.cfg.cast_unlock}+.", ephemeral=True)
            return

        now = _now()

        # кд кастера
        until_cast = await repo.cd_get(caster.id, "tendril:cast")
        if until_cast > now:
            await safe_send(interaction, f"КД ритуала ещё **{_fmt_left(until_cast - now)}**.", ephemeral=True)
            return

        # нужно ли достаточно рун у кастера
        if int(u.get("runes", 0)) < int(self.cfg.attack_cost):
            await safe_send(interaction, f"Не хватает рун. Нужно **{self.cfg.attack_cost}**.", ephemeral=True)
            return

        # собираем кандидатов: берем топ по рунам (до 40) и фильтруем по присутствию в guild
        cur = await repo.conn.execute("SELECT user_id, runes, level FROM users ORDER BY runes DESC LIMIT 50")
        rows = await cur.fetchall()
        await cur.close()

        lines: list[str] = []
        opts: list[discord.SelectOption] = []

        shown = 0
        for uid, runes, lvl_t in rows:
            uid = int(uid)
            if uid == caster.id:
                continue
            m = interaction.guild.get_member(uid)
            if not m or m.bot:
                continue

            shown += 1
            if shown > 20:
                break

            # причины запрета
            reasons: list[str] = []

            if int(lvl_t) < self.cfg.min_target_level:
                reasons.append(f"ур.{self.cfg.min_target_level}-")
            if int(runes) < int(self.cfg.min_target_runes):
                reasons.append("мало рун")
            if not _is_online_or_in_voice(m):
                reasons.append("оффлайн+не в войсе")

            # активный щит у цели
            sh = await repo.cd_get(uid, "tendril:shield")
            if sh > now:
                reasons.append("щит")

            # иммунитет 24ч
            imm = await repo.cd_get(uid, "tendril:immunity")
            if imm > now:
                reasons.append("иммунитет 24ч")

            # уже есть отросток
            if await self._db_get(uid):
                reasons.append("уже заражён")

            ok = (len(reasons) == 0)

            mark = "✅" if ok else "❌"
            reason_txt = "" if ok else (" — " + ", ".join(reasons))
            lines.append(f"{mark} {m.display_name}: **{int(runes)}** рун{reason_txt}")

            if ok:
                opts.append(discord.SelectOption(label=f"{m.display_name} ({int(runes)} рун)", value=str(uid)))

        if not lines:
            await safe_send(interaction, "Не нашёл целей в таблице (нет данных в БД).", ephemeral=True)
            return

        e = discord.Embed(
            title="🌿 Ритуал: наслать отросток",
            description=(
                f"Цена ритуала: **{self.cfg.attack_cost}** рун.\n"
                f"Цель должна быть **онлайн** или **в голосовом**.\n"
                f"Нельзя: цель < ур.{self.cfg.min_target_level}, щит, иммунитет 24ч.\n\n"
                f"**Таблица (топ по рунам):**\n" + "\n".join(lines)
            ),
            color=0x2B2D31,
        )

        if not opts:
            await safe_send(interaction, embed=e, ephemeral=True)
            return

        await safe_send(interaction, embed=e, view=CastView(self, caster.id, opts, note=""), ephemeral=True)

    async def ui_cast_finish(self, interaction: discord.Interaction, target_id: int) -> None:
        await safe_defer_update(interaction)
        repo = self._repo()
        if not repo:
            return
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        caster: discord.Member = interaction.user
        now = _now()

        # повторная валидация
        u_c = await repo.get_user(caster.id)
        lvl = int(u_c.get("level", 1))
        if lvl < self.cfg.cast_unlock:
            await safe_send(interaction, "Твой уровень недостаточен.", ephemeral=True)
            return

        until_cast = await repo.cd_get(caster.id, "tendril:cast")
        if until_cast > now:
            await safe_send(interaction, f"КД ритуала ещё **{_fmt_left(until_cast - now)}**.", ephemeral=True)
            return

        target = interaction.guild.get_member(int(target_id))
        if not target or target.bot:
            await safe_send(interaction, "Цель не найдена.", ephemeral=True)
            return

        u_t = await repo.get_user(target.id)
        if int(u_t.get("level", 1)) < self.cfg.min_target_level:
            await safe_send(interaction, "Не соблюдены условия ритуала: цель слишком низкого уровня.", ephemeral=True)
            return
        if int(u_t.get("runes", 0)) < int(self.cfg.min_target_runes):
            await safe_send(interaction, "Не соблюдены условия ритуала: у цели слишком мало рун.", ephemeral=True)
            return
        if not _is_online_or_in_voice(target):
            await safe_send(interaction, "Не соблюдены условия ритуала: цель оффлайн и не в войсе.", ephemeral=True)
            return

        sh = await repo.cd_get(target.id, "tendril:shield")
        if sh > now:
            await safe_send(interaction, "Не соблюдены условия ритуала: у цели активен щит.", ephemeral=True)
            return

        imm = await repo.cd_get(target.id, "tendril:immunity")
        if imm > now:
            await safe_send(interaction, "Не соблюдены условия ритуала: у цели иммунитет 24ч.", ephemeral=True)
            return

        if await self._db_get(target.id):
            await safe_send(interaction, "Не соблюдены условия ритуала: цель уже заражена.", ephemeral=True)
            return

        # списываем цену
        ok = await repo.spend_runes(caster.id, self.cfg.attack_cost)
        if not ok:
            await safe_send(interaction, f"Не хватает рун. Нужно **{self.cfg.attack_cost}**.", ephemeral=True)
            return

        # ставим кд кастера
        await repo.cd_set(caster.id, "tendril:cast", self.cfg.cast_cd_seconds)

        # заражаем
        ok2, why = await self.apply_attack(target, attacker=caster)
        if not ok2:
            # если не удалось — вернем руны и откатим кд (просто ставим 1 сек, чтобы не зависло)
            u_back = await repo.get_user(caster.id)
            await repo.set_user_fields(caster.id, runes=int(u_back.get("runes", 0)) + int(self.cfg.attack_cost))
            await repo.cd_set(caster.id, "tendril:cast", 1)
            await safe_send(interaction, f"Не получилось: {why}", ephemeral=True)
            return

        await safe_edit_message(interaction, content=f"✅ Ритуал завершён. Цель: {target.mention}", embed=None, view=None)

    # ---------------- admin panel ----------------

    
    @commands.command(name="tendril_self")
    async def tendril_self(self, ctx: commands.Context, mode: str = "curse") -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        if not ctx.guild or not isinstance(ctx.author, discord.Member):
            return

        mode = (mode or "curse").strip().lower()
        if await self._db_get(ctx.author.id):
            await ctx.reply("У тебя уже есть активный отросток. Сначала сними его кнопкой **Снять отросток** или командой `!tendril_clear`.")
            return

        if mode in ("attack", "ritual"):
            ok, why = await self.apply_attack(ctx.author, attacker=ctx.author)
            src = "attack"
        else:
            ok, why = await self.apply_curse(ctx.author)
            src = "curse"

        if ok:
            await ctx.reply(f"✅ Наложил отросток на тебя (source={src}).", mention_author=False)
        else:
            await ctx.reply(f"❌ Не получилось: {why}", mention_author=False)

    @commands.command(name="tendril_unshield")
    async def tendril_unshield(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self._repo()
        # ставим таймер в прошлое — значит щита больше нет
        await repo.cd_set(ctx.author.id, "tendril:shield", -1)
        await ctx.reply("✅ Щит снят (таймер обнулён).", mention_author=False)

    @commands.command(name="tendril_unimmune")
    async def tendril_unimmune(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        repo = self._repo()
        await repo.cd_set(ctx.author.id, "tendril:immunity", -1)
        await ctx.reply("✅ Иммунитет снят (таймер обнулён).", mention_author=False)

    @commands.command(name="tendril_clear")
    async def tendril_clear(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return
        await self._db_delete(ctx.author.id)
        await ctx.reply("✅ Активный отросток удалён из базы (только для тестов).", mention_author=False)



    @commands.command(name="post_tendril_panel")
    async def post_tendril_panel(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return

        if not ctx.guild:
            return

        ch = self._tendril_channel(ctx.guild)
        if not ch:
            await ctx.reply("❌ Канал tendril не найден в config.json (channels.tendril).")
            return

        e = discord.Embed(
            title="🌿 Отросток",
            description=(
                "Здесь совершают ритуалы и срывают паразитов.\n\n"
                f"• **Наслать отросток** (ур.15+, цена {self.cfg.attack_cost}): цель онлайн или в войсе, без щита, без иммунитета 24ч.\n"
                "• **Проклятая монетка** (ур.10+) может дать отросток бесплатно… но за поражение.\n"
                f"• **Щит** (72ч, цена {self.cfg.shield_cost}) — срывает отросток сразу и защищает от ритуалов."
            ),
            color=0x2B2D31,
        )
        m = await ch.send(
            embed=e,
            view=TendrilPanelView(attack_cost=self.cfg.attack_cost, shield_cost=self.cfg.shield_cost),
        )
        try:
            await m.pin()
        except Exception:
            pass
        await ctx.reply("✅ Панель отростка опубликована и закреплена.", mention_author=False)


def get_persistent_views(bot: commands.Bot):
    # если ты используешь cogs/persistent.py, он подхватит это
    cfg = getattr(bot, "cfg", {}) or {}
    tcfg = cfg.get("tendril", {}) or {}
    attack_cost = int(tcfg.get("attack_cost", TendrilCfg.attack_cost))
    shield_cost = int(tcfg.get("shield_cost", TendrilCfg.shield_cost))
    return [TendrilPanelView(attack_cost=attack_cost, shield_cost=shield_cost)]


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(TendrilCog(bot))
