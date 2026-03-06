# cogs/story.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import discord
from discord.ext import commands

from ._interactions import GuardedView, safe_defer_ephemeral, safe_defer_update, safe_edit_message, safe_send

log = logging.getLogger("void")

ROOT = Path(__file__).resolve().parent.parent


def _cfg(bot: commands.Bot) -> Dict[str, Any]:
    return getattr(bot, 'cfg', None) or getattr(bot, 'config', {})  # support both


def _repo(bot: commands.Bot):
    return bot.repo  # type: ignore


# ---------------- Attempt state (chapter 1) ----------------

@dataclass
class Ch1Attempt:
    vow: int = 0
    trust_delta: int = 0
    name_delta: int = 0
    evidence_delta: int = 0
    mask: int = 0
    gro_seed: int = 0
    rher_seed: int = 0
    vow_lock: str = ""
    evidence: int = 0
    secret_unlocked: int = 0  # 0/1

    @classmethod
    def from_json(cls, s: str) -> "Ch1Attempt":
        if not s:
            return cls()
        try:
            d = json.loads(s)
            return cls(**d)
        except Exception:
            return cls()

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)


async def _load_attempt(bot: commands.Bot, user_id: int) -> Ch1Attempt:
    repo = _repo(bot)
    st = await repo.get_story(user_id)
    return Ch1Attempt.from_json(st.get("attempt_json", ""))


async def _save_attempt(bot: commands.Bot, user_id: int, att: Ch1Attempt) -> None:
    repo = _repo(bot)
    await repo.set_story_fields(user_id, attempt_chapter=1, attempt_json=att.to_json())


# ---------------- UI helpers ----------------

async def _disable_buttons(interaction: discord.Interaction) -> None:
    if not interaction.message:
        return
    view = discord.ui.View.from_message(interaction.message)
    for item in view.children:
        if isinstance(item, discord.ui.Button):
            item.disabled = True
    try:
        await interaction.message.edit(view=view)
    except Exception:
        pass

async def _is_my_story_thread(bot: commands.Bot, interaction: discord.Interaction) -> bool:
    """Проверяем, что пользователь кликает внутри СВОЕГО треда истории."""
    try:
        if not interaction.user or not interaction.channel:
            return False
        # история у нас живёт в тредах
        if not isinstance(interaction.channel, discord.Thread):
            return False
        repo = bot.repo  # type: ignore[attr-defined]
        st = await repo.get_story(interaction.user.id)
        return int(st.get("thread_id", 0)) == int(interaction.channel.id)
    except Exception:
        return False



# ---------------- Story threads ----------------

async def _get_or_create_story_thread(bot: commands.Bot, member: discord.Member) -> discord.Thread:
    cfg = _cfg(bot)
    ch_id = cfg["channels"].get("story_threads")
    if not ch_id:
        raise RuntimeError("В config.json не задан channels.story_threads")

    ch = bot.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        raise RuntimeError("channels.story_threads должен быть текстовым каналом")

    repo = bot.repo  # type: ignore[attr-defined]

    # 1) Если тред уже привязан в БД — пробуем взять его по id (самый надёжный вариант)
    try:
        st = await repo.get_story(member.id)
        tid = int(st.get("thread_id", 0))
    except Exception:
        tid = 0

    if tid:
        th = bot.get_channel(tid)
        if isinstance(th, discord.Thread):
            return th
        try:
            fetched = await bot.fetch_channel(tid)
            if isinstance(fetched, discord.Thread):
                return fetched
        except Exception:
            pass

    # 2) Фолбэк по имени (для старых тредов)
    legacy_name = f"История — {member.display_name} — Сезон 1"
    target_name = f"История — S1 — {member.id}"

    for t in ch.threads:
        if t.name in (target_name, legacy_name):
            try:
                await repo.set_story_fields(member.id, thread_id=int(t.id))
            except Exception:
                pass
            return t

    try:
        archived = [t async for t in ch.archived_threads(limit=100)]
        for t in archived:
            if t.name in (target_name, legacy_name):
                try:
                    await repo.set_story_fields(member.id, thread_id=int(t.id))
                except Exception:
                    pass
                return t
    except Exception:
        pass

    # 3) Создаём новый тред и привязываем
    t = await ch.create_thread(
        name=target_name,
        type=discord.ChannelType.public_thread,
        auto_archive_duration=10080,
        reason="Личная история игрока (Сезон 1)",
    )
    try:
        await t.add_user(member)
    except Exception:
        pass

    try:
        await repo.set_story_fields(member.id, thread_id=int(t.id))
    except Exception:
        pass
    return t


    try:
        archived = [t async for t in ch.archived_threads(limit=50)]
        for t in archived:
            if t.name == target_name:
                return t
    except Exception:
        pass

    t = await ch.create_thread(
        name=target_name,
        type=discord.ChannelType.public_thread,
        auto_archive_duration=10080,
        reason="Личная история игрока (Сезон 1)",
    )
    try:
        await t.add_user(member)
    except Exception:
        pass
    return t


# ---------------- Order Hall Panel ----------------

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
    async def ch1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await orderhall_issue_code(self.bot, interaction, chapter_id=1)

    @discord.ui.button(
        label="Перепройти текущую главу",
        style=discord.ButtonStyle.danger,
        custom_id="orderhall:story:replay_current",
        row=1,
    )
    async def replay(self, interaction: discord.Interaction, button: discord.ui.Button):
        await orderhall_replay_current(self.bot, interaction)


def get_persistent_views(bot: commands.Bot):
    return [
        OrderHallView(bot),
        # story buttons (чтобы работали после перезапуска)
        Ch1Scene1View(bot),
        Ch1Scene2View(bot),
        Ch1Scene3View(bot),
        Ch1Scene4View(bot),
        Ch1Scene5View(bot),
    ]


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
        f"В Зале орденов тебе выдали ключ.\n\n"
        f"Ключ (уникальный): **`{code}`**\n\n"
        f"Вставь ключ в канал **#дверь-тайн** — и откроется сюжетный замок.\n"
        f"Если ты потеряешь это сообщение — нажми кнопку главы ещё раз, и ключ придёт снова."
    )

    ok = await _dm(member, msg)
    if not ok:
        await safe_send(interaction, 
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
        f"Текущая глава: **{ch_cfg.get('title','')}**\n\n"
        f"Ключ: **`{code}`**\n"
        f"Вставь его в **#дверь-тайн**.\n"
        f"До успешного прохождения печать (роль) снята.",
    )
    if not ok:
        await safe_send(interaction, "Не смог отправить ЛС (закрыты личные).", ephemeral=True)
        return

    await safe_send(interaction, "🕯️ Инструкция на перепрохождение отправлена в ЛС.", ephemeral=True)


# ---------------- Door of Secrets: code entry ----------------

async def _open_story_lock(bot: commands.Bot, member: discord.Member, kind: str) -> None:
    repo = _repo(bot)
    t = await _get_or_create_story_thread(bot, member)

    if kind == "story:s1:ch1":
        await repo.set_story_fields(member.id, attempt_chapter=1, attempt_json="")
        await t.send("🕯️ **Дверь приняла ключ.** Открывается замок главы 1…")
        await send_ch1_scene_1(bot, t, member)
        return

    if kind == "secret:archival_seam":
        text = """📎 **Приложение к делу: «Архивный шов»**

Материал: пробиён. Личность: не уточнена.

**Запись (текст для будущего аудио):**
«Я… не знаю, зачем я это записываю.
Наверное, чтобы потом не сказать себе, что “это было несерьёзно”.

Я нашёл шов.
И теперь у меня в голове это место, как заноза:
я могу жить, могу есть, могу смеяться —
но где-то внутри всё время щёлкает печать.
Они пишут: “исключение не предусмотрено”.
А мне кажется, что исключение — это я.

Я боюсь входить.
И всё равно… я вернусь.
Если ты это слушаешь — значит, ты тоже уже возвращаешься.»"""
        await t.send(text)

        # Optional audio attachment for the secret
        cfg = _cfg(bot)
        audio_path = (cfg.get("story", {})
            .get("secret_audio", {})
            .get("archival_seam", "assets/audio/archival_seam.mp3")
        )
        if audio_path:
            from pathlib import Path
            p = Path(audio_path)
        if not p.is_absolute():
            p = ROOT / p
            if p.exists():
                try:
                    await t.send(file=discord.File(str(p), filename=p.name))
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


# ---------------- Chapter 1 ----------------

class Ch1Scene1View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_my_story_thread(self.bot, interaction)

    @discord.ui.button(label="«Я пришёл учиться.»", style=discord.ButtonStyle.primary, custom_id="story:ch1:s1:1")
    async def b1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene1(self.bot, interaction, choice=1)

    @discord.ui.button(label="«Я пришёл за ответом.»", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s1:2")
    async def b2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene1(self.bot, interaction, choice=2)

    @discord.ui.button(label="«Я пришёл, потому что иначе не выйду.»", style=discord.ButtonStyle.danger, custom_id="story:ch1:s1:3")
    async def b3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene1(self.bot, interaction, choice=3)


async def send_ch1_scene_1(bot: commands.Bot, thread: discord.Thread, member: discord.Member) -> None:
    text = (
        "**Глава 1 — «Порог»**\n\n"
        "Каменная арка стоит в пустом зале, будто её принесли сюда на руках и забыли.\n"
        "На ней нет замка. Нет щелей. Нет ручки.\n"
        "Только тонкая трещина посередине — как линия, по которой ломают печать.\n\n"
        "Слева — табличка из чернёного металла:\n\n"
        "**РЕЕСТР ВХОДЯЩИХ.**\n"
        "**ЛОЖЕ НЕ ПРИНИМАЕТ НАБЛЮДАТЕЛЕЙ.**\n\n"
        "Трещина шевелится, будто пытается произнести слово.\n"
        "И произносит — не голосом, а записью в твоей голове:\n\n"
        "«Назовите основание входа.»"
    )
    await thread.send(text, view=Ch1Scene1View(bot))


async def ch1_apply_scene1(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    assert interaction.user
    att = await _load_attempt(bot, interaction.user.id)

    if choice == 1:
        att.vow = 1
    elif choice == 3:
        att.trust_delta -= 1

    await _save_attempt(bot, interaction.user.id, att)
    await safe_send(interaction, "🕯️ Пустота делает пометку в реестре.", ephemeral=True)

    if isinstance(interaction.channel, discord.Thread):
        await send_ch1_scene_2(bot, interaction.channel, interaction.user.id)


class Ch1Scene2View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_my_story_thread(self.bot, interaction)

    @discord.ui.button(label="Назваться своим именем.", style=discord.ButtonStyle.primary, custom_id="story:ch1:s2:1")
    async def b1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene2(self.bot, interaction, choice=1)

    @discord.ui.button(label="Назваться чужим именем.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s2:2")
    async def b2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene2(self.bot, interaction, choice=2)

    @discord.ui.button(label="Отказаться называться.", style=discord.ButtonStyle.danger, custom_id="story:ch1:s2:3")
    async def b3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene2(self.bot, interaction, choice=3)


async def send_ch1_scene_2(bot: commands.Bot, thread: discord.Thread, user_id: int) -> None:
    text = (
        "**Сцена 2 — Печать личности**\n\n"
        "Трещина раскрывается на толщину пальца.\n"
        "Оттуда пахнет не сыростью — **чернилами**.\n"
        "Словно за дверью годами заполняли книги.\n\n"
        "Пустота говорит ровно, как секретарь, который не имеет права на эмоции:\n"
        "«Паспорт личности предъявлен частично. Требуется уточнение.»\n\n"
        "Перед тобой появляется строка — будто в бланке:\n\n"
        "**КАК ВАС ОТМЕЧАТЬ В РЕЕСТРЕ?**"
    )
    await thread.send(text, view=Ch1Scene2View(bot))


async def ch1_apply_scene2(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    assert interaction.user
    att = await _load_attempt(bot, interaction.user.id)

    if choice == 1:
        att.name_delta -= 1
        att.trust_delta += 1
    elif choice == 2:
        att.mask = 1
    elif choice == 3:
        att.trust_delta -= 1

    await _save_attempt(bot, interaction.user.id, att)
    await safe_send(interaction, "🕯️ Отметка внесена.", ephemeral=True)

    if isinstance(interaction.channel, discord.Thread):
        await send_ch1_scene_3(bot, interaction.channel, interaction.user.id)


class Ch1Scene3View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_my_story_thread(self.bot, interaction)

    @discord.ui.button(label="Идти по разметке «проход».", style=discord.ButtonStyle.primary, custom_id="story:ch1:s3:1")
    async def b1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene3(self.bot, interaction, choice=1)

    @discord.ui.button(label="Встать в «зону ожидания» и прислушаться.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s3:2")
    async def b2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene3(self.bot, interaction, choice=2)

    @discord.ui.button(label="Шагнуть на «зону отказа» и посмотреть, что будет.", style=discord.ButtonStyle.danger, custom_id="story:ch1:s3:3")
    async def b3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene3(self.bot, interaction, choice=3)


async def send_ch1_scene_3(bot: commands.Bot, thread: discord.Thread, user_id: int) -> None:
    text = (
        "**Сцена 3 — Коридор следов**\n\n"
        "Внутри не темно и не светло.\n"
        "Внутри — как в архиве ночью: лампы погашены, но бумага всё равно белеет.\n\n"
        "Коридор уходит вперёд.\n"
        "По стенам — царапины, метки, куски воска.\n"
        "Как будто здесь стояли люди и ждали решения, пока им не стало всё равно.\n\n"
        "На полу — тонкие линии, как разметка на складе:\n"
        "проход • зона ожидания • зона отказа."
    )
    await thread.send(text, view=Ch1Scene3View(bot))


async def ch1_apply_scene3(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    assert interaction.user
    att = await _load_attempt(bot, interaction.user.id)

    if choice == 2:
        att.evidence_delta += 1
        att.evidence += 1
    elif choice == 3:
        att.evidence_delta += 1
        att.evidence += 1
        att.trust_delta -= 1

    await _save_attempt(bot, interaction.user.id, att)

    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    if choice == 1:
        branch = (
            "**Ветка — «проход»**\n"
            "Ты идёшь ровно по линии, как по инструкции.\n"
            "Лампы не загораются. Но ты чувствуешь, что тебя отметили как «удобного».\n"
            "Где-то вдалеке скрипит перо — будто в реестре поставили галочку.\n\n"
            "Пустота сухо:\n"
            "«Маршрут соблюдён.»"
        )
    elif choice == 2:
        branch = (
            "**Ветка — «ожидание»**\n"
            "Ты встаёшь на квадрат «ожидание».\n"
            "Тишина сначала кажется пустой… потом становится густой.\n"
            "Слышно, как кто-то шепчет цифры, будто считает шаги.\n"
            "И ещё — как будто далеко-далеко кто-то повторяет одно и то же слово: «возвращаться».\n\n"
            "Пустота:\n"
            "«Ожидание зарегистрировано. Вы слушаете не то, что вам положено.»\n\n"
            "На стене ты замечаешь крошечную отметку, похожую на незавершённую печать."
        )
    else:
        branch = (
            "**Ветка — «отказ»**\n"
            "Ты заходишь на «отказ».\n"
            "Пол чуть тёплый — будто здесь часто стоят те, кто уже понял, что не хочет входить.\n"
            "На стене — след ладони, смазанный воском.\n"
            "И рядом короткая строка, почти стёртая:\n\n"
            "«НЕ ПОДПИСЫВАЙ ПУСТОЕ.»\n\n"
            "Пустота не повышает голос, но становится ближе:\n"
            "«Зона отказа предназначена для завершения процедуры. Вы преждевременны.»"
        )

    await interaction.channel.send(branch)
    await safe_send(interaction, "🕯️ Дальше.", ephemeral=True)
    await send_ch1_scene_4(bot, interaction.channel, interaction.user.id)


class Ch1Scene4View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_my_story_thread(self.bot, interaction)

    @discord.ui.button(label="Забрать лист.", style=discord.ButtonStyle.primary, custom_id="story:ch1:s4:1")
    async def b1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene4(self.bot, interaction, choice=1)

    @discord.ui.button(label="Сжечь лист на месте.", style=discord.ButtonStyle.danger, custom_id="story:ch1:s4:2")
    async def b2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene4(self.bot, interaction, choice=2)

    @discord.ui.button(label="Оставить лист и переписать одну строку на стене.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s4:3")
    async def b3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene4(self.bot, interaction, choice=3)


async def send_ch1_scene_4(bot: commands.Bot, thread: discord.Thread, user_id: int) -> None:
    text = (
        "**Сцена 4 — Алтарь Микеллы**\n\n"
        "Коридор выводит к нише.\n"
        "В нише стоит фигура, закутанная в ткань. Лица нет — ткань пришита там, где должны быть черты.\n"
        "Остаются только руки. Руки слишком человечные.\n\n"
        "Перед фигурой — стол. На столе — лист.\n"
        "Не молитва. Не исповедь. Инструкция, написанная аккуратным почерком:\n\n"
        "«Паства должна чувствовать выбор.\n"
        "Но выбор должен приводить к двери.\n"
        "Если выбор ведёт прочь — он подлежит изъятию.»\n\n"
        "Пустота добавляет, будто ставит штамп:\n"
        "«Материал не является святыней. Материал является инструментом.»"
    )
    await thread.send(text, view=Ch1Scene4View(bot))


async def ch1_apply_scene4(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    assert interaction.user
    att = await _load_attempt(bot, interaction.user.id)

    if choice == 1:
        att.evidence_delta += 1
        att.evidence += 1
    elif choice == 2:
        att.gro_seed = 1
    elif choice == 3:
        att.rher_seed = 1

    await _save_attempt(bot, interaction.user.id, att)
    await safe_send(interaction, "🕯️ Пустота молчит дольше, чем нужно.", ephemeral=True)

    if isinstance(interaction.channel, discord.Thread):
        await send_ch1_scene_5(bot, interaction.channel, interaction.user.id)


class Ch1Scene5View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_my_story_thread(self.bot, interaction)

    @discord.ui.button(label="Подписать «возвращаться».", style=discord.ButtonStyle.primary, custom_id="story:ch1:s5:1")
    async def b1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene5(self.bot, interaction, choice=1)

    @discord.ui.button(label="Подписать «отвечать».", style=discord.ButtonStyle.primary, custom_id="story:ch1:s5:2")
    async def b2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene5(self.bot, interaction, choice=2)

    @discord.ui.button(label="Подписать «молчать».", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s5:3")
    async def b3(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene5(self.bot, interaction, choice=3)

    @discord.ui.button(label="Порвать бланк.", style=discord.ButtonStyle.danger, custom_id="story:ch1:s5:4")
    async def b4(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _disable_buttons(interaction)
        await ch1_apply_scene5(self.bot, interaction, choice=4)


async def send_ch1_scene_5(bot: commands.Bot, thread: discord.Thread, user_id: int) -> None:
    text = (
        "**Сцена 5 — Договор (замок главы)**\n\n"
        "Дальше — ещё одна арка.\n"
        "На ней три неглубоких углубления, как для печатей.\n"
        "И надпись, выведенная тем же почерком:\n\n"
        "«ВХОД РАЗРЕШАЕТСЯ ТОЛЬКО ПО ДОГОВОРУ.»\n\n"
        "Пустота, без раздражения и без сочувствия:\n"
        "«Договор требуется простой. Один пункт. Одна цена.»\n\n"
        "Рядом лежит чистый бланк. Три строки уже напечатаны, будто заранее знали, что ты придёшь:\n\n"
        "A) «Я обязуюсь возвращаться.»\n"
        "B) «Я обязуюсь отвечать.»\n"
        "C) «Я обязуюсь молчать.»\n\n"
        "Снизу место для подписи."
    )
    await thread.send(text, view=Ch1Scene5View(bot))


async def ch1_apply_scene5(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    assert interaction.user
    repo = _repo(bot)
    cfg = _cfg(bot)

    att = await _load_attempt(bot, interaction.user.id)

    # секретка: Evidence >= 1
    if att.evidence >= 1:
        att.secret_unlocked = 1

    if choice in (1, 2, 3):
        att.vow_lock = {1: "RETURN", 2: "ANSWER", 3: "SILENCE"}[choice]
        await _save_attempt(bot, interaction.user.id, att)

        await repo.story_commit_ch1(
            interaction.user.id,
            name_delta=att.name_delta,
            evidence_delta=att.evidence_delta,
            vow=att.vow,
            trust_delta=att.trust_delta,
            mask=att.mask,
            gro_seed=att.gro_seed,
            rher_seed=att.rher_seed,
            vow_lock=att.vow_lock,
        )

        # роль за главу 1
        reward_role_id = int(cfg.get("story", {}).get("chapters", [{}])[0].get("reward_role_id", 0))
        if reward_role_id and isinstance(interaction.user, discord.Member):
            role = interaction.user.guild.get_role(reward_role_id)
            if role:
                try:
                    await interaction.user.add_roles(role, reason="Сезон 1: Глава 1 пройдена")
                except Exception:
                    pass

        if isinstance(interaction.channel, discord.Thread):
            await interaction.channel.send(
                "**Сцена 6 — Итог**\n\n"
                "Пустота ставит штамп — слышно, как будто печать ударила по камню:\n\n"
                "✅ **ДОПУСК ОФОРМЛЕН**\n"
                "**СТАТУС: НОВОЕ ЭХО**\n\n"
                "Трещина на арке становится шире.\n"
                "Ты проходишь, и на секунду тебе кажется, что кто-то шепчет твоё имя — но это не голос.\n"
                "Это строка в реестре, которую записали навсегда.\n\n"
                "Пустота, уже тише:\n"
                "«Следующий раз вход будет не через дверь.\n"
                "Следующий раз вход будет через ставку.»"
            )

            found = 1 if att.secret_unlocked else 0
            await interaction.channel.send(f"**Найдено секретов:** {found} из 1")

            if att.secret_unlocked:
                code = await repo.get_or_create_code(interaction.user.id, kind="secret:archival_seam", length=10)
                await interaction.channel.send(
                    "**Сцена 7 — Секрет**\n\n"
                    "Под столом — узкий конверт без адреса.\n"
                    "Бумага сухая, старая, будто её держали в пальцах слишком долго.\n\n"
                    "Внутри одна строка:\n"
                    "«Если ты читаешь это — Ложе уже выбрало тебя.»\n\n"
                    f"На карточке — ключ: **`{code}`**\n"
                    f"Вставь его в **#дверь-тайн**."
                )

        await safe_send(interaction, "✅ Глава 1 пройдена. Печать выдана.", ephemeral=True)
        return

    if isinstance(interaction.channel, discord.Thread):
        await interaction.channel.send(
            "**Сцена 6 — Итог**\n\n"
            "В помещении не происходит взрывов и не падают стены.\n"
            "Просто становится тихо — как в кабинете, где только что поставили отказ.\n\n"
            "Пустота говорит спокойно:\n"
            "«Основание входа отозвано. Заявление отклонено.»\n\n"
            "❌ **ДОПУСК ОТКЛОНЁН**\n"
            "**ПРИЧИНА: ОТКАЗ ОТ ДОГОВОРА**\n\n"
            "Трещина на арке смыкается.\n"
            "На секунду тебя будто перечёркивают — не болью, а документом.\n\n"
            "Проклятие: **«Трещина в имени» (2 часа)**"
        )
        await interaction.channel.send("**Найдено секретов:** 0 из 1")

    await safe_send(interaction, "❌ Порог не принял. Проклятие оформлено.", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(StoryCog(bot))
