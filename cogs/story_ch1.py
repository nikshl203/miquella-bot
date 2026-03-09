from __future__ import annotations

import time

import discord
from discord.ext import commands

from ._interactions import GuardedView, safe_defer_ephemeral, safe_send
from .story_shared import Ch1Attempt, cfg, disable_buttons, is_my_story_thread, load_attempt, repo, save_attempt


def get_persistent_views(bot: commands.Bot):
    return [
        Ch1Scene1View(bot),
        Ch1Scene2View(bot),
        Ch1Scene3View(bot),
        Ch1Scene4View(bot),
        Ch1Scene5View(bot),
    ]


async def _send_next_and_disable(
    interaction: discord.Interaction,
    sender,
    *,
    fail_text: str,
) -> bool:
    try:
        await sender()
    except Exception:
        await safe_send(interaction, fail_text, ephemeral=True)
        return False

    await disable_buttons(interaction)
    return True


class Ch1Scene1View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        ok = await is_my_story_thread(self.bot, interaction)
        if not ok:
            await safe_send(interaction, "Это не твоя ветка истории.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="«Я пришёл учиться.»", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s1:1")
    async def b1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene1(self.bot, interaction, choice=1)

    @discord.ui.button(label="«Я пришёл за ответом.»", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s1:2")
    async def b2(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene1(self.bot, interaction, choice=2)

    @discord.ui.button(label="«Я пришёл, потому что иначе не выйду.»", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s1:3")
    async def b3(self, interaction: discord.Interaction, _: discord.ui.Button):
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
    await safe_defer_ephemeral(interaction)
    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    assert interaction.user
    att = await load_attempt(bot, interaction.user.id)
    if choice == 1:
        att.vow = 1
    elif choice == 3:
        att.trust_delta -= 1
    await save_attempt(bot, interaction.user.id, att)

    ok = await _send_next_and_disable(
        interaction,
        lambda: send_ch1_scene_2(bot, interaction.channel, interaction.user.id),
        fail_text="⚠️ Ошибка взаимодействия. Следующая сцена не отправлена, попробуй ещё раз.",
    )
    if ok:
        await safe_send(interaction, "🕯️ Пустота делает пометку в реестре.", ephemeral=True)


class Ch1Scene2View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        ok = await is_my_story_thread(self.bot, interaction)
        if not ok:
            await safe_send(interaction, "Это не твоя ветка истории.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Назваться своим именем.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s2:1")
    async def b1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene2(self.bot, interaction, choice=1)

    @discord.ui.button(label="Назваться чужим именем.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s2:2")
    async def b2(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene2(self.bot, interaction, choice=2)

    @discord.ui.button(label="Отказаться называться.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s2:3")
    async def b3(self, interaction: discord.Interaction, _: discord.ui.Button):
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
    await safe_defer_ephemeral(interaction)
    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    assert interaction.user
    att = await load_attempt(bot, interaction.user.id)
    if choice == 1:
        att.name_delta -= 1
        att.trust_delta += 1
    elif choice == 2:
        att.mask = 1
    elif choice == 3:
        att.trust_delta -= 1
    await save_attempt(bot, interaction.user.id, att)

    ok = await _send_next_and_disable(
        interaction,
        lambda: send_ch1_scene_3(bot, interaction.channel, interaction.user.id),
        fail_text="⚠️ Ошибка взаимодействия. Следующая сцена не отправлена, попробуй ещё раз.",
    )
    if ok:
        await safe_send(interaction, "🕯️ Отметка внесена.", ephemeral=True)


class Ch1Scene3View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        ok = await is_my_story_thread(self.bot, interaction)
        if not ok:
            await safe_send(interaction, "Это не твоя ветка истории.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Идти по разметке «проход».", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s3:1")
    async def b1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene3(self.bot, interaction, choice=1)

    @discord.ui.button(label="Встать в «зону ожидания» и прислушаться.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s3:2")
    async def b2(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene3(self.bot, interaction, choice=2)

    @discord.ui.button(label="Шагнуть на «зону отказа» и посмотреть, что будет.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s3:3")
    async def b3(self, interaction: discord.Interaction, _: discord.ui.Button):
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
    await safe_defer_ephemeral(interaction)
    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    assert interaction.user
    att = await load_attempt(bot, interaction.user.id)
    if choice == 2:
        att.evidence_delta += 1
        att.evidence += 1
    elif choice == 3:
        att.evidence_delta += 1
        att.evidence += 1
        att.trust_delta -= 1
    await save_attempt(bot, interaction.user.id, att)

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

    async def _send_flow():
        await interaction.channel.send(branch)
        await send_ch1_scene_4(bot, interaction.channel, interaction.user.id)

    ok = await _send_next_and_disable(
        interaction,
        _send_flow,
        fail_text="⚠️ Ошибка взаимодействия. Следующая сцена не отправлена, попробуй ещё раз.",
    )
    if ok:
        await safe_send(interaction, "🕯️ Дальше.", ephemeral=True)


class Ch1Scene4View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        ok = await is_my_story_thread(self.bot, interaction)
        if not ok:
            await safe_send(interaction, "Это не твоя ветка истории.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Забрать лист.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s4:1")
    async def b1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene4(self.bot, interaction, choice=1)

    @discord.ui.button(label="Сжечь лист на месте.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s4:2")
    async def b2(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene4(self.bot, interaction, choice=2)

    @discord.ui.button(label="Оставить лист и переписать одну строку на стене.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s4:3")
    async def b3(self, interaction: discord.Interaction, _: discord.ui.Button):
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
    await safe_defer_ephemeral(interaction)
    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    assert interaction.user
    att = await load_attempt(bot, interaction.user.id)
    if choice == 1:
        att.evidence_delta += 1
        att.evidence += 1
    elif choice == 2:
        att.gro_seed = 1
    elif choice == 3:
        att.rher_seed = 1
    await save_attempt(bot, interaction.user.id, att)

    ok = await _send_next_and_disable(
        interaction,
        lambda: send_ch1_scene_5(bot, interaction.channel, interaction.user.id),
        fail_text="⚠️ Ошибка взаимодействия. Следующая сцена не отправлена, попробуй ещё раз.",
    )
    if ok:
        await safe_send(interaction, "🕯️ Пустота молчит дольше, чем нужно.", ephemeral=True)


class Ch1Scene5View(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        ok = await is_my_story_thread(self.bot, interaction)
        if not ok:
            await safe_send(interaction, "Это не твоя ветка истории.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Подписать «возвращаться».", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s5:1")
    async def b1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene5(self.bot, interaction, choice=1)

    @discord.ui.button(label="Подписать «отвечать».", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s5:2")
    async def b2(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene5(self.bot, interaction, choice=2)

    @discord.ui.button(label="Подписать «молчать».", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s5:3")
    async def b3(self, interaction: discord.Interaction, _: discord.ui.Button):
        await ch1_apply_scene5(self.bot, interaction, choice=3)

    @discord.ui.button(label="Порвать бланк.", style=discord.ButtonStyle.secondary, custom_id="story:ch1:s5:4")
    async def b4(self, interaction: discord.Interaction, _: discord.ui.Button):
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


async def _commit_ch1_success(bot: commands.Bot, user_id: int, att: Ch1Attempt) -> None:
    r = repo(bot)
    st = await r.get_story(user_id)

    already_completed = int(st.get("chapter_completed", 0)) >= 1

    # On replay we keep progression counters stable and only refresh branch lock/status.
    name_shards = int(st.get("name_shards", 0))
    evidence = int(st.get("evidence", 0))
    vow = int(st.get("vow", 0))
    trust_void = int(st.get("trust_void", 0))
    mask = int(st.get("mask", 0))
    gro_seed = int(st.get("gro_seed", 0))
    rher_seed = int(st.get("rher_seed", 0))

    if not already_completed:
        name_shards = max(0, name_shards + int(att.name_delta))
        evidence = max(0, evidence + int(att.evidence_delta))
        vow = max(0, vow + int(att.vow))
        trust_void = trust_void + int(att.trust_delta)
        mask = max(mask, int(att.mask))
        gro_seed = max(gro_seed, int(att.gro_seed))
        rher_seed = max(rher_seed, int(att.rher_seed))

    await r.set_story_fields(
        user_id,
        chapter_completed=max(1, int(st.get("chapter_completed", 0))),
        name_shards=int(name_shards),
        evidence=int(evidence),
        vow=int(vow),
        trust_void=int(trust_void),
        mask=int(mask),
        gro_seed=int(gro_seed),
        rher_seed=int(rher_seed),
        vow_lock=str(att.vow_lock or st.get("vow_lock", "")),
        attempt_chapter=0,
        attempt_json="",
        updated_ts=int(time.time()),
    )


async def ch1_apply_scene5(bot: commands.Bot, interaction: discord.Interaction, choice: int) -> None:
    await safe_defer_ephemeral(interaction)
    if not isinstance(interaction.channel, discord.Thread):
        await safe_send(interaction, "Ошибка: история должна идти в треде.", ephemeral=True)
        return

    assert interaction.user
    r = repo(bot)
    c = cfg(bot)
    att = await load_attempt(bot, interaction.user.id)

    if att.evidence >= 1:
        att.secret_unlocked = 1

    if choice in (1, 2, 3):
        att.vow_lock = {1: "RETURN", 2: "ANSWER", 3: "SILENCE"}[choice]
        await save_attempt(bot, interaction.user.id, att)
        await _commit_ch1_success(bot, interaction.user.id, att)

        reward_role_id = int(c.get("story", {}).get("chapters", [{}])[0].get("reward_role_id", 0))
        if reward_role_id and isinstance(interaction.user, discord.Member):
            role = interaction.user.guild.get_role(reward_role_id)
            if role:
                try:
                    await interaction.user.add_roles(role, reason="Сезон 1: Глава 1 пройдена")
                except Exception:
                    pass

        async def _send_success():
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
                code = await r.get_or_create_code(interaction.user.id, kind="secret:archival_seam", length=10)
                await interaction.channel.send(
                    "**Сцена 7 — Секрет**\n\n"
                    "Под столом — узкий конверт без адреса.\n"
                    "Бумага сухая, старая, будто её держали в пальцах слишком долго.\n\n"
                    "Внутри одна строка:\n"
                    "«Если ты читаешь это — Ложе уже выбрало тебя.»\n\n"
                    f"На карточке — ключ: **`{code}`**\n"
                    "Вставь его в **#дверь-тайн**."
                )

        ok = await _send_next_and_disable(
            interaction,
            _send_success,
            fail_text="⚠️ Ошибка взаимодействия. Финальная сцена не отправлена, попробуй ещё раз.",
        )
        if ok:
            await safe_send(interaction, "✅ Глава 1 пройдена. Печать выдана.", ephemeral=True)
        return

    async def _send_fail():
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

    ok = await _send_next_and_disable(
        interaction,
        _send_fail,
        fail_text="⚠️ Ошибка взаимодействия. Финальная сцена не отправлена, попробуй ещё раз.",
    )
    if ok:
        await safe_send(interaction, "❌ Порог не принял. Проклятие оформлено.", ephemeral=True)

