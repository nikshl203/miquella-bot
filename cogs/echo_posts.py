from __future__ import annotations

import asyncio
import logging
import re
import time
import unicodedata
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from time_utils import msk_day_key

from ._interactions import GuardedView, safe_defer_ephemeral, safe_send

log = logging.getLogger("void")

CATEGORY_NAME = "📃Посты Отголосков"

RENT_DAYS = 30
RENT_SECONDS = RENT_DAYS * 24 * 60 * 60
FIRST_RENT_COST = 100
EXTEND_RENT_COST = 50
RENAME_CHANNEL_COST = 10
RETOPIC_CHANNEL_COST = 10

POSTS_PRAISED_DAILY_LIMIT = 10
PAID_RUNES_DAILY_LIMIT = 50
FREE_PRAISES_DAILY_LIMIT = 3
FREE_PRAISE_REWARD = 2

POST_STYLE_PRICE = 10
POST_FRAME_PRICE = 15

POST_STYLES: tuple[dict[str, Any], ...] = (
    {"id": "echo", "name": "\u041e\u0442\u0433\u043e\u043b\u043e\u0441\u043e\u043a", "emoji": "\U0001F56F", "price": POST_STYLE_PRICE},
    {"id": "chronicle", "name": "\u0425\u0440\u043e\u043d\u0438\u043a\u0430", "emoji": "\U0001F4DC", "price": POST_STYLE_PRICE},
    {"id": "whisper", "name": "\u0428\u0435\u043f\u043e\u0442", "emoji": "\U0001F32B", "price": POST_STYLE_PRICE},
    {"id": "writings", "name": "\u041f\u0438\u0441\u044c\u043c\u0435\u043d\u0430", "emoji": "\u2726", "price": POST_STYLE_PRICE},
    {"id": "covenant", "name": "\u0417\u0430\u0432\u0435\u0442", "emoji": "\u26E7", "price": POST_STYLE_PRICE},
)
POST_STYLES_BY_ID: dict[str, dict[str, Any]] = {str(x["id"]): x for x in POST_STYLES}

POST_FRAMES: tuple[dict[str, Any], ...] = (
    {
        "id": "line_simple",
        "name": "\u041f\u0440\u043e\u0441\u0442\u0430\u044f \u043b\u0438\u043d\u0438\u044f",
        "price": POST_FRAME_PRICE,
        "top": "\u2501\u2501\u2501 {emoji} \u2501\u2501\u2501",
        "bottom": "\u2501\u2501\u2501 {emoji} \u2501\u2501\u2501",
    },
    {
        "id": "line_thin",
        "name": "\u0422\u043e\u043d\u043a\u0430\u044f",
        "price": POST_FRAME_PRICE,
        "top": "\u2508\u2508\u2508 {emoji} \u2508\u2508\u2508",
        "bottom": "\u2508\u2508\u2508 {emoji} \u2508\u2508\u2508",
    },
    {
        "id": "line_heavy",
        "name": "\u0422\u044f\u0436\u0435\u043b\u0430\u044f",
        "price": POST_FRAME_PRICE,
        "top": "\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557",
        "bottom": "\u255A\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255D",
    },
    {
        "id": "line_angular",
        "name": "\u0423\u0433\u043b\u043e\u0432\u0430\u044f",
        "price": POST_FRAME_PRICE,
        "top": "\u250C\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510",
        "bottom": "\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518",
    },
)
POST_FRAMES_BY_ID: dict[str, dict[str, Any]] = {str(x["id"]): x for x in POST_FRAMES}


def _now() -> int:
    return int(time.time())


def _is_media_attachment(att: discord.Attachment) -> bool:
    ctype = (att.content_type or "").lower()
    if ctype.startswith("image/") or ctype.startswith("video/"):
        return True
    name = (att.filename or "").lower()
    for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".mp4", ".mov", ".webm", ".mkv"):
        if name.endswith(ext):
            return True
    return False


def _sanitize_channel_name(raw: str, max_len: int = 90) -> str:
    # Keep human-readable names while producing a Discord-safe slug.
    text = unicodedata.normalize("NFKC", str(raw or "")).strip().lower()
    text = text.replace(" ", "-").replace("_", "-")
    text = re.sub(r"[^\w\-]+", "-", text, flags=re.UNICODE)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    if not text:
        text = "otgolosok"
    text = text[:max_len].strip("-")
    return text or "otgolosok"


def _parse_choice_index(raw: str) -> Optional[int]:
    text = str(raw or "").strip()
    if not text:
        return None
    m = re.match(r"^\s*(\d+)", text)
    if not m:
        return None
    return int(m.group(1))


class PublishPostButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Опубликовать пост",
            custom_id="echo:publish_post",
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: EchoPostsCog = interaction.client.get_cog("EchoPostsCog")  # type: ignore
        if cog:
            await cog.handle_publish_button(interaction)


class PublishPostView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
        self.add_item(PublishPostButton())


class PraiseButton(discord.ui.Button):
    def __init__(self, *, amount: int, is_free: bool, row: int):
        if is_free:
            label = "Похвалить бесплатно"
            custom_id = "echo:praise:free"
            style = discord.ButtonStyle.success
        else:
            label = f"Похвалить на {amount} рун"
            custom_id = f"echo:praise:{amount}"
            style = discord.ButtonStyle.secondary
        super().__init__(label=label, custom_id=custom_id, style=style, row=row)
        self.amount = int(amount)
        self.is_free = bool(is_free)

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: EchoPostsCog = interaction.client.get_cog("EchoPostsCog")  # type: ignore
        if cog:
            await cog.handle_praise_button(interaction, amount=self.amount, is_free=self.is_free)


class PraiseView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
        self.add_item(PraiseButton(amount=2, is_free=False, row=0))
        self.add_item(PraiseButton(amount=5, is_free=False, row=0))
        self.add_item(PraiseButton(amount=10, is_free=False, row=0))
        self.add_item(PraiseButton(amount=0, is_free=True, row=1))


class EchoPostsCog(commands.Cog, name="EchoPostsCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.publish_view = PublishPostView(bot)
        self.praise_view = PraiseView(bot)
        self._publish_sessions: set[int] = set()
        self._channel_edit_sessions: set[int] = set()

    async def cog_load(self) -> None:
        if not self.expiry_watcher.is_running():
            self.expiry_watcher.start()
        asyncio.create_task(self._bootstrap_sync())

    async def cog_unload(self) -> None:
        if self.expiry_watcher.is_running():
            self.expiry_watcher.cancel()

    def _is_admin(self, user_id: int) -> bool:
        return self.bot.is_admin(int(user_id))  # type: ignore

    def _channel_name_base(self, member: discord.Member) -> str:
        base = (member.display_name or member.name or "").strip()
        return _sanitize_channel_name(base, max_len=90)

    def _unique_channel_name(
        self,
        guild: discord.Guild,
        base: str,
        *,
        exclude_channel_id: int | None = None,
    ) -> str:
        taken = {str(ch.name).lower() for ch in guild.channels if int(ch.id) != int(exclude_channel_id or 0)}
        if base.lower() not in taken:
            return base
        for i in range(2, 1000):
            suffix = f"-{i}"
            candidate = f"{base[: max(1, 90 - len(suffix))]}{suffix}"
            if candidate.lower() not in taken:
                return candidate
        return f"{base[:80]}-{int(_now() % 100000)}"

    async def _get_or_create_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        for cat in guild.categories:
            if str(cat.name) == CATEGORY_NAME:
                return cat
        return await guild.create_category(CATEGORY_NAME, reason="Личные каналы постов")

    async def _apply_channel_policy(
        self,
        channel: discord.TextChannel,
        owner_id: int,
        expires_at: int,
        *,
        active: bool,
    ) -> None:
        guild = channel.guild
        owner = guild.get_member(int(owner_id))
        admin_id = int(getattr(self.bot, "cfg", {}).get("admin_user_id", 0))
        admin = guild.get_member(admin_id) if admin_id > 0 else None
        me = guild.me

        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=False,
                add_reactions=False,
                create_public_threads=False,
                create_private_threads=False,
                send_messages_in_threads=False,
            )
        }

        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                attach_files=True,
                embed_links=True,
                manage_messages=True,
            )

        if owner is not None:
            overwrites[owner] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=active,
                attach_files=active,
                embed_links=active,
                manage_channels=False,
            )

        if admin is not None and admin.id != int(owner_id):
            overwrites[admin] = discord.PermissionOverwrite(
                view_channel=True,
                read_message_history=True,
                send_messages=True,
                attach_files=True,
                embed_links=True,
                manage_messages=True,
            )

        await channel.edit(overwrites=overwrites, reason="Sync rental channel policy")

    async def _ensure_controls_message(self, channel: discord.TextChannel, expires_at: int) -> None:
        try:
            async for msg in channel.history(limit=30):
                if msg.author.id != self.bot.user.id:  # type: ignore
                    continue
                for row in msg.components:
                    for comp in row.children:
                        if getattr(comp, "custom_id", "") == "echo:publish_post":
                            return
        except Exception:
            pass

        emb = discord.Embed(
            title="📜 Личный канал Отголосков",
            description=(
                "Нажми кнопку, чтобы опубликовать пост через ЛС.\n"
                f"Аренда действует до <t:{int(expires_at)}:f>."
            ),
        )
        m = await channel.send(embed=emb, view=self.publish_view)
        try:
            await m.pin()
        except Exception:
            pass

    async def _deactivate_expired_channels(self) -> None:
        repo = self.bot.repo  # type: ignore
        now = _now()
        expired = await repo.rental_get_expired_active(now)
        if not expired:
            return

        guild_id = int(getattr(self.bot, "cfg", {}).get("guild_id", 0))
        guild = self.bot.get_guild(guild_id) if guild_id > 0 else None
        for row in expired:
            uid = int(row["user_id"])
            ch_id = int(row["channel_id"])
            expires = int(row["expires_at"])
            await repo.rental_set_active(uid, 0)
            if guild is None:
                continue
            ch = guild.get_channel(ch_id)
            if isinstance(ch, discord.TextChannel):
                try:
                    await self._apply_channel_policy(ch, uid, expires, active=False)
                except Exception:
                    pass

    async def _bootstrap_sync(self) -> None:
        await self.bot.wait_until_ready()
        repo = self.bot.repo  # type: ignore
        guild_id = int(getattr(self.bot, "cfg", {}).get("guild_id", 0))
        guild = self.bot.get_guild(guild_id) if guild_id > 0 else None
        if guild is None:
            return

        rows = await repo.rental_list_all()
        now = _now()
        for row in rows:
            uid = int(row["user_id"])
            ch_id = int(row["channel_id"])
            expires = int(row["expires_at"])
            ch = guild.get_channel(ch_id)
            if not isinstance(ch, discord.TextChannel):
                continue

            active = int(row["is_active"]) == 1 and expires > now
            if not active and int(row["is_active"]) != 0:
                await repo.rental_set_active(uid, 0)
            try:
                await self._apply_channel_policy(ch, uid, expires, active=active)
            except Exception:
                pass
            try:
                await self._ensure_controls_message(ch, expires)
            except Exception:
                pass

    async def _rental_is_active(
        self,
        row: dict[str, int],
        channel: Optional[discord.TextChannel],
    ) -> bool:
        repo = self.bot.repo  # type: ignore
        uid = int(row["user_id"])
        expires = int(row["expires_at"])
        active = int(row["is_active"]) == 1 and expires > _now()
        if active:
            return True

        if int(row["is_active"]) != 0:
            await repo.rental_set_active(uid, 0)

        if isinstance(channel, discord.TextChannel):
            try:
                await self._apply_channel_policy(channel, uid, expires, active=False)
            except Exception:
                pass
        return False

    async def handle_shop_rental(self, interaction: discord.Interaction, action: str) -> None:
        repo = self.bot.repo  # type: ignore

        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Аренда доступна только на сервере.", ephemeral=True)
            return

        if action not in {"rent", "extend", "rename_name", "rename_topic"}:
            await safe_send(interaction, "❌ Неверный ритуал аренды.", ephemeral=True)
            return

        user = interaction.user
        uid = int(user.id)
        guild = interaction.guild
        now = _now()

        row = await repo.rental_get_by_user(uid)
        channel: Optional[discord.TextChannel] = None
        if row is not None:
            maybe = guild.get_channel(int(row["channel_id"]))
            if isinstance(maybe, discord.TextChannel):
                channel = maybe
            else:
                row = None

        if action in {"extend", "rename_name", "rename_topic"} and row is None:
            await safe_send(interaction, "❌ У тебя нет арендованного канала.", ephemeral=True)
            return

        if action in {"rename_name", "rename_topic"}:
            assert row is not None
            assert channel is not None
            await self._start_channel_edit_dialog(interaction, channel=channel, action=action)
            return

        is_new = row is None
        price = FIRST_RENT_COST if is_new else EXTEND_RENT_COST
        me = guild.me
        if me is None:
            await safe_send(interaction, "❌ Не вижу права бота на сервере. Попробуй позже.", ephemeral=True)
            return
        if not me.guild_permissions.manage_channels:
            await safe_send(
                interaction,
                "❌ Не хватает прав бота: **Управление каналами**. Выдай это право роли бота.",
                ephemeral=True,
            )
            return

        existing_category = None
        if is_new:
            for cat in guild.categories:
                if str(cat.name) == CATEGORY_NAME:
                    existing_category = cat
                    break
            if existing_category is not None and not existing_category.permissions_for(me).manage_channels:
                await safe_send(
                    interaction,
                    f"❌ В категории **{CATEGORY_NAME}** у бота нет права **Управление каналами**.",
                    ephemeral=True,
                )
                return
        else:
            assert channel is not None
            if not channel.permissions_for(me).manage_channels:
                await safe_send(
                    interaction,
                    "❌ В твоём канале у бота нет права **Управление каналами**. "
                    "Без этого нельзя продлить аренду.",
                    ephemeral=True,
                )
                return

        if not self._is_admin(uid):
            paid = await repo.spend_runes(uid, price)
            if not paid:
                u = await repo.get_user(uid)
                have = int(u.get("runes", 0))
                await safe_send(interaction, f"Не хватает рун: **{have}/{price}**.", ephemeral=True)
                return

        try:
            if is_new:
                category = existing_category if existing_category is not None else await self._get_or_create_category(guild)
                base_name = self._channel_name_base(user)
                channel_name = self._unique_channel_name(guild, base_name)
                channel = await category.create_text_channel(
                    name=channel_name,
                    reason="Новая аренда личного канала",
                )
                expires_at = now + RENT_SECONDS
                await repo.rental_upsert(uid, channel.id, expires_at, 1)
                await self._apply_channel_policy(channel, uid, expires_at, active=True)
                await self._ensure_controls_message(channel, expires_at)
                await safe_send(
                    interaction,
                    f"✅ Канал создан: {channel.mention}\nАренда до <t:{expires_at}:f>.",
                    ephemeral=True,
                )
                return

            assert row is not None
            assert channel is not None
            expires_at = max(int(row["expires_at"]), now) + RENT_SECONDS
            await repo.rental_upsert(uid, channel.id, expires_at, 1)
            await self._apply_channel_policy(channel, uid, expires_at, active=True)
            await self._ensure_controls_message(channel, expires_at)
            await safe_send(
                interaction,
                    f"✅ Аренда продлена до <t:{expires_at}:f> в канале {channel.mention}.",
                    ephemeral=True,
                )
        except discord.Forbidden:
            if not self._is_admin(uid):
                try:
                    await repo.add_runes(uid, price)
                except Exception:
                    pass
            await safe_send(
                interaction,
                "❌ Не удалось оформить аренду: у бота нет прав на создание/изменение каналов в этой категории.",
                ephemeral=True,
            )
        except Exception:
            if not self._is_admin(uid):
                try:
                    await repo.add_runes(uid, price)
                except Exception:
                    pass
            log.exception("Rental purchase failed")
            await safe_send(interaction, "❌ Не удалось оформить аренду. Руны возвращены.", ephemeral=True)

    async def _start_channel_edit_dialog(
        self,
        interaction: discord.Interaction,
        *,
        channel: discord.TextChannel,
        action: str,
    ) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Это доступно только на сервере.", ephemeral=True)
            return

        uid = int(interaction.user.id)
        if uid in self._channel_edit_sessions:
            await safe_send(interaction, "⏳ Диалог уже открыт в ЛС.", ephemeral=True)
            return

        cost = RENAME_CHANNEL_COST if action == "rename_name" else RETOPIC_CHANNEL_COST
        repo = self.bot.repo  # type: ignore
        if not self._is_admin(uid):
            u = await repo.get_user(uid)
            have = int(u.get("runes", 0))
            if have < cost:
                await safe_send(interaction, f"❌ Не хватает рун: **{have}/{cost}**.", ephemeral=True)
                return

        try:
            dm = await interaction.user.create_dm()
            if action == "rename_name":
                await dm.send(
                    "✍️ **Смена имени канала**\n"
                    "Отправь новое имя (не длиннее 20 символов).\n"
                    "Имя должно быть уникальным на сервере.\n"
                    "Таймаут: 10 минут."
                )
            else:
                await dm.send(
                    "🧾 **Смена описания канала**\n"
                    "Отправь новый текст описания (topic) канала.\n"
                    "Таймаут: 10 минут."
                )
        except Exception:
            await safe_send(interaction, "❌ Не могу написать тебе в ЛС. Открой личные сообщения.", ephemeral=True)
            return

        self._channel_edit_sessions.add(uid)
        await safe_send(interaction, "✉️ Написал в ЛС. Жду ответ.", ephemeral=True)
        asyncio.create_task(
            self._channel_edit_dialog(
                user_id=uid,
                guild_id=int(interaction.guild.id),
                channel_id=int(channel.id),
                dm_id=int(dm.id),
                action=action,
                cost=cost,
            )
        )

    async def _channel_edit_dialog(
        self,
        *,
        user_id: int,
        guild_id: int,
        channel_id: int,
        dm_id: int,
        action: str,
        cost: int,
    ) -> None:
        try:
            guild = self.bot.get_guild(int(guild_id))
            if guild is None:
                user = self.bot.get_user(int(user_id))
                if user:
                    dm = await user.create_dm()
                    await dm.send("❌ Сервер недоступен. Попробуй позже.")
                return

            channel = guild.get_channel(int(channel_id))
            if not isinstance(channel, discord.TextChannel):
                user = self.bot.get_user(int(user_id))
                if user:
                    dm = await user.create_dm()
                    await dm.send("❌ Канал не найден.")
                return

            repo = self.bot.repo  # type: ignore
            row = await repo.rental_get_by_channel(channel.id)
            if row is None or int(row["user_id"]) != int(user_id):
                user = self.bot.get_user(int(user_id))
                if user:
                    dm = await user.create_dm()
                    await dm.send("❌ Канал больше не принадлежит тебе.")
                return

            def _check_dm(msg: discord.Message) -> bool:
                return int(msg.author.id) == int(user_id) and int(msg.channel.id) == int(dm_id)

            user_obj = self.bot.get_user(int(user_id))
            if user_obj is None:
                return
            dm = await user_obj.create_dm()

            for attempt in range(3):
                msg = await self.bot.wait_for("message", timeout=600, check=_check_dm)
                payload = (msg.content or "").strip()

                if action == "rename_name":
                    if not payload:
                        await dm.send("❌ Имя не может быть пустым.")
                        continue
                    if len(payload) > 20:
                        await dm.send("❌ Имя длиннее 20 символов. Попробуй снова.")
                        continue

                    safe_name = _sanitize_channel_name(payload, max_len=20)
                    if not safe_name:
                        await dm.send("❌ Не удалось сформировать имя из этого текста.")
                        continue
                    if safe_name.lower() == str(channel.name).lower():
                        await dm.send("❌ Это имя уже используется твоим каналом.")
                        continue

                    exists = any(
                        str(ch.name).lower() == safe_name.lower() and int(ch.id) != int(channel.id)
                        for ch in guild.channels
                    )
                    if exists:
                        await dm.send("❌ Такое имя уже занято. Попробуй другое.")
                        continue

                    old_name = str(channel.name)
                    try:
                        await channel.edit(name=safe_name, reason="Paid channel rename")
                    except Exception:
                        await dm.send("❌ Не удалось изменить имя канала.")
                        return

                    if not self._is_admin(user_id):
                        paid = await repo.spend_runes(user_id, int(cost))
                        if not paid:
                            try:
                                await channel.edit(name=old_name, reason="Rename rollback: no funds")
                            except Exception:
                                pass
                            u = await repo.get_user(user_id)
                            have = int(u.get("runes", 0))
                            await dm.send(f"❌ Не хватает рун: **{have}/{cost}**. Имя не изменено.")
                            return

                    await dm.send(f"✅ Имя канала обновлено: `{safe_name}`. Списано **{cost}** рун.")
                    return

                # action == rename_topic
                if not payload:
                    await dm.send("❌ Описание не может быть пустым.")
                    continue
                if len(payload) > 1024:
                    await dm.send("❌ Описание слишком длинное (максимум 1024 символа).")
                    continue

                old_topic = channel.topic
                try:
                    await channel.edit(topic=payload, reason="Paid channel topic update")
                except Exception:
                    await dm.send("❌ Не удалось изменить описание канала.")
                    return

                if not self._is_admin(user_id):
                    paid = await repo.spend_runes(user_id, int(cost))
                    if not paid:
                        try:
                            await channel.edit(topic=old_topic, reason="Topic rollback: no funds")
                        except Exception:
                            pass
                        u = await repo.get_user(user_id)
                        have = int(u.get("runes", 0))
                        await dm.send(f"❌ Не хватает рун: **{have}/{cost}**. Описание не изменено.")
                        return

                await dm.send(f"✅ Описание канала обновлено. Списано **{cost}** рун.")
                return

            await dm.send("⌛ Слишком много неудачных попыток. Запусти покупку заново.")

        except asyncio.TimeoutError:
            user = self.bot.get_user(int(user_id))
            if user is not None:
                try:
                    dm = await user.create_dm()
                    await dm.send("⌛ Время ожидания истекло. Запусти покупку заново.")
                except Exception:
                    pass
        except Exception:
            log.exception("Channel edit dialog failed")
            user = self.bot.get_user(int(user_id))
            if user is not None:
                try:
                    dm = await user.create_dm()
                    await dm.send("❌ Ошибка изменения канала. Попробуй снова.")
                except Exception:
                    pass
        finally:
            self._channel_edit_sessions.discard(int(user_id))

    async def _ask_style_choice(
        self,
        *,
        owner_id: int,
        dm: discord.DMChannel,
        check_dm: Any,
    ) -> str:
        repo = self.bot.repo  # type: ignore
        owned = await repo.purchases_by_kind(owner_id, "post_style")
        available = [x for x in POST_STYLES if str(x["id"]) in owned]

        if not available:
            await dm.send("🎨 У тебя пока нет купленных стилей. Использую вариант: **Без стиля**.")
            return ""

        lines = ["1) Без стиля"]
        for i, item in enumerate(available, start=2):
            lines.append(f"{i}) {item['emoji']} {item['name']}")

        await dm.send(
            "🎨 **Выбор стиля**\n"
            "Отправь номер варианта:\n"
            + "\n".join(lines)
        )

        for _ in range(3):
            msg = await self.bot.wait_for("message", timeout=600, check=check_dm)
            idx = _parse_choice_index(msg.content)
            if idx is None or idx < 1 or idx > (len(available) + 1):
                await dm.send("❌ Неверный номер. Отправь число из списка.")
                continue
            if idx == 1:
                return ""
            return str(available[idx - 2]["id"])

        await dm.send("⌛ Слишком много ошибок. Использую вариант: **Без стиля**.")
        return ""

    async def _ask_frame_choice(
        self,
        *,
        owner_id: int,
        dm: discord.DMChannel,
        check_dm: Any,
    ) -> str:
        repo = self.bot.repo  # type: ignore
        owned = await repo.purchases_by_kind(owner_id, "post_frame")
        available = [x for x in POST_FRAMES if str(x["id"]) in owned]

        if not available:
            await dm.send("🧱 У тебя пока нет купленных рамок. Использую вариант: **Без рамки**.")
            return ""

        lines = ["1) Без рамки"]
        for i, item in enumerate(available, start=2):
            lines.append(f"{i}) {item['name']}")

        await dm.send(
            "🧱 **Выбор рамки**\n"
            "Отправь номер варианта:\n"
            + "\n".join(lines)
        )

        for _ in range(3):
            msg = await self.bot.wait_for("message", timeout=600, check=check_dm)
            idx = _parse_choice_index(msg.content)
            if idx is None or idx < 1 or idx > (len(available) + 1):
                await dm.send("❌ Неверный номер. Отправь число из списка.")
                continue
            if idx == 1:
                return ""
            return str(available[idx - 2]["id"])

        await dm.send("⌛ Слишком много ошибок. Использую вариант: **Без рамки**.")
        return ""

    def _render_post_content(
        self,
        *,
        display_name: str,
        text: str,
        style_id: str,
        frame_id: str,
    ) -> Optional[str]:
        style = POST_STYLES_BY_ID.get(str(style_id or ""))
        frame = POST_FRAMES_BY_ID.get(str(frame_id or ""))
        body = str(text or "").strip()

        header = ""
        style_emoji = "*"
        if style is not None:
            style_emoji = str(style.get("emoji", "*"))
            header = f"{style_emoji} {style.get('name', '')} {display_name}".strip()
        elif frame is not None:
            header = str(display_name).strip()

        cap = ""
        if frame is not None and header:
            top = str(frame.get("top", "")).format(emoji=style_emoji)
            bottom = str(frame.get("bottom", "")).format(emoji=style_emoji)
            cap = f"{top}\n{header}\n{bottom}".strip()
        elif header:
            cap = header

        if cap and body:
            return f"{cap}\n\n{body}"
        if cap:
            return cap
        if body:
            return body
        return None

    async def handle_publish_button(self, interaction: discord.Interaction) -> None:
        ok = await safe_defer_ephemeral(interaction)
        if not ok:
            return

        if interaction.guild is None or not isinstance(interaction.channel, discord.TextChannel):
            await safe_send(interaction, "❌ Кнопка работает только в личном канале постов.", ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Команда доступна только на сервере.", ephemeral=True)
            return

        repo = self.bot.repo  # type: ignore
        row = await repo.rental_get_by_channel(interaction.channel.id)
        if row is None:
            await safe_send(interaction, "❌ Этот канал не зарегистрирован как арендный.", ephemeral=True)
            return

        owner_id = int(row["user_id"])
        if interaction.user.id != owner_id:
            await safe_send(interaction, "❌ Нажимать кнопку публикации может только владелец канала.", ephemeral=True)
            return

        is_active = await self._rental_is_active(row, interaction.channel)
        if not is_active:
            await safe_send(interaction, "⛓️ Аренда канала истекла. Продли канал в магазине.", ephemeral=True)
            return

        if owner_id in self._publish_sessions:
            await safe_send(interaction, "⏳ Диалог публикации уже открыт в ЛС.", ephemeral=True)
            return

        try:
            dm = await interaction.user.create_dm()
            await dm.send(
                "📝 **Публикация поста 1/2**\n"
                "Отправь текст поста одним сообщением.\n"
                "Можно сразу прикрепить фото/видео.\n"
                "Таймаут: 10 минут."
            )
        except Exception:
            await safe_send(interaction, "❌ Не могу написать тебе в ЛС. Открой личные сообщения.", ephemeral=True)
            return

        self._publish_sessions.add(owner_id)
        await safe_send(interaction, "✉️ Написал в ЛС. Жду текст поста.", ephemeral=True)
        asyncio.create_task(
            self._publish_dialog(owner_id=owner_id, guild_id=interaction.guild.id, channel_id=interaction.channel.id, dm_id=dm.id)
        )

    async def _publish_dialog(self, owner_id: int, guild_id: int, channel_id: int, dm_id: int) -> None:
        try:
            def _check_dm(msg: discord.Message) -> bool:
                return int(msg.author.id) == int(owner_id) and int(msg.channel.id) == int(dm_id)

            text_msg = await self.bot.wait_for("message", timeout=600, check=_check_dm)
            text = (text_msg.content or "").strip()
            attachments: list[discord.Attachment] = list(text_msg.attachments)

            dm = text_msg.channel
            await dm.send(
                "📎 **Публикация поста 2/2**\n"
                "Отправь вложения (фото/видео) одним сообщением.\n"
                "Если без вложений — напиши `пропуск`."
            )

            media_msg = await self.bot.wait_for("message", timeout=600, check=_check_dm)
            if (media_msg.content or "").strip().lower() not in {"пропуск", "skip", "нет"}:
                attachments.extend(media_msg.attachments)

            media_attachments = [a for a in attachments if _is_media_attachment(a)]

            if not text and not media_attachments:
                await dm.send("❌ Пустой пост не отправлен. Нужен текст или фото/видео.")
                return

            guild = self.bot.get_guild(int(guild_id))
            if guild is None:
                await dm.send("❌ Сервер недоступен. Попробуй позже.")
                return

            channel = guild.get_channel(int(channel_id))
            if not isinstance(channel, discord.TextChannel):
                await dm.send("❌ Канал не найден. Оформи аренду заново.")
                return

            repo = self.bot.repo  # type: ignore
            row = await repo.rental_get_by_channel(channel.id)
            if row is None or int(row["user_id"]) != int(owner_id):
                await dm.send("❌ Канал больше не принадлежит тебе.")
                return

            if not await self._rental_is_active(row, channel):
                await dm.send("⛔ Аренда канала истекла. Продли канал в магазине.")
                return

            style_id = await self._ask_style_choice(owner_id=owner_id, dm=dm, check_dm=_check_dm)
            frame_id = await self._ask_frame_choice(owner_id=owner_id, dm=dm, check_dm=_check_dm)

            member = guild.get_member(int(owner_id))
            if member is not None:
                display_name = str(member.display_name or member.name)
            else:
                user_obj = self.bot.get_user(int(owner_id))
                display_name = str(getattr(user_obj, "name", f"user-{owner_id}"))

            content = self._render_post_content(
                display_name=display_name,
                text=text,
                style_id=style_id,
                frame_id=frame_id,
            )

            files: list[discord.File] = []
            for att in media_attachments[:10]:
                try:
                    files.append(await att.to_file())
                except Exception:
                    continue

            sent = await channel.send(
                content=content,
                files=files if files else None,
                view=self.praise_view,
            )
            await repo.rental_post_add(sent.id, owner_id, channel.id)
            await dm.send(f"✅ Пост опубликован: {sent.jump_url}")

        except asyncio.TimeoutError:
            user = self.bot.get_user(int(owner_id))
            if user is not None:
                try:
                    dm = await user.create_dm()
                    await dm.send("⌛ Время ожидания истекло. Нажми кнопку публикации ещё раз.")
                except Exception:
                    pass
        except Exception:
            log.exception("Publish dialog failed")
            user = self.bot.get_user(int(owner_id))
            if user is not None:
                try:
                    dm = await user.create_dm()
                    await dm.send("❌ Ошибка публикации. Попробуй снова через кнопку в канале.")
                except Exception:
                    pass
        finally:
            self._publish_sessions.discard(int(owner_id))

    async def handle_praise_button(self, interaction: discord.Interaction, *, amount: int, is_free: bool) -> None:
        ok = await safe_defer_ephemeral(interaction)
        if not ok:
            return

        if interaction.message is None:
            await safe_send(interaction, "❌ Не найдено сообщение поста.", ephemeral=True)
            return

        repo = self.bot.repo  # type: ignore
        message_id = int(interaction.message.id)
        post = await repo.rental_post_get(message_id)
        if post is None:
            await safe_send(interaction, "❌ Эти кнопки работают только под постами отголосков.", ephemeral=True)
            return

        praised_by = int(interaction.user.id)
        author_id = int(post["author_user_id"])
        if praised_by == author_id:
            await safe_send(interaction, "❌ Нельзя хвалить свой пост.", ephemeral=True)
            return

        today = msk_day_key()
        limits = await repo.praise_daily_get(praised_by, today)
        posts_praised = int(limits.get("posts_praised_count", 0))
        paid_given = int(limits.get("paid_runes_given", 0))
        free_used = int(limits.get("free_praises_used", 0))

        if posts_praised >= POSTS_PRAISED_DAILY_LIMIT:
            await safe_send(interaction, "⛓️ Дневной лимит: максимум 10 похваленных постов.", ephemeral=True)
            return

        if is_free:
            if free_used >= FREE_PRAISES_DAILY_LIMIT:
                await safe_send(interaction, "⛓️ Дневной лимит бесплатной похвалы: 3/3.", ephemeral=True)
                return

            await repo.add_runes(author_id, FREE_PRAISE_REWARD)
            await repo.post_praise_add(message_id, author_id, praised_by, FREE_PRAISE_REWARD, True)
            await repo.praise_daily_add(praised_by, today, 0, 1, 1)
            await safe_send(
                interaction,
                f"✅ Бесплатная похвала отправлена. Автор получил **{FREE_PRAISE_REWARD}** руны.",
                ephemeral=True,
            )
            return

        amt = int(amount)
        if amt <= 0:
            await safe_send(interaction, "❌ Неверная сумма похвалы.", ephemeral=True)
            return

        if paid_given + amt > PAID_RUNES_DAILY_LIMIT:
            left = max(0, PAID_RUNES_DAILY_LIMIT - paid_given)
            await safe_send(
                interaction,
                f"⛓️ Дневной лимит платной похвалы: 50 рун. Осталось: **{left}**.",
                ephemeral=True,
            )
            return

        transferred = await repo.transfer_runes(praised_by, author_id, amt)
        if not transferred:
            u = await repo.get_user(praised_by)
            have = int(u.get("runes", 0))
            await safe_send(interaction, f"❌ Не хватает рун: **{have}/{amt}**.", ephemeral=True)
            return

        await repo.post_praise_add(message_id, author_id, praised_by, amt, False)
        await repo.praise_daily_add(praised_by, today, amt, 1, 0)
        await safe_send(interaction, f"✅ Похвала отправлена. Автор получил **{amt}** рун.", ephemeral=True)

    @tasks.loop(minutes=5)
    async def expiry_watcher(self) -> None:
        try:
            await self._deactivate_expired_channels()
        except Exception:
            log.exception("expiry_watcher failed")

    @expiry_watcher.before_loop
    async def _before_expiry_watcher(self) -> None:
        await self.bot.wait_until_ready()


def get_persistent_views(bot: commands.Bot):
    return [PublishPostView(bot), PraiseView(bot)]


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(EchoPostsCog(bot))
