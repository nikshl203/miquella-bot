# cogs/shop.py
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import discord
from discord.ext import commands

from ._interactions import GuardedView, safe_defer_ephemeral, safe_send


def _find_item(items: list[dict[str, Any]], item_id: str) -> Optional[dict[str, Any]]:
    for it in items:
        if it.get("id") == item_id:
            return it
    return None


def _fmt_price(price: int) -> str:
    return f"{int(price)} рун"


def _fmt_unlock(lvl: int) -> str:
    return f"ур.{int(lvl)}+"


class TokenButton(discord.ui.Button):
    def __init__(self, item_id: str, label: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=label[:80],
            custom_id=f"shop:token:{item_id}",
            row=row,
        )
        self.item_id = item_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: ShopCog = interaction.client.get_cog("ShopCog")  # type: ignore
        if cog:
            await cog.handle_purchase(interaction, "token", self.item_id)


class SoundButton(discord.ui.Button):
    def __init__(self, item_id: str, label: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=label[:80],
            custom_id=f"shop:sound:{item_id}",
            row=row,
        )
        self.item_id = item_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: ShopCog = interaction.client.get_cog("ShopCog")  # type: ignore
        if cog:
            await cog.handle_purchase(interaction, "sound", self.item_id)


class CustomRoleButton(discord.ui.Button):
    def __init__(self, label: str):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=label[:80],
            custom_id="shop:custom_role:custom_role",
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: ShopCog = interaction.client.get_cog("ShopCog")  # type: ignore
        if cog:
            await cog.handle_purchase(interaction, "custom_role", "custom_role")


class TokensView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        cfg = bot.cfg  # type: ignore
        tokens = list(cfg.get("shop", {}).get("tokens", []))

        for i, t in enumerate(tokens):
            price = int(t.get("price", 0))
            unlock = int(t.get("unlock_level", 0))
            item_id = str(t.get("id"))
            label = f"🎴 {t['label']} • {_fmt_unlock(unlock)} • {_fmt_price(price)}"
            row = 0 if i < 5 else 1
            self.add_item(TokenButton(item_id=item_id, label=label, row=row))


class SoundsView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        cfg = bot.cfg  # type: ignore
        sounds = list(cfg.get("shop", {}).get("sounds", []))[:15]

        for i, s in enumerate(sounds):
            price = int(s.get("price", 0))
            unlock = int(s.get("unlock_level", 0))
            item_id = str(s.get("id"))
            label = f"🔊 {s['label']} • {_fmt_unlock(unlock)} • {_fmt_price(price)}"
            row = i // 5  # 0,1,2
            self.add_item(SoundButton(item_id=item_id, label=label, row=row))


class CustomRoleView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        cfg = bot.cfg  # type: ignore
        price = int(cfg.get("shop", {}).get("custom_role_price", 80))
        self.add_item(CustomRoleButton(label=f"🪪 Своя роль • {_fmt_price(price)}"))


class ShopCog(commands.Cog, name="ShopCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.tokens_view = TokensView(bot)
        # sounds disabled if list empty
        cfg = bot.cfg  # type: ignore
        sounds = list(cfg.get("shop", {}).get("sounds", []))
        self.sounds_view = SoundsView(bot) if sounds else None
        self.custom_role_view = CustomRoleView(bot)

    def _is_admin(self, user_id: int) -> bool:
        return self.bot.is_admin(user_id)  # type: ignore

    async def _get_user(self, user_id: int) -> Dict[str, Any]:
        repo = self.bot.repo  # type: ignore
        await repo.ensure_user(user_id)
        return await repo.get_user(user_id)

    async def _can_pay(self, user_id: int, price: int) -> bool:
        if self._is_admin(user_id):
            return True
        u = await self._get_user(user_id)
        return int(u["runes"]) >= int(price)

    async def _pay(self, user_id: int, price: int) -> bool:
        """Try to pay `price` runes atomically. Returns True if paid."""
        if self._is_admin(user_id):
            return True
        repo = self.bot.repo  # type: ignore
        return bool(await repo.spend_runes(user_id, int(price)))

    async def _reply_ephemeral(self, interaction: discord.Interaction, text: str) -> None:
        # after defer -> followup
        try:
            await safe_send(interaction, text, ephemeral=True)
        except Exception:
            if not interaction.response.is_done():
                await safe_send(interaction, text, ephemeral=True)

    async def handle_purchase(self, interaction: discord.Interaction, kind: str, item_id: str) -> None:
        # ✅ Сразу подтверждаем (иначе 404 Unknown interaction)
        try:
            if not interaction.response.is_done():
                await safe_defer_ephemeral(interaction)
        except Exception:
            return

        cfg = self.bot.cfg  # type: ignore
        repo = self.bot.repo  # type: ignore

        user_id = interaction.user.id
        u = await self._get_user(user_id)
        level = int(u["level"])
        runes = int(u["runes"])
        shop_cfg = cfg.get("shop", {})

        # ---------- TOKEN ----------
        if kind == "token":
            item = _find_item(shop_cfg.get("tokens", []), item_id)
            if not item:
                await self._reply_ephemeral(interaction, "❌ Этот жетон исчез в тумане.")
                return

            unlock_level = int(item.get("unlock_level", 9999))
            price = int(item.get("price", 999999))
            role_id = int(item.get("role_id", 0))

            if not self._is_admin(user_id) and level < unlock_level:
                await self._reply_ephemeral(
                    interaction,
                    f"⛔ Тебе рано.\nНужно: **ур.{unlock_level}+**\nУ тебя: **{level}**",
                )
                return

            if await repo.purchase_has(user_id, "token", item_id):
                await self._reply_ephemeral(interaction, "Ты уже купил это воспоминание.")
                return

            if not await self._can_pay(user_id, price):
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes}/{price}**.")
                return

            paid = await self._pay(user_id, price)
            if not paid:
                u2 = await self._get_user(user_id)
                runes2 = int(u2.get('runes', 0))
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes2}/{price}**.")
                return
            await repo.purchase_add(user_id, "token", item_id)

            g = interaction.guild
            if g and role_id:
                role = g.get_role(role_id)
                if role:
                    try:
                        await interaction.user.add_roles(role, reason="shop token")
                    except Exception:
                        pass

            await self._reply_ephemeral(interaction, f"✅ Жетон получен: **{item['label']}**.")
            return

        # ---------- SOUND ----------
        if kind == "sound":
            # Покупка отдельных звуков отключена: используем роль доступа к Soundboard
            if not shop_cfg.get("sounds"):
                await self._reply_ephemeral(
                    interaction,
                    "🔊 Покупка отдельных звуков отключена. Купи **доступ к звуковой панели** в разделе жетонов.",
                )
                return
            item = _find_item(shop_cfg.get("sounds", []), item_id)
            if not item:
                await self._reply_ephemeral(interaction, "❌ Этот звук утонул в пустоте.")
                return

            unlock_level = int(item.get("unlock_level", 9999))
            price = int(item.get("price", 999999))

            if not self._is_admin(user_id) and level < unlock_level:
                await self._reply_ephemeral(
                    interaction,
                    f"⛔ Тебе рано.\nНужно: **ур.{unlock_level}+**\nУ тебя: **{level}**",
                )
                return

            if await repo.purchase_has(user_id, "sound", item_id):
                await self._reply_ephemeral(interaction, "Этот звук уже твой.")
                return

            if not await self._can_pay(user_id, price):
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes}/{price}**.")
                return

            paid = await self._pay(user_id, price)
            if not paid:
                u2 = await self._get_user(user_id)
                runes2 = int(u2.get('runes', 0))
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes2}/{price}**.")
                return
            await repo.purchase_add(user_id, "sound", item_id)

            await self._reply_ephemeral(interaction, f"✅ Куплено: **{item['label']}**.")
            return

        # ---------- CUSTOM ROLE ----------
        if kind == "custom_role":
            price = int(shop_cfg.get("custom_role_price", 80))

            cd_until = await repo.cd_get(user_id, "custom_role")
            now = int(time.time())
            if not self._is_admin(user_id) and cd_until > now:
                mins = (cd_until - now) // 60 + 1
                await self._reply_ephemeral(interaction, f"⏳ Подожди ~{mins} мин.")
                return

            if not await self._can_pay(user_id, price):
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes}/{price}**.")
                return

            paid = await self._pay(user_id, price)
            if not paid:
                u2 = await self._get_user(user_id)
                runes2 = int(u2.get('runes', 0))
                await self._reply_ephemeral(interaction, f"Не хватает рун: **{runes2}/{price}**.")
                return
            await repo.cd_set(user_id, "custom_role", 60 * 60)

            admin_ch_id = int(cfg["channels"]["admin_panel"])
            g = interaction.guild
            admin_ch = g.get_channel(admin_ch_id) if g else None

            if isinstance(admin_ch, discord.TextChannel):
                await admin_ch.send(
                    f"🧾 **Запрос на кастомную роль**\n"
                    f"Покупатель: {interaction.user.mention} (`{interaction.user.id}`)\n"
                    f"Цена: {price} рун\n"
                    f"Жду название роли (в админ-канале/ЛС), создай и выдай вручную.",
                    allowed_mentions=discord.AllowedMentions(users=True),
                )

            await self._reply_ephemeral(interaction, "✅ Запрос отправлен Микеле.")
            return

    @commands.command(name="post_shop")
    async def post_shop(self, ctx: commands.Context) -> None:
        if not self.bot.is_admin(ctx.author.id):  # type: ignore
            return

        cfg = self.bot.cfg  # type: ignore
        ch = ctx.guild.get_channel(int(cfg["channels"]["shop"])) if ctx.guild else None
        if not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Канал магазина не найден.")
            return

        shop_cfg = cfg.get("shop", {})
        tokens = shop_cfg.get("tokens", [])
        sounds = shop_cfg.get("sounds", [])
        role_price = int(shop_cfg.get("custom_role_price", 80))

        posted = 0

        tok_lines = [
            f"• 🎴 **{t['label']}** — {_fmt_unlock(int(t.get('unlock_level', 0)))} — {_fmt_price(int(t.get('price', 0)))}"
            for t in tokens
        ]

        emb1 = discord.Embed(
            title="🎴 Жетоны Пустоты",
            description="Покупка выдаёт роль. В том числе — доступ к звуковой панели.",
        )
        emb1.add_field(name="Список", value="\n".join(tok_lines) if tok_lines else "—", inline=False)
        m1 = await ch.send(embed=emb1, view=self.tokens_view)
        try:
            await m1.pin()
        except Exception:
            pass


        posted += 1

        if sounds:
            emb2 = discord.Embed(
                title="🔊 Звуки",
                description="Покупка закрепляет звук за тобой. Проигрывание (алтарь + очередь + кд) — отдельным блоком.",
            )
            emb2.add_field(name="Правило", value="Цена/уровень — на кнопках.", inline=False)
            m2 = await ch.send(embed=emb2, view=self.sounds_view)
            try:
                await m2.pin()
            except Exception:
                pass

            posted += 1

        emb3 = discord.Embed(
            title="🪪 Своя роль",
            description="Ты платишь — Пустота передаёт Микеле просьбу. Название роли напишешь в админ-канал/ЛС.",
        )
        emb3.add_field(name="Цена", value=_fmt_price(role_price), inline=False)
        m3 = await ch.send(embed=emb3, view=self.custom_role_view)
        try:
            await m3.pin()
        except Exception:
            pass


        posted += 1

        await ctx.send(f"✅ Магазин опубликован **{posted}** сообщениями и закреплён.")


def get_persistent_views(bot: commands.Bot):
    """Persistent кнопки магазина (чтобы работали после перезапуска)."""
    cfg = bot.cfg  # type: ignore
    shop_cfg = cfg.get("shop", {}) if cfg else {}
    sounds = list(shop_cfg.get("sounds", []))[:15]

    views = [TokensView(bot)]
    if sounds:
        views.append(SoundsView(bot))
    views.append(CustomRoleView(bot))
    return views



async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ShopCog(bot))

