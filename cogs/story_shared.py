from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import discord
from discord.ext import commands

ROOT = Path(__file__).resolve().parents[1]


def cfg(bot: commands.Bot) -> Dict[str, Any]:
    return getattr(bot, "cfg", {})  # type: ignore


def repo(bot: commands.Bot):
    return getattr(bot, "repo", None)  # type: ignore


@dataclass
class Ch1Attempt:
    name_delta: int = 0
    evidence_delta: int = 0
    vow: int = 0
    trust_delta: int = 0
    mask: int = 0
    gro_seed: int = 0
    rher_seed: int = 0
    vow_lock: str = ""
    evidence: int = 0
    secret_unlocked: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(s: str) -> "Ch1Attempt":
        try:
            d = json.loads(s or "{}")
            return Ch1Attempt(
                name_delta=int(d.get("name_delta", 0)),
                evidence_delta=int(d.get("evidence_delta", 0)),
                vow=int(d.get("vow", 0)),
                trust_delta=int(d.get("trust_delta", 0)),
                mask=int(d.get("mask", 0)),
                gro_seed=int(d.get("gro_seed", 0)),
                rher_seed=int(d.get("rher_seed", 0)),
                vow_lock=str(d.get("vow_lock", "")),
                evidence=int(d.get("evidence", 0)),
                secret_unlocked=int(d.get("secret_unlocked", 0)),
            )
        except Exception:
            return Ch1Attempt()


async def load_attempt(bot: commands.Bot, user_id: int) -> Ch1Attempt:
    r = repo(bot)
    st = await r.get_story(user_id)
    return Ch1Attempt.from_json(st.get("attempt_json", ""))


async def save_attempt(bot: commands.Bot, user_id: int, att: Ch1Attempt) -> None:
    r = repo(bot)
    await r.set_story_fields(user_id, attempt_chapter=1, attempt_json=att.to_json(), updated_ts=int(time.time()))


async def disable_buttons(interaction: discord.Interaction) -> bool:
    if not interaction.message:
        return False

    view = discord.ui.View.from_message(interaction.message)
    changed = False
    for item in view.children:
        if isinstance(item, discord.ui.Button) and not item.disabled:
            item.disabled = True
            changed = True

    if not changed:
        return True

    try:
        await interaction.message.edit(view=view)
        return True
    except Exception:
        return False


async def is_my_story_thread(bot: commands.Bot, interaction: discord.Interaction) -> bool:
    """Проверяем, что пользователь кликает внутри своего личного треда истории."""
    try:
        if not interaction.user or not interaction.channel:
            return False
        if not isinstance(interaction.channel, discord.Thread):
            return False
        r = repo(bot)
        st = await r.get_story(interaction.user.id)
        return int(st.get("thread_id", 0)) == int(interaction.channel.id)
    except Exception:
        return False


def _is_private_thread(th: discord.Thread) -> bool:
    return th.type == discord.ChannelType.private_thread


async def _resolve_thread(bot: commands.Bot, thread_id: int) -> Optional[discord.Thread]:
    if not thread_id:
        return None
    th = bot.get_channel(int(thread_id))
    if isinstance(th, discord.Thread):
        return th
    try:
        fetched = await bot.fetch_channel(int(thread_id))
        if isinstance(fetched, discord.Thread):
            return fetched
    except Exception:
        return None
    return None


async def _archive_public_thread(thread: discord.Thread) -> None:
    if _is_private_thread(thread):
        return
    try:
        await thread.delete(reason="Migrated story to private thread")
        return
    except Exception:
        pass
    try:
        await thread.edit(archived=True, locked=True, reason="Migrated story to private thread")
    except Exception:
        pass


async def _ensure_private_membership(
    thread: discord.Thread,
    member: discord.Member,
    admin_user_id: int,
) -> None:
    # private thread participants: only player + admin (+ bot).
    try:
        await thread.add_user(member)
    except Exception:
        pass

    if int(admin_user_id) > 0 and int(admin_user_id) != int(member.id):
        admin_member = member.guild.get_member(int(admin_user_id))
        if admin_member:
            try:
                await thread.add_user(admin_member)
            except Exception:
                pass


async def _create_private_story_thread(ch: discord.TextChannel, target_name: str) -> discord.Thread:
    kwargs = {
        "name": target_name,
        "type": discord.ChannelType.private_thread,
        "auto_archive_duration": 10080,
        "reason": "Личная история игрока (private)",
    }
    try:
        return await ch.create_thread(invitable=False, **kwargs)
    except TypeError:
        # Some discord.py builds may not expose `invitable` kwarg.
        return await ch.create_thread(**kwargs)


async def get_or_create_story_thread(bot: commands.Bot, member: discord.Member) -> discord.Thread:
    c = cfg(bot)
    ch_id = c["channels"].get("story_threads")
    if not ch_id:
        raise RuntimeError("В config.json не задан channels.story_threads")

    ch = bot.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        raise RuntimeError("channels.story_threads должен быть текстовым каналом")

    r = repo(bot)
    admin_id = int(c.get("admin_user_id", 0))

    legacy_name = f"История — {member.display_name} — Сезон 1"
    target_name = f"История — S1 — {member.id}"

    # 1) If DB has thread_id, try it first.
    tid = 0
    try:
        st = await r.get_story(member.id)
        tid = int(st.get("thread_id", 0))
    except Exception:
        tid = 0

    if tid:
        th = await _resolve_thread(bot, tid)
        if isinstance(th, discord.Thread):
            if _is_private_thread(th):
                await _ensure_private_membership(th, member, admin_id)
                return th
            # Old public threads are archived for privacy.
            await _archive_public_thread(th)

    # 2) Fallback by thread name (active)
    public_matches: list[discord.Thread] = []
    for t in ch.threads:
        if t.name in (target_name, legacy_name):
            if _is_private_thread(t):
                try:
                    await r.set_story_fields(member.id, thread_id=int(t.id), updated_ts=int(time.time()))
                except Exception:
                    pass
                await _ensure_private_membership(t, member, admin_id)
                return t
            public_matches.append(t)

    # 3) Fallback by archived name
    try:
        archived = [t async for t in ch.archived_threads(limit=100)]
        for t in archived:
            if t.name in (target_name, legacy_name):
                if _is_private_thread(t):
                    try:
                        await r.set_story_fields(member.id, thread_id=int(t.id), updated_ts=int(time.time()))
                    except Exception:
                        pass
                    await _ensure_private_membership(t, member, admin_id)
                    return t
                public_matches.append(t)
    except Exception:
        pass

    for t in public_matches:
        await _archive_public_thread(t)

    # 4) Create a new private thread.
    t = await _create_private_story_thread(ch, target_name)
    await _ensure_private_membership(t, member, admin_id)
    try:
        await r.set_story_fields(member.id, thread_id=int(t.id), updated_ts=int(time.time()))
    except Exception:
        pass
    return t
