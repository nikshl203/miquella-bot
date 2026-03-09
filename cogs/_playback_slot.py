from __future__ import annotations

from typing import Any, Optional

from discord.ext import commands

PLAYBACK_MODE_MUSIC = "music"
PLAYBACK_MODE_STORIES = "stories"


def _music_cog(bot: commands.Bot):
    return bot.get_cog("MusicCog")


def _stories_cog(bot: commands.Bot):
    return bot.get_cog("StoryHallCog")


def get_active_playback(bot: commands.Bot) -> Optional[dict[str, Any]]:
    music = _music_cog(bot)
    music_session = getattr(music, "session", None) if music else None
    if music_session is not None:
        return {
            "mode": PLAYBACK_MODE_MUSIC,
            "cog": music,
            "session": music_session,
            "voice_channel_id": int(getattr(music_session, "voice_channel_id", 0) or 0),
        }

    stories = _stories_cog(bot)
    story_session = getattr(stories, "session", None) if stories else None
    if story_session is not None:
        return {
            "mode": PLAYBACK_MODE_STORIES,
            "cog": stories,
            "session": story_session,
            "voice_channel_id": int(getattr(story_session, "voice_channel_id", 0) or 0),
        }

    return None


def conflict_text_for_request(bot: commands.Bot, requested_mode: str) -> str:
    active = get_active_playback(bot)
    if not active:
        return ""
    if str(active.get("mode") or "") == str(requested_mode or ""):
        return ""

    if requested_mode == PLAYBACK_MODE_STORIES:
        return "Сейчас Алтарь уже звучит музыкой. Останови музыку, чтобы открыть Зал историй."
    if requested_mode == PLAYBACK_MODE_MUSIC:
        return "Сейчас в голосовом звучит история. Останови ее, чтобы вернуть музыку."
    return "Сейчас voice-slot занят другим режимом."
