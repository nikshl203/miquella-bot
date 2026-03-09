from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import discord
from discord.ext import commands, tasks

from ._interactions import GuardedView, safe_defer_ephemeral, safe_send

try:
    from yt_dlp import YoutubeDL
except Exception:  # pragma: no cover - dependency is optional at runtime
    YoutubeDL = None

try:
    import imageio_ffmpeg
except Exception:  # pragma: no cover - dependency is optional at runtime
    imageio_ffmpeg = None


log = logging.getLogger("void.music")

SOUNDCLOUD_ALTAR_CHANNEL_ID = 1473328597606334496
IDLE_DISCONNECT_SECONDS = 5 * 60
QUEUE_PREVIEW_LIMIT = 5


@dataclass
class MusicTrack:
    title: str
    artist: str
    duration: int
    webpage_url: str
    stream_url: str
    requested_by_user_id: int
    source_name: str = "SoundCloud"


@dataclass
class MusicSession:
    guild_id: int
    text_channel_id: int
    voice_channel_id: int
    started_by_user_id: int
    queue: list[MusicTrack] = field(default_factory=list)
    current_track: Optional[MusicTrack] = None
    paused: bool = False
    state: str = "silent"
    idle_disconnect_at: int = 0
    suppress_after: bool = False


def _now() -> int:
    return int(time.time())


def _fmt_duration(seconds: int) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _is_soundcloud_url(raw: str) -> bool:
    try:
        host = (urlparse(str(raw).strip()).netloc or "").lower()
    except Exception:
        return False
    return "soundcloud.com" in host or "snd.sc" in host


def _ffmpeg_path() -> str:
    env_path = str(os.getenv("FFMPEG_PATH", "")).strip()
    if env_path:
        return env_path
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    if imageio_ffmpeg is not None:
        try:
            return str(imageio_ffmpeg.get_ffmpeg_exe())
        except Exception:
            return ""
    return ""


def _track_lines(track: MusicTrack, guild: Optional[discord.Guild]) -> str:
    requester = f"<@{track.requested_by_user_id}>"
    if guild:
        member = guild.get_member(int(track.requested_by_user_id))
        if member:
            requester = member.mention
    return (
        f"**{track.title}**\n"
        f"Исполнитель: **{track.artist}**\n"
        f"Длительность: **{_fmt_duration(track.duration)}**\n"
        f"Принес: {requester}\n"
        f"Источник: **{track.source_name}**"
    )


class AddTrackModal(discord.ui.Modal, title="Добавить трек"):
    query = discord.ui.TextInput(
        label="Ссылка SoundCloud или название трека",
        placeholder="Например: https://soundcloud.com/... или название трека",
        max_length=200,
        required=True,
    )

    def __init__(self, cog: "MusicCog"):
        super().__init__(timeout=5 * 60)
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_add_submit(interaction, str(self.query))


class MusicPanelView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Добавить трек", style=discord.ButtonStyle.primary, custom_id="music:add")
    async def add_track(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if not cog:
            await safe_send(interaction, "Музыкальный модуль недоступен.", ephemeral=True)
            return
        await cog.open_add_modal(interaction)

    @discord.ui.button(label="Пауза", style=discord.ButtonStyle.secondary, custom_id="music:pause")
    async def pause(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.pause(interaction)

    @discord.ui.button(label="Продолжить", style=discord.ButtonStyle.secondary, custom_id="music:resume")
    async def resume(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.resume(interaction)

    @discord.ui.button(label="Следующий", style=discord.ButtonStyle.secondary, custom_id="music:next")
    async def next_track(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.skip(interaction)

    @discord.ui.button(label="Стоп", style=discord.ButtonStyle.danger, custom_id="music:stop")
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.stop(interaction)

    @discord.ui.button(label="Обновить", style=discord.ButtonStyle.secondary, custom_id="music:refresh")
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.refresh_panel(interaction, announce=True)


class MusicCog(commands.Cog, name="MusicCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.panel_view = MusicPanelView(bot)
        self.session: Optional[MusicSession] = None
        self.panel_message_id: int = 0
        self._last_panel_state: str = "silent"
        self._lock = asyncio.Lock()
        self._panel_lock = asyncio.Lock()
        self._ffmpeg = _ffmpeg_path()

    async def cog_load(self) -> None:
        if not self.panel_watcher.is_running():
            self.panel_watcher.start()
        asyncio.create_task(self._bootstrap_panel())

    async def cog_unload(self) -> None:
        if self.panel_watcher.is_running():
            self.panel_watcher.cancel()

    def _is_admin(self, user_id: int) -> bool:
        try:
            return bool(self.bot.is_admin(int(user_id)))  # type: ignore[attr-defined]
        except Exception:
            return False

    def _guild_id(self) -> int:
        try:
            return int(getattr(self.bot, "cfg", {}).get("guild_id", 0))  # type: ignore[attr-defined]
        except Exception:
            return 0

    def _dependency_error(self) -> str:
        missing: list[str] = []
        if YoutubeDL is None:
            missing.append("`yt-dlp`")
        if not self._ffmpeg:
            missing.append("`ffmpeg`")
        if not missing:
            return ""
        return "Музыкальный модуль требует: " + ", ".join(missing) + "."

    def _altar_channel_id(self) -> int:
        cfg = getattr(self.bot, "cfg", {}) or {}  # type: ignore[attr-defined]
        channels = cfg.get("channels", {}) or {}
        return int(channels.get("sound_altar", SOUNDCLOUD_ALTAR_CHANNEL_ID))

    def _guild(self) -> Optional[discord.Guild]:
        gid = self._guild_id()
        return self.bot.get_guild(gid) if gid > 0 else None

    def _altar_channel(self) -> Optional[discord.TextChannel]:
        ch = self.bot.get_channel(self._altar_channel_id())
        return ch if isinstance(ch, discord.TextChannel) else None

    def _voice_client(self) -> Optional[discord.VoiceClient]:
        guild = self._guild()
        if not guild:
            return None
        return guild.voice_client

    def _session_voice_channel(self) -> Optional[discord.abc.Connectable]:
        guild = self._guild()
        if not guild or not self.session:
            return None
        return guild.get_channel(int(self.session.voice_channel_id))

    def _non_bot_listeners(self) -> list[discord.Member]:
        vc = self._voice_client()
        if not vc or not vc.channel:
            return []
        return [m for m in vc.channel.members if not m.bot]

    def _same_active_voice(self, member: discord.Member) -> bool:
        if not self.session:
            return False
        if self._is_admin(member.id):
            return True
        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            return False
        return int(voice.channel.id) == int(self.session.voice_channel_id)

    async def _bootstrap_panel(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(1)
        try:
            await self.ensure_panel_message()
        except Exception:
            log.exception("music bootstrap failed")

    async def ensure_panel_message(self) -> Optional[discord.Message]:
        async with self._panel_lock:
            channel = self._altar_channel()
            if not channel:
                return None

            if self.panel_message_id:
                try:
                    msg = await channel.fetch_message(int(self.panel_message_id))
                    return msg
                except Exception:
                    self.panel_message_id = 0

            async for msg in channel.history(limit=40):
                if msg.author.id != self.bot.user.id:  # type: ignore[union-attr]
                    continue
                if not msg.components:
                    continue
                found = False
                for row in msg.components:
                    for child in row.children:
                        if getattr(child, "custom_id", "").startswith("music:"):
                            found = True
                            break
                    if found:
                        break
                if found:
                    self.panel_message_id = int(msg.id)
                    return msg

            embed = self._make_panel_embed()
            msg = await channel.send(embed=embed, view=self.panel_view)
            self.panel_message_id = int(msg.id)
            try:
                await msg.pin()
            except Exception:
                pass
            return msg

    def _make_panel_embed(self) -> discord.Embed:
        guild = self._guild()
        session = self.session
        now = _now()
        dep_error = self._dependency_error()

        if session is None and self._last_panel_state == "stopped":
            state_text = "Поток звука прерван."
        elif session is None:
            state_text = "Алтарь безмолвен. Ни один отголосок еще не был принесен."
        elif session.paused:
            state_text = "Звук удержан в тишине."
        elif session.state == "stopped":
            state_text = "Поток звука прерван."
        elif session.current_track is not None:
            state_text = "Сейчас звучит"
        else:
            state_text = "Алтарь безмолвен. Ни один отголосок еще не был принесен."

        embed = discord.Embed(
            title="Алтарь звуков",
            description="Здесь Пустота принимает отголоски песен.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(name="Состояние", value=state_text, inline=False)

        if dep_error:
            embed.add_field(name="Модуль пока не может звучать", value=dep_error, inline=False)

        if session and session.current_track:
            embed.add_field(
                name="Сейчас звучит",
                value=_track_lines(session.current_track, guild),
                inline=False,
            )
        else:
            embed.add_field(
                name="Сейчас звучит",
                value="Алтарь безмолвен. Ни один отголосок еще не был принесен.",
                inline=False,
            )

        if session:
            voice_ch = self._session_voice_channel()
            voice_text = voice_ch.mention if isinstance(voice_ch, (discord.VoiceChannel, discord.StageChannel)) else "—"
        else:
            voice_text = "—"
        embed.add_field(name="Зал, где течет звук", value=voice_text, inline=False)

        if session and session.queue:
            lines: list[str] = []
            shown = session.queue[:QUEUE_PREVIEW_LIMIT]
            for idx, track in enumerate(shown, start=1):
                lines.append(f"{idx}. **{track.title}** — {track.artist} ({_fmt_duration(track.duration)})")
            rest = len(session.queue) - len(shown)
            if rest > 0:
                lines.append(f"...и еще **{rest}** треков")
            queue_text = "\n".join(lines)
        else:
            queue_text = "Очередь пуста."
        embed.add_field(name="Следом зазвучат", value=queue_text, inline=False)

        if session and session.idle_disconnect_at > now:
            embed.add_field(
                name="Тишина уже подкралась",
                value=f"Алтарь опустел. Через пять минут тишина поглотит этот поток. Отключение {_discord_rel(session.idle_disconnect_at)}.",
                inline=False,
            )

        embed.set_footer(text="Один сервер. Одна очередь. Один поток звука.")
        return embed

    async def _update_panel_message(self) -> None:
        msg = await self.ensure_panel_message()
        if not msg:
            return
        try:
            await msg.edit(embed=self._make_panel_embed(), view=self.panel_view)
        except Exception:
            log.exception("music panel update failed")

    async def refresh_panel(self, interaction: discord.Interaction, *, announce: bool = False) -> None:
        await safe_defer_ephemeral(interaction)
        await self._update_panel_message()
        if announce:
            await safe_send(interaction, "Панель Алтаря обновлена.", ephemeral=True)

    async def _voice_access_check(
        self,
        interaction: discord.Interaction,
        *,
        require_active_session: bool = False,
        allow_admin_remote: bool = True,
    ) -> tuple[bool, Optional[discord.Member], str]:
        if not isinstance(interaction.user, discord.Member):
            return False, None, "Действие доступно только на сервере."

        member = interaction.user
        if self.session is None:
            if require_active_session:
                return False, member, "Алтарь безмолвен. Сначала принеси первый трек."
            voice = getattr(member, "voice", None)
            if voice is None or voice.channel is None:
                return False, member, "Чтобы призвать музыку, войди в голосовой канал."
            return True, member, ""

        if allow_admin_remote and self._is_admin(member.id):
            return True, member, ""

        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            return False, member, "Чтобы управлять музыкой, войди в голосовой канал."
        if int(voice.channel.id) != int(self.session.voice_channel_id):
            return False, member, "Алтарь уже звучит в другом зале. Приди туда, где уже течет поток звука."
        return True, member, ""

    async def open_add_modal(self, interaction: discord.Interaction) -> None:
        ok, _, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        dep_error = self._dependency_error()
        if dep_error:
            await safe_send(interaction, dep_error, ephemeral=True)
            return
        await interaction.response.send_modal(AddTrackModal(self))

    async def handle_add_submit(self, interaction: discord.Interaction, raw_query: str) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return

        query = str(raw_query or "").strip()
        if not query:
            await safe_send(interaction, "Нужна ссылка SoundCloud или поисковый запрос.", ephemeral=True)
            return

        dep_error = self._dependency_error()
        if dep_error:
            await safe_send(interaction, dep_error, ephemeral=True)
            return

        async with self._lock:
            if self.session is not None and not self._same_active_voice(member):
                await safe_send(
                    interaction,
                    "Алтарь уже звучит в другом зале. Приди туда, где уже течет поток звука.",
                    ephemeral=True,
                )
                return

            if self.session is None:
                voice = getattr(member, "voice", None)
                if voice is None or voice.channel is None:
                    await safe_send(interaction, "Чтобы призвать музыку, войди в голосовой канал.", ephemeral=True)
                    return
                self.session = MusicSession(
                    guild_id=member.guild.id,
                    text_channel_id=self._altar_channel_id(),
                    voice_channel_id=voice.channel.id,
                    started_by_user_id=member.id,
                    state="playing",
                )
                self._last_panel_state = "playing"

            try:
                track = await self._resolve_track(query, requested_by_user_id=member.id)
            except Exception as exc:
                await safe_send(interaction, f"Не удалось призвать трек: {exc}", ephemeral=True)
                return

            try:
                await self._ensure_voice_connection(member)
            except Exception as exc:
                if self.session and self.session.current_track is None and not self.session.queue:
                    self.session = None
                await safe_send(interaction, f"Не удалось войти в голосовой канал: {exc}", ephemeral=True)
                await self._update_panel_message()
                return

            session = self.session
            assert session is not None
            session.idle_disconnect_at = 0
            session.state = "playing"
            self._last_panel_state = "playing"

            if session.current_track is None and not self._voice_client_playing():
                session.current_track = track
                await self._play_current_locked()
                await safe_send(interaction, f"Алтарь принял трек: **{track.title}**.", ephemeral=True)
            else:
                session.queue.append(track)
                await safe_send(
                    interaction,
                    f"Трек добавлен в очередь: **{track.title}**. Позиция: **{len(session.queue)}**.",
                    ephemeral=True,
                )

        await self._update_panel_message()

    async def pause(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            vc = self._voice_client()
            if not vc or not vc.is_connected() or not vc.is_playing():
                await safe_send(interaction, "Сейчас нечего удерживать в тишине.", ephemeral=True)
                return
            vc.pause()
            assert self.session is not None
            self.session.paused = True
            self.session.state = "paused"
            self._last_panel_state = "paused"
        await self._update_panel_message()
        await safe_send(interaction, "Звук удержан в тишине.", ephemeral=True)

    async def resume(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            vc = self._voice_client()
            if not vc or not vc.is_connected() or not vc.is_paused():
                await safe_send(interaction, "Поток звука сейчас не удержан паузой.", ephemeral=True)
                return
            vc.resume()
            assert self.session is not None
            self.session.paused = False
            self.session.state = "playing"
            self._last_panel_state = "playing"
        await self._update_panel_message()
        await safe_send(interaction, "Поток снова течет.", ephemeral=True)

    async def skip(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            vc = self._voice_client()
            if not vc or not vc.is_connected():
                await safe_send(interaction, "Алтарь сейчас не связан с голосовым залом.", ephemeral=True)
                return
            if not (vc.is_playing() or vc.is_paused()):
                await safe_send(interaction, "Следующий отголосок звать пока не из чего.", ephemeral=True)
                return
            if self.session:
                self.session.paused = False
                self.session.state = "playing"
            vc.stop()
        await safe_send(interaction, "Текущий отголосок отпущен. Алтарь зовет следующий.", ephemeral=True)

    async def stop(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            session = self.session
            if session is None:
                await safe_send(interaction, "Алтарь и так безмолвен.", ephemeral=True)
                return
            session.suppress_after = True
            session.queue.clear()
            session.current_track = None
            session.paused = False
            session.state = "stopped"
            session.idle_disconnect_at = 0
            self._last_panel_state = "stopped"
            vc = self._voice_client()
            if vc and vc.is_connected():
                try:
                    vc.stop()
                except Exception:
                    pass
                try:
                    await vc.disconnect(force=True)
                except Exception:
                    pass
        self.session = None
        await self._update_panel_message()
        await safe_send(interaction, "Поток звука прерван.", ephemeral=True)

    async def _resolve_track(self, query: str, *, requested_by_user_id: int) -> MusicTrack:
        if YoutubeDL is None:
            raise RuntimeError("yt-dlp не установлен")
        target = query
        if _is_soundcloud_url(query):
            target = query
        elif "://" in query:
            raise RuntimeError("Первая версия принимает только SoundCloud.")
        else:
            target = f"scsearch1:{query}"

        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "noplaylist": True,
            "format": "bestaudio/best",
            "default_search": "scsearch",
            "source_address": "0.0.0.0",
        }

        def _extract() -> MusicTrack:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(target, download=False)
            entry = info
            if isinstance(info, dict) and info.get("entries"):
                entry = next((x for x in info["entries"] if x), None)
            if not entry:
                raise RuntimeError("SoundCloud не вернул подходящий трек.")

            webpage_url = str(entry.get("webpage_url") or "")
            original_url = str(entry.get("original_url") or "")
            extractor_key = str(entry.get("extractor_key") or entry.get("extractor") or "").lower()
            if (
                "soundcloud" not in extractor_key
                and not _is_soundcloud_url(webpage_url)
                and not _is_soundcloud_url(original_url)
            ):
                raise RuntimeError("Первая версия принимает только треки SoundCloud.")

            stream_url = str(entry.get("url") or "")
            if not stream_url:
                raise RuntimeError("Не удалось получить поток трека.")

            title = str(entry.get("title") or "Безымянный отголосок")
            artist = str(entry.get("uploader") or entry.get("artist") or "Неизвестный исполнитель")
            duration = int(entry.get("duration") or 0)
            return MusicTrack(
                title=title,
                artist=artist,
                duration=duration,
                webpage_url=webpage_url or original_url or target,
                stream_url=stream_url,
                requested_by_user_id=requested_by_user_id,
            )

        return await asyncio.to_thread(_extract)

    async def _ensure_voice_connection(self, member: discord.Member) -> None:
        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            raise RuntimeError("Пользователь не находится в голосовом канале.")
        target = voice.channel
        if not isinstance(target, (discord.VoiceChannel, discord.StageChannel)):
            raise RuntimeError("Нужен обычный голосовой или stage-канал.")

        vc = self._voice_client()
        if vc and vc.is_connected():
            if int(vc.channel.id) != int(target.id):
                raise RuntimeError("Алтарь уже звучит в другом зале.")
            return
        if vc is not None:
            try:
                await vc.disconnect(force=True)
            except Exception:
                pass
        await target.connect(timeout=20.0, reconnect=False, self_deaf=True)

    def _voice_client_playing(self) -> bool:
        vc = self._voice_client()
        if not vc or not vc.is_connected():
            return False
        return bool(vc.is_playing() or vc.is_paused())

    async def _play_current_locked(self) -> None:
        session = self.session
        vc = self._voice_client()
        if session is None or session.current_track is None or vc is None or not vc.is_connected():
            return
        if not self._ffmpeg:
            raise RuntimeError("ffmpeg не найден")

        source = discord.FFmpegPCMAudio(
            session.current_track.stream_url,
            executable=self._ffmpeg,
            before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
            options="-vn",
        )
        session.paused = False
        session.state = "playing"
        self._last_panel_state = "playing"

        def _after_playback(error: Optional[Exception]) -> None:
            loop = self.bot.loop
            loop.call_soon_threadsafe(asyncio.create_task, self._after_track_finished(error))

        vc.play(source, after=_after_playback)

    async def _after_track_finished(self, error: Optional[Exception]) -> None:
        if error:
            log.warning("music playback after-callback error: %s", error)
        async with self._lock:
            session = self.session
            if session is None:
                return
            if session.suppress_after:
                return
            if session.queue:
                session.current_track = session.queue.pop(0)
                try:
                    await self._play_current_locked()
                except Exception:
                    log.exception("music next-track start failed")
                    await self._close_session_locked(state="silent")
            else:
                await self._close_session_locked(state="silent")
        await self._update_panel_message()

    async def _close_session_locked(self, *, state: str) -> None:
        session = self.session
        vc = self._voice_client()
        if session:
            session.suppress_after = True
            session.queue.clear()
            session.current_track = None
            session.paused = False
            session.idle_disconnect_at = 0
            session.state = state
        self._last_panel_state = state
        if vc and vc.is_connected():
            try:
                vc.stop()
            except Exception:
                pass
            try:
                await vc.disconnect(force=True)
            except Exception:
                pass
        self.session = None

    async def _update_idle_deadline(self) -> None:
        session = self.session
        if session is None:
            return
        listeners = self._non_bot_listeners()
        if listeners:
            if session.idle_disconnect_at != 0:
                session.idle_disconnect_at = 0
                await self._update_panel_message()
            return
        vc = self._voice_client()
        if not vc or not vc.is_connected():
            return
        if session.idle_disconnect_at == 0:
            session.idle_disconnect_at = _now() + IDLE_DISCONNECT_SECONDS
            await self._update_panel_message()

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if self.session is None:
            return
        if int(member.guild.id) != int(self.session.guild_id):
            return
        tracked_id = int(self.session.voice_channel_id)
        before_id = int(before.channel.id) if before.channel else 0
        after_id = int(after.channel.id) if after.channel else 0
        if tracked_id not in {before_id, after_id}:
            return
        if member.id == self.bot.user.id and after.channel is None:  # type: ignore[union-attr]
            async with self._lock:
                if self.session is not None:
                    self._last_panel_state = "silent"
                    self.session = None
            await self._update_panel_message()
            return
        await self._update_idle_deadline()

    @tasks.loop(seconds=10)
    async def panel_watcher(self) -> None:
        if not self.bot.is_ready():
            return
        if self.session is None:
            return
        async with self._lock:
            session = self.session
            if session is None:
                return
            if session.idle_disconnect_at and _now() >= int(session.idle_disconnect_at):
                await self._close_session_locked(state="silent")
        await self._update_panel_message()

    @panel_watcher.before_loop
    async def panel_watcher_before_loop(self) -> None:
        await self.bot.wait_until_ready()

    @commands.command(name="post_music_panel")
    async def post_music_panel(self, ctx: commands.Context) -> None:
        if not self._is_admin(ctx.author.id):
            return
        msg = await self.ensure_panel_message()
        if not msg:
            await ctx.reply("Не удалось найти канал Алтаря звуков.")
            return
        await self._update_panel_message()
        await ctx.reply(f"Музыкальная панель готова: канал <#{self._altar_channel_id()}>.")


def _discord_rel(ts: int) -> str:
    return f"<t:{int(ts)}:R>"


def get_persistent_views(bot: commands.Bot) -> list[discord.ui.View]:
    return [MusicPanelView(bot)]


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MusicCog(bot))
