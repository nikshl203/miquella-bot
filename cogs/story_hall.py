from __future__ import annotations

import asyncio
import html
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

import discord
from discord.ext import commands, tasks

try:
    import imageio_ffmpeg
except Exception:  # pragma: no cover - optional at runtime
    imageio_ffmpeg = None

from ._interactions import GuardedView, safe_defer_ephemeral, safe_defer_update, safe_edit_message, safe_send
from ._playback_slot import PLAYBACK_MODE_STORIES, conflict_text_for_request


log = logging.getLogger("void.storyhall")

STORY_HALL_CHANNEL_ID = 1480699852500500560
IDLE_DISCONNECT_SECONDS = 5 * 60
SHOW_SEARCH_LIMIT = 8
SHOW_RANDOM_LIMIT = 12
EPISODE_LIST_LIMIT = 20
SEARCH_CACHE_TTL = 10 * 60
FEED_CACHE_TTL = 20 * 60

CATALOG_UNAVAILABLE_TEXT = "Внешние ленты сейчас молчат или отвечают слишком медленно. Попробуй позже."
NO_RANDOM_TEXT = "Зал не нашел подходящего выпуска в доступных лентах."
NO_RESUME_TEXT = "У тебя нет незавершенного выпуска, к которому можно вернуться."
SEARCH_PROGRESS_TEXT = "Зал вслушивается в дальние каталоги и ищет нужное шоу. Подожди немного."
RANDOM_STORY_PROGRESS_TEXT = "Зал перебирает случайные истории и ищет голос, который откроется первым."
RANDOM_PODCAST_PROGRESS_TEXT = "Зал раскрывает случайный подкаст и ищет первый полноценный выпуск."
COLLECTION_PROGRESS_TEXT = "Зал раскрывает подборку и собирает доступные голоса."
EPISODE_START_TEXT = "Зал открыл выпуск: **{title}**."
STOP_TEXT = "Голоса в Зале историй стихли."


CATEGORY_DEFS: dict[str, dict[str, Any]] = {
    "stories": {
        "label": "Истории",
        "queries": (
            "short stories podcast",
            "storytelling podcast",
            "audio drama podcast",
        ),
        "group": "stories",
    },
    "horror": {
        "label": "Страшные истории",
        "queries": (
            "horror stories podcast",
            "scary stories podcast",
            "creepypasta podcast",
        ),
        "group": "stories",
    },
    "fairy": {
        "label": "Сказки",
        "queries": (
            "fairy tales podcast",
            "bedtime stories podcast",
            "folk tales podcast",
        ),
        "group": "stories",
    },
    "podcasts": {
        "label": "Подкасты",
        "queries": (
            "interview podcast",
            "conversation podcast",
            "talk podcast",
        ),
        "group": "podcasts",
    },
    "educational": {
        "label": "Познавательное",
        "queries": (
            "science podcast",
            "history podcast",
            "educational podcast",
        ),
        "group": "podcasts",
    },
}

CATEGORY_ORDER = ("stories", "horror", "fairy", "podcasts", "educational")
RANDOM_STORY_KEYS = ("stories", "horror", "fairy")
RANDOM_PODCAST_KEYS = ("podcasts", "educational")


def _now() -> int:
    return int(time.time())


def _local_name(tag: str) -> str:
    return str(tag or "").rsplit("}", 1)[-1]


def _truncate(text: str, limit: int) -> str:
    text = str(text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _strip_html(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(text or ""))
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _fmt_duration(seconds: int) -> str:
    sec = max(0, int(seconds or 0))
    hours, rem = divmod(sec, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}:{minutes:02}:{seconds:02}"
    return f"{minutes}:{seconds:02}"


def _parse_duration(value: str) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    if raw.isdigit():
        return int(raw)
    parts = raw.split(":")
    try:
        nums = [int(part) for part in parts]
    except Exception:
        return 0
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    return 0


def _find_first_text(parent: ET.Element, *names: str) -> str:
    wanted = set(names)
    for child in list(parent):
        if _local_name(child.tag) not in wanted:
            continue
        text = "".join(child.itertext()).strip()
        if text:
            return _strip_html(text)
    return ""


def _find_first_attr(parent: ET.Element, name: str, attr: str) -> str:
    for child in list(parent):
        if _local_name(child.tag) != name:
            continue
        value = str(child.attrib.get(attr) or "").strip()
        if value:
            return value
    return ""


def _find_link(parent: ET.Element, *, enclosure: bool) -> str:
    for child in list(parent):
        if _local_name(child.tag) != "link":
            continue
        href = str(child.attrib.get("href") or "").strip()
        rel = str(child.attrib.get("rel") or "").strip().lower()
        if enclosure:
            if href and rel == "enclosure":
                return href
        else:
            if href and rel in {"", "alternate"}:
                return href
        if not child.attrib and not enclosure:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return ""


def _ffmpeg_path() -> str:
    if imageio_ffmpeg is not None:
        try:
            return str(imageio_ffmpeg.get_ffmpeg_exe())
        except Exception:
            pass
    return "ffmpeg"


@dataclass(frozen=True)
class StoryShow:
    title: str
    author: str
    feed_url: str
    artwork_url: str = ""
    webpage_url: str = ""


@dataclass(frozen=True)
class StoryEpisode:
    title: str
    show_title: str
    audio_url: str
    webpage_url: str
    artwork_url: str
    duration_sec: int
    guid: str
    feed_url: str
    category_key: str
    category_label: str

    @property
    def identity(self) -> str:
        return f"{self.feed_url}|{self.guid or self.audio_url}"


@dataclass
class StorySession:
    guild_id: int
    text_channel_id: int
    voice_channel_id: int
    started_by_user_id: int
    current_episode: Optional[StoryEpisode] = None
    state: str = "playing"
    idle_disconnect_at: int = 0
    resume_position_sec: int = 0
    play_started_monotonic: float = 0.0
    suppress_after_until: float = 0.0


class CatalogUnavailableError(RuntimeError):
    pass


class StorySearchModal(discord.ui.Modal, title="Найти шоу"):
    query = discord.ui.TextInput(
        label="Название шоу",
        placeholder="Например: horror stories, сказки, science podcast",
        max_length=120,
        required=True,
    )

    def __init__(self, cog: "StoryHallCog"):
        super().__init__(timeout=5 * 60)
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_show_search_submit(interaction, str(self.query))


class StoryHallView(GuardedView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Случайная история", style=discord.ButtonStyle.primary, custom_id="storyhall:random_story")
    async def random_story(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.play_random_story(interaction)

    @discord.ui.button(label="Случайный подкаст", style=discord.ButtonStyle.primary, custom_id="storyhall:random_podcast")
    async def random_podcast(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.play_random_podcast(interaction)

    @discord.ui.button(label="Подборки", style=discord.ButtonStyle.secondary, custom_id="storyhall:collections")
    async def collections(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.open_collections(interaction)

    @discord.ui.button(label="Найти шоу", style=discord.ButtonStyle.secondary, custom_id="storyhall:search")
    async def search(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.open_search_modal(interaction)

    @discord.ui.button(label="Продолжить", style=discord.ButtonStyle.secondary, custom_id="storyhall:resume")
    async def resume(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.resume_last_episode(interaction)

    @discord.ui.button(label="Стоп", style=discord.ButtonStyle.danger, custom_id="storyhall:stop")
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[StoryHallCog] = interaction.client.get_cog("StoryHallCog")  # type: ignore
        if cog:
            await cog.stop(interaction)


class CollectionCategorySelect(discord.ui.Select):
    def __init__(self, cog: "StoryHallCog"):
        options = [
            discord.SelectOption(label=CATEGORY_DEFS[key]["label"], value=key)
            for key in CATEGORY_ORDER
        ]
        super().__init__(
            placeholder="Выбери подборку",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog

    async def callback(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        await self.cog.open_category_shows(interaction, str(self.values[0]))


class CollectionView(GuardedView):
    def __init__(self, cog: "StoryHallCog"):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.add_item(CollectionCategorySelect(cog))

    def build_embed(self) -> discord.Embed:
        lines = [f"• {CATEGORY_DEFS[key]['label']}" for key in CATEGORY_ORDER]
        embed = discord.Embed(
            title="Подборки Зала",
            description="Выбери направление. Зал откроет несколько доступных шоу.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(name="Категории", value="\n".join(lines), inline=False)
        return embed


class ShowSelect(discord.ui.Select):
    def __init__(self, cog: "StoryHallCog", shows: list[StoryShow], *, category_key: str, category_label: str):
        options: list[discord.SelectOption] = []
        for idx, show in enumerate(shows[:25]):
            author = _truncate(show.author or "без автора", 90)
            options.append(
                discord.SelectOption(
                    label=_truncate(show.title, 100),
                    description=author,
                    value=str(idx),
                )
            )
        super().__init__(
            placeholder="Выбери шоу",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog
        self.shows = shows[:25]
        self.category_key = category_key
        self.category_label = category_label

    async def callback(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        show = self.shows[int(self.values[0])]
        await self.cog.open_show_episodes(
            interaction,
            show,
            category_key=self.category_key,
            category_label=self.category_label,
        )


class ShowResultsView(GuardedView):
    def __init__(
        self,
        cog: "StoryHallCog",
        shows: list[StoryShow],
        *,
        title: str,
        description: str,
        category_key: str = "",
        category_label: str = "",
    ):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.shows = shows[:25]
        self.title = title
        self.description = description
        self.category_key = category_key
        self.category_label = category_label
        if self.shows:
            self.add_item(
                ShowSelect(
                    cog,
                    self.shows,
                    category_key=category_key,
                    category_label=category_label,
                )
            )

    def build_embed(self) -> discord.Embed:
        lines = []
        for idx, show in enumerate(self.shows[:8], start=1):
            author = show.author or "без автора"
            lines.append(f"{idx}. **{show.title}**\n{author}")
        embed = discord.Embed(
            title=self.title,
            description=self.description,
            color=discord.Color.dark_teal(),
        )
        embed.add_field(
            name="Шоу",
            value="\n\n".join(lines) if lines else "Подходящих шоу не найдено.",
            inline=False,
        )
        return embed


class EpisodeSelect(discord.ui.Select):
    def __init__(self, cog: "StoryHallCog", episodes: list[StoryEpisode]):
        options: list[discord.SelectOption] = []
        for idx, episode in enumerate(episodes[:25]):
            desc = f"{episode.show_title} • {_fmt_duration(episode.duration_sec)}"
            options.append(
                discord.SelectOption(
                    label=_truncate(episode.title, 100),
                    description=_truncate(desc, 100),
                    value=str(idx),
                )
            )
        super().__init__(
            placeholder="Выбери выпуск",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog
        self.episodes = episodes[:25]

    async def callback(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        episode = self.episodes[int(self.values[0])]
        await self.cog.start_episode_from_choice(interaction, episode)


class EpisodeResultsView(GuardedView):
    def __init__(self, cog: "StoryHallCog", show: StoryShow, episodes: list[StoryEpisode], *, hint: str = ""):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.show = show
        self.episodes = episodes[:25]
        self.hint = hint
        if self.episodes:
            self.add_item(EpisodeSelect(cog, self.episodes))

    def build_embed(self) -> discord.Embed:
        lines = []
        for idx, episode in enumerate(self.episodes[:10], start=1):
            lines.append(f"{idx}. **{episode.title}**\n{_fmt_duration(episode.duration_sec)}")
        embed = discord.Embed(
            title=self.show.title,
            description=self.hint or "Выбери выпуск, который должен зазвучать в зале.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(
            name="Выпуски",
            value="\n\n".join(lines) if lines else "В этой ленте пока не открылось ни одного доступного выпуска.",
            inline=False,
        )
        if self.show.author:
            embed.add_field(name="Автор", value=self.show.author, inline=False)
        if self.show.artwork_url:
            embed.set_image(url=self.show.artwork_url)
        return embed


class StoryHallCog(commands.Cog, name="StoryHallCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.panel_view = StoryHallView(bot)
        self.session: Optional[StorySession] = None
        self.panel_message_id: int = 0
        self._last_panel_state: str = "silent"
        self._lock = asyncio.Lock()
        self._panel_lock = asyncio.Lock()
        self._ffmpeg = _ffmpeg_path()
        self._show_cache: dict[tuple[str, int], tuple[float, list[StoryShow]]] = {}
        self._feed_cache: dict[tuple[str, str, str, int], tuple[float, list[StoryEpisode]]] = {}
        self._recent_episode_ids: list[str] = []

    async def cog_load(self) -> None:
        if not self.panel_watcher.is_running():
            self.panel_watcher.start()
        asyncio.create_task(self._bootstrap_panel())

    async def cog_unload(self) -> None:
        if self.panel_watcher.is_running():
            self.panel_watcher.cancel()

    def _repo(self):
        return getattr(self.bot, "repo", None)

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

    def _altar_channel_id(self) -> int:
        cfg = getattr(self.bot, "cfg", {}) or {}  # type: ignore[attr-defined]
        channels = cfg.get("channels", {}) or {}
        return int(channels.get("story_altar", STORY_HALL_CHANNEL_ID))

    def _guild(self) -> Optional[discord.Guild]:
        gid = self._guild_id()
        return self.bot.get_guild(gid) if gid > 0 else None

    def _altar_channel(self) -> Optional[discord.TextChannel]:
        guild = self._guild()
        if not guild:
            return None
        channel = guild.get_channel(self._altar_channel_id())
        return channel if isinstance(channel, discord.TextChannel) else None

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

    def _remember_recent_episode(self, episode: StoryEpisode) -> None:
        key = episode.identity
        self._recent_episode_ids = [item for item in self._recent_episode_ids if item != key]
        self._recent_episode_ids.insert(0, key)
        del self._recent_episode_ids[20:]

    async def _bootstrap_panel(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(1)
        try:
            await self.ensure_panel_message()
        except Exception:
            log.exception("story hall bootstrap failed")

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
                if not self.bot.user or msg.author.id != self.bot.user.id:
                    continue
                if not msg.components:
                    continue
                found = False
                for row in msg.components:
                    for child in row.children:
                        if getattr(child, "custom_id", "").startswith("storyhall:"):
                            found = True
                            break
                    if found:
                        break
                if found:
                    self.panel_message_id = int(msg.id)
                    return msg

            msg = await channel.send(embed=self._make_panel_embed(), view=self.panel_view)
            self.panel_message_id = int(msg.id)
            try:
                await msg.pin()
            except Exception:
                pass
            return msg

    def _make_panel_embed(self) -> discord.Embed:
        session = self.session
        now = _now()
        if session is None and self._last_panel_state == "stopped":
            state_text = "Голоса были прерваны."
        elif session is None:
            state_text = "Зал безмолвен."
        elif session.current_episode is not None:
            state_text = "Сейчас говорит выбранный выпуск."
        elif session.state == "ended":
            state_text = "Выпуск завершен. Зал ждёт новый голос."
        else:
            state_text = "Голос стих, но зал еще удерживает эхо."

        embed = discord.Embed(
            title="Зал историй",
            description="Здесь звучат рассказы, беседы и чужие голоса.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(name="Состояние", value=state_text, inline=False)

        if session and session.current_episode is not None:
            episode = session.current_episode
            started_by = f"<@{int(session.started_by_user_id)}>" if session.started_by_user_id else "—"
            value = (
                f"**{episode.title}**\n"
                f"Шоу: **{episode.show_title}**\n"
                f"Категория: **{episode.category_label or '—'}**\n"
                f"Длительность: **{_fmt_duration(episode.duration_sec)}**\n"
                f"Начал: {started_by}"
            )
            embed.add_field(name="Сейчас звучит", value=value, inline=False)
            if episode.artwork_url:
                embed.set_image(url=episode.artwork_url)
        else:
            embed.add_field(name="Сейчас звучит", value="Зал молчит. Ни один выпуск еще не раскрыт.", inline=False)

        voice_ch = self._session_voice_channel()
        voice_text = voice_ch.mention if isinstance(voice_ch, (discord.VoiceChannel, discord.StageChannel)) else "—"
        embed.add_field(name="Зал, где звучит голос", value=voice_text, inline=False)

        if session and session.idle_disconnect_at > now:
            embed.add_field(
                name="Тишина уже подступает",
                value=f"Если никто не вернется, зал опустеет <t:{int(session.idle_disconnect_at)}:R>.",
                inline=False,
            )

        embed.set_footer(text="Один голосовой зал. Один рассказ за раз.")
        return embed

    async def _update_panel_message(self) -> None:
        msg = await self.ensure_panel_message()
        if not msg:
            return
        try:
            await msg.edit(embed=self._make_panel_embed(), view=self.panel_view)
        except Exception:
            log.exception("story hall panel update failed")

    async def _voice_access_check(
        self,
        interaction: discord.Interaction,
        *,
        require_active_session: bool = False,
        require_voice_if_inactive: bool = True,
        allow_admin_remote: bool = True,
    ) -> tuple[bool, Optional[discord.Member], str]:
        if not isinstance(interaction.user, discord.Member):
            return False, None, "Действие доступно только на сервере."

        member = interaction.user
        if self.session is None:
            conflict_text = conflict_text_for_request(self.bot, PLAYBACK_MODE_STORIES)
            if conflict_text:
                return False, member, conflict_text
            if require_active_session:
                return False, member, "Зал безмолвен. Сейчас нечего останавливать."
            if not require_voice_if_inactive:
                return True, member, ""
            voice = getattr(member, "voice", None)
            if voice is None or voice.channel is None:
                return False, member, "Чтобы открыть Зал историй, войди в голосовой канал."
            return True, member, ""

        if allow_admin_remote and self._is_admin(member.id):
            return True, member, ""

        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            return False, member, "Чтобы открыть Зал историй, войди в голосовой канал."
        if int(voice.channel.id) != int(self.session.voice_channel_id):
            return False, member, "История уже звучит в другом зале. Приди туда, где сейчас говорит Алтарь."
        return True, member, ""

    async def _ensure_voice_connection(self, member: discord.Member) -> None:
        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            raise RuntimeError("Чтобы открыть Зал историй, войди в голосовой канал.")
        target = voice.channel
        if not isinstance(target, (discord.VoiceChannel, discord.StageChannel)):
            raise RuntimeError("Нужен обычный голосовой или stage-канал.")

        vc = self._voice_client()
        if vc and vc.is_connected():
            if int(vc.channel.id) != int(target.id):
                raise RuntimeError("История уже звучит в другом зале. Приди туда, где сейчас говорит Алтарь.")
            return
        if vc is not None:
            try:
                await vc.disconnect(force=True)
            except Exception:
                pass
        await target.connect(timeout=20.0, reconnect=False, self_deaf=True)

    def _http_bytes_sync(self, url: str, *, accept: str = "*/*") -> bytes:
        req = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": accept,
            },
        )
        with urlopen(req, timeout=20) as response:
            return response.read()

    def _http_json_sync(self, url: str) -> dict[str, Any]:
        payload = self._http_bytes_sync(url, accept="application/json, text/plain, */*")
        import json

        return json.loads(payload.decode("utf-8"))

    async def _search_shows(self, query: str, *, limit: int = SHOW_SEARCH_LIMIT) -> list[StoryShow]:
        key = (str(query or "").strip().lower(), int(limit))
        cached = self._show_cache.get(key)
        now = time.monotonic()
        if cached and cached[0] > now:
            return list(cached[1])

        url = "https://itunes.apple.com/search?" + urlencode(
            {
                "media": "podcast",
                "entity": "podcast",
                "term": query,
                "limit": limit,
            }
        )
        try:
            data = await asyncio.to_thread(self._http_json_sync, url)
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            log.warning("story hall show search failed query=%r: %s", query, exc)
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT) from exc
        except Exception as exc:
            log.exception("story hall show search crashed query=%r: %s", query, exc)
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT) from exc

        shows: list[StoryShow] = []
        seen: set[str] = set()
        for item in data.get("results") or []:
            feed_url = str(item.get("feedUrl") or "").strip()
            if not feed_url or feed_url in seen:
                continue
            seen.add(feed_url)
            shows.append(
                StoryShow(
                    title=str(item.get("collectionName") or item.get("trackName") or "Безымянное шоу"),
                    author=str(item.get("artistName") or ""),
                    feed_url=feed_url,
                    artwork_url=str(item.get("artworkUrl600") or item.get("artworkUrl100") or ""),
                    webpage_url=str(item.get("collectionViewUrl") or item.get("trackViewUrl") or ""),
                )
            )
            if len(shows) >= limit:
                break

        self._show_cache[key] = (now + SEARCH_CACHE_TTL, shows)
        log.info("story hall show search query=%r results=%d", query, len(shows))
        return list(shows)

    async def _episodes_for_show(
        self,
        show: StoryShow,
        *,
        category_key: str,
        category_label: str,
        limit: int = EPISODE_LIST_LIMIT,
    ) -> list[StoryEpisode]:
        key = (show.feed_url, category_key, category_label, int(limit))
        cached = self._feed_cache.get(key)
        now = time.monotonic()
        if cached and cached[0] > now:
            return list(cached[1])

        try:
            episodes = await asyncio.to_thread(
                self._parse_feed_sync,
                show.feed_url,
                show.title,
                show.artwork_url,
                category_key,
                category_label,
                limit,
            )
        except (HTTPError, URLError, TimeoutError, ET.ParseError, ValueError) as exc:
            log.warning("story hall feed parse failed feed=%r: %s", show.feed_url, exc)
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT) from exc
        except Exception as exc:
            log.exception("story hall feed parse crashed feed=%r: %s", show.feed_url, exc)
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT) from exc

        self._feed_cache[key] = (now + FEED_CACHE_TTL, episodes)
        return list(episodes)

    def _parse_feed_sync(
        self,
        feed_url: str,
        fallback_show_title: str,
        fallback_artwork_url: str,
        category_key: str,
        category_label: str,
        limit: int,
    ) -> list[StoryEpisode]:
        payload = self._http_bytes_sync(
            feed_url,
            accept="application/rss+xml, application/xml, text/xml, application/atom+xml;q=0.9, */*;q=0.8",
        )
        root = ET.fromstring(payload)
        root_name = _local_name(root.tag)
        episodes: list[StoryEpisode] = []

        if root_name == "rss":
            channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), None)
            if channel is None:
                return []
            image_node = next((child for child in list(channel) if _local_name(child.tag) == "image"), None)
            channel_image = _find_first_text(image_node, "url") if image_node is not None else ""
            show_title = _find_first_text(channel, "title") or fallback_show_title
            channel_artwork = _find_first_attr(channel, "image", "href") or channel_image or fallback_artwork_url
            for item in list(channel):
                if _local_name(item.tag) != "item":
                    continue
                episode = self._rss_item_to_episode(
                    item,
                    show_title=show_title,
                    channel_artwork=channel_artwork,
                    feed_url=feed_url,
                    category_key=category_key,
                    category_label=category_label,
                )
                if episode:
                    episodes.append(episode)
                if len(episodes) >= limit:
                    break
            return episodes

        if root_name == "feed":
            show_title = _find_first_text(root, "title") or fallback_show_title
            channel_artwork = _find_first_attr(root, "logo", "href") or fallback_artwork_url
            for item in list(root):
                if _local_name(item.tag) != "entry":
                    continue
                episode = self._atom_entry_to_episode(
                    item,
                    show_title=show_title,
                    channel_artwork=channel_artwork,
                    feed_url=feed_url,
                    category_key=category_key,
                    category_label=category_label,
                )
                if episode:
                    episodes.append(episode)
                if len(episodes) >= limit:
                    break
        return episodes

    def _rss_item_to_episode(
        self,
        item: ET.Element,
        *,
        show_title: str,
        channel_artwork: str,
        feed_url: str,
        category_key: str,
        category_label: str,
    ) -> Optional[StoryEpisode]:
        title = _find_first_text(item, "title") or "Безымянный выпуск"
        guid = _find_first_text(item, "guid") or ""
        webpage_url = _find_first_text(item, "link") or ""
        duration_sec = _parse_duration(_find_first_text(item, "duration"))
        artwork_url = _find_first_attr(item, "image", "href") or channel_artwork
        audio_url = ""
        for child in list(item):
            name = _local_name(child.tag)
            if name == "enclosure":
                url = str(child.attrib.get("url") or "").strip()
                media_type = str(child.attrib.get("type") or "").lower()
                if url and (not media_type or "audio" in media_type):
                    audio_url = url
                    break
            if name == "content":
                url = str(child.attrib.get("url") or "").strip()
                media_type = str(child.attrib.get("type") or "").lower()
                if url and (not media_type or "audio" in media_type):
                    audio_url = url
                    break
        if not audio_url:
            return None
        return StoryEpisode(
            title=title,
            show_title=show_title,
            audio_url=audio_url,
            webpage_url=webpage_url,
            artwork_url=artwork_url,
            duration_sec=duration_sec,
            guid=guid,
            feed_url=feed_url,
            category_key=category_key,
            category_label=category_label,
        )

    def _atom_entry_to_episode(
        self,
        item: ET.Element,
        *,
        show_title: str,
        channel_artwork: str,
        feed_url: str,
        category_key: str,
        category_label: str,
    ) -> Optional[StoryEpisode]:
        title = _find_first_text(item, "title") or "Безымянный выпуск"
        guid = _find_first_text(item, "id") or ""
        webpage_url = _find_link(item, enclosure=False)
        audio_url = _find_link(item, enclosure=True)
        if not audio_url:
            return None
        duration_sec = _parse_duration(_find_first_text(item, "duration"))
        artwork_url = _find_first_attr(item, "image", "href") or channel_artwork
        return StoryEpisode(
            title=title,
            show_title=show_title,
            audio_url=audio_url,
            webpage_url=webpage_url,
            artwork_url=artwork_url,
            duration_sec=duration_sec,
            guid=guid,
            feed_url=feed_url,
            category_key=category_key,
            category_label=category_label,
        )

    async def _search_shows_for_category(self, category_key: str, *, limit: int = SHOW_SEARCH_LIMIT) -> list[StoryShow]:
        category = CATEGORY_DEFS[category_key]
        shows: list[StoryShow] = []
        seen: set[str] = set()
        had_failure = False
        for query in category["queries"]:
            try:
                batch = await self._search_shows(query, limit=limit)
            except CatalogUnavailableError:
                had_failure = True
                continue
            for show in batch:
                key = show.feed_url or show.webpage_url or show.title
                if key in seen:
                    continue
                seen.add(key)
                shows.append(show)
                if len(shows) >= limit:
                    return shows
        if not shows and had_failure:
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT)
        return shows

    async def _pick_random_episode_from_category(self, category_key: str) -> Optional[StoryEpisode]:
        category = CATEGORY_DEFS[category_key]
        queries = list(category["queries"])
        random.shuffle(queries)
        had_failure = False
        for query in queries:
            try:
                shows = await self._search_shows(query, limit=SHOW_RANDOM_LIMIT)
            except CatalogUnavailableError:
                had_failure = True
                continue
            if not shows:
                continue
            random.shuffle(shows)
            for show in shows[:SHOW_RANDOM_LIMIT]:
                try:
                    episodes = await self._episodes_for_show(
                        show,
                        category_key=category_key,
                        category_label=str(category["label"]),
                        limit=EPISODE_LIST_LIMIT,
                    )
                except CatalogUnavailableError:
                    had_failure = True
                    continue
                pool = [episode for episode in episodes if episode.audio_url]
                if not pool:
                    continue
                unseen = [episode for episode in pool if episode.identity not in self._recent_episode_ids]
                choice = random.choice(unseen or pool)
                self._remember_recent_episode(choice)
                return choice
        if had_failure:
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT)
        return None

    async def _pick_random_episode_from_group(self, category_keys: tuple[str, ...]) -> Optional[StoryEpisode]:
        keys = list(category_keys)
        random.shuffle(keys)
        had_failure = False
        for key in keys:
            try:
                episode = await self._pick_random_episode_from_category(key)
            except CatalogUnavailableError:
                had_failure = True
                continue
            if episode is not None:
                return episode
        if had_failure:
            raise CatalogUnavailableError(CATALOG_UNAVAILABLE_TEXT)
        return None

    async def open_collections(self, interaction: discord.Interaction) -> None:
        ok, _, text = await self._voice_access_check(
            interaction,
            require_active_session=False,
            require_voice_if_inactive=False,
        )
        if not ok and conflict_text_for_request(self.bot, PLAYBACK_MODE_STORIES):
            await safe_send(interaction, text, ephemeral=True)
            return
        view = CollectionView(self)
        await safe_send(interaction, embed=view.build_embed(), view=view, ephemeral=True)

    async def open_search_modal(self, interaction: discord.Interaction) -> None:
        ok, _, text = await self._voice_access_check(
            interaction,
            require_active_session=False,
            require_voice_if_inactive=False,
        )
        if not ok and conflict_text_for_request(self.bot, PLAYBACK_MODE_STORIES):
            await safe_send(interaction, text, ephemeral=True)
            return
        try:
            await interaction.response.send_modal(StorySearchModal(self))
        except Exception:
            await safe_send(interaction, "Не удалось открыть поиск шоу.", ephemeral=True)
