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
            "истории подкаст",
            "аудиорассказы",
            "рассказы вслух",
        ),
        "group": "stories",
    },
    "horror": {
        "label": "Страшные истории",
        "queries": (
            "страшные истории подкаст",
            "мистические истории подкаст",
            "ужасы подкаст",
        ),
        "group": "stories",
    },
    "fairy": {
        "label": "Сказки",
        "queries": (
            "сказки подкаст",
            "русские сказки подкаст",
            "сказки для детей подкаст",
        ),
        "group": "stories",
    },
    "podcasts": {
        "label": "Подкасты",
        "queries": (
            "русский подкаст",
            "разговорный подкаст",
            "подкаст интервью",
        ),
        "group": "podcasts",
    },
    "educational": {
        "label": "Познавательное",
        "queries": (
            "научпоп подкаст",
            "история подкаст",
            "образовательный подкаст",
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


def _has_cyrillic(text: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", str(text or "")))


def _is_russian_lang(value: str) -> bool:
    raw = str(value or "").strip().lower().replace("_", "-")
    return raw.startswith("ru")


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
        self._feed_language_cache: dict[str, tuple[float, bool]] = {}
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

    def _feed_is_russian_sync(self, feed_url: str) -> bool:
        cached = self._feed_language_cache.get(feed_url)
        now = time.monotonic()
        if cached and cached[0] > now:
            return bool(cached[1])

        payload = self._http_bytes_sync(
            feed_url,
            accept="application/rss+xml, application/xml, text/xml, application/atom+xml;q=0.9, */*;q=0.8",
        )
        root = ET.fromstring(payload)
        root_name = _local_name(root.tag)

        result = False
        if root_name == "rss":
            channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), None)
            if channel is not None:
                language = _find_first_text(channel, "language")
                if _is_russian_lang(language):
                    result = True
                else:
                    probe_parts = [_find_first_text(channel, "title"), _find_first_text(channel, "description")]
                    item_titles: list[str] = []
                    for child in list(channel):
                        if _local_name(child.tag) != "item":
                            continue
                        title = _find_first_text(child, "title")
                        if title:
                            item_titles.append(title)
                        if len(item_titles) >= 3:
                            break
                    result = any(_has_cyrillic(part) for part in [*probe_parts, *item_titles] if part)
        elif root_name == "feed":
            xml_lang = str(root.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") or "").strip()
            if _is_russian_lang(xml_lang):
                result = True
            else:
                probe_parts = [_find_first_text(root, "title"), _find_first_text(root, "subtitle")]
                entry_titles: list[str] = []
                for child in list(root):
                    if _local_name(child.tag) != "entry":
                        continue
                    title = _find_first_text(child, "title")
                    if title:
                        entry_titles.append(title)
                    if len(entry_titles) >= 3:
                        break
                result = any(_has_cyrillic(part) for part in [*probe_parts, *entry_titles] if part)

        self._feed_language_cache[feed_url] = (now + FEED_CACHE_TTL, result)
        return result

    async def _filter_russian_shows(self, shows: list[StoryShow], *, limit: int) -> list[StoryShow]:
        filtered: list[StoryShow] = []
        for show in shows:
            try:
                is_russian = await asyncio.to_thread(self._feed_is_russian_sync, show.feed_url)
            except Exception as exc:
                log.info("story hall skipped show feed=%r language_probe_failed=%s", show.feed_url, exc)
                continue
            if not is_russian:
                continue
            filtered.append(show)
            if len(filtered) >= limit:
                break
        return filtered

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
                "limit": max(limit * 4, 24),
                "country": "RU",
                "lang": "ru_ru",
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

        raw_shows: list[StoryShow] = []
        seen: set[str] = set()
        for item in data.get("results") or []:
            feed_url = str(item.get("feedUrl") or "").strip()
            if not feed_url or feed_url in seen:
                continue
            seen.add(feed_url)
            raw_shows.append(
                StoryShow(
                    title=str(item.get("collectionName") or item.get("trackName") or "Безымянное шоу"),
                    author=str(item.get("artistName") or ""),
                    feed_url=feed_url,
                    artwork_url=str(item.get("artworkUrl600") or item.get("artworkUrl100") or ""),
                    webpage_url=str(item.get("collectionViewUrl") or item.get("trackViewUrl") or ""),
                )
            )
            if len(raw_shows) >= max(limit * 4, 24):
                break

        shows = await self._filter_russian_shows(raw_shows, limit=limit)
        self._show_cache[key] = (now + SEARCH_CACHE_TTL, shows)
        log.info("story hall show search query=%r raw=%d russian=%d", query, len(raw_shows), len(shows))
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
            is_russian = await asyncio.to_thread(self._feed_is_russian_sync, show.feed_url)
            if not is_russian:
                self._feed_cache[key] = (now + FEED_CACHE_TTL, [])
                return []
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
        if not ok:
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
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        try:
            await interaction.response.send_modal(StorySearchModal(self))
        except Exception:
            await safe_send(interaction, "Не удалось открыть поиск шоу.", ephemeral=True)

    async def handle_show_search_submit(self, interaction: discord.Interaction, query: str) -> None:
        await safe_defer_ephemeral(interaction)
        raw_query = str(query or "").strip()
        if not raw_query:
            await safe_send(interaction, "Название шоу пустое.", ephemeral=True)
            return
        await safe_send(interaction, SEARCH_PROGRESS_TEXT, ephemeral=True)
        try:
            shows = await self._search_shows(raw_query, limit=SHOW_SEARCH_LIMIT)
        except CatalogUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        if not shows:
            await safe_send(interaction, "Зал не нашел подходящих русскоязычных шоу по этому имени.", ephemeral=True)
            return
        view = ShowResultsView(
            self,
            shows,
            title="Результаты поиска",
            description="Выбери шоу, чтобы раскрыть его выпуски.",
        )
        await safe_send(interaction, embed=view.build_embed(), view=view, ephemeral=True)

    async def open_category_shows(self, interaction: discord.Interaction, category_key: str) -> None:
        category = CATEGORY_DEFS.get(category_key)
        if not category:
            await safe_send(interaction, "Эта подборка недоступна.", ephemeral=True)
            return
        loading = discord.Embed(
            title=f"Подборка: {category['label']}",
            description=COLLECTION_PROGRESS_TEXT,
            color=discord.Color.dark_teal(),
        )
        await safe_edit_message(interaction, embed=loading, view=None)
        try:
            shows = await self._search_shows_for_category(category_key, limit=SHOW_SEARCH_LIMIT)
        except CatalogUnavailableError as exc:
            await safe_edit_message(
                interaction,
                embed=discord.Embed(
                    title=f"Подборка: {category['label']}",
                    description=str(exc),
                    color=discord.Color.dark_teal(),
                ),
                view=None,
            )
            return
        if not shows:
            await safe_edit_message(
                interaction,
                embed=discord.Embed(
                    title=f"Подборка: {category['label']}",
                    description="Зал пока не нашел подходящих шоу в этой подборке.",
                    color=discord.Color.dark_teal(),
                ),
                view=None,
            )
            return
        view = ShowResultsView(
            self,
            shows,
            title=f"Подборка: {category['label']}",
            description="Выбери шоу, чтобы раскрыть его выпуски.",
            category_key=category_key,
            category_label=str(category["label"]),
        )
        await safe_edit_message(interaction, embed=view.build_embed(), view=view)

    async def open_show_episodes(
        self,
        interaction: discord.Interaction,
        show: StoryShow,
        *,
        category_key: str,
        category_label: str,
    ) -> None:
        if not category_label:
            category_label = "Поиск шоу"
        if not category_key:
            category_key = "search"
        loading = discord.Embed(
            title=show.title,
            description="Зал раскрывает ленту шоу и собирает доступные выпуски.",
            color=discord.Color.dark_teal(),
        )
        await safe_edit_message(interaction, embed=loading, view=None)
        try:
            episodes = await self._episodes_for_show(
                show,
                category_key=category_key,
                category_label=category_label,
                limit=EPISODE_LIST_LIMIT,
            )
        except CatalogUnavailableError as exc:
            await safe_edit_message(
                interaction,
                embed=discord.Embed(
                    title=show.title,
                    description=str(exc),
                    color=discord.Color.dark_teal(),
                ),
                view=None,
            )
            return
        if not episodes:
            await safe_edit_message(
                interaction,
                embed=discord.Embed(
                    title=show.title,
                    description="У этого шоу сейчас не открылось ни одного доступного выпуска.",
                    color=discord.Color.dark_teal(),
                ),
                view=None,
            )
            return
        view = EpisodeResultsView(self, show, episodes)
        await safe_edit_message(interaction, embed=view.build_embed(), view=view)

    async def play_random_story(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False, require_voice_if_inactive=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        await safe_send(interaction, RANDOM_STORY_PROGRESS_TEXT, ephemeral=True)
        try:
            episode = await self._pick_random_episode_from_group(RANDOM_STORY_KEYS)
        except CatalogUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        if episode is None:
            await safe_send(interaction, NO_RANDOM_TEXT, ephemeral=True)
            return
        await self._start_episode(interaction, member, episode, announce=True)

    async def play_random_podcast(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False, require_voice_if_inactive=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        await safe_send(interaction, RANDOM_PODCAST_PROGRESS_TEXT, ephemeral=True)
        try:
            episode = await self._pick_random_episode_from_group(RANDOM_PODCAST_KEYS)
        except CatalogUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        if episode is None:
            await safe_send(interaction, NO_RANDOM_TEXT, ephemeral=True)
            return
        await self._start_episode(interaction, member, episode, announce=True)

    async def resume_last_episode(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False, require_voice_if_inactive=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Хранилище историй сейчас недоступно.", ephemeral=True)
            return
        resume = await repo.get_story_audio_resume(member.id)
        if not resume:
            await safe_send(interaction, NO_RESUME_TEXT, ephemeral=True)
            return
        episode = StoryEpisode(
            title=str(resume.get("episode_title") or "Безымянный выпуск"),
            show_title=str(resume.get("show_title") or "Безымянное шоу"),
            audio_url=str(resume.get("audio_url") or ""),
            webpage_url=str(resume.get("webpage_url") or ""),
            artwork_url=str(resume.get("artwork_url") or ""),
            duration_sec=int(resume.get("duration_sec") or 0),
            guid=str(resume.get("episode_guid") or ""),
            feed_url=str(resume.get("feed_url") or ""),
            category_key="resume",
            category_label=str(resume.get("category") or "Продолжение"),
        )
        if not episode.audio_url:
            await repo.clear_story_audio_resume(member.id)
            await safe_send(interaction, NO_RESUME_TEXT, ephemeral=True)
            return
        await self._start_episode(
            interaction,
            member,
            episode,
            resume_from_sec=int(resume.get("resume_position_sec") or 0),
            announce=True,
        )

    async def start_episode_from_choice(self, interaction: discord.Interaction, episode: StoryEpisode) -> None:
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False, require_voice_if_inactive=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        await self._start_episode(interaction, member, episode, announce=True)

    def _build_resume_payload_locked(self, session: StorySession) -> Optional[dict[str, Any]]:
        episode = session.current_episode
        if episode is None:
            return None
        position = int(session.resume_position_sec or 0)
        if session.play_started_monotonic > 0:
            position += max(0, int(time.monotonic() - session.play_started_monotonic))
        if episode.duration_sec > 0 and position >= max(episode.duration_sec - 15, 0):
            return None
        return {
            "user_id": int(session.started_by_user_id),
            "show_title": episode.show_title,
            "episode_title": episode.title,
            "category": episode.category_label,
            "audio_url": episode.audio_url,
            "webpage_url": episode.webpage_url,
            "artwork_url": episode.artwork_url,
            "feed_url": episode.feed_url,
            "episode_guid": episode.guid,
            "duration_sec": int(episode.duration_sec or 0),
            "resume_position_sec": int(position),
        }

    async def _persist_resume_payload(self, payload: Optional[dict[str, Any]]) -> None:
        if not payload:
            return
        repo = self._repo()
        if not repo:
            return
        try:
            await repo.set_story_audio_resume(**payload)
        except Exception:
            log.exception("story hall failed to persist resume payload")

    async def _start_episode(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        episode: StoryEpisode,
        *,
        resume_from_sec: int = 0,
        announce: bool = False,
    ) -> None:
        await self._ensure_voice_connection(member)
        repo = self._repo()
        created_session = False
        try:
            async with self._lock:
                if self.session is None:
                    conflict_text = conflict_text_for_request(self.bot, PLAYBACK_MODE_STORIES)
                    if conflict_text:
                        raise RuntimeError(conflict_text)
                elif not self._same_active_voice(member):
                    raise RuntimeError("История уже звучит в другом зале. Приди туда, где сейчас говорит Алтарь.")
                if self.session is None:
                    self.session = StorySession(
                        guild_id=member.guild.id,
                        text_channel_id=self._altar_channel_id(),
                        voice_channel_id=int(member.voice.channel.id),  # type: ignore[union-attr]
                        started_by_user_id=member.id,
                    )
                    created_session = True
                session = self.session
                assert session is not None
                session.guild_id = member.guild.id
                session.text_channel_id = self._altar_channel_id()
                session.voice_channel_id = int(member.voice.channel.id)  # type: ignore[union-attr]
                session.started_by_user_id = member.id
                session.state = "playing"
                session.idle_disconnect_at = 0
                session.resume_position_sec = max(0, int(resume_from_sec or 0))
                session.current_episode = episode
                session.suppress_after_until = time.monotonic() + 1.0
                self._last_panel_state = "playing"
                vc = self._voice_client()
                if vc and vc.is_connected() and (vc.is_playing() or vc.is_paused()):
                    try:
                        vc.stop()
                    except Exception:
                        pass
                await self._play_episode_locked()
        except Exception as exc:
            async with self._lock:
                if self.session is not None:
                    self.session.current_episode = None
                    self.session.resume_position_sec = 0
                    self.session.play_started_monotonic = 0.0
                    self.session.state = "idle"
                if created_session:
                    self.session = None
                    self._last_panel_state = "silent"
            await self._update_panel_message()
            await safe_send(interaction, f"Не удалось запустить выпуск: {exc}", ephemeral=True)
            return

        if repo:
            try:
                await repo.clear_story_audio_resume(member.id)
            except Exception:
                log.exception("story hall failed to clear previous resume")
        self._remember_recent_episode(episode)
        await self._update_panel_message()
        if announce:
            extra = ""
            if resume_from_sec > 0:
                extra = f" Продолжено с **{_fmt_duration(resume_from_sec)}**."
            await safe_send(
                interaction,
                EPISODE_START_TEXT.format(title=episode.title) + extra,
                ephemeral=True,
            )

    async def _play_episode_locked(self) -> None:
        session = self.session
        vc = self._voice_client()
        if session is None or session.current_episode is None or vc is None or not vc.is_connected():
            return
        if not self._ffmpeg:
            raise RuntimeError("ffmpeg не найден")

        before = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
        if session.resume_position_sec > 0:
            before = f"-ss {int(session.resume_position_sec)} " + before

        source = discord.FFmpegPCMAudio(
            session.current_episode.audio_url,
            executable=self._ffmpeg,
            before_options=before,
            options="-vn",
        )
        session.state = "playing"
        session.play_started_monotonic = time.monotonic()
        self._last_panel_state = "playing"

        def _after_playback(error: Optional[Exception]) -> None:
            loop = self.bot.loop
            loop.call_soon_threadsafe(asyncio.create_task, self._after_episode_finished(error))

        vc.play(source, after=_after_playback)

    async def _after_episode_finished(self, error: Optional[Exception]) -> None:
        if error:
            log.warning("story hall playback after-callback error: %s", error)

        clear_user_id = 0
        async with self._lock:
            session = self.session
            if session is None:
                return
            if time.monotonic() < float(session.suppress_after_until or 0.0):
                return
            clear_user_id = int(session.started_by_user_id or 0)
            session.current_episode = None
            session.resume_position_sec = 0
            session.play_started_monotonic = 0.0
            session.state = "ended"
            session.idle_disconnect_at = _now() + IDLE_DISCONNECT_SECONDS
            self._last_panel_state = "ended"
        repo = self._repo()
        if repo and clear_user_id:
            try:
                await repo.clear_story_audio_resume(clear_user_id)
            except Exception:
                log.exception("story hall failed to clear finished resume")
        await self._update_panel_message()

    async def stop(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        payload: Optional[dict[str, Any]]
        async with self._lock:
            payload = await self._close_session_locked(state="stopped", save_resume=True)
        await self._persist_resume_payload(payload)
        await self._update_panel_message()
        await safe_send(interaction, STOP_TEXT, ephemeral=True)

    async def _close_session_locked(self, *, state: str, save_resume: bool) -> Optional[dict[str, Any]]:
        session = self.session
        vc = self._voice_client()
        payload = self._build_resume_payload_locked(session) if session and save_resume else None
        if session is not None:
            session.suppress_after_until = time.monotonic() + 1.0
            session.current_episode = None
            session.resume_position_sec = 0
            session.play_started_monotonic = 0.0
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
        return payload

    async def _update_idle_deadline(self) -> None:
        session = self.session
        if session is None:
            return
        vc = self._voice_client()
        if not vc or not vc.is_connected():
            return
        listeners = self._non_bot_listeners()
        if listeners:
            if session.idle_disconnect_at != 0 and session.current_episode is not None:
                session.idle_disconnect_at = 0
                await self._update_panel_message()
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

        if self.bot.user and member.id == self.bot.user.id and after.channel is None:
            payload: Optional[dict[str, Any]]
            async with self._lock:
                payload = self._build_resume_payload_locked(self.session) if self.session else None
                self.session = None
                self._last_panel_state = "silent"
            await self._persist_resume_payload(payload)
            await self._update_panel_message()
            return

        await self._update_idle_deadline()

    @tasks.loop(seconds=10)
    async def panel_watcher(self) -> None:
        if not self.bot.is_ready():
            return
        if self.session is None:
            return
        payload: Optional[dict[str, Any]] = None
        async with self._lock:
            session = self.session
            if session is None:
                return
            if session.idle_disconnect_at and _now() >= int(session.idle_disconnect_at):
                payload = await self._close_session_locked(state="silent", save_resume=True)
        if payload is not None:
            await self._persist_resume_payload(payload)
        await self._update_panel_message()

    @panel_watcher.before_loop
    async def panel_watcher_before_loop(self) -> None:
        await self.bot.wait_until_ready()

    @commands.command(name="post_story_hall_panel")
    async def post_story_hall_panel(self, ctx: commands.Context) -> None:
        if not self._is_admin(ctx.author.id):
            return
        msg = await self.ensure_panel_message()
        if not msg:
            await ctx.reply("Не удалось найти канал Зала историй.")
            return
        await self._update_panel_message()
        await ctx.reply(f"Панель Зала историй готова: канал <#{self._altar_channel_id()}>.")


def get_persistent_views(bot: commands.Bot) -> list[discord.ui.View]:
    return [StoryHallView(bot)]


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(StoryHallCog(bot))
