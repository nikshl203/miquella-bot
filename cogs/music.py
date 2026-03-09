from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

import discord
from discord.ext import commands, tasks

from ._interactions import GuardedView, safe_defer_ephemeral, safe_defer_update, safe_edit_message, safe_send
from ._playback_slot import PLAYBACK_MODE_MUSIC, conflict_text_for_request

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
SEARCH_RESULT_LIMIT = 5
SEARCH_FETCH_LIMIT = 10

SOURCE_SOUNDCLOUD = "soundcloud"
SOURCE_AUDIUS = "audius"
SOURCE_JAMENDO = "jamendo"

SOURCE_NAMES = {
    SOURCE_SOUNDCLOUD: "SoundCloud",
    SOURCE_AUDIUS: "Audius",
    SOURCE_JAMENDO: "Jamendo",
}

RADIO_MODE_OFF = "off"
RADIO_MODE_TRACK = "track"
RADIO_MODE_GENRE = "genre"
RADIO_MODE_STATION = "station"
RADIO_MODE_ORDINARY = "ordinary"
RADIO_HISTORY_LIMIT = 10
RADIO_AUTOPICK_RETRY_LIMIT = 3

NO_PLAYABLE_RESULTS_TEXT = (
    "Алтарь не нашел полного звучания этого отголоска "
    "ни в одном из доступных источников."
)

UNSUPPORTED_LINK_TEXT = (
    "Алтарь принимает только ссылки SoundCloud, Audius или Jamendo."
)

UNPLAYABLE_TRACK_TEXT = (
    "Этот отголосок не отдает полный поток звучания. "
    "Алтарь не может принять его целиком."
)

SEARCH_RESULTS_TEXT = (
    "Алтарь услышал несколько отголосков. "
    "Выбери тот, который должен зазвучать."
)

SEARCH_PROGRESS_TEXT = (
    "Алтарь вслушивается в дальние залы и ищет подходящие версии. "
    "Это может занять несколько секунд."
)

RESOLVE_PROGRESS_TEXT = (
    "Алтарь принимает этот отголосок и проверяет, может ли он звучать полностью."
)

PLAYLIST_NAME_MAX_LENGTH = 40
PLAYLIST_SELECT_LIMIT = 25
PLAYLIST_TRACK_SELECT_LIMIT = 25
FAVORITES_PLAYLIST_NAME = "Избранное"

VARIANT_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("remix", (" remix", "(remix", "[remix", "rmx", " bootleg", " edit", " vip")),
    ("slowed", ("slowed", "slow + reverb", "slowed + reverb", "slow reverb")),
    ("sped up", ("sped up", "speed up", "speedup")),
    ("nightcore", ("nightcore",)),
    ("live", (" live", "live at", "concert", "acoustic live", "session live")),
    ("cover", (" cover", "(cover", "[cover", "кавер")),
)

VARIANT_PENALTIES = {
    "remix": 10,
    "slowed": 12,
    "sped up": 12,
    "nightcore": 15,
    "live": 10,
    "cover": 12,
}

RADIO_GENRES: tuple[dict[str, Any], ...] = (
    {"key": "ambient", "label": "ambient", "queries": ("ambient instrumental", "ambient", "atmospheric ambient")},
    {"key": "dark_ambient", "label": "dark ambient", "queries": ("dark ambient", "ritual ambient", "cinematic dark ambient")},
    {"key": "dungeon_synth", "label": "dungeon synth", "queries": ("dungeon synth", "medieval ambient", "fantasy synth")},
    {"key": "metal", "label": "metal", "queries": ("metal instrumental", "metal", "dark metal")},
    {"key": "instrumental", "label": "instrumental", "queries": ("instrumental", "instrumental soundtrack", "cinematic instrumental")},
    {"key": "electronic", "label": "electronic", "queries": ("electronic", "dark electronic", "downtempo electronic")},
    {"key": "folk", "label": "folk", "queries": ("folk", "dark folk", "neofolk instrumental")},
)
RADIO_GENRES_BY_KEY: dict[str, dict[str, Any]] = {str(item["key"]): item for item in RADIO_GENRES}

RADIO_STATIONS: tuple[dict[str, Any], ...] = (
    {
        "key": "void",
        "label": "Пустота",
        "queries": (
            "dark ambient instrumental",
            "drone ambient",
            "cold ambient",
            "sparse instrumental",
        ),
        "boost_terms": ("dark ambient", "drone", "cold ambient", "sparse instrumental", "low vocal"),
        "avoid_terms": ("party", "dance", "bright pop", "aggressive metal"),
    },
    {
        "key": "ashes",
        "label": "Пепел",
        "queries": (
            "doom ambient",
            "ritual ambient",
            "slow dark instrumental",
            "ashen ritual music",
        ),
        "boost_terms": ("doom", "ritual", "heavy ambient", "slow dark", "ashen"),
        "avoid_terms": ("happy", "dance", "glossy pop", "playful"),
    },
    {
        "key": "night",
        "label": "Ночь",
        "queries": (
            "night ambient",
            "calm synth",
            "darkwave instrumental",
            "soft instrumental night",
        ),
        "boost_terms": ("night ambient", "calm synth", "darkwave", "soft instrumental"),
        "avoid_terms": ("noisy", "heavy", "cheerful"),
    },
    {
        "key": "abyss",
        "label": "Бездна",
        "queries": (
            "oppressive ambient",
            "ominous dark ambient",
            "industrial ambient",
            "threatening instrumental",
        ),
        "boost_terms": ("oppressive ambient", "ominous", "industrial", "threatening"),
        "avoid_terms": ("warm", "cozy", "uplifting", "celebratory"),
    },
    {
        "key": "hunt",
        "label": "Охота",
        "queries": (
            "tense dark electronic",
            "battle instrumental",
            "energetic instrumental",
            "rhythmic dark synth",
        ),
        "boost_terms": ("tense", "rhythmic", "battle", "dark electronic", "energetic instrumental"),
        "avoid_terms": ("slow drone", "sleepy ambient", "static textures"),
    },
    {
        "key": "dawn",
        "label": "Рассвет",
        "queries": (
            "warm ambient instrumental",
            "melodic instrumental",
            "hopeful synth",
            "gentle folk instrumental",
        ),
        "boost_terms": ("warm ambient", "melodic instrumental", "hopeful synth", "gentle folk"),
        "avoid_terms": ("oppressive", "terrifying", "harsh industrial"),
    },
    {
        "key": "feast",
        "label": "Пир",
        "queries": (
            "lively folk instrumental",
            "festive instrumental",
            "upbeat instrumental",
            "rhythmic folk",
        ),
        "boost_terms": ("lively", "rhythmic", "folk", "upbeat instrumental", "festive"),
        "avoid_terms": ("drone", "sleep music", "funeral dark"),
    },
)
RADIO_STATIONS_BY_KEY: dict[str, dict[str, Any]] = {str(item["key"]): item for item in RADIO_STATIONS}
ORDINARY_RADIO_STATION = RADIO_STATIONS_BY_KEY["void"]
RADIO_FEEDBACK_MEMORY_LIMIT = 12
RADIO_RECENT_ARTIST_LIMIT = 6
RADIO_QUERY_LIMIT = 4
RADIO_SEARCH_SOFT_TIMEOUT = 3.0
RADIO_GOOD_MATCH_SCORE = 34.0
RADIO_SKIP_WAIT_SECONDS = 2.5
SEARCH_TIMEOUT_SOUND_CLOUD = 5.0
SEARCH_TIMEOUT_AUDIUS = 4.0
SEARCH_TIMEOUT_JAMENDO = 4.0
SEARCH_TIMEOUT_SOUND_CLOUD_FULL = 9.0
SEARCH_TIMEOUT_AUDIUS_FULL = 6.0
SEARCH_TIMEOUT_JAMENDO_FULL = 6.0
RADIO_PREFETCH_NOTICE = "Алтарь уже тянется к следующему радио-отголоску."
RADIO_STATION_PROGRESS_TEXT = "Алтарь вслушивается в выбранную станцию и собирает первые созвучия. Подожди немного."
RADIO_GENRE_PROGRESS_TEXT = "Алтарь настраивается на жанровый поток и ищет первый полноценный отголосок."
SOURCES_UNAVAILABLE_TEXT = "Внешние источники сейчас молчат или отвечают слишком медленно. Алтарь попробует снова позже."
RADIO_TRACK_START_TEXT = "Алтарь подхватил отголосок и продолжит его эхо."
RADIO_GENRE_START_TEXT = "Алтарь настроен на нужный поток и начнет подбирать созвучные отголоски."
RADIO_STATION_START_TEXT = "Алтарь открыл станцию и впустил в зал новый поток звучания."
RADIO_MORE_TEXT = "Алтарь запомнил этот отклик и будет тянуться к похожему звучанию."
RADIO_LESS_TEXT = "Алтарь ослабил это направление и отойдет от похожих звучаний."
RADIO_DISABLED_TEXT = "Поток радио угас. Алтарь вернулся к обычной очереди."
RADIO_NO_RESULT_TEXT = "Алтарь не нашел полного звучания для этого радио-режима."
RADIO_NO_ACTIVE_TEXT = "Радио сейчас не активно."
RADIO_NOTHING_PLAYING_TEXT = "Сейчас нет звучащего радио-отголоска, от которого можно сместить поток."
RADIO_ONLY_FOR_ACTIVE_TEXT = "Эти кнопки отвечают только за текущую радио-сессию."
RADIO_TOKEN_STOPWORDS = {
    "the",
    "and",
    "feat",
    "featuring",
    "ft",
    "prod",
    "official",
    "audio",
    "version",
    "music",
    "track",
    "song",
    "with",
    "from",
    "edit",
    "mix",
    "remix",
    "live",
    "cover",
    "slowed",
    "speed",
    "sped",
    "nightcore",
    "instrumental",
    "radio",
    "это",
    "как",
    "для",
    "без",
    "feat",
    "на",
    "из",
    "под",
    "или",
}


@dataclass
class MusicTrack:
    title: str
    artist: str
    duration: int
    webpage_url: str
    stream_url: str
    requested_by_user_id: int
    source_key: str = SOURCE_SOUNDCLOUD
    source_name: str = "SoundCloud"
    artwork_url: str = ""
    is_radio_track: bool = False
    radio_mode: str = RADIO_MODE_OFF
    radio_reason: str = ""


@dataclass
class RadioState:
    mode: str = RADIO_MODE_OFF
    genre_key: str = ""
    genre_label: str = ""
    station_key: str = ""
    station_label: str = ""
    requested_by_user_id: int = 0
    history: list[str] = field(default_factory=list)
    artist_history: list[str] = field(default_factory=list)
    liked_terms: list[str] = field(default_factory=list)
    disliked_terms: list[str] = field(default_factory=list)
    liked_artists: list[str] = field(default_factory=list)
    disliked_artists: list[str] = field(default_factory=list)
    pending: bool = False
    notice: str = ""
    prefetched_track: Optional[MusicTrack] = None
    prefetched_basis_identity: str = ""


@dataclass
class TrackCandidate:
    source_key: str
    title: str
    artist: str
    duration: int
    webpage_url: str
    stream_url: str
    is_preview: bool
    is_playable: bool
    artwork_url: str = ""
    variant_flags: tuple[str, ...] = ()
    native_rank: int = 0
    native_score: float = 0.0

    @property
    def source_name(self) -> str:
        return SOURCE_NAMES.get(self.source_key, self.source_key.title())


@dataclass
class SearchBatch:
    source_key: str
    candidates: list[TrackCandidate]
    total_found: int = 0
    preview_only_found: int = 0
    unplayable_found: int = 0

    @property
    def source_name(self) -> str:
        return SOURCE_NAMES.get(self.source_key, self.source_key.title())


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
    last_skip_requested_at: float = 0.0
    radio: RadioState = field(default_factory=RadioState)


class PreviewOnlyTrackError(RuntimeError):
    """Raised when SoundCloud exposes only preview snippets for a track."""


class SearchSourcesUnavailableError(RuntimeError):
    """Raised when all external music sources fail before returning any result."""


def _now() -> int:
    return int(time.time())


def _fmt_duration(seconds: int) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _compact_spaces(raw: str) -> str:
    return re.sub(r"\s+", " ", str(raw or "")).strip()


def _seed_title(raw: str) -> str:
    text = _compact_spaces(re.sub(r"[\(\[].*?[\)\]]", " ", str(raw or "")))
    for token in ("remix", "rmx", "edit", "vip", "slowed", "speed up", "sped up", "nightcore", "live", "cover"):
        text = re.sub(re.escape(token), " ", text, flags=re.IGNORECASE)
    return _compact_spaces(text)


def _radio_track_identity(
    *,
    source_key: str,
    title: str,
    artist: str,
    webpage_url: str = "",
) -> str:
    canonical = str(webpage_url or "").strip().lower()
    if canonical:
        return canonical
    return f"{source_key}:{_normalize_text(artist)}::{_normalize_text(title)}"


def _track_identity(track: MusicTrack) -> str:
    return _radio_track_identity(
        source_key=str(track.source_key or SOURCE_SOUNDCLOUD),
        title=str(track.title or ""),
        artist=str(track.artist or ""),
        webpage_url=str(track.webpage_url or ""),
    )


def _candidate_identity(candidate: TrackCandidate) -> str:
    return _radio_track_identity(
        source_key=str(candidate.source_key or SOURCE_SOUNDCLOUD),
        title=str(candidate.title or ""),
        artist=str(candidate.artist or ""),
        webpage_url=str(candidate.webpage_url or ""),
    )


def _radio_mode_text(radio: RadioState) -> str:
    if radio.mode == RADIO_MODE_TRACK:
        base = "радио по треку"
    elif radio.mode == RADIO_MODE_GENRE:
        suffix = f" — {radio.genre_label}" if radio.genre_label else ""
        base = f"радио по жанру{suffix}"
    elif radio.mode == RADIO_MODE_STATION:
        suffix = f" — {radio.station_label}" if radio.station_label else ""
        base = f"станция{suffix}"
    elif radio.mode == RADIO_MODE_ORDINARY:
        suffix = f" — {radio.station_label}" if radio.station_label else ""
        base = f"обычное радио{suffix}"
    else:
        base = "обычная очередь"
    if radio.notice:
        return f"{base}\n{radio.notice}"
    return base


def _radio_queries_from_track(track: MusicTrack) -> list[str]:
    artist = _compact_spaces(track.artist)
    title = _seed_title(track.title) or _compact_spaces(track.title)
    queries: list[str] = []
    if artist and title:
        queries.append(f"{artist} {title}")
    if artist:
        queries.append(artist)
    if title:
        queries.append(title)
    if artist and title:
        queries.append(f"{artist} similar {title}")
    unique: list[str] = []
    seen: set[str] = set()
    for query in queries:
        key = query.casefold()
        if key in seen or not query:
            continue
        seen.add(key)
        unique.append(query)
    return unique


def _radio_reason_text(radio: RadioState) -> str:
    if radio.mode == RADIO_MODE_TRACK:
        return "по текущему треку"
    if radio.mode == RADIO_MODE_GENRE:
        return f"жанр: {radio.genre_label}" if radio.genre_label else "по жанру"
    if radio.mode in {RADIO_MODE_STATION, RADIO_MODE_ORDINARY}:
        return f"станция: {radio.station_label}" if radio.station_label else "по станции"
    return ""


def _radio_terms(*parts: str) -> list[str]:
    tokens: list[str] = []
    for part in parts:
        for token in _normalize_text(part).split():
            if len(token) < 3:
                continue
            if token in RADIO_TOKEN_STOPWORDS:
                continue
            tokens.append(token)
    seen: set[str] = set()
    unique: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
    return unique


def _push_unique_limited(target: list[str], values: list[str], *, limit: int) -> list[str]:
    result = [str(item) for item in target if str(item).strip()]
    for raw in values:
        item = str(raw or "").strip()
        if not item:
            continue
        result = [existing for existing in result if existing != item]
        result.append(item)
    return result[-limit:]


def _is_soundcloud_url(raw: str) -> bool:
    try:
        host = (urlparse(str(raw).strip()).netloc or "").lower()
    except Exception:
        return False
    return "soundcloud.com" in host or "snd.sc" in host


def _is_audius_url(raw: str) -> bool:
    try:
        host = (urlparse(str(raw).strip()).netloc or "").lower()
    except Exception:
        return False
    return host == "audius.co" or host.endswith(".audius.co")


def _is_jamendo_url(raw: str) -> bool:
    try:
        host = (urlparse(str(raw).strip()).netloc or "").lower()
    except Exception:
        return False
    return "jamendo.com" in host


def _detect_source_from_url(raw: str) -> Optional[str]:
    if _is_soundcloud_url(raw):
        return SOURCE_SOUNDCLOUD
    if _is_audius_url(raw):
        return SOURCE_AUDIUS
    if _is_jamendo_url(raw):
        return SOURCE_JAMENDO
    return None


def _best_soundcloud_artwork(entry: dict) -> str:
    thumbnails = list(entry.get("thumbnails") or [])
    best_url = ""
    best_score = -1
    for item in thumbnails:
        url = str(item.get("url") or "")
        if not url:
            continue
        score = int(item.get("width") or 0) * int(item.get("height") or 0)
        if score > best_score:
            best_score = score
            best_url = url
    if best_url:
        return best_url
    return str(entry.get("thumbnail") or "")


def _best_artwork_from_mapping(value: Any) -> str:
    best_url = ""
    best_score = -1

    def _walk(node: Any, path: tuple[str, ...] = ()) -> None:
        nonlocal best_url, best_score
        if isinstance(node, dict):
            for key, inner in node.items():
                _walk(inner, (*path, str(key)))
            return
        if isinstance(node, list):
            for inner in node:
                _walk(inner, path)
            return
        url = str(node or "")
        if not url.startswith("http"):
            return
        score = 1
        for segment in path:
            nums = [int(x) for x in re.findall(r"\d+", segment)]
            if nums:
                score = max(score, max(nums))
        for query_part in re.findall(r"(?:width|height|size)(?:=|%3D)(\d+)", url):
            score = max(score, int(query_part))
        if score > best_score:
            best_score = score
            best_url = url

    _walk(value)
    return best_url


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


PREVIEW_ONLY_TRACK_TEXT = (
    "Этот отголосок доступен лишь как краткое превью. "
    "Алтарь не может принять его целиком."
)


def _soundcloud_format_is_preview(fmt: dict) -> bool:
    format_id = str(fmt.get("format_id") or "").lower()
    format_note = str(fmt.get("format_note") or "").lower()
    url = str(fmt.get("url") or "").lower()
    preference = fmt.get("preference")
    return (
        "preview" in format_id
        or "preview" in format_note
        or preference == -10
        or "/preview/" in url
        or "/playlist/0/30/" in url
    )


def _normalize_text(raw: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9а-яё]+", " ", str(raw or "").lower())).strip()


def _truncate(text: str, limit: int) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    return raw[: max(0, limit - 1)].rstrip() + "…"


def _variant_flags(text: str) -> tuple[str, ...]:
    lowered = f" {_normalize_text(text)} "
    found: list[str] = []
    for label, patterns in VARIANT_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            found.append(label)
    return tuple(found)


def _query_requested_variant_flags(query: str) -> set[str]:
    return set(_variant_flags(query))


def _candidate_score(candidate: TrackCandidate, query: str) -> float:
    norm_query = _normalize_text(query)
    combined = _normalize_text(f"{candidate.title} {candidate.artist}")
    title_norm = _normalize_text(candidate.title)
    artist_norm = _normalize_text(candidate.artist)
    query_tokens = [token for token in norm_query.split() if len(token) > 1]
    requested_flags = _query_requested_variant_flags(query)

    score = 0.0
    if norm_query and norm_query == title_norm:
        score += 40
    if norm_query and norm_query in combined:
        score += 22
    if norm_query and norm_query in title_norm:
        score += 12
    if query_tokens:
        score += sum(3 for token in query_tokens if token in title_norm)
        score += sum(1 for token in query_tokens if token in artist_norm)
    if candidate.duration > 0:
        score += 1
    score += max(0.0, min(float(candidate.native_score), 1_000_000.0) / 100_000.0)
    score -= float(candidate.native_rank)
    if not candidate.variant_flags and not requested_flags:
        score += 4
    for flag in candidate.variant_flags:
        if flag not in requested_flags:
            score -= VARIANT_PENALTIES.get(flag, 6)
    return score


def _candidate_notes(candidate: TrackCandidate) -> str:
    if not candidate.variant_flags:
        return ""
    return ", ".join(candidate.variant_flags)


def _candidate_select_label(candidate: TrackCandidate, index: int) -> str:
    return _truncate(f"{index}. {candidate.title}", 100)


def _candidate_select_description(candidate: TrackCandidate) -> str:
    bits = [candidate.source_name, _truncate(candidate.artist, 32)]
    if candidate.duration > 0:
        bits.append(_fmt_duration(candidate.duration))
    notes = _candidate_notes(candidate)
    if notes:
        bits.append(notes)
    return _truncate(" | ".join(bit for bit in bits if bit), 100)


def _candidate_embed_line(candidate: TrackCandidate, index: int) -> str:
    bits = [candidate.source_name, _truncate(candidate.artist, 48)]
    if candidate.duration > 0:
        bits.append(_fmt_duration(candidate.duration))
    notes = _candidate_notes(candidate)
    if notes:
        bits.append(notes)
    meta = " | ".join(bit for bit in bits if bit)
    return f"**{index}. {_truncate(candidate.title, 80)}**\n{meta}"


def _pick_best_audio_format(formats: list[dict]) -> Optional[dict]:
    if not formats:
        return None
    return max(
        formats,
        key=lambda fmt: (
            float(fmt.get("abr") or 0),
            float(fmt.get("tbr") or 0),
            float(fmt.get("asr") or 0),
            float(fmt.get("filesize") or 0),
        ),
    )


def _jamendo_signature(path: str) -> str:
    nonce = str(random.random())
    digest = hashlib.sha1(f"{path}{nonce}".encode("utf-8")).hexdigest()
    return f"${digest}*{nonce}~"


def _track_lines(track: MusicTrack, guild: Optional[discord.Guild]) -> str:
    requester = f"<@{track.requested_by_user_id}>"
    if guild:
        member = guild.get_member(int(track.requested_by_user_id))
        if member:
            requester = member.mention
    lines = [
        f"**{track.title}**\n"
        f"Исполнитель: **{track.artist}**",
        f"Длительность: **{_fmt_duration(track.duration)}**",
        f"Принес: {requester}",
        f"Источник: **{track.source_name}**",
    ]
    if track.radio_reason:
        lines.append(f"Почему выбран: **{track.radio_reason}**")
    return "\n".join(lines)


def _playlist_line(playlist: dict, index: int) -> str:
    marker = "★ " if bool(playlist.get("is_favorites")) else ""
    count = int(playlist.get("track_count") or 0)
    suffix = "трек" if count == 1 else ("трека" if 2 <= count <= 4 else "треков")
    return f"**{index}. {marker}{playlist.get('name', 'Безымянный плейлист')}** — {count} {suffix}"


def _playlist_track_line(track: dict, index: int) -> str:
    return (
        f"**{index}. {_truncate(str(track.get('title') or 'Безымянный отголосок'), 70)}**\n"
        f"{_truncate(str(track.get('artist') or 'Неизвестный исполнитель'), 42)} | "
        f"{_fmt_duration(int(track.get('duration') or 0))}"
    )


def _playlist_track_option_label(track: dict) -> str:
    pos = int(track.get("track_position") or 0)
    title = str(track.get("title") or "Безымянный отголосок")
    return _truncate(f"{pos}. {title}", 100)


def _playlist_track_option_description(track: dict) -> str:
    bits = [
        _truncate(str(track.get("artist") or "Неизвестный исполнитель"), 35),
        _fmt_duration(int(track.get("duration") or 0)),
    ]
    source = str(track.get("source") or "")
    if source:
        bits.append(source.title())
    return _truncate(" | ".join(bits), 100)


def _radio_menu_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Радио Алтаря",
        description="Выбери, как Пустота должна подбирать следующие отголоски.",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(
        name="Доступные режимы",
        value=(
            "• По текущему треку\n"
            "• По жанру\n"
            "• Станции\n"
            "• Выключить радио"
        ),
        inline=False,
    )
    return embed


def _radio_genre_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Радио по жанру",
        description="Пустота настраивается на выбранный жанр и продолжит поток, когда ручная очередь иссякнет.",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(
        name="Жанры",
        value="\n".join(f"• {item['label']}" for item in RADIO_GENRES),
        inline=False,
    )
    return embed


def _radio_station_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Станции Алтаря",
        description="Выбери станцию. В списке показаны только названия, без лишнего шума.",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(
        name="Станции",
        value="\n".join(f"• {item['label']}" for item in RADIO_STATIONS),
        inline=False,
    )
    return embed


class TrackChoiceSelect(discord.ui.Select):
    def __init__(self, view: "TrackChoiceView", candidates: list[TrackCandidate]):
        self._owner = view
        options = [
            discord.SelectOption(
                label=_candidate_select_label(candidate, idx),
                description=_candidate_select_description(candidate),
                value=str(idx - 1),
            )
            for idx, candidate in enumerate(candidates, start=1)
        ]
        super().__init__(
            placeholder="Выбери отголосок",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_choice(interaction, int(self.values[0]))


class TrackChoiceView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int, candidates: list[TrackCandidate]):
        super().__init__(timeout=60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.candidates = list(candidates)
        self._used = False
        self.add_item(TrackChoiceSelect(self, self.candidates))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Этот выбор был открыт не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_choice(self, interaction: discord.Interaction, index: int) -> None:
        if self._used:
            await safe_send(interaction, "Этот выбор уже закрыт. Если нужно, открой поиск заново.", ephemeral=True)
            return
        self._used = True
        for child in self.children:
            child.disabled = True
        await safe_defer_ephemeral(interaction)
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass
        await self.cog.enqueue_candidate(interaction, self.candidates[index])

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True


class RadioGenreSelect(discord.ui.Select):
    def __init__(self, view: "RadioGenreView"):
        self._owner = view
        options = [
            discord.SelectOption(
                label=str(item["label"]),
                value=str(item["key"]),
                description=_truncate(f"Жанровый поток: {item['label']}", 100),
            )
            for item in RADIO_GENRES
        ]
        super().__init__(
            placeholder="Выбери жанр для радио",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_select(interaction, self.values[0])


class RadioGenreView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.add_item(RadioGenreSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Это меню радио открыто не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_select(self, interaction: discord.Interaction, genre_key: str) -> None:
        await self.cog.start_radio_genre(interaction, genre_key)

    @discord.ui.button(label="Назад", style=discord.ButtonStyle.secondary, row=1)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(embed=_radio_menu_embed(), view=RadioModeView(self.cog, self.owner_user_id))


class RadioStationSelect(discord.ui.Select):
    def __init__(self, view: "RadioStationView"):
        self._owner = view
        options = [
            discord.SelectOption(
                label=str(item["label"]),
                value=str(item["key"]),
            )
            for item in RADIO_STATIONS
        ]
        super().__init__(
            placeholder="Выбери станцию Алтаря",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_select(interaction, self.values[0])


class RadioStationView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.add_item(RadioStationSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Это меню радио открыто не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_select(self, interaction: discord.Interaction, station_key: str) -> None:
        await self.cog.start_radio_station(interaction, station_key)

    @discord.ui.button(label="Назад", style=discord.ButtonStyle.secondary, row=1)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(embed=_radio_menu_embed(), view=RadioModeView(self.cog, self.owner_user_id))


class RadioModeView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Это меню радио открыто не для тебя.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="По текущему треку", style=discord.ButtonStyle.primary, row=0)
    async def by_track(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.start_radio_from_current_track(interaction)

    @discord.ui.button(label="По жанру", style=discord.ButtonStyle.secondary, row=0)
    async def by_genre(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(embed=_radio_genre_embed(), view=RadioGenreView(self.cog, self.owner_user_id))

    @discord.ui.button(label="Станции", style=discord.ButtonStyle.secondary, row=1)
    async def stations(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(embed=_radio_station_embed(), view=RadioStationView(self.cog, self.owner_user_id))

    @discord.ui.button(label="Выключить радио", style=discord.ButtonStyle.danger, row=1)
    async def disable(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.disable_radio(interaction)


class PlaylistTargetSelect(discord.ui.Select):
    def __init__(self, view: "PlaylistTargetView", playlists: list[dict]):
        self._owner = view
        options: list[discord.SelectOption] = []
        shown = playlists[: max(0, PLAYLIST_SELECT_LIMIT - 1)]
        for playlist in shown:
            desc = "Сохранить в этот плейлист"
            if playlist.get("is_favorites"):
                desc = "Главный плейлист пользователя"
            options.append(
                discord.SelectOption(
                    label=_truncate(str(playlist.get("name") or "Безымянный плейлист"), 100),
                    description=_truncate(desc, 100),
                    value=str(int(playlist.get("playlist_id") or 0)),
                )
            )
        options.append(
            discord.SelectOption(
                label="Создать новый плейлист",
                description="Открыть ввод названия и сразу сохранить туда текущий трек",
                value="__new__",
            )
        )
        super().__init__(
            placeholder="Куда сохранить текущий отголосок",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_select(interaction, self.values[0])


class PlaylistTargetView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int, track_snapshot: dict, playlists: list[dict]):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.track_snapshot = dict(track_snapshot)
        self.playlists = list(playlists)
        self.add_item(PlaylistTargetSelect(self, self.playlists))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Этот выбор сохранения был открыт не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_select(self, interaction: discord.Interaction, value: str) -> None:
        if value == "__new__":
            await interaction.response.send_modal(
                CreatePlaylistModal(self.cog, self.owner_user_id, track_snapshot=self.track_snapshot)
            )
            return
        await self.cog.save_track_snapshot_to_playlist(
            interaction,
            self.owner_user_id,
            self.track_snapshot,
            int(value),
        )


class PlaylistPickerSelect(discord.ui.Select):
    def __init__(self, view: "PlaylistBrowserView", playlists: list[dict]):
        self._owner = view
        options: list[discord.SelectOption] = []
        for playlist in playlists[:PLAYLIST_SELECT_LIMIT]:
            marker = "★ " if playlist.get("is_favorites") else ""
            count = int(playlist.get("track_count") or 0)
            options.append(
                discord.SelectOption(
                    label=_truncate(f"{marker}{playlist.get('name', 'Безымянный плейлист')}", 100),
                    description=_truncate(f"{count} сохраненных отголосков", 100),
                    value=str(int(playlist.get("playlist_id") or 0)),
                )
            )
        super().__init__(
            placeholder="Выбери плейлист",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_select(interaction, int(self.values[0]))


class PlaylistBrowserView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int, playlists: list[dict]):
        super().__init__(timeout=10 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.playlists = list(playlists)
        if self.playlists:
            self.add_item(PlaylistPickerSelect(self, self.playlists))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Эти плейлисты раскрыты не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_select(self, interaction: discord.Interaction, playlist_id: int) -> None:
        await self.cog.open_playlist_contents(interaction, self.owner_user_id, playlist_id)

    @discord.ui.button(label="Создать плейлист", style=discord.ButtonStyle.secondary, row=1)
    async def create_playlist(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(CreatePlaylistModal(self.cog, self.owner_user_id))


class PlaylistTrackSelect(discord.ui.Select):
    def __init__(self, view: "PlaylistTracksView", tracks: list[dict]):
        self._owner = view
        options = [
            discord.SelectOption(
                label=_playlist_track_option_label(track),
                description=_playlist_track_option_description(track),
                value=str(int(track.get("playlist_track_id") or 0)),
            )
            for track in tracks[:PLAYLIST_TRACK_SELECT_LIMIT]
        ]
        super().__init__(
            placeholder="Выбери один или несколько отголосков",
            min_values=1,
            max_values=max(1, len(options)),
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._owner.handle_selection(interaction, [int(v) for v in self.values])


class PlaylistTracksView(GuardedView):
    def __init__(self, cog: "MusicCog", owner_user_id: int, playlist: dict, tracks: list[dict]):
        super().__init__(timeout=10 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.playlist = dict(playlist)
        self.tracks = list(tracks)
        self.selected_track_ids: list[int] = []
        self.visible_tracks = self.tracks[:PLAYLIST_TRACK_SELECT_LIMIT]
        if self.visible_tracks:
            self.add_item(PlaylistTrackSelect(self, self.visible_tracks))

    def build_embed(self) -> discord.Embed:
        title = str(self.playlist.get("name") or "Безымянный плейлист")
        embed = discord.Embed(
            title=f"Плейлист: {title}",
            description="Выбери отголоски и реши, должны ли они зазвучать снова или исчезнуть из списка.",
            color=discord.Color.dark_teal(),
        )
        if self.tracks:
            embed.add_field(
                name="Состав",
                value="\n\n".join(
                    _playlist_track_line(track, idx)
                    for idx, track in enumerate(self.visible_tracks, start=1)
                ),
                inline=False,
            )
            hidden = len(self.tracks) - len(self.visible_tracks)
            if hidden > 0:
                embed.add_field(
                    name="Важно",
                    value=f"Сейчас показаны первые {len(self.visible_tracks)} записей. Остальные {hidden} пока скрыты.",
                    inline=False,
                )
        else:
            embed.add_field(name="Состав", value="В этом плейлисте пока нет отголосков.", inline=False)
        if self.selected_track_ids:
            embed.set_footer(text=f"Выбрано отголосков: {len(self.selected_track_ids)}")
        else:
            embed.set_footer(text="Сначала выдели один или несколько треков.")
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_user_id:
            await safe_send(interaction, "Этот плейлист раскрыт не для тебя.", ephemeral=True)
            return False
        return True

    async def handle_selection(self, interaction: discord.Interaction, track_ids: list[int]) -> None:
        ok = await safe_defer_update(interaction)
        if not ok:
            return
        self.selected_track_ids = list(track_ids)
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    @discord.ui.button(label="Добавить выбранное в очередь", style=discord.ButtonStyle.primary, row=1)
    async def add_selected(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.enqueue_playlist_selection(interaction, self)

    @discord.ui.button(label="Удалить выбранное", style=discord.ButtonStyle.danger, row=1)
    async def delete_selected(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.delete_playlist_selection(interaction, self)

    @discord.ui.button(label="К плейлистам", style=discord.ButtonStyle.secondary, row=1)
    async def back_to_playlists(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.open_user_playlists(interaction, force_refresh=True)


class CreatePlaylistModal(discord.ui.Modal, title="Новый плейлист"):
    name = discord.ui.TextInput(
        label="Название плейлиста",
        placeholder="Например: Ночные отголоски",
        max_length=PLAYLIST_NAME_MAX_LENGTH,
        required=True,
    )

    def __init__(self, cog: "MusicCog", owner_user_id: int, *, track_snapshot: Optional[dict] = None):
        super().__init__(timeout=5 * 60)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.track_snapshot = dict(track_snapshot) if track_snapshot else None

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.create_playlist_from_modal(
            interaction,
            self.owner_user_id,
            str(self.name),
            track_snapshot=self.track_snapshot,
        )


class AddTrackModal(discord.ui.Modal, title="Добавить трек"):
    query = discord.ui.TextInput(
        label="Ссылка или название трека",
        placeholder="Например: ссылка SoundCloud / Audius / Jamendo или название трека",
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

    @discord.ui.button(
        label="Добавить в плейлист",
        style=discord.ButtonStyle.secondary,
        custom_id="music:add_playlist",
        row=1,
    )
    async def add_to_playlist(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.open_add_to_playlist(interaction)

    @discord.ui.button(
        label="Мои плейлисты",
        style=discord.ButtonStyle.secondary,
        custom_id="music:my_playlists",
        row=1,
    )
    async def my_playlists(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.open_user_playlists(interaction)

    @discord.ui.button(label="Радио", style=discord.ButtonStyle.secondary, custom_id="music:radio", row=1)
    async def radio(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.open_radio_menu(interaction)

    @discord.ui.button(label="Больше такого", style=discord.ButtonStyle.secondary, custom_id="music:radio_more", row=2)
    async def radio_more(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.radio_more_like_this(interaction)

    @discord.ui.button(label="Меньше такого", style=discord.ButtonStyle.secondary, custom_id="music:radio_less", row=2)
    async def radio_less(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.radio_less_like_this(interaction)

    @discord.ui.button(label="Выключить радио", style=discord.ButtonStyle.danger, custom_id="music:radio_disable", row=2)
    async def radio_disable(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        cog: Optional[MusicCog] = interaction.client.get_cog("MusicCog")  # type: ignore
        if cog:
            await cog.disable_radio(interaction)


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
        self._audius_api_host: Optional[str] = None
        self._radio_prefetch_task: Optional[asyncio.Task] = None
        self._radio_prefetch_basis_identity: str = ""

    async def cog_load(self) -> None:
        if not self.panel_watcher.is_running():
            self.panel_watcher.start()
        asyncio.create_task(self._bootstrap_panel())

    async def cog_unload(self) -> None:
        if self.panel_watcher.is_running():
            self.panel_watcher.cancel()
        self._cancel_radio_prefetch()

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

    def _repo(self):
        return getattr(self.bot, "repo", None)

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
        elif session.radio.pending:
            state_text = "Алтарь ищет следующий радио-отголосок."
        elif session.state == "idle":
            state_text = "Поток стих. Алтарь ждёт следующий трек."
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
            if session.current_track.artwork_url:
                embed.set_image(url=session.current_track.artwork_url)
        elif session and session.radio.pending:
            embed.add_field(
                name="Сейчас звучит",
                value="Алтарь перебирает созвучные отголоски и готовит следующий поток.",
                inline=False,
            )
        elif session and session.state == "idle":
            embed.add_field(
                name="Сейчас звучит",
                value="Сейчас тишина. Алтарь ждёт следующий трек.",
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

        radio_text = _radio_mode_text(session.radio) if session else "обычная очередь"
        embed.add_field(name="Режим", value=radio_text, inline=False)

        if session and session.queue:
            lines: list[str] = []
            shown = session.queue[:QUEUE_PREVIEW_LIMIT]
            for idx, track in enumerate(shown, start=1):
                lines.append(f"{idx}. **{track.title}** — {track.artist} ({_fmt_duration(track.duration)})")
            rest = len(session.queue) - len(shown)
            if rest > 0:
                lines.append(f"...и еще **{rest}** треков")
            queue_text = "\n".join(lines)
        elif session and session.radio.mode != RADIO_MODE_OFF:
            queue_text = "Ручная очередь пуста. Радио само призовет следующий отголосок."
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
        await safe_defer_update(interaction)
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
        conflict_text = conflict_text_for_request(self.bot, PLAYBACK_MODE_MUSIC)
        if self.session is None:
            if conflict_text:
                return False, member, conflict_text
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

    async def _playlist_panel_access_check(
        self,
        interaction: discord.Interaction,
    ) -> tuple[bool, Optional[discord.Member], str]:
        if not isinstance(interaction.user, discord.Member):
            return False, None, "Действие доступно только на сервере."
        member = interaction.user
        if self.session is None:
            return True, member, ""
        if self._is_admin(member.id):
            return True, member, ""
        voice = getattr(member, "voice", None)
        if voice is None or voice.channel is None:
            return False, member, "Алтарь уже звучит. Чтобы управлять его плейлистами, приди в тот же голосовой зал."
        if int(voice.channel.id) != int(self.session.voice_channel_id):
            return False, member, "Алтарь уже звучит в другом зале. Приди туда, где уже течет поток звука."
        return True, member, ""

    def _cancel_radio_prefetch(self) -> None:
        task = self._radio_prefetch_task
        self._radio_prefetch_task = None
        self._radio_prefetch_basis_identity = ""
        if task and not task.done():
            task.cancel()

    def _clear_radio_prefetch_locked(self, session: MusicSession) -> None:
        session.radio.prefetched_track = None
        session.radio.prefetched_basis_identity = ""

    def _clone_radio_state(self, radio: RadioState, *, pending: Optional[bool] = None) -> RadioState:
        return RadioState(
            mode=str(radio.mode or RADIO_MODE_OFF),
            genre_key=str(radio.genre_key or ""),
            genre_label=str(radio.genre_label or ""),
            station_key=str(radio.station_key or ""),
            station_label=str(radio.station_label or ""),
            requested_by_user_id=int(radio.requested_by_user_id or 0),
            history=list(radio.history),
            artist_history=list(radio.artist_history),
            liked_terms=list(radio.liked_terms),
            disliked_terms=list(radio.disliked_terms),
            liked_artists=list(radio.liked_artists),
            disliked_artists=list(radio.disliked_artists),
            pending=radio.pending if pending is None else bool(pending),
            notice=str(radio.notice or ""),
            prefetched_track=None,
            prefetched_basis_identity="",
        )

    def _make_radio_state(
        self,
        *,
        mode: str,
        requested_by_user_id: int,
        genre_key: str = "",
        genre_label: str = "",
        station_key: str = "",
        station_label: str = "",
    ) -> RadioState:
        return RadioState(
            mode=mode,
            genre_key=str(genre_key or ""),
            genre_label=str(genre_label or ""),
            station_key=str(station_key or ""),
            station_label=str(station_label or ""),
            requested_by_user_id=int(requested_by_user_id),
            history=[],
            artist_history=[],
            liked_terms=[],
            disliked_terms=[],
            liked_artists=[],
            disliked_artists=[],
            pending=False,
            notice="",
            prefetched_track=None,
            prefetched_basis_identity="",
        )

    def _reset_radio_locked(self, session: MusicSession, *, notice: str = "") -> None:
        session.radio = RadioState(mode=RADIO_MODE_OFF, notice=str(notice or ""))

    def _remember_radio_track_locked(self, session: MusicSession, track: MusicTrack) -> None:
        ident = _track_identity(track)
        history = [item for item in session.radio.history if item != ident]
        history.append(ident)
        session.radio.history = history[-RADIO_HISTORY_LIMIT:]
        artist = _normalize_text(track.artist)
        if artist:
            session.radio.artist_history = _push_unique_limited(
                session.radio.artist_history,
                [artist],
                limit=RADIO_RECENT_ARTIST_LIMIT,
            )
        session.radio.notice = ""

    def _radio_queries_for_state(self, radio: RadioState, basis_track: Optional[MusicTrack]) -> list[str]:
        queries: list[str] = []
        if radio.mode == RADIO_MODE_TRACK:
            if basis_track is None:
                return []
            queries.extend(_radio_queries_from_track(basis_track))
        elif radio.mode == RADIO_MODE_GENRE:
            genre = RADIO_GENRES_BY_KEY.get(str(radio.genre_key))
            if genre:
                queries.extend(str(item) for item in genre.get("queries", ()) if str(item).strip())
            elif radio.genre_label:
                queries.append(radio.genre_label)
            if basis_track is not None:
                queries.extend(_radio_queries_from_track(basis_track)[:2])
        elif radio.mode in {RADIO_MODE_STATION, RADIO_MODE_ORDINARY}:
            station = RADIO_STATIONS_BY_KEY.get(str(radio.station_key or "")) or ORDINARY_RADIO_STATION
            station_queries = [str(item) for item in station.get("queries", ()) if str(item).strip()]
            random.shuffle(station_queries)
            queries.extend(station_queries)
            if basis_track is not None:
                queries.extend(_radio_queries_from_track(basis_track)[:2])
        else:
            return []

        if radio.liked_artists:
            queries.extend(radio.liked_artists[-2:])
        if radio.liked_terms:
            queries.extend(radio.liked_terms[-3:])

        unique: list[str] = []
        seen: set[str] = set()
        for query in queries:
            clean = _compact_spaces(query)
            if not clean:
                continue
            key = clean.casefold()
            if key in seen:
                continue
            seen.add(key)
            unique.append(clean)
        return unique[:RADIO_QUERY_LIMIT]

    def _radio_candidate_session_score(
        self,
        candidate: TrackCandidate,
        radio: RadioState,
        *,
        basis_track: Optional[MusicTrack],
    ) -> float:
        score = 0.0
        candidate_text = _normalize_text(f"{candidate.title} {candidate.artist}")
        candidate_terms = set(_radio_terms(candidate.title, candidate.artist))
        candidate_artist = _normalize_text(candidate.artist)

        if basis_track is not None:
            basis_terms = set(_radio_terms(basis_track.title, basis_track.artist))
            basis_title_terms = set(_radio_terms(basis_track.title))
            basis_artist = _normalize_text(basis_track.artist)
            if basis_artist and candidate_artist == basis_artist:
                score += 12.0
            score += 3.0 * len(candidate_terms & basis_title_terms)
            score += 1.5 * len(candidate_terms & basis_terms)

        if radio.genre_label:
            genre_terms = set(_radio_terms(radio.genre_label))
            score += 2.5 * len(candidate_terms & genre_terms)

        station = RADIO_STATIONS_BY_KEY.get(str(radio.station_key or ""))
        if station:
            for term in station.get("boost_terms", ()):
                token = _normalize_text(str(term))
                if token and token in candidate_text:
                    score += 4.5
            for term in station.get("avoid_terms", ()):
                token = _normalize_text(str(term))
                if token and token in candidate_text:
                    score -= 5.5

        if candidate_artist:
            if candidate_artist == (radio.artist_history[-1] if radio.artist_history else ""):
                score -= 14.0
            elif candidate_artist in radio.artist_history:
                score -= 7.0
            if candidate_artist in radio.liked_artists:
                score += 11.0
            if candidate_artist in radio.disliked_artists:
                score -= 15.0

        if radio.liked_terms:
            score += 2.5 * len(candidate_terms & set(radio.liked_terms))
        if radio.disliked_terms:
            score -= 3.5 * len(candidate_terms & set(radio.disliked_terms))

        if 0 < int(candidate.duration or 0) < 55:
            score -= 10.0
        return score

    async def _find_radio_candidate(
        self,
        radio: RadioState,
        *,
        basis_track: Optional[MusicTrack],
        fast_mode: bool = False,
    ) -> Optional[TrackCandidate]:
        excluded: set[str] = set(radio.history)
        if basis_track is not None:
            excluded.add(_track_identity(basis_track))
        queries = self._radio_queries_for_state(radio, basis_track)
        if not queries:
            return None

        seen_local: set[str] = set()
        ranked: list[tuple[float, int, int, TrackCandidate]] = []
        source_unavailable = False
        for query_index, query in enumerate(queries):
            try:
                result = await self._search_candidates(query, fast_mode=fast_mode)
            except SearchSourcesUnavailableError:
                source_unavailable = True
                continue
            except Exception as exc:
                log.warning("music radio candidate build crashed: %s", exc)
                continue
            for candidate in result:
                ident = _candidate_identity(candidate)
                if ident in excluded or ident in seen_local:
                    continue
                seen_local.add(ident)
                if basis_track is not None:
                    same_title = _normalize_text(candidate.title) == _normalize_text(basis_track.title)
                    same_artist = _normalize_text(candidate.artist) == _normalize_text(basis_track.artist)
                    if same_title and same_artist:
                        continue
                score = _candidate_score(candidate, query) + self._radio_candidate_session_score(
                    candidate,
                    radio,
                    basis_track=basis_track,
                ) - float(query_index)
                ranked.append((score, candidate.native_rank, query_index, candidate))
            if fast_mode and ranked and max(item[0] for item in ranked) >= RADIO_GOOD_MATCH_SCORE:
                break

        if not ranked:
            if source_unavailable:
                raise SearchSourcesUnavailableError(SOURCES_UNAVAILABLE_TEXT)
            return None
        ranked.sort(key=lambda item: (-item[0], item[1], item[2]))
        return ranked[0][3]

    async def _build_radio_track_for_state(
        self,
        radio: RadioState,
        *,
        basis_track: Optional[MusicTrack],
        requested_by_user_id: int,
        fast_mode: bool = False,
    ) -> Optional[MusicTrack]:
        candidate = await self._find_radio_candidate(radio, basis_track=basis_track, fast_mode=fast_mode)
        if candidate is None:
            return None
        track = self._candidate_to_track(candidate, requested_by_user_id=requested_by_user_id)
        track.is_radio_track = True
        track.radio_mode = str(radio.mode or RADIO_MODE_OFF)
        track.radio_reason = _radio_reason_text(radio)
        return track

    async def _kickoff_radio_if_needed(
        self,
        member: discord.Member,
        radio: RadioState,
    ) -> tuple[bool, str]:
        session = self.session
        if session is not None and (session.current_track is not None or session.queue or session.radio.pending):
            async with self._lock:
                if self.session is not None:
                    self._cancel_radio_prefetch()
                    self.session.radio = radio
                    self._clear_radio_prefetch_locked(self.session)
            await self._update_panel_message()
            await self._schedule_radio_prefetch()
            return False, ""

        track = await self._build_radio_track_for_state(
            radio,
            basis_track=session.current_track if session is not None else None,
            requested_by_user_id=int(radio.requested_by_user_id or member.id),
            fast_mode=False,
        )
        if track is None:
            return False, RADIO_NO_RESULT_TEXT

        started_now, queued_now = await self._enqueue_tracks(member, [track])
        async with self._lock:
            if self.session is not None:
                self.session.radio = radio
                self._remember_radio_track_locked(self.session, track)
        await self._update_panel_message()
        await self._schedule_radio_prefetch()
        return started_now > 0 or queued_now > 0, ""

    async def open_radio_menu(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        await safe_send(interaction, embed=_radio_menu_embed(), view=RadioModeView(self, member.id), ephemeral=True)

    async def start_radio_from_current_track(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            session = self.session
            if session is None or session.current_track is None:
                await safe_send(
                    interaction,
                    "Радио по текущему треку доступно только тогда, когда уже звучит отголосок.",
                    ephemeral=True,
                )
                return
            self._cancel_radio_prefetch()
            session.radio = self._make_radio_state(mode=RADIO_MODE_TRACK, requested_by_user_id=member.id)
        await self._update_panel_message()
        await self._schedule_radio_prefetch()
        await safe_send(interaction, RADIO_TRACK_START_TEXT, ephemeral=True)

    async def start_radio_genre(self, interaction: discord.Interaction, genre_key: str) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        genre = RADIO_GENRES_BY_KEY.get(str(genre_key))
        if genre is None:
            await safe_send(interaction, "Не удалось распознать жанр для радио.", ephemeral=True)
            return
        await safe_send(interaction, RADIO_GENRE_PROGRESS_TEXT, ephemeral=True)

        radio = self._make_radio_state(
            mode=RADIO_MODE_GENRE,
            requested_by_user_id=member.id,
            genre_key=str(genre["key"]),
            genre_label=str(genre["label"]),
        )
        try:
            _, error_text = await self._kickoff_radio_if_needed(member, radio)
        except SearchSourcesUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        except Exception as exc:
            await safe_send(interaction, f"Не удалось запустить жанровое радио: {exc}", ephemeral=True)
            return
        if error_text:
            await safe_send(
                interaction,
                f"Алтарь не смог настроиться на жанр **{genre['label']}**. Полных playable-отголосков не найдено.",
                ephemeral=True,
            )
            return
        await safe_send(interaction, f"{RADIO_GENRE_START_TEXT}\nЖанр: **{genre['label']}**.", ephemeral=True)

    async def start_radio_station(self, interaction: discord.Interaction, station_key: str) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        station = RADIO_STATIONS_BY_KEY.get(str(station_key))
        if station is None:
            await safe_send(interaction, "Не удалось распознать станцию Алтаря.", ephemeral=True)
            return
        await safe_send(interaction, f"{RADIO_STATION_PROGRESS_TEXT}\nСтанция: **{station['label']}**.", ephemeral=True)

        radio = self._make_radio_state(
            mode=RADIO_MODE_STATION,
            requested_by_user_id=member.id,
            station_key=str(station["key"]),
            station_label=str(station["label"]),
        )
        try:
            _, error_text = await self._kickoff_radio_if_needed(member, radio)
        except SearchSourcesUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        except Exception as exc:
            await safe_send(interaction, f"Не удалось запустить станцию: {exc}", ephemeral=True)
            return
        if error_text:
            await safe_send(
                interaction,
                f"Алтарь не смог раскрыть станцию **{station['label']}**. Подходящий поток отголосков не найден.",
                ephemeral=True,
            )
            return
        await safe_send(interaction, f"{RADIO_STATION_START_TEXT}\nСтанция: **{station['label']}**.", ephemeral=True)

    async def start_ordinary_radio(self, interaction: discord.Interaction) -> None:
        await self.start_radio_station(interaction, str(ORDINARY_RADIO_STATION["key"]))

    def _apply_radio_feedback_locked(self, session: MusicSession, track: MusicTrack, *, positive: bool) -> None:
        artist = _normalize_text(track.artist)
        terms = _radio_terms(track.title, track.artist)
        if positive:
            session.radio.liked_artists = _push_unique_limited(
                [item for item in session.radio.liked_artists if item != artist],
                [artist] if artist else [],
                limit=RADIO_FEEDBACK_MEMORY_LIMIT,
            )
            session.radio.liked_terms = _push_unique_limited(
                [item for item in session.radio.liked_terms if item not in terms],
                terms,
                limit=RADIO_FEEDBACK_MEMORY_LIMIT,
            )
            session.radio.disliked_artists = [item for item in session.radio.disliked_artists if item != artist]
            session.radio.disliked_terms = [item for item in session.radio.disliked_terms if item not in terms]
            session.radio.notice = RADIO_MORE_TEXT
        else:
            session.radio.disliked_artists = _push_unique_limited(
                [item for item in session.radio.disliked_artists if item != artist],
                [artist] if artist else [],
                limit=RADIO_FEEDBACK_MEMORY_LIMIT,
            )
            session.radio.disliked_terms = _push_unique_limited(
                [item for item in session.radio.disliked_terms if item not in terms],
                terms,
                limit=RADIO_FEEDBACK_MEMORY_LIMIT,
            )
            session.radio.liked_artists = [item for item in session.radio.liked_artists if item != artist]
            session.radio.liked_terms = [item for item in session.radio.liked_terms if item not in terms]
            session.radio.notice = RADIO_LESS_TEXT
        self._clear_radio_prefetch_locked(session)
        self._cancel_radio_prefetch()

    async def radio_more_like_this(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            session = self.session
            if session is None or session.radio.mode == RADIO_MODE_OFF:
                await safe_send(interaction, RADIO_NO_ACTIVE_TEXT, ephemeral=True)
                return
            if session.current_track is None:
                await safe_send(interaction, RADIO_NOTHING_PLAYING_TEXT, ephemeral=True)
                return
            self._apply_radio_feedback_locked(session, session.current_track, positive=True)
        await self._update_panel_message()
        await self._schedule_radio_prefetch()
        await safe_send(interaction, RADIO_MORE_TEXT, ephemeral=True)

    async def radio_less_like_this(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            session = self.session
            if session is None or session.radio.mode == RADIO_MODE_OFF:
                await safe_send(interaction, RADIO_NO_ACTIVE_TEXT, ephemeral=True)
                return
            if session.current_track is None:
                await safe_send(interaction, RADIO_NOTHING_PLAYING_TEXT, ephemeral=True)
                return
            self._apply_radio_feedback_locked(session, session.current_track, positive=False)
        await self._update_panel_message()
        await self._schedule_radio_prefetch()
        await safe_send(interaction, RADIO_LESS_TEXT, ephemeral=True)

    async def disable_radio(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        if self.session is None:
            await safe_send(interaction, RADIO_NO_ACTIVE_TEXT, ephemeral=True)
            return
        ok, _, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        async with self._lock:
            if self.session is None:
                await safe_send(interaction, RADIO_NO_ACTIVE_TEXT, ephemeral=True)
                return
            self._cancel_radio_prefetch()
            self._reset_radio_locked(self.session)
        await self._update_panel_message()
        await safe_send(interaction, RADIO_DISABLED_TEXT, ephemeral=True)

    async def _schedule_radio_prefetch(self) -> None:
        async with self._lock:
            session = self.session
            if session is None or session.radio.mode == RADIO_MODE_OFF:
                self._cancel_radio_prefetch()
                return
            if session.current_track is None or session.queue or session.radio.pending:
                return
            basis_track = session.current_track
            basis_identity = _track_identity(basis_track)
            if (
                session.radio.prefetched_track is not None
                and session.radio.prefetched_basis_identity == basis_identity
            ):
                return
            if (
                self._radio_prefetch_task is not None
                and not self._radio_prefetch_task.done()
                and self._radio_prefetch_basis_identity == basis_identity
            ):
                return

            radio_snapshot = self._clone_radio_state(session.radio, pending=False)
            requester_id = int(session.radio.requested_by_user_id or session.started_by_user_id)
            self._clear_radio_prefetch_locked(session)
            self._cancel_radio_prefetch()
            self._radio_prefetch_basis_identity = basis_identity
            self._radio_prefetch_task = asyncio.create_task(
                self._radio_prefetch_worker(radio_snapshot, basis_track, requester_id, basis_identity)
            )

    async def _radio_prefetch_worker(
        self,
        radio_snapshot: RadioState,
        basis_track: MusicTrack,
        requester_id: int,
        basis_identity: str,
    ) -> None:
        next_track: Optional[MusicTrack] = None
        source_unavailable = False
        try:
            for _ in range(RADIO_AUTOPICK_RETRY_LIMIT):
                try:
                    next_track = await self._build_radio_track_for_state(
                        radio_snapshot,
                        basis_track=basis_track,
                        requested_by_user_id=requester_id,
                        fast_mode=True,
                    )
                except SearchSourcesUnavailableError:
                    source_unavailable = True
                    next_track = None
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("music radio prefetch build failed")
                    next_track = None
                if next_track is not None:
                    break

            async with self._lock:
                session = self.session
                if session is None or session.radio.mode == RADIO_MODE_OFF:
                    return
                current_identity = _track_identity(session.current_track) if session.current_track else ""
                if current_identity != basis_identity:
                    return
                if session.queue:
                    return
                if session.radio.mode != radio_snapshot.mode:
                    return
                if str(session.radio.genre_key or "") != str(radio_snapshot.genre_key or ""):
                    return
                if str(session.radio.station_key or "") != str(radio_snapshot.station_key or ""):
                    return
                session.radio.prefetched_track = next_track
                session.radio.prefetched_basis_identity = basis_identity if next_track is not None else ""
                if source_unavailable and next_track is None:
                    session.radio.notice = SOURCES_UNAVAILABLE_TEXT
                if next_track is not None:
                    log.info(
                        "music radio prefetch ready: title=%r mode=%s reason=%s",
                        next_track.title,
                        session.radio.mode,
                        next_track.radio_reason,
                    )
        except asyncio.CancelledError:
            raise
        finally:
            if self._radio_prefetch_task is asyncio.current_task():
                self._radio_prefetch_task = None
                if self._radio_prefetch_basis_identity == basis_identity:
                    self._radio_prefetch_basis_identity = ""

    async def _wait_ready_radio_track_for_skip(self) -> bool:
        async with self._lock:
            session = self.session
            if session is None or session.radio.mode == RADIO_MODE_OFF:
                return False
            if session.queue:
                return True
            if session.radio.prefetched_track is not None:
                return True
            current_track = session.current_track
            if current_track is None:
                return False
            session.radio.notice = RADIO_PREFETCH_NOTICE
        await self._update_panel_message()
        await self._schedule_radio_prefetch()
        task = self._radio_prefetch_task
        if task is None:
            return False
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=RADIO_SKIP_WAIT_SECONDS)
        except asyncio.TimeoutError:
            return False
        except asyncio.CancelledError:
            return False
        async with self._lock:
            session = self.session
            return bool(session and session.radio.prefetched_track is not None)

    def _track_to_playlist_snapshot(self, track: MusicTrack) -> dict:
        payload = {
            "source_key": str(track.source_key or SOURCE_SOUNDCLOUD),
            "source_name": str(track.source_name or SOURCE_NAMES.get(track.source_key, "SoundCloud")),
            "title": str(track.title or ""),
            "artist": str(track.artist or ""),
            "duration": int(track.duration or 0),
            "webpage_url": str(track.webpage_url or ""),
            "stream_url": str(track.stream_url or ""),
            "artwork_url": str(track.artwork_url or ""),
        }
        return {
            "source": str(track.source_key or SOURCE_SOUNDCLOUD),
            "source_track_ref": str(track.webpage_url or track.stream_url or ""),
            "title": str(track.title or ""),
            "artist": str(track.artist or ""),
            "duration": int(track.duration or 0),
            "artwork_url": str(track.artwork_url or ""),
            "canonical_url": str(track.webpage_url or ""),
            "playable_payload": payload,
        }

    async def _track_from_playlist_entry(self, entry: dict, *, requested_by_user_id: int) -> MusicTrack:
        canonical_url = str(entry.get("canonical_url") or entry.get("source_track_ref") or "")
        if canonical_url:
            try:
                track = await self._resolve_track(canonical_url, requested_by_user_id=requested_by_user_id)
                if not track.artwork_url:
                    track.artwork_url = str(entry.get("artwork_url") or "")
                return track
            except Exception as exc:
                log.info(
                    "playlist restore fallback: playlist_track_id=%s canonical=%r failed=%s",
                    entry.get("playlist_track_id"),
                    canonical_url,
                    exc,
                )

        payload = entry.get("playable_payload") or {}
        stream_url = str(payload.get("stream_url") or "")
        if not stream_url:
            raise RuntimeError("Не удалось восстановить сохраненный отголосок.")
        source_key = str(payload.get("source_key") or entry.get("source") or SOURCE_SOUNDCLOUD)
        source_name = str(payload.get("source_name") or SOURCE_NAMES.get(source_key, source_key.title()))
        return MusicTrack(
            title=str(payload.get("title") or entry.get("title") or "Безымянный отголосок"),
            artist=str(payload.get("artist") or entry.get("artist") or "Неизвестный исполнитель"),
            duration=int(payload.get("duration") or entry.get("duration") or 0),
            webpage_url=str(payload.get("webpage_url") or canonical_url),
            stream_url=stream_url,
            requested_by_user_id=requested_by_user_id,
            source_key=source_key,
            source_name=source_name,
            artwork_url=str(payload.get("artwork_url") or entry.get("artwork_url") or ""),
        )

    async def _render_playlist_browser(
        self,
        interaction: discord.Interaction,
        owner_user_id: int,
        *,
        edit: bool,
        notice_text: str = "",
    ) -> None:
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        playlists = await repo.playlist_list(int(owner_user_id))
        embed = discord.Embed(
            title="Мои плейлисты",
            description="Алтарь раскрыл твои плейлисты. Выбери, что должно зазвучать дальше.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(
            name="Список",
            value="\n".join(
                _playlist_line(playlist, idx)
                for idx, playlist in enumerate(playlists[:PLAYLIST_SELECT_LIMIT], start=1)
            ),
            inline=False,
        )
        hidden = len(playlists) - min(len(playlists), PLAYLIST_SELECT_LIMIT)
        if hidden > 0:
            embed.set_footer(text=f"Скрыто плейлистов: {hidden}")
        view = PlaylistBrowserView(self, int(owner_user_id), playlists)
        if edit:
            await interaction.edit_original_response(content=notice_text or None, embed=embed, view=view)
        else:
            await safe_send(interaction, notice_text or None, embed=embed, view=view, ephemeral=True)

    async def _render_playlist_contents(
        self,
        interaction: discord.Interaction,
        owner_user_id: int,
        playlist_id: int,
        *,
        edit: bool,
    ) -> None:
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        playlist = await repo.playlist_get(int(owner_user_id), int(playlist_id))
        if not playlist:
            await safe_send(interaction, "Плейлист не найден.", ephemeral=True)
            return
        tracks = await repo.playlist_tracks_list(int(owner_user_id), int(playlist_id))
        view = PlaylistTracksView(self, int(owner_user_id), playlist, tracks)
        if edit:
            await interaction.response.edit_message(embed=view.build_embed(), view=view)
        else:
            await safe_send(interaction, embed=view.build_embed(), view=view, ephemeral=True)

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

    async def open_add_to_playlist(self, interaction: discord.Interaction) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        session = self.session
        if session is None or session.current_track is None:
            await safe_send(interaction, "Сейчас Алтарь безмолвен — сохранять пока нечего.", ephemeral=True)
            return
        await repo.playlist_ensure_favorites(member.id)
        playlists = await repo.playlist_list(member.id)
        snapshot = self._track_to_playlist_snapshot(session.current_track)
        embed = discord.Embed(
            title="Сохранить текущий отголосок",
            description="Выбери плейлист, куда должен лечь именно этот вариант трека.",
            color=discord.Color.dark_teal(),
        )
        embed.add_field(name="Сейчас звучит", value=_track_lines(session.current_track, self._guild()), inline=False)
        if session.current_track.artwork_url:
            embed.set_thumbnail(url=session.current_track.artwork_url)
        view = PlaylistTargetView(self, member.id, snapshot, playlists)
        await safe_send(interaction, embed=embed, view=view, ephemeral=True)

    async def open_user_playlists(self, interaction: discord.Interaction, *, force_refresh: bool = False) -> None:
        if force_refresh:
            await safe_defer_ephemeral(interaction)
        else:
            await safe_defer_ephemeral(interaction)
        ok, member, text = await self._playlist_panel_access_check(interaction)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        await repo.playlist_ensure_favorites(member.id)
        await self._render_playlist_browser(interaction, member.id, edit=force_refresh)

    async def open_playlist_contents(self, interaction: discord.Interaction, owner_user_id: int, playlist_id: int) -> None:
        await self._render_playlist_contents(interaction, owner_user_id, playlist_id, edit=True)

    async def create_playlist_from_modal(
        self,
        interaction: discord.Interaction,
        owner_user_id: int,
        raw_name: str,
        *,
        track_snapshot: Optional[dict] = None,
    ) -> None:
        await safe_defer_ephemeral(interaction)
        if int(interaction.user.id) != int(owner_user_id):
            await safe_send(interaction, "Этот плейлист создается не для тебя.", ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        clean_name = str(raw_name or "").strip()
        if not clean_name:
            await safe_send(interaction, "Название плейлиста не должно быть пустым.", ephemeral=True)
            return
        try:
            playlist = await repo.playlist_create(owner_user_id, clean_name)
        except ValueError as exc:
            code = str(exc)
            if "duplicate_name" in code:
                await safe_send(interaction, "Плейлист с таким именем уже существует.", ephemeral=True)
                return
            if "too_long" in code:
                await safe_send(interaction, f"Название плейлиста должно быть не длиннее {PLAYLIST_NAME_MAX_LENGTH} символов.", ephemeral=True)
                return
            await safe_send(interaction, "Не удалось создать плейлист.", ephemeral=True)
            return

        if track_snapshot:
            await repo.playlist_add_track(owner_user_id, int(playlist["playlist_id"]), track_snapshot)
            await safe_send(
                interaction,
                f"Этот отголосок сохранен в плейлисте «{playlist['name']}».",
                ephemeral=True,
            )
            return

        await self._render_playlist_browser(
            interaction,
            owner_user_id,
            edit=True,
            notice_text=f"Плейлист «{playlist['name']}» создан.",
        )

    async def save_track_snapshot_to_playlist(
        self,
        interaction: discord.Interaction,
        owner_user_id: int,
        track_snapshot: dict,
        playlist_id: int,
    ) -> None:
        await safe_defer_ephemeral(interaction)
        if int(interaction.user.id) != int(owner_user_id):
            await safe_send(interaction, "Этот выбор сохранения был открыт не для тебя.", ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        playlist = await repo.playlist_get(owner_user_id, playlist_id)
        if not playlist:
            await safe_send(interaction, "Плейлист не найден.", ephemeral=True)
            return
        await repo.playlist_add_track(owner_user_id, playlist_id, track_snapshot)
        await safe_send(
            interaction,
            f"Этот отголосок сохранен в плейлисте «{playlist['name']}».",
            ephemeral=True,
        )

    async def handle_add_submit(self, interaction: discord.Interaction, raw_query: str) -> None:
        await safe_defer_ephemeral(interaction)
        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return
        repo = self._repo()
        if repo:
            try:
                await repo.playlist_ensure_favorites(member.id)
            except Exception:
                log.exception("music favorites bootstrap failed for user=%s", member.id)

        query = str(raw_query or "").strip()
        if not query:
            await safe_send(interaction, "Нужна ссылка SoundCloud, Audius, Jamendo или поисковый запрос.", ephemeral=True)
            return

        dep_error = self._dependency_error()
        if dep_error:
            await safe_send(interaction, dep_error, ephemeral=True)
            return
        source_key = _detect_source_from_url(query) if "://" in query else None
        if "://" in query:
            await safe_send(interaction, RESOLVE_PROGRESS_TEXT, ephemeral=True)
            if not source_key:
                await safe_send(interaction, UNSUPPORTED_LINK_TEXT, ephemeral=True)
                return
            try:
                track = await self._resolve_track(query, requested_by_user_id=member.id)
            except PreviewOnlyTrackError as exc:
                await safe_send(interaction, str(exc), ephemeral=True)
                return
            except Exception as exc:
                await safe_send(interaction, f"Не удалось призвать трек: {exc}", ephemeral=True)
                return
            await self._enqueue_track(interaction, member, track)
            return

        await safe_send(interaction, SEARCH_PROGRESS_TEXT, ephemeral=True)
        try:
            candidates = await self._search_candidates(query)
        except SearchSourcesUnavailableError as exc:
            await safe_send(interaction, str(exc), ephemeral=True)
            return
        if not candidates:
            await safe_send(interaction, NO_PLAYABLE_RESULTS_TEXT, ephemeral=True)
            return

        view = TrackChoiceView(self, member.id, candidates)
        embed = discord.Embed(
            title="Выбор отголоска",
            description=SEARCH_RESULTS_TEXT,
            color=discord.Color.dark_teal(),
        )
        embed.add_field(
            name="Доступные варианты",
            value="\n\n".join(
                _candidate_embed_line(candidate, idx)
                for idx, candidate in enumerate(candidates, start=1)
            ),
            inline=False,
        )
        embed.set_footer(text="Показаны только полные playable-варианты.")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    async def enqueue_playlist_selection(self, interaction: discord.Interaction, view: PlaylistTracksView) -> None:
        await safe_defer_ephemeral(interaction)
        if not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Действие доступно только на сервере.", ephemeral=True)
            return
        if int(interaction.user.id) != view.owner_user_id:
            await safe_send(interaction, "Этот плейлист раскрыт не для тебя.", ephemeral=True)
            return
        if not view.selected_track_ids:
            await safe_send(interaction, "Сначала выдели один или несколько отголосков.", ephemeral=True)
            return

        ok, member, text = await self._voice_access_check(interaction, require_active_session=False)
        if not ok or member is None:
            await safe_send(interaction, text, ephemeral=True)
            return

        selected_entries = [
            track
            for track in view.tracks
            if int(track.get("playlist_track_id") or 0) in set(view.selected_track_ids)
        ]
        if not selected_entries:
            await safe_send(interaction, "Выбранные записи уже исчезли из этого плейлиста.", ephemeral=True)
            return

        await safe_send(
            interaction,
            "Алтарь извлекает сохраненные отголоски и готовит их к новому звучанию.",
            ephemeral=True,
        )

        resolved_tracks: list[MusicTrack] = []
        failed_titles: list[str] = []
        for entry in selected_entries:
            try:
                track = await self._track_from_playlist_entry(entry, requested_by_user_id=member.id)
            except Exception:
                failed_titles.append(str(entry.get("title") or "Безымянный отголосок"))
                continue
            resolved_tracks.append(track)

        if not resolved_tracks:
            await safe_send(interaction, "Ни один из выбранных отголосков не удалось восстановить для проигрывания.", ephemeral=True)
            return

        try:
            started_now, queued_now = await self._enqueue_tracks(member, resolved_tracks)
        except Exception as exc:
            await safe_send(interaction, f"Не удалось добавить плейлист в поток: {exc}", ephemeral=True)
            return

        view.selected_track_ids = []
        try:
            await interaction.edit_original_response(embed=view.build_embed(), view=view)
        except Exception:
            pass

        parts: list[str] = []
        if started_now > 0 and queued_now > 0:
            parts.append(f"Один отголосок уже зазвучал, ещё {queued_now} добавлено в очередь.")
        elif started_now > 0:
            parts.append("Выбранный отголосок уже зазвучал.")
        elif queued_now > 0:
            parts.append(f"В очередь добавлено {queued_now} отголосков.")
        if failed_titles:
            parts.append(f"Не удалось восстановить: {len(failed_titles)}.")
        await safe_send(interaction, " ".join(parts), ephemeral=True)

    async def delete_playlist_selection(self, interaction: discord.Interaction, view: PlaylistTracksView) -> None:
        await safe_defer_ephemeral(interaction)
        if int(interaction.user.id) != view.owner_user_id:
            await safe_send(interaction, "Этот плейлист раскрыт не для тебя.", ephemeral=True)
            return
        if not view.selected_track_ids:
            await safe_send(interaction, "Сначала выдели один или несколько отголосков.", ephemeral=True)
            return
        repo = self._repo()
        if not repo:
            await safe_send(interaction, "Музыкальное хранилище сейчас недоступно.", ephemeral=True)
            return
        removed = await repo.playlist_remove_tracks(view.owner_user_id, int(view.playlist.get("playlist_id") or 0), view.selected_track_ids)
        playlist = await repo.playlist_get(view.owner_user_id, int(view.playlist.get("playlist_id") or 0))
        tracks = await repo.playlist_tracks_list(view.owner_user_id, int(view.playlist.get("playlist_id") or 0))
        new_view = PlaylistTracksView(self, view.owner_user_id, playlist or view.playlist, tracks)
        try:
            await interaction.edit_original_response(embed=new_view.build_embed(), view=new_view)
        except Exception:
            pass
        if removed > 0:
            await safe_send(interaction, "Выбранные отголоски удалены из плейлиста.", ephemeral=True)
        else:
            await safe_send(interaction, "Нечего было удалять: выбранные записи уже исчезли.", ephemeral=True)

    async def enqueue_candidate(self, interaction: discord.Interaction, candidate: TrackCandidate) -> None:
        if not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "Действие доступно только на сервере.", ephemeral=True)
            return
        track = self._candidate_to_track(candidate, requested_by_user_id=interaction.user.id)
        await self._enqueue_track(interaction, interaction.user, track)

    def _candidate_to_track(self, candidate: TrackCandidate, *, requested_by_user_id: int) -> MusicTrack:
        return MusicTrack(
            title=candidate.title,
            artist=candidate.artist,
            duration=candidate.duration,
            webpage_url=candidate.webpage_url,
            stream_url=candidate.stream_url,
            requested_by_user_id=requested_by_user_id,
            source_key=candidate.source_key,
            source_name=candidate.source_name,
            artwork_url=candidate.artwork_url,
        )

    async def _enqueue_tracks(
        self,
        member: discord.Member,
        tracks: list[MusicTrack],
    ) -> tuple[int, int]:
        started_now = 0
        queued_now = 0
        async with self._lock:
            conflict_text = conflict_text_for_request(self.bot, PLAYBACK_MODE_MUSIC)
            if conflict_text and self.session is None:
                raise RuntimeError(conflict_text)
            if self.session is not None and not self._same_active_voice(member):
                raise RuntimeError("Алтарь уже звучит в другом зале.")

            if self.session is None:
                voice = getattr(member, "voice", None)
                if voice is None or voice.channel is None:
                    raise RuntimeError("Чтобы призвать музыку, войди в голосовой канал.")
                self.session = MusicSession(
                    guild_id=member.guild.id,
                    text_channel_id=self._altar_channel_id(),
                    voice_channel_id=voice.channel.id,
                    started_by_user_id=member.id,
                    state="playing",
                )
                self._last_panel_state = "playing"

            await self._ensure_voice_connection(member)

            session = self.session
            assert session is not None
            session.idle_disconnect_at = 0
            session.state = "playing"
            self._last_panel_state = "playing"
            if session.radio.mode != RADIO_MODE_OFF and any(not track.is_radio_track for track in tracks):
                self._cancel_radio_prefetch()
                self._clear_radio_prefetch_locked(session)

            for track in tracks:
                if session.current_track is None and not self._voice_client_playing():
                    session.current_track = track
                    try:
                        await self._play_current_locked()
                    except Exception:
                        if session.current_track is track:
                            session.current_track = None
                        raise
                    started_now += 1
                else:
                    session.queue.append(track)
                    queued_now += 1

        await self._update_panel_message()
        return started_now, queued_now

    async def _enqueue_track(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        track: MusicTrack,
    ) -> None:
        try:
            started_now, queued_now = await self._enqueue_tracks(member, [track])
        except Exception as exc:
            if self.session and self.session.current_track is None and not self.session.queue:
                self.session = None
                self._last_panel_state = "silent"
            await safe_send(interaction, f"Не удалось запустить трек: {exc}", ephemeral=True)
            await self._update_panel_message()
            return

        if started_now > 0:
            response_text = f"Алтарь принял трек: **{track.title}**."
        else:
            queue_position = len(self.session.queue) if self.session is not None else queued_now
            response_text = (
                f"Трек добавлен в очередь: **{track.title}**. "
                f"Позиция: **{queue_position}**."
            )
        await safe_send(interaction, response_text, ephemeral=True)

    async def pause(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
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
        await safe_defer_update(interaction)
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
        await safe_defer_update(interaction)
        ok, _, text = await self._voice_access_check(interaction, require_active_session=True)
        if not ok:
            await safe_send(interaction, text, ephemeral=True)
            return
        wait_for_radio_track = False
        async with self._lock:
            vc = self._voice_client()
            if not vc or not vc.is_connected():
                await safe_send(interaction, "Алтарь сейчас не связан с голосовым залом.", ephemeral=True)
                return
            if not (vc.is_playing() or vc.is_paused()):
                await safe_send(interaction, "Следующий отголосок звать пока не из чего.", ephemeral=True)
                return
            if self.session:
                now = time.monotonic()
                if now - float(self.session.last_skip_requested_at or 0.0) < 2.5:
                    await safe_send(interaction, "Алтарь уже перелистывает очередь. Подожди мгновение.", ephemeral=True)
                    return
                self.session.last_skip_requested_at = now
                self.session.paused = False
                self.session.state = "playing"
                if self.session.radio.mode != RADIO_MODE_OFF and not self.session.queue and self.session.radio.prefetched_track is None:
                    wait_for_radio_track = True
                    self.session.radio.notice = RADIO_PREFETCH_NOTICE
        if wait_for_radio_track:
            ready = await self._wait_ready_radio_track_for_skip()
            if not ready:
                await safe_send(
                    interaction,
                    "Алтарь еще вытягивает следующий отголосок. Текущий поток не оборван: подожди мгновение и попробуй снова.",
                    ephemeral=True,
                )
                return
        async with self._lock:
            vc = self._voice_client()
            if not vc or not vc.is_connected():
                await safe_send(interaction, "Алтарь сейчас не связан с голосовым залом.", ephemeral=True)
                return
            if not (vc.is_playing() or vc.is_paused()):
                await safe_send(interaction, "Следующий отголосок звать пока не из чего.", ephemeral=True)
                return
            vc.stop()
        await safe_send(interaction, "Текущий отголосок отпущен. Алтарь зовет следующий.", ephemeral=True)

    async def stop(self, interaction: discord.Interaction) -> None:
        await safe_defer_update(interaction)
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
            session.last_skip_requested_at = 0.0
            self._cancel_radio_prefetch()
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

    def _http_json_sync(self, url: str, *, headers: Optional[dict[str, str]] = None):
        merged_headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            merged_headers.update(headers)
        req = Request(url, headers=merged_headers)
        with urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)

    def _ydl_opts(self) -> dict:
        return {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "noplaylist": True,
            "format": "bestaudio/best",
            "default_search": "scsearch",
            "source_address": "0.0.0.0",
            "socket_timeout": 8,
        }

    def _soundcloud_candidate_from_entry(
        self,
        entry: dict,
        *,
        native_rank: int,
        strict_source: bool,
    ) -> Optional[TrackCandidate]:
        webpage_url = str(entry.get("webpage_url") or "")
        original_url = str(entry.get("original_url") or "")
        extractor_key = str(entry.get("extractor_key") or entry.get("extractor") or "").lower()
        if (
            "soundcloud" not in extractor_key
            and not _is_soundcloud_url(webpage_url)
            and not _is_soundcloud_url(original_url)
        ):
            if strict_source:
                raise RuntimeError("Нужна ссылка на трек SoundCloud.")
            return None

        audio_formats = [
            fmt
            for fmt in (entry.get("formats") or [])
            if fmt.get("vcodec") == "none" and fmt.get("url")
        ]
        preview_formats = [fmt for fmt in audio_formats if _soundcloud_format_is_preview(fmt)]
        playable_formats = [fmt for fmt in audio_formats if not _soundcloud_format_is_preview(fmt)]

        selected_format = {
            "format_id": entry.get("format_id"),
            "format_note": entry.get("format_note"),
            "preference": entry.get("preference"),
            "url": entry.get("url"),
        }
        selected_is_preview = _soundcloud_format_is_preview(selected_format)
        is_preview_only = (audio_formats and not playable_formats) or (not audio_formats and selected_is_preview)

        stream_url = str(entry.get("url") or "")
        if (not stream_url or selected_is_preview) and playable_formats:
            best_playable = _pick_best_audio_format(playable_formats)
            stream_url = str((best_playable or {}).get("url") or "")
            if selected_is_preview and best_playable:
                log.debug(
                    "Switching SoundCloud track '%s' from preview format %s to playable format %s",
                    str(entry.get("title") or "unknown"),
                    str(entry.get("format_id") or "unknown"),
                    str(best_playable.get("format_id") or "unknown"),
                )

        title = str(entry.get("title") or "Безымянный отголосок")
        artist = str(entry.get("uploader") or entry.get("artist") or "Неизвестный исполнитель")
        duration = int(entry.get("duration") or 0)
        is_playable = bool(stream_url) and not is_preview_only
        if not is_playable:
            stream_url = ""

        return TrackCandidate(
            source_key=SOURCE_SOUNDCLOUD,
            title=title,
            artist=artist,
            duration=duration,
            webpage_url=webpage_url or original_url,
            stream_url=stream_url,
            is_preview=is_preview_only,
            is_playable=is_playable,
            artwork_url=_best_soundcloud_artwork(entry),
            variant_flags=_variant_flags(f"{title} {artist}"),
            native_rank=native_rank,
        )

    def _search_soundcloud_sync(self, query: str) -> SearchBatch:
        if YoutubeDL is None:
            raise RuntimeError("yt-dlp не установлен")
        target = f"scsearch{SEARCH_FETCH_LIMIT}:{query}"
        with YoutubeDL(self._ydl_opts()) as ydl:
            info = ydl.extract_info(target, download=False)
        entries = list(info.get("entries") or []) if isinstance(info, dict) else []
        candidates: list[TrackCandidate] = []
        preview_only_found = 0
        unplayable_found = 0
        total_found = 0
        for idx, entry in enumerate(entries):
            if not entry:
                continue
            total_found += 1
            candidate = self._soundcloud_candidate_from_entry(entry, native_rank=idx, strict_source=False)
            if candidate is None:
                continue
            if candidate.is_preview:
                preview_only_found += 1
            elif not candidate.is_playable:
                unplayable_found += 1
            candidates.append(candidate)
        return SearchBatch(
            source_key=SOURCE_SOUNDCLOUD,
            candidates=candidates,
            total_found=total_found,
            preview_only_found=preview_only_found,
            unplayable_found=unplayable_found,
        )

    def _resolve_soundcloud_url_sync(self, query: str) -> TrackCandidate:
        if YoutubeDL is None:
            raise RuntimeError("yt-dlp не установлен")
        with YoutubeDL(self._ydl_opts()) as ydl:
            info = ydl.extract_info(query, download=False)
        entry = info
        if isinstance(info, dict) and info.get("entries"):
            entry = next((item for item in info["entries"] if item), None)
        if not entry:
            raise RuntimeError("SoundCloud не вернул подходящий трек.")
        candidate = self._soundcloud_candidate_from_entry(entry, native_rank=0, strict_source=True)
        if candidate is None:
            raise RuntimeError("SoundCloud не вернул подходящий трек.")
        if candidate.is_preview:
            log.info(
                "Rejected SoundCloud preview-only track '%s': query=%s",
                candidate.title,
                query,
            )
        return candidate

    async def _get_audius_api_host(self) -> str:
        if self._audius_api_host:
            return self._audius_api_host

        def _fetch_host() -> str:
            data = self._http_json_sync("https://api.audius.co/")
            hosts = data.get("data") if isinstance(data, dict) else None
            if not hosts:
                raise RuntimeError("Audius API не вернул хост.")
            return str(hosts[0]).rstrip("/")

        self._audius_api_host = await asyncio.to_thread(_fetch_host)
        return self._audius_api_host

    def _audius_candidate_from_item(self, item: dict, *, native_rank: int) -> TrackCandidate:
        stream = item.get("stream") or {}
        preview = item.get("preview") or {}
        if isinstance(stream, dict):
            stream_url = str(stream.get("url") or "")
        else:
            stream_url = str(stream or "")
        if isinstance(preview, dict):
            preview_url = str(preview.get("url") or "")
        else:
            preview_url = str(preview or "")
        is_streamable = bool(item.get("is_streamable", True))
        is_available = bool(item.get("is_available", True))
        is_stream_gated = bool(item.get("is_stream_gated", False))
        is_preview = bool(preview_url and not stream_url)
        is_playable = bool(stream_url) and is_streamable and is_available and not is_stream_gated
        if not is_playable:
            stream_url = ""

        permalink = str(item.get("permalink") or "")
        if permalink.startswith("/"):
            webpage_url = f"https://audius.co{permalink}"
        else:
            webpage_url = permalink
        title = str(item.get("title") or "Безымянный отголосок")
        user = item.get("user") or {}
        artist = str(user.get("name") or item.get("artist") or "Неизвестный исполнитель")
        duration = int(item.get("duration") or 0)
        return TrackCandidate(
            source_key=SOURCE_AUDIUS,
            title=title,
            artist=artist,
            duration=duration,
            webpage_url=webpage_url,
            stream_url=stream_url,
            is_preview=is_preview,
            is_playable=is_playable,
            artwork_url=_best_artwork_from_mapping(item.get("artwork") or item.get("cover_art") or {}),
            variant_flags=_variant_flags(f"{title} {artist}"),
            native_rank=native_rank,
            native_score=float(item.get("play_count") or 0),
        )

    async def _search_audius(self, query: str) -> SearchBatch:
        host = await self._get_audius_api_host()

        def _fetch() -> SearchBatch:
            url = f"{host}/v1/tracks/search?query={quote(query)}&limit={SEARCH_FETCH_LIMIT}"
            data = self._http_json_sync(url)
            items = list(data.get("data") or []) if isinstance(data, dict) else []
            candidates: list[TrackCandidate] = []
            preview_only_found = 0
            unplayable_found = 0
            for idx, item in enumerate(items):
                candidate = self._audius_candidate_from_item(item, native_rank=idx)
                if candidate.is_preview:
                    preview_only_found += 1
                elif not candidate.is_playable:
                    unplayable_found += 1
                candidates.append(candidate)
            return SearchBatch(
                source_key=SOURCE_AUDIUS,
                candidates=candidates,
                total_found=len(items),
                preview_only_found=preview_only_found,
                unplayable_found=unplayable_found,
            )

        return await asyncio.to_thread(_fetch)

    async def _resolve_audius_url(self, query: str) -> TrackCandidate:
        host = await self._get_audius_api_host()

        def _fetch() -> TrackCandidate:
            url = f"{host}/v1/resolve?url={quote(query, safe='')}"
            data = self._http_json_sync(url)
            item = data.get("data") if isinstance(data, dict) else None
            if not isinstance(item, dict) or not item.get("track_id"):
                raise RuntimeError("Нужна ссылка именно на трек Audius.")
            return self._audius_candidate_from_item(item, native_rank=0)

        return await asyncio.to_thread(_fetch)

    def _jamendo_api_get_sync(self, path: str, params: Optional[dict] = None):
        url = f"https://www.jamendo.com{path}"
        if params:
            url += "?" + urlencode(params, doseq=True)
        headers = {"X-Jam-Call": _jamendo_signature(path)}
        return self._http_json_sync(url, headers=headers)

    def _jamendo_candidate_from_item(self, item: dict, *, native_rank: int) -> TrackCandidate:
        stream = item.get("stream") or {}
        if isinstance(stream, dict):
            stream_url = str(stream.get("mp3") or stream.get("ogg") or "")
        else:
            stream_url = str(stream or "")
        status = item.get("status") or {}
        is_available = bool(status.get("available", True))
        is_playable = is_available and bool(stream_url)
        if not is_playable:
            stream_url = ""
        title = str(item.get("name") or item.get("title") or "Безымянный отголосок")
        artist_data = item.get("artist") or {}
        artist = str(artist_data.get("name") or item.get("artistName") or "Неизвестный исполнитель")
        duration = int(item.get("duration") or 0)
        track_id = str(item.get("id") or "")
        webpage_url = f"https://www.jamendo.com/track/{track_id}" if track_id else ""
        return TrackCandidate(
            source_key=SOURCE_JAMENDO,
            title=title,
            artist=artist,
            duration=duration,
            webpage_url=webpage_url,
            stream_url=stream_url,
            is_preview=False,
            is_playable=is_playable,
            artwork_url=_best_artwork_from_mapping(item.get("cover") or {}),
            variant_flags=_variant_flags(f"{title} {artist}"),
            native_rank=native_rank,
            native_score=float(item.get("score") or 0),
        )

    async def _search_jamendo(self, query: str) -> SearchBatch:
        def _fetch() -> SearchBatch:
            data = self._jamendo_api_get_sync(
                "/api/search",
                {
                    "query": query,
                    "type": "track",
                    "limit": SEARCH_FETCH_LIMIT,
                    "identities": "www",
                },
            )
            items = list(data) if isinstance(data, list) else []
            candidates: list[TrackCandidate] = []
            unplayable_found = 0
            for idx, item in enumerate(items):
                candidate = self._jamendo_candidate_from_item(item, native_rank=idx)
                if not candidate.is_playable:
                    unplayable_found += 1
                candidates.append(candidate)
            return SearchBatch(
                source_key=SOURCE_JAMENDO,
                candidates=candidates,
                total_found=len(items),
                preview_only_found=0,
                unplayable_found=unplayable_found,
            )

        return await asyncio.to_thread(_fetch)

    async def _resolve_jamendo_url(self, query: str) -> TrackCandidate:
        parsed = urlparse(query)
        if not re.search(r"/track/(\d+)", parsed.path or ""):
            raise RuntimeError("Нужна ссылка именно на трек Jamendo.")
        if YoutubeDL is None:
            raise RuntimeError("yt-dlp не установлен")

        def _fetch() -> TrackCandidate:
            with YoutubeDL(self._ydl_opts()) as ydl:
                info = ydl.extract_info(query, download=False)
            if not isinstance(info, dict):
                raise RuntimeError("Jamendo не вернул подходящий трек.")
            extractor_key = str(info.get("extractor_key") or info.get("extractor") or "").lower()
            if "jamendo" not in extractor_key:
                raise RuntimeError("Нужна ссылка именно на трек Jamendo.")
            stream_url = str(info.get("url") or "")
            if not stream_url:
                raise RuntimeError("Jamendo не отдал поток трека.")
            title = str(info.get("title") or "Безымянный отголосок")
            artist = str(info.get("artist") or info.get("uploader") or "Неизвестный исполнитель")
            duration = int(info.get("duration") or 0)
            return TrackCandidate(
                source_key=SOURCE_JAMENDO,
                title=title,
                artist=artist,
                duration=duration,
                webpage_url=str(info.get("webpage_url") or query),
                stream_url=stream_url,
                is_preview=False,
                is_playable=True,
                artwork_url=str(info.get("thumbnail") or ""),
                variant_flags=_variant_flags(f"{title} {artist}"),
                native_rank=0,
            )

        return await asyncio.to_thread(_fetch)

    async def _search_candidates(self, query: str, *, fast_mode: bool = False) -> list[TrackCandidate]:
        searchers = (
            (SOURCE_SOUNDCLOUD, lambda: asyncio.to_thread(self._search_soundcloud_sync, query)),
            (SOURCE_AUDIUS, lambda: self._search_audius(query)),
            (SOURCE_JAMENDO, lambda: self._search_jamendo(query)),
        )
        timeout_map = {
            SOURCE_SOUNDCLOUD: SEARCH_TIMEOUT_SOUND_CLOUD if fast_mode else SEARCH_TIMEOUT_SOUND_CLOUD_FULL,
            SOURCE_AUDIUS: SEARCH_TIMEOUT_AUDIUS if fast_mode else SEARCH_TIMEOUT_AUDIUS_FULL,
            SOURCE_JAMENDO: SEARCH_TIMEOUT_JAMENDO if fast_mode else SEARCH_TIMEOUT_JAMENDO_FULL,
        }
        async def _run_source(
            source_key: str,
            runner,
        ) -> tuple[str, Optional[SearchBatch], bool]:
            try:
                batch = await asyncio.wait_for(runner(), timeout=timeout_map[source_key])
                return source_key, batch, False
            except asyncio.TimeoutError:
                log.warning(
                    "music search source=%s query=%r timed out after %.1fs",
                    SOURCE_NAMES[source_key],
                    query,
                    timeout_map[source_key],
                )
                return source_key, None, True
            except (HTTPError, URLError, ValueError, RuntimeError) as exc:
                log.warning(
                    "music search source=%s query=%r failed: %s",
                    SOURCE_NAMES[source_key],
                    query,
                    exc,
                )
                return source_key, None, True
            except Exception as exc:
                log.exception(
                    "music search source=%s query=%r crashed: %s",
                    SOURCE_NAMES[source_key],
                    query,
                    exc,
                )
                return source_key, None, True

        results = await asyncio.gather(
            *[asyncio.create_task(_run_source(source_key, runner)) for source_key, runner in searchers],
            return_exceptions=False,
        )
        batches: dict[str, SearchBatch] = {}
        had_failures = False
        for source_key, batch, failed in results:
            if failed:
                had_failures = True
            if batch is not None:
                batches[source_key] = batch

        for source_key, _runner in searchers:
            batch = batches.get(source_key)
            if batch is None:
                continue
            playable = [
                candidate
                for candidate in batch.candidates
                if candidate.is_playable and not candidate.is_preview and candidate.stream_url
            ]
            if playable:
                playable.sort(key=lambda candidate: (-_candidate_score(candidate, query), candidate.native_rank))
                log.info(
                    "music search query=%r selected_source=%s playable=%d total=%d",
                    query,
                    batch.source_name,
                    len(playable),
                    batch.total_found,
                )
                return playable[:SEARCH_RESULT_LIMIT]

            log.info(
                "music search fallback query=%r source=%s playable=0 total=%d preview=%d unplayable=%d",
                query,
                batch.source_name,
                batch.total_found,
                batch.preview_only_found,
                batch.unplayable_found,
            )

        if not batches and had_failures:
            raise SearchSourcesUnavailableError(SOURCES_UNAVAILABLE_TEXT)
        log.info("music search query=%r ended without playable results", query)
        return []

    async def _resolve_track(self, query: str, *, requested_by_user_id: int) -> MusicTrack:
        source_key = _detect_source_from_url(query)
        if source_key == SOURCE_SOUNDCLOUD:
            candidate = await asyncio.to_thread(self._resolve_soundcloud_url_sync, query)
        elif source_key == SOURCE_AUDIUS:
            candidate = await self._resolve_audius_url(query)
        elif source_key == SOURCE_JAMENDO:
            candidate = await self._resolve_jamendo_url(query)
        else:
            raise RuntimeError(UNSUPPORTED_LINK_TEXT)

        if candidate.is_preview:
            raise PreviewOnlyTrackError(PREVIEW_ONLY_TRACK_TEXT)
        if not candidate.is_playable:
            log.info(
                "Rejected unplayable direct track source=%s title=%r url=%s",
                candidate.source_name,
                candidate.title,
                query,
            )
            raise RuntimeError(UNPLAYABLE_TRACK_TEXT)

        return self._candidate_to_track(candidate, requested_by_user_id=requested_by_user_id)

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
        if session.radio.mode != RADIO_MODE_OFF and not session.queue:
            asyncio.create_task(self._schedule_radio_prefetch())

    async def _enter_idle_locked(self, *, state: str = "idle") -> None:
        session = self.session
        if session is None:
            return
        session.current_track = None
        session.paused = False
        session.state = state
        if session.idle_disconnect_at == 0:
            session.idle_disconnect_at = _now() + IDLE_DISCONNECT_SECONDS
        self._last_panel_state = state

    async def _after_track_finished(self, error: Optional[Exception]) -> None:
        if error:
            log.warning("music playback after-callback error: %s", error)

        finished_track: Optional[MusicTrack] = None
        pending_radio: Optional[tuple[RadioState, int]] = None
        ready_radio_track: Optional[MusicTrack] = None

        async with self._lock:
            session = self.session
            if session is None:
                return
            if session.suppress_after:
                return

            finished_track = session.current_track
            session.current_track = None
            session.paused = False
            session.radio.pending = False

            while session.queue:
                next_track = session.queue.pop(0)
                session.current_track = next_track
                session.state = "playing"
                session.idle_disconnect_at = 0
                try:
                    await self._play_current_locked()
                    break
                except Exception:
                    log.exception("music next-track start failed: %s", next_track.title)
                    session.current_track = None
            else:
                if session.radio.mode != RADIO_MODE_OFF:
                    ready_radio_track = session.radio.prefetched_track
                    session.radio.prefetched_track = None
                    session.radio.prefetched_basis_identity = ""
                    if ready_radio_track is not None:
                        pass
                    else:
                        pending_radio = (
                            self._clone_radio_state(session.radio, pending=False),
                            int(session.radio.requested_by_user_id or session.started_by_user_id),
                        )
                        session.radio.pending = True
                        session.radio.notice = ""
                        session.state = "radio_wait"
                        session.idle_disconnect_at = 0
                else:
                    await self._enter_idle_locked()

        if pending_radio is None and ready_radio_track is None:
            await self._update_panel_message()
            return

        await self._update_panel_message()

        next_radio_track = ready_radio_track
        source_unavailable = False
        if pending_radio is not None:
            radio_snapshot, requester_id = pending_radio
            for _ in range(RADIO_AUTOPICK_RETRY_LIMIT):
                try:
                    next_radio_track = await self._build_radio_track_for_state(
                        radio_snapshot,
                        basis_track=finished_track,
                        requested_by_user_id=requester_id,
                        fast_mode=False,
                    )
                except SearchSourcesUnavailableError:
                    source_unavailable = True
                    next_radio_track = None
                    break
                except Exception:
                    log.exception("music radio next-track build failed")
                    next_radio_track = None
                if next_radio_track is not None:
                    break

        async with self._lock:
            session = self.session
            if session is None:
                return
            if session.suppress_after:
                return

            session.radio.pending = False

            radio_still_enabled = session.radio.mode != RADIO_MODE_OFF
            if next_radio_track is not None and radio_still_enabled:
                session.queue.append(next_radio_track)
                self._remember_radio_track_locked(session, next_radio_track)

            if session.current_track is None and session.queue:
                next_track = session.queue.pop(0)
                session.current_track = next_track
                session.state = "playing"
                session.idle_disconnect_at = 0
                try:
                    await self._play_current_locked()
                except Exception:
                    log.exception("music queued-track start after radio failed: %s", next_track.title)
                    session.current_track = None

            if session.current_track is None:
                if session.queue:
                    pass
                elif next_radio_track is None and radio_still_enabled:
                    if source_unavailable:
                        session.radio.notice = SOURCES_UNAVAILABLE_TEXT
                        await self._enter_idle_locked(state="radio_wait")
                    else:
                        self._reset_radio_locked(session, notice="Алтарь не нашел следующего созвучного отголоска. Поток угас.")
                        await self._enter_idle_locked()
                elif not radio_still_enabled:
                    await self._enter_idle_locked()
            elif next_radio_track is None and radio_still_enabled:
                session.radio.notice = SOURCES_UNAVAILABLE_TEXT if source_unavailable else "Следующий радио-отголосок не найден. Алтарь попробует снова позже."

        await self._update_panel_message()
        await self._schedule_radio_prefetch()

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
            session.last_skip_requested_at = 0.0
            self._cancel_radio_prefetch()
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
        vc = self._voice_client()
        if not vc or not vc.is_connected():
            return

        should_idle = (session.current_track is None and not session.radio.pending) or not listeners
        if not should_idle:
            if session.idle_disconnect_at != 0:
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
        if member.id == self.bot.user.id and after.channel is None:  # type: ignore[union-attr]
            async with self._lock:
                if self.session is not None:
                    self._cancel_radio_prefetch()
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
