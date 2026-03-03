# cogs/db.py
from __future__ import annotations

import logging
import secrets
import string
import time
from pathlib import Path
from typing import Any, Dict, Optional

import aiosqlite
from discord.ext import commands

log = logging.getLogger("void")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "bot.sqlite3"


def _now() -> int:
    return int(time.time())


def _gen_code(n: int = 10) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


class Repo:
    def __init__(self) -> None:
        self.conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(DB_PATH, timeout=30)
        # Use Row for name-based access, but we still return dicts outward.
        self.conn.row_factory = aiosqlite.Row

        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA foreign_keys=ON;")
        await self.conn.execute("PRAGMA busy_timeout=5000;")
        await self._create_tables()
        await self._migrate_schema()
        await self.conn.commit()

        log.info("DB ready: %s", DB_PATH)

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def _create_tables(self) -> None:
        """Create schema for a fresh DB. No legacy constraints, safe defaults."""
        assert self.conn

        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                runes INTEGER NOT NULL DEFAULT 0,
                xp INTEGER NOT NULL DEFAULT 0,
                level INTEGER NOT NULL DEFAULT 1,

                day_key TEXT NOT NULL DEFAULT '',
                voice_runes_today INTEGER NOT NULL DEFAULT 0,
                chat_runes_today INTEGER NOT NULL DEFAULT 0,
                voice_xp_today INTEGER NOT NULL DEFAULT 0,
                chat_xp_today INTEGER NOT NULL DEFAULT 0,
                voice_sec_bank INTEGER NOT NULL DEFAULT 0,

                -- used by Activity/void_info
                last_voice_award_ts INTEGER NOT NULL DEFAULT 0,
                last_chat_award_ts INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS story_progress(
                user_id INTEGER PRIMARY KEY,
                season INTEGER NOT NULL DEFAULT 1,
                chapter_completed INTEGER NOT NULL DEFAULT 0,

                name_shards INTEGER NOT NULL DEFAULT 0,
                evidence INTEGER NOT NULL DEFAULT 0,
                vow INTEGER NOT NULL DEFAULT 0,
                trust_void INTEGER NOT NULL DEFAULT 0,
                mask INTEGER NOT NULL DEFAULT 0,
                gro_seed INTEGER NOT NULL DEFAULT 0,
                rher_seed INTEGER NOT NULL DEFAULT 0,

                vow_lock TEXT NOT NULL DEFAULT '',
                attempt_chapter INTEGER NOT NULL DEFAULT 0,
                attempt_json TEXT NOT NULL DEFAULT '',
                updated_ts INTEGER NOT NULL DEFAULT 0,

                thread_id INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS codes(
                code TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                chapter INTEGER NOT NULL,
                kind TEXT NOT NULL DEFAULT '',
                created_ts INTEGER NOT NULL,
                used_ts INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cooldowns(
                user_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                until_ts INTEGER NOT NULL DEFAULT 0,
                expires_ts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, key)
            );

            CREATE TABLE IF NOT EXISTS purchases(
                user_id INTEGER NOT NULL,
                sku TEXT NOT NULL,
                bought_ts INTEGER NOT NULL,
                PRIMARY KEY(user_id, sku)
            );

            
            CREATE TABLE IF NOT EXISTS daily_profit(
                user_id INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                key TEXT NOT NULL,
                value INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, day_key, key)
            );

CREATE TABLE IF NOT EXISTS tendrils(
                victim_id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                attacker_id INTEGER NOT NULL,
                started_ts INTEGER NOT NULL,
                expires_ts INTEGER NOT NULL,
                last_tick_ts INTEGER NOT NULL,
                last_announce_ts INTEGER NOT NULL,
                stolen_total INTEGER NOT NULL DEFAULT 0,
                attempts_left INTEGER NOT NULL DEFAULT 0
            );
            """
        )

    async def _table_cols(self, table: str) -> set[str]:
        """Return set of column names for a table (empty if missing)."""
        assert self.conn
        cur = await self.conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        await cur.close()
        return {str(r[1]) for r in rows}  # r[1] = column name

    async def _migrate_schema(self) -> None:
        """Idempotent migrations for older DBs."""
        assert self.conn

        # ---- users: award timestamps ----
        ucols = await self._table_cols("users")
        if "last_voice_award_ts" not in ucols:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN last_voice_award_ts INTEGER NOT NULL DEFAULT 0"
            )
        if "last_chat_award_ts" not in ucols:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN last_chat_award_ts INTEGER NOT NULL DEFAULT 0"
            )

        # ---- codes: kind ----
        ccols = await self._table_cols("codes")
        if "kind" not in ccols:
            await self.conn.execute(
                "ALTER TABLE codes ADD COLUMN kind TEXT NOT NULL DEFAULT ''"
            )

    # ---------- Users ----------

    async def ensure_user(self, user_id: int) -> None:
        assert self.conn
        await self.conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (int(user_id),))
        await self.conn.execute("INSERT OR IGNORE INTO story_progress(user_id) VALUES (?)", (int(user_id),))
        await self.conn.commit()

    async def get_user(self, user_id: int) -> Dict[str, Any]:
        """Always returns a dict with the keys used by other cogs."""
        assert self.conn
        await self.ensure_user(user_id)

        cur = await self.conn.execute(
            """
            SELECT user_id, runes, xp, level,
                   day_key, voice_runes_today, chat_runes_today, voice_xp_today, chat_xp_today, voice_sec_bank,
                   last_voice_award_ts, last_chat_award_ts
            FROM users WHERE user_id=?
            """,
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()

        if not row:
            # shouldn't happen because ensure_user inserts
            return {
                "user_id": int(user_id),
                "runes": 0,
                "xp": 0,
                "level": 1,
                "day_key": "",
                "voice_runes_today": 0,
                "chat_runes_today": 0,
                "voice_xp_today": 0,
                "chat_xp_today": 0,
                "voice_sec_bank": 0,
                "last_voice_award_ts": 0,
                "last_chat_award_ts": 0,
            }

        # sqlite3.Row supports dict-like access via keys()
        keys = row.keys() if hasattr(row, "keys") else []

        def g(k: str, default: Any) -> Any:
            try:
                if k in keys:
                    return row[k]
            except Exception:
                pass
            return default

        return {
            "user_id": int(g("user_id", user_id)),
            "runes": int(g("runes", 0)),
            "xp": int(g("xp", 0)),
            "level": int(g("level", 1)),
            "day_key": str(g("day_key", "")) or "",
            "voice_runes_today": int(g("voice_runes_today", 0)),
            "chat_runes_today": int(g("chat_runes_today", 0)),
            "voice_xp_today": int(g("voice_xp_today", 0)),
            "chat_xp_today": int(g("chat_xp_today", 0)),
            "voice_sec_bank": int(g("voice_sec_bank", 0)),
            "last_voice_award_ts": int(g("last_voice_award_ts", 0)),
            "last_chat_award_ts": int(g("last_chat_award_ts", 0)),
        }

    async def set_user_fields(self, user_id: int, **fields: Any) -> None:
        assert self.conn
        if not fields:
            return
        keys = list(fields.keys())
        vals = [fields[k] for k in keys]
        set_clause = ", ".join(f"{k}=?" for k in keys)
        await self.conn.execute(
            f"UPDATE users SET {set_clause} WHERE user_id=?",
            (*vals, int(user_id)),
        )
        await self.conn.commit()

    async def add_runes(self, user_id: int, delta: int) -> None:
        assert self.conn
        await self.ensure_user(user_id)
        await self.conn.execute("UPDATE users SET runes = runes + ? WHERE user_id=?", (int(delta), int(user_id)))
        await self.conn.commit()

    async def spend_runes(self, user_id: int, cost: int) -> bool:
        """Subtract runes if enough. Returns True if spent (atomic)."""
        assert self.conn
        await self.ensure_user(user_id)

        cost_i = int(cost)
        # атомарно: UPDATE с условием по балансу, чтобы избежать гонок при одновременных кликах
        cur = await self.conn.execute(
            "UPDATE users SET runes = runes - ? WHERE user_id=? AND runes >= ?",
            (cost_i, int(user_id), cost_i),
        )
        await self.conn.commit()
        ok = cur.rowcount == 1
        await cur.close()
        return ok

    

    # ---------- Daily profit (per day_key) ----------

    async def dp_get(self, user_id: int, day_key: str, key: str) -> int:
        """Get daily counter value (defaults to 0)."""
        assert self.conn
        await self.ensure_user(user_id)
        cur = await self.conn.execute(
            "SELECT value FROM daily_profit WHERE user_id=? AND day_key=? AND key=?",
            (int(user_id), str(day_key), str(key)),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def dp_add(self, user_id: int, day_key: str, key: str, delta: int) -> None:
        """Add delta to daily counter (upsert)."""
        assert self.conn
        await self.ensure_user(user_id)
        delta_i = int(delta)
        await self.conn.execute(
            """
            INSERT INTO daily_profit(user_id, day_key, key, value)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id, day_key, key) DO UPDATE SET value = value + excluded.value
            """,
            (int(user_id), str(day_key), str(key), delta_i),
        )
        await self.conn.commit()

# ---------- Cooldowns ----------

    async def cd_get(self, user_id: int, key: str) -> int:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT until_ts FROM cooldowns WHERE user_id=? AND key=?",
            (int(user_id), str(key)),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0
        return int(row[0] or 0)

    async def cd_set(self, user_id: int, key: str, seconds: int) -> None:
        """Set cooldown to now+seconds. If seconds<=0 => clear."""
        assert self.conn
        if int(seconds) <= 0:
            await self.conn.execute("DELETE FROM cooldowns WHERE user_id=? AND key=?", (int(user_id), str(key)))
            await self.conn.commit()
            return

        until = _now() + int(seconds)
        # keep both columns in sync (some code/old schema used either)
        await self.conn.execute(
            """
            INSERT INTO cooldowns(user_id, key, until_ts, expires_ts)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id, key) DO UPDATE SET until_ts=excluded.until_ts, expires_ts=excluded.expires_ts
            """,
            (int(user_id), str(key), int(until), int(until)),
        )
        await self.conn.commit()

    # ---------- Purchases ----------
    # shop.py uses (user_id, kind, item_id); older code may use (user_id, sku)

    async def purchase_has(self, user_id: int, sku_or_kind: str, item_id: Any = None) -> bool:
        assert self.conn
        sku = str(sku_or_kind) if item_id is None else f"{sku_or_kind}:{item_id}"
        cur = await self.conn.execute(
            "SELECT 1 FROM purchases WHERE user_id=? AND sku=?",
            (int(user_id), sku),
        )
        row = await cur.fetchone()
        await cur.close()
        return bool(row)

    async def purchase_add(self, user_id: int, sku_or_kind: str, item_id: Any = None) -> None:
        assert self.conn
        sku = str(sku_or_kind) if item_id is None else f"{sku_or_kind}:{item_id}"
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO purchases(user_id, sku, bought_ts)
            VALUES(?,?,?)
            """,
            (int(user_id), sku, _now()),
        )
        await self.conn.commit()

    # ---------- Story ----------

    async def get_story(self, user_id: int) -> Dict[str, Any]:
        assert self.conn
        await self.ensure_user(user_id)
        cur = await self.conn.execute(
            """
            SELECT season, chapter_completed,
                   name_shards, evidence, vow, trust_void, mask, gro_seed, rher_seed,
                   vow_lock, attempt_chapter, attempt_json, updated_ts, thread_id
            FROM story_progress WHERE user_id=?
            """,
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return {
                "season": 1,
                "chapter_completed": 0,
                "name_shards": 0,
                "evidence": 0,
                "vow": 0,
                "trust_void": 0,
                "mask": 0,
                "gro_seed": 0,
                "rher_seed": 0,
                "vow_lock": "",
                "attempt_chapter": 0,
                "attempt_json": "",
                "updated_ts": 0,
                "thread_id": 0,
            }
        return {
            "season": int(row[0]),
            "chapter_completed": int(row[1]),
            "name_shards": int(row[2]),
            "evidence": int(row[3]),
            "vow": int(row[4]),
            "trust_void": int(row[5]),
            "mask": int(row[6]),
            "gro_seed": int(row[7]),
            "rher_seed": int(row[8]),
            "vow_lock": str(row[9]) or "",
            "attempt_chapter": int(row[10]),
            "attempt_json": str(row[11]) or "",
            "updated_ts": int(row[12]),
            "thread_id": int(row[13]) if len(row) > 13 else 0,
        }

    async def set_story_fields(self, user_id: int, **fields: Any) -> None:
        assert self.conn
        if not fields:
            return
        keys = list(fields.keys())
        vals = [fields[k] for k in keys]
        set_clause = ", ".join(f"{k}=?" for k in keys)
        await self.conn.execute(
            f"UPDATE story_progress SET {set_clause} WHERE user_id=?",
            (*vals, int(user_id)),
        )
        await self.conn.commit()

    async def story_commit_ch1(self, user_id: int, **fields: Any) -> None:
        """Helper used by story cog to commit chapter 1 choices + timestamp."""
        fields = dict(fields)
        fields["updated_ts"] = _now()
        await self.set_story_fields(user_id, **fields)

    async def get_or_create_code(
        self,
        user_id: int,
        chapter: int | None = None,
        *,
        kind: str | None = None,
        length: int = 10,
    ) -> str:
        """
        Compatible API:
          - legacy: get_or_create_code(user_id, chapter=1)
          - story:  get_or_create_code(user_id, kind="secret:archival_seam", length=10)
        """
        assert self.conn
        await self.ensure_user(user_id)

        if kind is not None:
            kind_s = str(kind)
            cur = await self.conn.execute(
                "SELECT code FROM codes WHERE user_id=? AND kind=? AND used_ts=0 ORDER BY created_ts DESC LIMIT 1",
                (int(user_id), kind_s),
            )
            row = await cur.fetchone()
            await cur.close()
            if row:
                return str(row[0])

            code = _gen_code(int(length))
            await self.conn.execute(
                "INSERT INTO codes(code, user_id, chapter, kind, created_ts, used_ts) VALUES(?,?,?,?,?,0)",
                (code, int(user_id), 0, kind_s, _now()),
            )
            await self.conn.commit()
            return code

        ch = int(chapter or 0)
        cur = await self.conn.execute(
            "SELECT code FROM codes WHERE user_id=? AND chapter=? AND used_ts=0 ORDER BY created_ts DESC LIMIT 1",
            (int(user_id), ch),
        )
        row = await cur.fetchone()
        await cur.close()
        if row:
            return str(row[0])

        code = _gen_code(int(length))
        await self.conn.execute(
            "INSERT INTO codes(code, user_id, chapter, kind, created_ts, used_ts) VALUES(?,?,?,?,?,0)",
            (code, int(user_id), ch, "", _now()),
        )
        await self.conn.commit()
        return code

    async def code_status(self, code: str) -> Optional[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT code, user_id, chapter, kind, created_ts, used_ts FROM codes WHERE code=?",
            (str(code),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None

        kind = str(row[3] or "")
        chapter = int(row[2] or 0)
        if not kind and chapter > 0:
            kind = f"story:s1:ch{chapter}"

        return {
            "code": str(row[0]),
            "user_id": int(row[1]),
            "chapter": chapter,
            "kind": kind,
            "created_ts": int(row[4]),
            "used_ts": int(row[5]),
        }

    async def consume_code(self, arg1: int | str, arg2: str | None = None):
        """
        Compatible API:
          - legacy: consume_code("ABC123...") -> dict|None
          - story:  consume_code(user_id, "ABC123...") -> (ok, kind, status)
                   status: "ok" | "used" | "чужой" | "invalid"
        """
        assert self.conn

        # legacy: consume_code(code)
        if arg2 is None:
            code = str(arg1)
            cur = await self.conn.execute(
                "SELECT user_id, chapter, kind, used_ts FROM codes WHERE code=?",
                (code,),
            )
            row = await cur.fetchone()
            await cur.close()
            if not row:
                return None
            used_ts = int(row[3] or 0)
            if used_ts != 0:
                return None
            await self.conn.execute("UPDATE codes SET used_ts=? WHERE code=?", (_now(), code))
            await self.conn.commit()

            kind = str(row[2] or "")
            chapter = int(row[1] or 0)
            if not kind and chapter > 0:
                kind = f"story:s1:ch{chapter}"

            return {"user_id": int(row[0]), "chapter": chapter, "kind": kind}

        # story: consume_code(user_id, code)
        user_id = int(arg1)
        code = str(arg2)

        cur = await self.conn.execute(
            "SELECT user_id, chapter, kind, used_ts FROM codes WHERE code=?",
            (code,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return (False, "", "invalid")

        owner_id = int(row[0])
        chapter = int(row[1] or 0)
        kind = str(row[2] or "")
        if not kind and chapter > 0:
            kind = f"story:s1:ch{chapter}"
        used_ts = int(row[3] or 0)

        if owner_id != user_id:
            return (False, "", "чужой")

        if used_ts != 0:
            return (False, kind, "used")

        await self.conn.execute("UPDATE codes SET used_ts=? WHERE code=?", (_now(), code))
        await self.conn.commit()
        return (True, kind, "ok")


class DBCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.repo = Repo()

    async def cog_load(self) -> None:
        await self.repo.connect()
        # attach
        setattr(self.bot, "repo", self.repo)

    async def cog_unload(self) -> None:
        await self.repo.close()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(DBCog(bot))