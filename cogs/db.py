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
        self._daily_profit_col_cache: str | None = None

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
        self._daily_profit_col_cache = None
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
                last_chat_award_ts INTEGER NOT NULL DEFAULT 0,

                -- active player decoration (empty = disabled)
                active_decoration TEXT NOT NULL DEFAULT ''
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

            CREATE TABLE IF NOT EXISTS rental_channels(
                user_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS rental_posts(
                message_id INTEGER PRIMARY KEY,
                author_user_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS post_praises(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                author_user_id INTEGER NOT NULL,
                praised_by_user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                is_free INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS praise_daily_limits(
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                paid_runes_given INTEGER NOT NULL DEFAULT 0,
                posts_praised_count INTEGER NOT NULL DEFAULT 0,
                free_praises_used INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, date)
            );

            CREATE TABLE IF NOT EXISTS order_members(
                user_id INTEGER PRIMARY KEY,
                order_id TEXT NOT NULL,
                joined_ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_influence(
                order_id TEXT PRIMARY KEY,
                influence INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_herald(
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL DEFAULT 0,
                elected_ts INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_elections(
                order_id TEXT PRIMARY KEY,
                stage TEXT NOT NULL DEFAULT '',
                started_ts INTEGER NOT NULL DEFAULT 0,
                collect_deadline_ts INTEGER NOT NULL DEFAULT 0,
                vote_deadline_ts INTEGER NOT NULL DEFAULT 0,
                last_finished_ts INTEGER NOT NULL DEFAULT 0,
                collect_message_id INTEGER NOT NULL DEFAULT 0,
                vote_message_id INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_election_candidates(
                order_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                willing INTEGER NOT NULL DEFAULT 0,
                applied_ts INTEGER NOT NULL DEFAULT 0,
                responded_ts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(order_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS order_election_votes(
                order_id TEXT NOT NULL,
                voter_user_id INTEGER NOT NULL,
                candidate_user_id INTEGER NOT NULL,
                voted_ts INTEGER NOT NULL,
                PRIMARY KEY(order_id, voter_user_id)
            );

            CREATE TABLE IF NOT EXISTS order_wars(
                war_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attacker_order_id TEXT NOT NULL,
                defender_order_id TEXT NOT NULL,
                game_name TEXT NOT NULL,
                needed_count INTEGER NOT NULL,
                match_note TEXT NOT NULL DEFAULT '',
                started_by_user_id INTEGER NOT NULL,
                stage TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                attacker_deadline_ts INTEGER NOT NULL DEFAULT 0,
                defender_deadline_ts INTEGER NOT NULL DEFAULT 0,
                attacker_letter_message_id INTEGER NOT NULL DEFAULT 0,
                attacker_status_message_id INTEGER NOT NULL DEFAULT 0,
                defender_letter_message_id INTEGER NOT NULL DEFAULT 0,
                defender_status_message_id INTEGER NOT NULL DEFAULT 0,
                resolved_ts INTEGER NOT NULL DEFAULT 0,
                result_order_id TEXT NOT NULL DEFAULT '',
                cancel_reason TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS order_war_responses(
                war_id INTEGER NOT NULL,
                order_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                ready INTEGER NOT NULL DEFAULT 0,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY(war_id, order_id, user_id)
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

    async def _daily_profit_col(self) -> str:
        """Return the active numeric column name for daily_profit counters."""
        if self._daily_profit_col_cache:
            return self._daily_profit_col_cache
        cols = await self._table_cols("daily_profit")
        if "value" in cols:
            self._daily_profit_col_cache = "value"
        elif "amount" in cols:
            self._daily_profit_col_cache = "amount"
        else:
            self._daily_profit_col_cache = "value"
        return self._daily_profit_col_cache

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
        if "active_decoration" not in ucols:
            await self.conn.execute(
                "ALTER TABLE users ADD COLUMN active_decoration TEXT NOT NULL DEFAULT ''"
            )

        # ---- codes: kind ----
        ccols = await self._table_cols("codes")
        if "kind" not in ccols:
            await self.conn.execute(
                "ALTER TABLE codes ADD COLUMN kind TEXT NOT NULL DEFAULT ''"
            )

        # ---- daily_profit: legacy column name compatibility ----
        # Older DBs used `amount` instead of `value`.
        dpcols = await self._table_cols("daily_profit")
        if "value" not in dpcols and "amount" in dpcols:
            await self.conn.execute(
                "ALTER TABLE daily_profit ADD COLUMN value INTEGER NOT NULL DEFAULT 0"
            )
            # Backfill existing counters from legacy column once.
            await self.conn.execute(
                "UPDATE daily_profit SET value = amount WHERE value = 0"
            )
        elif "value" not in dpcols and "amount" not in dpcols:
            # Defensive fallback for malformed schemas.
            await self.conn.execute(
                "ALTER TABLE daily_profit ADD COLUMN value INTEGER NOT NULL DEFAULT 0"
            )

        # ---- rental/praise tables ----
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS rental_channels(
                user_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS rental_posts(
                message_id INTEGER PRIMARY KEY,
                author_user_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS post_praises(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                author_user_id INTEGER NOT NULL,
                praised_by_user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                is_free INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS praise_daily_limits(
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                paid_runes_given INTEGER NOT NULL DEFAULT 0,
                posts_praised_count INTEGER NOT NULL DEFAULT 0,
                free_praises_used INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, date)
            );
            """
        )

        # ---- orders / herald elections / wars ----
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS order_members(
                user_id INTEGER PRIMARY KEY,
                order_id TEXT NOT NULL,
                joined_ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_influence(
                order_id TEXT PRIMARY KEY,
                influence INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_herald(
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL DEFAULT 0,
                elected_ts INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_elections(
                order_id TEXT PRIMARY KEY,
                stage TEXT NOT NULL DEFAULT '',
                started_ts INTEGER NOT NULL DEFAULT 0,
                collect_deadline_ts INTEGER NOT NULL DEFAULT 0,
                vote_deadline_ts INTEGER NOT NULL DEFAULT 0,
                last_finished_ts INTEGER NOT NULL DEFAULT 0,
                collect_message_id INTEGER NOT NULL DEFAULT 0,
                vote_message_id INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_election_candidates(
                order_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                willing INTEGER NOT NULL DEFAULT 0,
                applied_ts INTEGER NOT NULL DEFAULT 0,
                responded_ts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(order_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS order_election_votes(
                order_id TEXT NOT NULL,
                voter_user_id INTEGER NOT NULL,
                candidate_user_id INTEGER NOT NULL,
                voted_ts INTEGER NOT NULL,
                PRIMARY KEY(order_id, voter_user_id)
            );

            CREATE TABLE IF NOT EXISTS order_wars(
                war_id INTEGER PRIMARY KEY AUTOINCREMENT,
                attacker_order_id TEXT NOT NULL,
                defender_order_id TEXT NOT NULL,
                game_name TEXT NOT NULL,
                needed_count INTEGER NOT NULL,
                match_note TEXT NOT NULL DEFAULT '',
                started_by_user_id INTEGER NOT NULL,
                stage TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                attacker_deadline_ts INTEGER NOT NULL DEFAULT 0,
                defender_deadline_ts INTEGER NOT NULL DEFAULT 0,
                attacker_letter_message_id INTEGER NOT NULL DEFAULT 0,
                attacker_status_message_id INTEGER NOT NULL DEFAULT 0,
                defender_letter_message_id INTEGER NOT NULL DEFAULT 0,
                defender_status_message_id INTEGER NOT NULL DEFAULT 0,
                resolved_ts INTEGER NOT NULL DEFAULT 0,
                result_order_id TEXT NOT NULL DEFAULT '',
                cancel_reason TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS order_war_responses(
                war_id INTEGER NOT NULL,
                order_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                ready INTEGER NOT NULL DEFAULT 0,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY(war_id, order_id, user_id)
            );
            """
        )

        pdcols = await self._table_cols("praise_daily_limits")
        if "paid_runes_given" not in pdcols:
            await self.conn.execute(
                "ALTER TABLE praise_daily_limits ADD COLUMN paid_runes_given INTEGER NOT NULL DEFAULT 0"
            )
        if "posts_praised_count" not in pdcols:
            await self.conn.execute(
                "ALTER TABLE praise_daily_limits ADD COLUMN posts_praised_count INTEGER NOT NULL DEFAULT 0"
            )
        if "free_praises_used" not in pdcols:
            await self.conn.execute(
                "ALTER TABLE praise_daily_limits ADD COLUMN free_praises_used INTEGER NOT NULL DEFAULT 0"
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
                   last_voice_award_ts, last_chat_award_ts, active_decoration
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
                "active_decoration": "",
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
            "active_decoration": str(g("active_decoration", "")) or "",
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
        if cost_i < 0:
            log.warning("Rejected negative spend_runes cost: user=%s cost=%s", user_id, cost_i)
            return False
        if cost_i == 0:
            return True

        # Атомарно: UPDATE с условием по балансу, чтобы избежать гонок при одновременных кликах.
        cur = await self.conn.execute(
            "UPDATE users SET runes = runes - ? WHERE user_id=? AND runes >= ?",
            (cost_i, int(user_id), cost_i),
        )
        await self.conn.commit()
        ok = cur.rowcount == 1
        await cur.close()
        return ok

    async def transfer_runes(self, from_user_id: int, to_user_id: int, amount: int) -> bool:
        """Atomic rune transfer. Returns False if sender lacks funds."""
        assert self.conn
        src = int(from_user_id)
        dst = int(to_user_id)
        delta = int(amount)

        if delta <= 0 or src == dst:
            return False

        await self.ensure_user(src)
        await self.ensure_user(dst)

        try:
            await self.conn.execute("BEGIN IMMEDIATE")
            cur = await self.conn.execute(
                "UPDATE users SET runes = runes - ? WHERE user_id=? AND runes >= ?",
                (delta, src, delta),
            )
            ok = int(cur.rowcount or 0) == 1
            await cur.close()
            if not ok:
                await self.conn.execute("ROLLBACK")
                return False
            await self.conn.execute(
                "UPDATE users SET runes = runes + ? WHERE user_id=?",
                (delta, dst),
            )
            await self.conn.execute("COMMIT")
            return True
        except Exception:
            try:
                await self.conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    async def get_runes_rank_info(
        self,
        user_id: int,
        scope_user_ids: set[int] | None = None,
    ) -> Dict[str, Any]:
        """
        Return player rank context in rune leaderboard.
        If scope_user_ids is provided, ranking is calculated only within that scope.
        """
        assert self.conn
        await self.ensure_user(user_id)

        scope: set[int] | None = None
        if scope_user_ids is not None:
            scope = {int(x) for x in scope_user_ids}

        cur = await self.conn.execute(
            "SELECT user_id, runes, level FROM users ORDER BY runes DESC, level DESC"
        )
        rows = await cur.fetchall()
        await cur.close()

        rank = 0
        prev_runes: int | None = None
        target_uid = int(user_id)

        for row in rows:
            uid = int(row[0])
            runes = int(row[1])

            if scope is not None and uid not in scope:
                continue

            rank += 1
            if uid == target_uid:
                target = prev_runes if rank > 1 else None
                needed = max(0, int(target or 0) - runes) if target is not None else 0
                return {
                    "rank": rank,
                    "current_runes": runes,
                    "next_rank_target_runes": target,
                    "runes_needed": needed,
                }

            prev_runes = runes

        u = await self.get_user(target_uid)
        return {
            "rank": 0,
            "current_runes": int(u.get("runes", 0)),
            "next_rank_target_runes": None,
            "runes_needed": 0,
        }

    

    # ---------- Daily profit (per day_key) ----------

    async def dp_get(self, user_id: int, day_key: str, key: str) -> int:
        """Get daily counter value (defaults to 0)."""
        assert self.conn
        await self.ensure_user(user_id)
        col = await self._daily_profit_col()
        cur = await self.conn.execute(
            f"SELECT COALESCE(SUM({col}), 0) FROM daily_profit WHERE user_id=? AND day_key=? AND key=?",
            (int(user_id), str(day_key), str(key)),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def dp_add(self, user_id: int, day_key: str, key: str, delta: int) -> None:
        """Add delta to daily counter (upsert)."""
        assert self.conn
        await self.ensure_user(user_id)
        col = await self._daily_profit_col()
        delta_i = int(delta)
        uid = int(user_id)
        dkey = str(day_key)
        metric = str(key)
        try:
            await self.conn.execute(
                """
                INSERT INTO daily_profit(user_id, day_key, key, {col})
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id, day_key, key) DO UPDATE SET {col} = {col} + excluded.{col}
                """.format(col=col),
                (uid, dkey, metric, delta_i),
            )
        except aiosqlite.OperationalError:
            # Legacy DB fallback: old daily_profit tables might not have the unique
            # constraint required by ON CONFLICT.
            cur = await self.conn.execute(
                f"UPDATE daily_profit SET {col} = {col} + ? WHERE user_id=? AND day_key=? AND key=?",
                (delta_i, uid, dkey, metric),
            )
            updated = int(cur.rowcount or 0)
            await cur.close()
            if updated == 0:
                await self.conn.execute(
                    f"INSERT INTO daily_profit(user_id, day_key, key, {col}) VALUES(?, ?, ?, ?)",
                    (uid, dkey, metric, delta_i),
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

    async def purchases_by_kind(self, user_id: int, kind: str) -> set[str]:
        assert self.conn
        prefix = f"{str(kind)}:"
        cur = await self.conn.execute(
            "SELECT sku FROM purchases WHERE user_id=? AND sku LIKE ?",
            (int(user_id), f"{prefix}%"),
        )
        rows = await cur.fetchall()
        await cur.close()

        out: set[str] = set()
        for row in rows:
            sku = str(row[0] or "")
            if not sku.startswith(prefix):
                continue
            item_id = sku[len(prefix) :]
            if item_id:
                out.add(item_id)
        return out

    async def get_active_decoration(self, user_id: int) -> str:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT active_decoration FROM users WHERE user_id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return ""
        return str(row[0] or "")

    async def set_active_decoration(self, user_id: int, decoration_id: str | None) -> None:
        assert self.conn
        await self.ensure_user(user_id)
        value = str(decoration_id or "").strip()
        await self.conn.execute(
            "UPDATE users SET active_decoration=? WHERE user_id=?",
            (value, int(user_id)),
        )
        await self.conn.commit()

    # ---------- Rental channels / posts / praises ----------

    async def rental_get_by_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT user_id, channel_id, expires_at, is_active FROM rental_channels WHERE user_id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return {
            "user_id": int(row[0]),
            "channel_id": int(row[1]),
            "expires_at": int(row[2]),
            "is_active": int(row[3]),
        }

    async def rental_get_by_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT user_id, channel_id, expires_at, is_active FROM rental_channels WHERE channel_id=?",
            (int(channel_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return {
            "user_id": int(row[0]),
            "channel_id": int(row[1]),
            "expires_at": int(row[2]),
            "is_active": int(row[3]),
        }

    async def rental_upsert(self, user_id: int, channel_id: int, expires_at: int, is_active: int) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT INTO rental_channels(user_id, channel_id, expires_at, is_active)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                channel_id=excluded.channel_id,
                expires_at=excluded.expires_at,
                is_active=excluded.is_active
            """,
            (int(user_id), int(channel_id), int(expires_at), int(is_active)),
        )
        await self.conn.commit()

    async def rental_set_active(self, user_id: int, is_active: int) -> None:
        assert self.conn
        await self.conn.execute(
            "UPDATE rental_channels SET is_active=? WHERE user_id=?",
            (int(is_active), int(user_id)),
        )
        await self.conn.commit()

    async def rental_get_expired_active(self, now_ts: int) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT user_id, channel_id, expires_at, is_active
            FROM rental_channels
            WHERE is_active=1 AND expires_at <= ?
            """,
            (int(now_ts),),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "user_id": int(r[0]),
                "channel_id": int(r[1]),
                "expires_at": int(r[2]),
                "is_active": int(r[3]),
            }
            for r in rows
        ]

    async def rental_list_all(self) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT user_id, channel_id, expires_at, is_active FROM rental_channels"
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "user_id": int(r[0]),
                "channel_id": int(r[1]),
                "expires_at": int(r[2]),
                "is_active": int(r[3]),
            }
            for r in rows
        ]

    async def rental_post_add(self, message_id: int, author_user_id: int, channel_id: int) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO rental_posts(message_id, author_user_id, channel_id, created_at)
            VALUES(?,?,?,?)
            """,
            (int(message_id), int(author_user_id), int(channel_id), _now()),
        )
        await self.conn.commit()

    async def rental_post_get(self, message_id: int) -> Optional[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT message_id, author_user_id, channel_id, created_at
            FROM rental_posts WHERE message_id=?
            """,
            (int(message_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return {
            "message_id": int(row[0]),
            "author_user_id": int(row[1]),
            "channel_id": int(row[2]),
            "created_at": int(row[3]),
        }

    async def post_praise_add(
        self,
        message_id: int,
        author_user_id: int,
        praised_by_user_id: int,
        amount: int,
        is_free: bool,
    ) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT INTO post_praises(message_id, author_user_id, praised_by_user_id, amount, is_free, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                int(message_id),
                int(author_user_id),
                int(praised_by_user_id),
                int(amount),
                1 if bool(is_free) else 0,
                _now(),
            ),
        )
        await self.conn.commit()

    async def praise_daily_get(self, user_id: int, date_key: str) -> Dict[str, int]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT paid_runes_given, posts_praised_count, free_praises_used
            FROM praise_daily_limits WHERE user_id=? AND date=?
            """,
            (int(user_id), str(date_key)),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return {
                "paid_runes_given": 0,
                "posts_praised_count": 0,
                "free_praises_used": 0,
            }
        return {
            "paid_runes_given": int(row[0]),
            "posts_praised_count": int(row[1]),
            "free_praises_used": int(row[2]),
        }

    async def praise_daily_add(
        self,
        user_id: int,
        date_key: str,
        paid_runes_delta: int,
        posts_delta: int,
        free_delta: int,
    ) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT INTO praise_daily_limits(user_id, date, paid_runes_given, posts_praised_count, free_praises_used)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id, date) DO UPDATE SET
                paid_runes_given = paid_runes_given + excluded.paid_runes_given,
                posts_praised_count = posts_praised_count + excluded.posts_praised_count,
                free_praises_used = free_praises_used + excluded.free_praises_used
            """,
            (
                int(user_id),
                str(date_key),
                int(paid_runes_delta),
                int(posts_delta),
                int(free_delta),
            ),
        )
        await self.conn.commit()

    # ---------- Orders / Herald / Wars ----------

    async def order_get_user(self, user_id: int) -> str:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT order_id FROM order_members WHERE user_id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return ""
        return str(row[0] or "")

    async def order_join(self, user_id: int, order_id: str) -> bool:
        """
        Join user to an order exactly once.
        Returns:
          True  -> joined now OR already in this same order
          False -> user already in another order
        """
        assert self.conn
        await self.ensure_user(user_id)
        oid = str(order_id).strip().lower()
        cur = await self.conn.execute(
            "SELECT order_id FROM order_members WHERE user_id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        await cur.close()

        if row:
            have = str(row[0] or "").strip().lower()
            return have == oid

        await self.conn.execute(
            "INSERT INTO order_members(user_id, order_id, joined_ts) VALUES(?,?,?)",
            (int(user_id), oid, _now()),
        )
        await self.conn.commit()
        return True

    async def order_member_ids_from_db(self, order_id: str) -> list[int]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT user_id FROM order_members WHERE order_id=?",
            (str(order_id).strip().lower(),),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [int(r[0]) for r in rows]

    async def order_get_influence(self, order_id: str) -> int:
        assert self.conn
        oid = str(order_id).strip().lower()
        await self.conn.execute(
            "INSERT OR IGNORE INTO order_influence(order_id, influence) VALUES(?, 0)",
            (oid,),
        )
        cur = await self.conn.execute(
            "SELECT influence FROM order_influence WHERE order_id=?",
            (oid,),
        )
        row = await cur.fetchone()
        await cur.close()
        await self.conn.commit()
        if not row:
            return 0
        return int(row[0] or 0)

    async def order_add_influence(self, order_id: str, delta: int) -> int:
        assert self.conn
        oid = str(order_id).strip().lower()
        d = int(delta)
        await self.conn.execute(
            """
            INSERT INTO order_influence(order_id, influence)
            VALUES(?, ?)
            ON CONFLICT(order_id) DO UPDATE SET influence = influence + excluded.influence
            """,
            (oid, d),
        )
        cur = await self.conn.execute(
            "SELECT influence FROM order_influence WHERE order_id=?",
            (oid,),
        )
        row = await cur.fetchone()
        await cur.close()
        await self.conn.commit()
        return int(row[0] if row else 0)

    async def order_all_influence(self) -> dict[str, int]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT order_id, influence FROM order_influence"
        )
        rows = await cur.fetchall()
        await cur.close()
        return {str(r[0]): int(r[1]) for r in rows}

    async def order_get_herald(self, order_id: str) -> dict[str, int]:
        assert self.conn
        oid = str(order_id).strip().lower()
        cur = await self.conn.execute(
            "SELECT user_id, elected_ts FROM order_herald WHERE order_id=?",
            (oid,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return {"user_id": 0, "elected_ts": 0}
        return {"user_id": int(row[0] or 0), "elected_ts": int(row[1] or 0)}

    async def order_set_herald(self, order_id: str, user_id: int) -> None:
        assert self.conn
        oid = str(order_id).strip().lower()
        await self.conn.execute(
            """
            INSERT INTO order_herald(order_id, user_id, elected_ts)
            VALUES(?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET
                user_id=excluded.user_id,
                elected_ts=excluded.elected_ts
            """,
            (oid, int(user_id), _now()),
        )
        await self.conn.commit()

    async def order_get_election(self, order_id: str) -> Optional[Dict[str, Any]]:
        assert self.conn
        oid = str(order_id).strip().lower()
        cur = await self.conn.execute(
            """
            SELECT order_id, stage, started_ts, collect_deadline_ts, vote_deadline_ts,
                   last_finished_ts, collect_message_id, vote_message_id
            FROM order_elections WHERE order_id=?
            """,
            (oid,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return {
            "order_id": str(row[0]),
            "stage": str(row[1] or ""),
            "started_ts": int(row[2] or 0),
            "collect_deadline_ts": int(row[3] or 0),
            "vote_deadline_ts": int(row[4] or 0),
            "last_finished_ts": int(row[5] or 0),
            "collect_message_id": int(row[6] or 0),
            "vote_message_id": int(row[7] or 0),
        }

    async def order_upsert_election(self, order_id: str, **fields: Any) -> None:
        assert self.conn
        oid = str(order_id).strip().lower()
        await self.conn.execute(
            "INSERT OR IGNORE INTO order_elections(order_id) VALUES(?)",
            (oid,),
        )
        if fields:
            keys = list(fields.keys())
            vals = [fields[k] for k in keys]
            set_clause = ", ".join(f"{k}=?" for k in keys)
            await self.conn.execute(
                f"UPDATE order_elections SET {set_clause} WHERE order_id=?",
                (*vals, oid),
            )
        await self.conn.commit()

    async def order_active_elections(self) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT order_id, stage, started_ts, collect_deadline_ts, vote_deadline_ts,
                   last_finished_ts, collect_message_id, vote_message_id
            FROM order_elections
            WHERE stage IN ('collect', 'vote')
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        out: list[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "order_id": str(row[0]),
                    "stage": str(row[1] or ""),
                    "started_ts": int(row[2] or 0),
                    "collect_deadline_ts": int(row[3] or 0),
                    "vote_deadline_ts": int(row[4] or 0),
                    "last_finished_ts": int(row[5] or 0),
                    "collect_message_id": int(row[6] or 0),
                    "vote_message_id": int(row[7] or 0),
                }
            )
        return out

    async def order_election_set_candidate(self, order_id: str, user_id: int, willing: bool) -> None:
        assert self.conn
        oid = str(order_id).strip().lower()
        uid = int(user_id)
        now = _now()
        w = 1 if bool(willing) else 0
        applied = now if w == 1 else 0
        await self.conn.execute(
            """
            INSERT INTO order_election_candidates(order_id, user_id, willing, applied_ts, responded_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(order_id, user_id) DO UPDATE SET
                willing=excluded.willing,
                responded_ts=excluded.responded_ts,
                applied_ts=CASE
                    WHEN excluded.willing=1 AND order_election_candidates.applied_ts=0
                    THEN excluded.applied_ts
                    ELSE order_election_candidates.applied_ts
                END
            """,
            (oid, uid, w, applied, now),
        )
        await self.conn.commit()

    async def order_election_candidates(self, order_id: str) -> list[Dict[str, Any]]:
        assert self.conn
        oid = str(order_id).strip().lower()
        cur = await self.conn.execute(
            """
            SELECT user_id, willing, applied_ts, responded_ts
            FROM order_election_candidates
            WHERE order_id=?
            """,
            (oid,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "user_id": int(r[0]),
                "willing": int(r[1]),
                "applied_ts": int(r[2] or 0),
                "responded_ts": int(r[3] or 0),
            }
            for r in rows
        ]

    async def order_election_set_vote(self, order_id: str, voter_user_id: int, candidate_user_id: int) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT INTO order_election_votes(order_id, voter_user_id, candidate_user_id, voted_ts)
            VALUES(?,?,?,?)
            ON CONFLICT(order_id, voter_user_id) DO UPDATE SET
                candidate_user_id=excluded.candidate_user_id,
                voted_ts=excluded.voted_ts
            """,
            (
                str(order_id).strip().lower(),
                int(voter_user_id),
                int(candidate_user_id),
                _now(),
            ),
        )
        await self.conn.commit()

    async def order_election_votes(self, order_id: str) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT voter_user_id, candidate_user_id, voted_ts
            FROM order_election_votes
            WHERE order_id=?
            """,
            (str(order_id).strip().lower(),),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "voter_user_id": int(r[0]),
                "candidate_user_id": int(r[1]),
                "voted_ts": int(r[2] or 0),
            }
            for r in rows
        ]

    async def order_election_reset_runtime(self, order_id: str, *, stage: str, last_finished_ts: int) -> None:
        assert self.conn
        oid = str(order_id).strip().lower()
        await self.conn.execute(
            "DELETE FROM order_election_candidates WHERE order_id=?",
            (oid,),
        )
        await self.conn.execute(
            "DELETE FROM order_election_votes WHERE order_id=?",
            (oid,),
        )
        await self.order_upsert_election(
            oid,
            stage=str(stage),
            collect_deadline_ts=0,
            vote_deadline_ts=0,
            collect_message_id=0,
            vote_message_id=0,
            last_finished_ts=int(last_finished_ts),
        )
        await self.conn.commit()

    async def order_create_war(
        self,
        *,
        attacker_order_id: str,
        defender_order_id: str,
        game_name: str,
        needed_count: int,
        match_note: str,
        started_by_user_id: int,
        stage: str,
        attacker_deadline_ts: int,
    ) -> int:
        assert self.conn
        cur = await self.conn.execute(
            """
            INSERT INTO order_wars(
                attacker_order_id, defender_order_id, game_name, needed_count, match_note,
                started_by_user_id, stage, created_ts, attacker_deadline_ts
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                str(attacker_order_id).strip().lower(),
                str(defender_order_id).strip().lower(),
                str(game_name),
                int(needed_count),
                str(match_note),
                int(started_by_user_id),
                str(stage),
                _now(),
                int(attacker_deadline_ts),
            ),
        )
        war_id = int(cur.lastrowid or 0)
        await cur.close()
        await self.conn.commit()
        return war_id

    async def order_get_war(self, war_id: int) -> Optional[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT war_id, attacker_order_id, defender_order_id, game_name, needed_count, match_note,
                   started_by_user_id, stage, created_ts, attacker_deadline_ts, defender_deadline_ts,
                   attacker_letter_message_id, attacker_status_message_id,
                   defender_letter_message_id, defender_status_message_id,
                   resolved_ts, result_order_id, cancel_reason
            FROM order_wars WHERE war_id=?
            """,
            (int(war_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return {
            "war_id": int(row[0]),
            "attacker_order_id": str(row[1]),
            "defender_order_id": str(row[2]),
            "game_name": str(row[3]),
            "needed_count": int(row[4]),
            "match_note": str(row[5] or ""),
            "started_by_user_id": int(row[6]),
            "stage": str(row[7]),
            "created_ts": int(row[8]),
            "attacker_deadline_ts": int(row[9] or 0),
            "defender_deadline_ts": int(row[10] or 0),
            "attacker_letter_message_id": int(row[11] or 0),
            "attacker_status_message_id": int(row[12] or 0),
            "defender_letter_message_id": int(row[13] or 0),
            "defender_status_message_id": int(row[14] or 0),
            "resolved_ts": int(row[15] or 0),
            "result_order_id": str(row[16] or ""),
            "cancel_reason": str(row[17] or ""),
        }

    async def order_update_war(self, war_id: int, **fields: Any) -> None:
        assert self.conn
        if not fields:
            return
        keys = list(fields.keys())
        vals = [fields[k] for k in keys]
        set_clause = ", ".join(f"{k}=?" for k in keys)
        await self.conn.execute(
            f"UPDATE order_wars SET {set_clause} WHERE war_id=?",
            (*vals, int(war_id)),
        )
        await self.conn.commit()

    async def order_active_wars(self) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT war_id, attacker_order_id, defender_order_id, game_name, needed_count, match_note,
                   started_by_user_id, stage, created_ts, attacker_deadline_ts, defender_deadline_ts,
                   attacker_letter_message_id, attacker_status_message_id,
                   defender_letter_message_id, defender_status_message_id,
                   resolved_ts, result_order_id, cancel_reason
            FROM order_wars
            WHERE stage IN ('collect_attacker', 'collect_defender')
            ORDER BY war_id ASC
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        out: list[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "war_id": int(row[0]),
                    "attacker_order_id": str(row[1]),
                    "defender_order_id": str(row[2]),
                    "game_name": str(row[3]),
                    "needed_count": int(row[4]),
                    "match_note": str(row[5] or ""),
                    "started_by_user_id": int(row[6]),
                    "stage": str(row[7]),
                    "created_ts": int(row[8]),
                    "attacker_deadline_ts": int(row[9] or 0),
                    "defender_deadline_ts": int(row[10] or 0),
                    "attacker_letter_message_id": int(row[11] or 0),
                    "attacker_status_message_id": int(row[12] or 0),
                    "defender_letter_message_id": int(row[13] or 0),
                    "defender_status_message_id": int(row[14] or 0),
                    "resolved_ts": int(row[15] or 0),
                    "result_order_id": str(row[16] or ""),
                    "cancel_reason": str(row[17] or ""),
                }
            )
        return out

    async def order_has_active_war_for(self, order_id: str) -> bool:
        assert self.conn
        oid = str(order_id).strip().lower()
        cur = await self.conn.execute(
            """
            SELECT 1
            FROM order_wars
            WHERE stage IN ('collect_attacker', 'collect_defender', 'confirmed')
              AND (attacker_order_id=? OR defender_order_id=?)
            LIMIT 1
            """,
            (oid, oid),
        )
        row = await cur.fetchone()
        await cur.close()
        return bool(row)

    async def order_last_war_attack_ts(self, attacker_order_id: str) -> int:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT MAX(created_ts)
            FROM order_wars
            WHERE attacker_order_id=?
            """,
            (str(attacker_order_id).strip().lower(),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0
        return int(row[0] or 0)

    async def order_set_war_response(self, war_id: int, order_id: str, user_id: int, ready: bool) -> None:
        assert self.conn
        await self.conn.execute(
            """
            INSERT INTO order_war_responses(war_id, order_id, user_id, ready, updated_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(war_id, order_id, user_id) DO UPDATE SET
                ready=excluded.ready,
                updated_ts=excluded.updated_ts
            """,
            (
                int(war_id),
                str(order_id).strip().lower(),
                int(user_id),
                1 if bool(ready) else 0,
                _now(),
            ),
        )
        await self.conn.commit()

    async def order_get_war_responses(self, war_id: int, order_id: str) -> list[Dict[str, Any]]:
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT user_id, ready, updated_ts
            FROM order_war_responses
            WHERE war_id=? AND order_id=?
            ORDER BY updated_ts ASC
            """,
            (int(war_id), str(order_id).strip().lower()),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "user_id": int(r[0]),
                "ready": int(r[1]),
                "updated_ts": int(r[2] or 0),
            }
            for r in rows
        ]

    async def order_finish_war(
        self,
        war_id: int,
        *,
        stage: str,
        cancel_reason: str = "",
        result_order_id: str = "",
    ) -> None:
        assert self.conn
        await self.conn.execute(
            """
            UPDATE order_wars
            SET stage=?, cancel_reason=?, result_order_id=?, resolved_ts=?
            WHERE war_id=?
            """,
            (
                str(stage),
                str(cancel_reason),
                str(result_order_id).strip().lower(),
                _now(),
                int(war_id),
            ),
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
