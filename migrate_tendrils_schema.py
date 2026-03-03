import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "bot.sqlite3"
print("DB:", DB_PATH)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Проверка таблицы
tbl = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tendrils'").fetchone()
if not tbl:
    raise SystemExit(
        "Таблица 'tendrils' не найдена. Проверь, что запускаешь скрипт в папке проекта (где main.py)."
    )


def get_cols() -> list[str]:
    return [r["name"] for r in conn.execute("PRAGMA table_info(tendrils)").fetchall()]


def has_col(name: str) -> bool:
    return name in get_cols()


cols_before = get_cols()
print("Columns before:", cols_before)


def add_column_if_missing(col_name: str, ddl: str):
    cols = get_cols()
    if col_name in cols:
        print(f"OK: column '{col_name}' already exists")
        return
    conn.execute(f"ALTER TABLE tendrils ADD COLUMN {ddl}")
    print(f"OK: added column '{col_name}'")


# ДОБАВЛЯЕМ НЕДОСТАЮЩИЕ КОЛОНКИ (которые ждёт новый tendril.py)
add_column_if_missing("expires_ts", "expires_ts INTEGER NOT NULL DEFAULT 0")
add_column_if_missing("last_tick_ts", "last_tick_ts INTEGER NOT NULL DEFAULT 0")
add_column_if_missing("last_announce_ts", "last_announce_ts INTEGER NOT NULL DEFAULT 0")
add_column_if_missing("stolen_total", "stolen_total INTEGER NOT NULL DEFAULT 0")
add_column_if_missing("attempts_left", "attempts_left INTEGER NOT NULL DEFAULT 3")

# Если вдруг source ещё нет (на будущее)
add_column_if_missing("source", "source TEXT NOT NULL DEFAULT 'attack'")

# ПЕРЕНОСИМ ДАННЫЕ из старых колонок в новые (если legacy-колонки присутствуют)

# last_steal_ts -> last_tick_ts
if has_col("last_steal_ts"):
    conn.execute("UPDATE tendrils SET last_tick_ts = COALESCE(last_tick_ts, 0) + 0")
    conn.execute(
        """
        UPDATE tendrils
        SET last_tick_ts = last_steal_ts
        WHERE (last_tick_ts = 0) AND (last_steal_ts IS NOT NULL)
        """
    )
else:
    print("SKIP: legacy column last_steal_ts not found")

# last_notify_ts -> last_announce_ts
if has_col("last_notify_ts"):
    conn.execute("UPDATE tendrils SET last_announce_ts = COALESCE(last_announce_ts, 0) + 0")
    conn.execute(
        """
        UPDATE tendrils
        SET last_announce_ts = last_notify_ts
        WHERE (last_announce_ts = 0) AND (last_notify_ts IS NOT NULL)
        """
    )
else:
    print("SKIP: legacy column last_notify_ts not found")

# expires_ts: если активный и expires_ts пустой — выставим "ещё 6 часов" от started_ts
SIX_HOURS = 6 * 60 * 60
now = int(time.time())

if has_col("active"):
    conn.execute(
        """
        UPDATE tendrils
        SET expires_ts =
            CASE
                WHEN expires_ts != 0 THEN expires_ts
                WHEN active = 1 AND started_ts IS NOT NULL AND started_ts > 0 THEN started_ts + ?
                WHEN active = 1 THEN ?
                ELSE COALESCE(started_ts, 0)
            END
        WHERE expires_ts = 0
        """,
        (SIX_HOURS, now + SIX_HOURS),
    )
else:
    # нет колонки active: просто выставим expires_ts, если он пустой
    conn.execute(
        """
        UPDATE tendrils
        SET expires_ts =
            CASE
                WHEN expires_ts != 0 THEN expires_ts
                WHEN started_ts IS NOT NULL AND started_ts > 0 THEN started_ts + ?
                ELSE ?
            END
        WHERE expires_ts = 0
        """,
        (SIX_HOURS, now + SIX_HOURS),
    )

# attempts_left: если вдруг 0 — вернём 3
conn.execute("UPDATE tendrils SET attempts_left = 3 WHERE attempts_left IS NULL OR attempts_left < 1")

conn.commit()

cols_after = get_cols()
print("Columns after:", cols_after)
print("DONE")
conn.close()
