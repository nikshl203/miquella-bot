# main.py
from __future__ import annotations

import json
import os
import sys
import logging
import traceback
from pathlib import Path
from typing import Any, Dict, List

import discord
from discord.ext import commands

CONFIG_PATH = Path("data") / "config.json"



def load_dotenv(path: Path = Path(".env")) -> None:
    """Minimal .env loader (KEY=VALUE), to avoid extra deps on Windows."""
    try:
        if not path.exists():
            return
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            # do not override already-set environment variables
            os.environ.setdefault(key, val)
    except Exception:
        # .env is optional; ignore failures
        return


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Не найден {CONFIG_PATH} (должен быть data/config.json)")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


class VoidBot(commands.Bot):
    def __init__(self, cfg: Dict[str, Any]) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True
        intents.reactions = True
        intents.voice_states = True  # нужно для начислений в войсе
        intents.presences = True

        super().__init__(command_prefix="!", intents=intents)

        self.cfg = cfg
        self.repo = None
        self._admin_user_id = int(cfg.get("admin_user_id", 0))

    def is_admin(self, user_id: int) -> bool:
        return int(user_id) == self._admin_user_id

    async def setup_hook(self) -> None:
        extensions: List[str] = [
            "cogs.db",
            "cogs.activity",   # ✅ начисления XP/рун за войс/чат
            "cogs.level_roles",
            "cogs.admin",
            "cogs.welcome",
            "cogs.survey",
            "cogs.shop",
            "cogs.echo_posts",
            "cogs.void_info",
            "cogs.tendril",
            "cogs.coin",
            "cogs.story",
            "cogs.duel",
            "cogs.persistent",
        ]

        for ext in extensions:
            await self.load_extension(ext)
            logging.info("Loaded: %s", ext)


def main() -> None:
    print("=== Void of Miquella starting... ===", flush=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        cfg = load_config()
        bot = VoidBot(cfg)
        print("Config OK. Connecting to Discord...", flush=True)
        load_dotenv()
        token = os.getenv("BOT_TOKEN") or cfg.get("bot_token", "")
        if not token:
            raise RuntimeError("BOT_TOKEN is not set and config has no bot_token")
        bot.run(token)

    except Exception:
        print("\n!!! CRASH DURING STARTUP !!!", flush=True)
        traceback.print_exc()
        # For local desktop usage we keep pause-on-crash,
        # but avoid blocking in headless/server environments.
        if sys.stdin is not None and sys.stdin.isatty():
            try:
                input("\nНажми Enter чтобы закрыть окно...")
            except EOFError:
                pass


if __name__ == "__main__":
    main()
