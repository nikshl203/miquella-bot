# economy_utils.py
from __future__ import annotations

from typing import Any, Dict, Tuple


def xp_need(cfg: Dict[str, Any], level: int) -> int:
    """need_xp(level) = base + per_level * level (clamped to >=1)."""
    leveling = cfg.get("leveling", {}) or {}
    formula = leveling.get("need_xp_formula", {}) or {}
    base = int(formula.get("base", 250) or 250)
    per_level = int(formula.get("per_level", 50) or 50)
    lvl = int(level)
    return max(1, base + per_level * lvl)


def economy_per_day(cfg: Dict[str, Any], level: int) -> int:
    """Runes income per day by level bands from config."""
    economy = (cfg.get("economy", {}) or {}).get("rune_income_per_day", []) or []
    lvl = int(level)
    for band in economy:
        try:
            f = int(band.get("from", 0))
            t = int(band.get("to", 0))
            p = int(band.get("per_day", 0))
        except Exception:
            continue
        if f <= lvl <= t:
            return max(0, p)
    return 0


def calc_caps(cfg: Dict[str, Any], level: int) -> Tuple[int, int, int, int, int]:
    """
    Returns:
      per_day, voice_runes_cap, chat_runes_cap, voice_xp_cap, chat_xp_cap

    Caps are split by voice_share (rest is chat).
    """
    economy_cfg = cfg.get("economy", {}) or {}
    voice_share = float(economy_cfg.get("voice_share", 0.6) or 0.6)
    voice_share = max(0.0, min(1.0, voice_share))

    per_day = economy_per_day(cfg, level)
    voice_cap = int(round(per_day * voice_share))
    voice_cap = max(0, min(per_day, voice_cap))
    chat_cap = max(0, per_day - voice_cap)

    leveling = cfg.get("leveling", {}) or {}
    voice_xp_per_rune = float(leveling.get("voice_xp_per_rune", 3.0) or 3.0)
    chat_xp_per_rune = float(leveling.get("chat_xp_per_rune", 2.0) or 2.0)

    voice_xp_cap = int(round(voice_cap * voice_xp_per_rune))
    chat_xp_cap = int(round(chat_cap * chat_xp_per_rune))
    return per_day, voice_cap, chat_cap, voice_xp_cap, chat_xp_cap
