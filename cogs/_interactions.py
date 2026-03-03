# cogs/_interactions.py
from __future__ import annotations

import logging
from typing import Any, Optional

import discord

log = logging.getLogger("void")


async def safe_defer_ephemeral(interaction: discord.Interaction) -> bool:
    """Ack interaction ASAP with an ephemeral deferred response."""
    try:
        if interaction.response.is_done():
            return True
        await interaction.response.defer(ephemeral=True, thinking=False)
        return True
    except discord.NotFound:
        return False
    except Exception:
        log.exception("safe_defer_ephemeral failed")
        return False


async def safe_defer_update(interaction: discord.Interaction) -> bool:
    """Ack component interaction ASAP without sending a message (deferred update)."""
    try:
        if interaction.response.is_done():
            return True
        await interaction.response.defer(thinking=False)
        return True
    except discord.NotFound:
        return False
    except Exception:
        log.exception("safe_defer_update failed")
        return False


async def safe_send(
    interaction: discord.Interaction,
    content: str,
    *,
    ephemeral: bool = False,
    **kwargs: Any,
) -> Optional[discord.Message]:
    """Send a message responding to the interaction.

    If the interaction response is already used (defer/edit), falls back to followup.
    """
    try:
        if interaction.response.is_done():
            return await interaction.followup.send(content=content, ephemeral=ephemeral, **kwargs)
        return await interaction.response.send_message(content=content, ephemeral=ephemeral, **kwargs)
    except discord.NotFound:
        return None
    except Exception:
        log.exception("safe_send failed")
        return None


async def safe_edit_message(interaction: discord.Interaction, **kwargs: Any) -> bool:
    """Edit the message that owns the component.

    Ephemeral interaction messages are edited via interaction/webhook endpoints.
    A plain `Message.edit()` can 404 for ephemeral messages.

    Strategy:
    1) If we haven't responded yet -> `interaction.response.edit_message` (best).
    2) If we already responded (often after defer_update) -> try `interaction.edit_original_response`.
    3) Fallbacks: followup.edit_message(message_id) then normal message.edit.
    """
    try:
        # 1) Fast path (component interaction, within 3s)
        if not interaction.response.is_done():
            await interaction.response.edit_message(**kwargs)
            return True

        # 2) After defer_update(), this is the correct way to update the component message
        try:
            await interaction.edit_original_response(**kwargs)
            return True
        except Exception:
            pass

        # 3) Fallbacks
        if interaction.message is not None:
            try:
                await interaction.followup.edit_message(interaction.message.id, **kwargs)
                return True
            except Exception:
                pass
            try:
                await interaction.message.edit(**kwargs)
                return True
            except Exception:
                pass

        return False
    except discord.NotFound:
        return False
    except Exception:
        log.exception("safe_edit_message failed")
        return False
