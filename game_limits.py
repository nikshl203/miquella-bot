from time_utils import msk_day_key

# Daily limit values for different game keys. Extend this mapping to add new limits.
DEFAULT_LIMITS = {
    "coin:easy": 30,
    "coin:mid": 140,
    "coin:hard": 250,
    "duel:player": 60,
    "duel:bettor": 60,
}

async def get_progress(repo, user_id: int, key: str) -> int:
    """Return the current progress for a limit key for the given user.

    Looks up the value in the daily_profit table via Repo.dp_get(). If no value is
    stored, zero is returned.

    Parameters
    ----------
    repo: Repo
        The repository instance from cogs/db.py.
    user_id: int
        The Discord user ID.
    key: str
        The limit key (e.g. 'coin:easy').

    Returns
    -------
    int
        Current recorded progress for this user and key.
    """
    day = msk_day_key()
    # Repo.dp_get returns 0 if no record exists
    try:
        return int(await repo.dp_get(user_id, day, key))
    except Exception:
        return 0

async def add_progress(repo, user_id: int, key: str, amount: int) -> None:
    """Increase the progress for a given limit key.

    Uses Repo.dp_add() to increment the counter. If amount is zero or negative,
    no change is recorded.

    Parameters
    ----------
    repo: Repo
        The repository instance.
    user_id: int
        The Discord user ID.
    key: str
        The limit key.
    amount: int
        The amount to add to the progress.
    """
    amt = int(amount)
    if amt <= 0:
        return
    day = msk_day_key()
    try:
        await repo.dp_add(user_id, day, key, amt)
    except Exception:
        pass

def get_limit_value(key: str) -> int:
    """Return the daily limit value for a given key.

    If the key is unknown, returns 0.
    """
    return int(DEFAULT_LIMITS.get(key, 0))

async def can_play(repo, user_id: int, key: str, expected_gain: int = 0) -> tuple[bool, int, int]:
    """Check whether a user can start a game governed by this limit.

    This function compares the current progress against the limit. It returns a
    tuple ``(allowed, current, limit)`` where:

    - **allowed** (bool) is True if the user has remaining capacity for this limit;
    - **current** (int) is the current recorded progress;
    - **limit** (int) is the maximum allowed value.

    The optional ``expected_gain`` parameter should contain the amount of progress
    that would be added upon a successful game (for example, the net profit of a
    coin flip or the payout of a bet). According to the project requirements,
    players are allowed to start a game even if the prospective gain would push
    them beyond the limit, as long as they have not yet reached the limit prior
    to playing. To support this behaviour, ``can_play`` returns ``True`` if
    either of the following holds:

    * ``current`` < ``limit`` (the player still has capacity), or
    * ``current`` < ``limit`` + ``expected_gain`` (the player's current progress
      is below the future threshold).

    This allows the last game to potentially exceed the cap on the day of play.
    """
    limit_value = get_limit_value(key)
    current = await get_progress(repo, user_id, key)
    # Always allow if there is any capacity left or if the overshoot would occur only after this game
    if current < limit_value:
        return True, current, limit_value
    # At this point current >= limit; allow only if expected_gain pushes current over but not before
    if expected_gain > 0 and current < limit_value + expected_gain:
        return True, current, limit_value
    return False, current, limit_value

async def format_progress(repo, user_id: int, key: str) -> str:
    """Return a formatted string showing current and maximum limit for a user.

    The result is of the form "current/limit". Unknown keys will display "0/0".
    """
    current = await get_progress(repo, user_id, key)
    limit_value = get_limit_value(key)
    return f"{current}/{limit_value}"