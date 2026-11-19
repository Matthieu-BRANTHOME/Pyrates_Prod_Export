"""MySQL connection helpers."""

from __future__ import annotations

import logging
import os
import re
import ssl

import pymysql
import pymysql.cursors

from pyrates_export.config import DbConfig

log = logging.getLogger(__name__)



def traces_view() -> str:
    """Name of the source view, taken from MYSQL_TRACES_VIEW and backtick-quoted.

    The name is case-sensitive on the server: a wrong case fails with error 1146.
    """
    name = os.environ.get("MYSQL_TRACES_VIEW", "").strip()
    if not name:
        raise SystemExit(
            "Missing setting MYSQL_TRACES_VIEW. Copy .env.example to .env and fill it in."
        )
    if not re.fullmatch(r"[A-Za-z0-9_$]+", name):
        raise SystemExit(f"MYSQL_TRACES_VIEW is not a valid identifier: {name!r}")
    return f"`{name}`"


TRACE_COLUMNS = (
    "id",
    "date",
    "game_id",
    "level_id",
    "game_time",
    "progression",
    "action_name",
    "object_name",
    "duration",
    "code",
    "value",
    "reason_name",
    "error_message",
)

# Rows without a game id cannot be attributed to a game and are excluded everywhere.
VALID_GAME_FILTER = "game_id IS NOT NULL AND game_id <> ''"


def connect(cfg: DbConfig, *, streaming: bool = False) -> pymysql.connections.Connection:
    """Open a connection.

    With ``streaming=True`` the cursor is unbuffered (SSCursor): rows are read
    from the socket as they are fetched instead of being loaded in memory.
    """
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        cursorclass=(
            pymysql.cursors.SSCursor if streaming else pymysql.cursors.Cursor
        ),
        connect_timeout=30,
        read_timeout=cfg.read_timeout,
        write_timeout=60,
        ssl=ssl.create_default_context() if cfg.ssl else None,
    )
    if streaming:
        # Keep the server from dropping the connection while we are busy
        # writing a chunk to disk. Not all hosts allow changing it.
        try:
            with conn.cursor() as cur:
                cur.execute("SET SESSION net_write_timeout = 3600")
        except pymysql.MySQLError as exc:
            log.debug("Could not raise net_write_timeout: %s", exc)
    return conn
