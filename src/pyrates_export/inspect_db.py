"""Read-only checks of the source view: connectivity, column types, query plan."""

from __future__ import annotations

from datetime import date, timedelta

from pyrates_export.db import traces_view
from pyrates_export.export import export_query


def _print_result(cursor, title: str) -> None:
    print(f"\n== {title} ==")
    headers = [column[0] for column in cursor.description]
    print(" | ".join(headers))
    for row in cursor.fetchall():
        print(" | ".join("" if value is None else str(value) for value in row))


def inspect(conn, sample_day: date) -> None:
    """Print server version, view definition, column types and a query plan.

    The plan is computed for a one-day export query; EXPLAIN does not execute it.
    """
    view = traces_view()
    with conn.cursor() as cur:
        cur.execute("SELECT VERSION()")
        _print_result(cur, "Server version")
        cur.execute(f"DESCRIBE {view}")
        _print_result(cur, "View columns")
        cur.execute(f"SHOW CREATE VIEW {view}")
        _print_result(cur, "View definition")
        cur.execute(
            "EXPLAIN " + export_query(), (sample_day, sample_day + timedelta(days=1))
        )
        _print_result(cur, f"Query plan for one day ({sample_day})")
