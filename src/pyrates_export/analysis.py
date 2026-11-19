"""Local analysis of the exported Parquet files (no database access)."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

COMBINATIONS_QUERY = """
    SELECT
        level_id,
        action_name,
        object_name,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT game_id) AS n_games,
        MIN("date") AS first_seen,
        MAX("date") AS last_seen
    FROM read_parquet(?)
    GROUP BY level_id, action_name, object_name
    ORDER BY level_id, n_rows DESC
"""


def level_event_combinations(traces_dir: Path, out_csv: Path) -> tuple[list[str], list[tuple]]:
    """List the distinct (level_id, action_name, object_name) combinations.

    For each combination: number of rows, number of distinct games, and the
    first and last date it was seen. The result is written to ``out_csv``.
    """
    files = sorted(traces_dir.glob("traces_*.parquet"))
    if not files:
        raise SystemExit(f"No exported files found in {traces_dir}. Run the export first.")
    con = duckdb.connect()
    result = con.execute(COMBINATIONS_QUERY, [str(traces_dir / "traces_*.parquet")])
    headers = [column[0] for column in result.description]
    rows = result.fetchall()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
    return headers, rows


def format_table(headers: list[str], rows: list[tuple]) -> str:
    cells = [headers] + [["" if v is None else str(v) for v in row] for row in rows]
    widths = [max(len(line[i]) for line in cells) for i in range(len(headers))]
    return "\n".join(
        "  ".join(value.ljust(width) for value, width in zip(line, widths)).rstrip()
        for line in cells
    )
