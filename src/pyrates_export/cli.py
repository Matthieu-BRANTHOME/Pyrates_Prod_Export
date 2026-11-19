"""Command-line interface: inspect, export, merge, combinations."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from pyrates_export import config


def _setup_logging(verbose: bool) -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "export.log", encoding="utf-8"),
        ],
    )


def _resolve_range(args) -> tuple[date, date]:
    start = args.start or config.env_date("EXPORT_START")
    end = args.end or config.env_date("EXPORT_END")
    if start is None or end is None:
        raise SystemExit(
            "No date range: pass --start/--end or set EXPORT_START/EXPORT_END in .env."
        )
    if end < start:
        raise SystemExit(f"End date {end} is before start date {start}.")
    if end >= date.today():
        # Today's traces are still being written: a file exported now would be
        # incomplete yet look finished to later runs.
        raise SystemExit(f"End date {end} must be in the past (before {date.today()}).")
    return start, end


def _cmd_export(args) -> None:
    from pyrates_export import db
    from pyrates_export.export import merge_segments, run_export

    _setup_logging(args.verbose)
    start, end = _resolve_range(args)
    cfg = config.load_db_config()
    out_dir = args.out_dir or config.env_path("EXPORT_DIR", config.DEFAULT_EXPORT_DIR)
    run_export(
        connect_fn=lambda: db.connect(cfg, streaming=True),
        start=start,
        end_inclusive=end,
        out_dir=out_dir,
        chunk_size=args.chunk_size or config.env_int("CHUNK_SIZE", config.DEFAULT_CHUNK_SIZE),
        force=args.force,
    )
    if args.merge:
        merge_segments(out_dir)


def _cmd_merge(args) -> None:
    from pyrates_export.export import merge_segments

    _setup_logging(verbose=False)
    traces_dir = args.traces_dir or config.env_path("EXPORT_DIR", config.DEFAULT_EXPORT_DIR)
    merge_segments(traces_dir, args.out)


def _cmd_inspect(args) -> None:
    from pyrates_export import db
    from pyrates_export.inspect_db import inspect

    cfg = config.load_db_config()
    sample_day = args.day or config.env_date("EXPORT_START") or date.today()
    conn = db.connect(cfg)
    try:
        inspect(conn, sample_day)
    finally:
        conn.close()


def _cmd_combinations(args) -> None:
    from pyrates_export.analysis import format_table, level_event_combinations

    traces_dir = args.traces_dir or config.env_path("EXPORT_DIR", config.DEFAULT_EXPORT_DIR)
    out_csv = args.out or config.DEFAULT_ANALYSIS_DIR / "level_event_combinations.csv"
    headers, rows = level_event_combinations(traces_dir, out_csv)
    print(format_table(headers, rows))
    print(f"\n{len(rows)} combination(s) written to {out_csv}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyrates-export",
        description="Export Pyrates interaction traces from MySQL to monthly Parquet files.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export", help="export traces month by month (resumable)")
    p_export.add_argument("--start", type=date.fromisoformat, help="first day, YYYY-MM-DD (inclusive)")
    p_export.add_argument("--end", type=date.fromisoformat, help="last day, YYYY-MM-DD (inclusive)")
    p_export.add_argument("--out-dir", type=Path, help="output directory (default data/traces)")
    p_export.add_argument("--chunk-size", type=int, help="rows fetched per chunk (default 50000)")
    p_export.add_argument("--force", action="store_true", help="re-export months already done")
    p_export.add_argument(
        "--merge", action="store_true", help="also write a single merged file at the end"
    )
    p_export.add_argument("-v", "--verbose", action="store_true", help="log progress per chunk")
    p_export.set_defaults(func=_cmd_export)

    p_merge = sub.add_parser("merge", help="merge the monthly files into a single Parquet file")
    p_merge.add_argument("--traces-dir", type=Path, help="directory of monthly files")
    p_merge.add_argument("--out", type=Path, help="output file (default data/traces_<start>_<end>.parquet)")
    p_merge.set_defaults(func=_cmd_merge)

    p_inspect = sub.add_parser("inspect", help="check the connection, column types and query plan")
    p_inspect.add_argument("--day", type=date.fromisoformat, help="day used for the query plan")
    p_inspect.set_defaults(func=_cmd_inspect)

    p_comb = sub.add_parser(
        "combinations",
        help="list distinct (level_id, action_name, object_name) from the exported files",
    )
    p_comb.add_argument("--traces-dir", type=Path, help="directory of exported files")
    p_comb.add_argument("--out", type=Path, help="CSV output path")
    p_comb.set_defaults(func=_cmd_combinations)

    return parser


def main(argv: list[str] | None = None) -> None:
    config.load_env()
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
