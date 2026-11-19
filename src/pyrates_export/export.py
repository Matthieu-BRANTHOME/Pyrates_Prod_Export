"""Month-by-month export of the traces view to Parquet files."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import pymysql
from pymysql.constants import FIELD_TYPE

from pyrates_export.db import TRACE_COLUMNS, VALID_GAME_FILTER, traces_view

log = logging.getLogger(__name__)


def export_query() -> str:
    """Query for one segment; parameters are (start inclusive, end exclusive)."""
    return (
        f"SELECT {', '.join(f'`{c}`' for c in TRACE_COLUMNS)} "
        f"FROM {traces_view()} "
        f"WHERE `date` >= %s AND `date` < %s AND {VALID_GAME_FILTER} "
        # Reading in primary-key order replaces the view's own ORDER BY date,
        # which would force the server to sort the whole result before sending
        # the first row.
        f"ORDER BY `id`"
    )


MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 30

_INT_TYPES = {
    FIELD_TYPE.TINY,
    FIELD_TYPE.SHORT,
    FIELD_TYPE.LONG,
    FIELD_TYPE.LONGLONG,
    FIELD_TYPE.INT24,
    FIELD_TYPE.YEAR,
}
_FLOAT_TYPES = {FIELD_TYPE.FLOAT, FIELD_TYPE.DOUBLE}
_DECIMAL_TYPES = {FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL}
_DATETIME_TYPES = {FIELD_TYPE.DATETIME, FIELD_TYPE.TIMESTAMP}
_DATE_TYPES = {FIELD_TYPE.DATE, FIELD_TYPE.NEWDATE}


@dataclass(frozen=True)
class Segment:
    """Part of the export range that falls into one calendar month."""

    label: str  # "YYYY-MM"
    start: date  # inclusive
    end: date  # exclusive


def month_segments(start: date, end_inclusive: date) -> list[Segment]:
    """Split [start, end_inclusive] into one segment per calendar month."""
    if end_inclusive < start:
        raise ValueError(f"End date {end_inclusive} is before start date {start}")
    end = end_inclusive + timedelta(days=1)
    segments = []
    cursor = start
    while cursor < end:
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        seg_end = min(next_month, end)
        segments.append(Segment(f"{cursor:%Y-%m}", cursor, seg_end))
        cursor = seg_end
    return segments


def segment_path(out_dir: Path, segment: Segment) -> Path:
    return out_dir / f"traces_{segment.label}.parquet"


def arrow_schema(description, segment: Segment) -> pa.Schema:
    """Build an explicit Arrow schema from the MySQL cursor description.

    An explicit schema keeps every chunk and every monthly file consistent,
    even when a chunk contains only NULLs for a column.
    """
    fields = []
    for column in description:
        name, type_code, scale = column[0], column[1], column[5]
        if type_code in _INT_TYPES:
            arrow_type = pa.int64()
        elif type_code in _FLOAT_TYPES:
            arrow_type = pa.float64()
        elif type_code in _DECIMAL_TYPES:
            arrow_type = pa.decimal128(38, scale or 0)
        elif type_code in _DATETIME_TYPES:
            arrow_type = pa.timestamp("us")
        elif type_code in _DATE_TYPES:
            arrow_type = pa.date32()
        elif type_code == FIELD_TYPE.TIME:
            # Parquet has no duration type: TIME values are stored as seconds.
            arrow_type = pa.float64()
        elif type_code == FIELD_TYPE.BIT:
            arrow_type = pa.binary()
        else:
            arrow_type = pa.string()
        fields.append(pa.field(name, arrow_type))
    metadata = {
        "range_start": segment.start.isoformat(),
        "range_end": segment.end.isoformat(),
    }
    return pa.schema(fields, metadata=metadata)


def _coerce(value, arrow_type: pa.DataType):
    """Convert the few values the driver returns in a non-Arrow-friendly form."""
    if value is None:
        return None
    if pa.types.is_floating(arrow_type) and isinstance(value, timedelta):
        return value.total_seconds()
    if pa.types.is_string(arrow_type):
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    if pa.types.is_timestamp(arrow_type) and not isinstance(value, datetime):
        return None  # invalid "zero" dates such as 0000-00-00 00:00:00
    return value


def rows_to_table(rows, schema: pa.Schema) -> pa.Table:
    arrays = []
    for field, values in zip(schema, zip(*rows)):
        try:
            array = pa.array(values, type=field.type)
        except (pa.ArrowException, TypeError, ValueError):
            array = pa.array([_coerce(v, field.type) for v in values], type=field.type)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)


def is_done(path: Path, segment: Segment) -> bool:
    """True if a complete file already covers this segment's date range."""
    if not path.exists():
        return False
    try:
        metadata = pq.read_schema(path).metadata or {}
        covered_start = date.fromisoformat(metadata[b"range_start"].decode())
        covered_end = date.fromisoformat(metadata[b"range_end"].decode())
    except Exception as exc:  # unreadable or foreign file: export it again
        log.warning("%s is not a valid export (%s); it will be rewritten", path, exc)
        return False
    return covered_start <= segment.start and covered_end >= segment.end


def _write_segment(conn, segment: Segment, tmp_path: Path, chunk_size: int) -> int:
    row_count = 0
    writer = None
    cursor = conn.cursor()
    try:
        cursor.execute(export_query(), (segment.start, segment.end))
        schema = arrow_schema(cursor.description, segment)
        writer = pq.ParquetWriter(tmp_path, schema, compression="zstd")
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            writer.write_table(rows_to_table(rows, schema))
            row_count += len(rows)
            log.debug("%s: %d rows so far", segment.label, row_count)
    finally:
        if writer is not None:
            writer.close()
    cursor.close()
    return row_count


def export_segment(
    connect_fn: Callable[[], object],
    segment: Segment,
    out_dir: Path,
    chunk_size: int,
    retry_delay: float = RETRY_DELAY_SECONDS,
) -> int:
    """Export one segment to its Parquet file and return the row count.

    Data is written to a temporary file that is renamed only once the whole
    segment has been read, so an interrupted run never leaves a partial file
    that could be mistaken for a finished month.
    """
    path = segment_path(out_dir, segment)
    tmp_path = path.with_name(path.name + ".tmp")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        conn = connect_fn()
        try:
            row_count = _write_segment(conn, segment, tmp_path, chunk_size)
            os.replace(tmp_path, path)
            return row_count
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as exc:
            tmp_path.unlink(missing_ok=True)
            if attempt == MAX_ATTEMPTS:
                raise
            log.warning(
                "%s: attempt %d/%d failed (%s); retrying in %.0f s",
                segment.label, attempt, MAX_ATTEMPTS, exc, retry_delay * attempt,
            )
            time.sleep(retry_delay * attempt)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
    raise AssertionError("unreachable")


def run_export(
    connect_fn: Callable[[], object],
    start: date,
    end_inclusive: date,
    out_dir: Path,
    chunk_size: int,
    force: bool = False,
) -> dict[str, int]:
    """Export every month of the range, skipping the ones already done.

    Returns the row count of each segment exported during this run.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    segments = month_segments(start, end_inclusive)
    log.info(
        "Export range %s to %s (inclusive): %d monthly segment(s) into %s",
        start, end_inclusive, len(segments), out_dir,
    )
    exported: dict[str, int] = {}
    for segment in segments:
        path = segment_path(out_dir, segment)
        if not force and is_done(path, segment):
            rows = pq.read_metadata(path).num_rows
            log.info("%s: already exported (%s rows), skipped", segment.label, f"{rows:,}")
            continue
        log.info("%s: exporting %s <= date < %s", segment.label, segment.start, segment.end)
        started = time.monotonic()
        rows = export_segment(connect_fn, segment, out_dir, chunk_size)
        elapsed = time.monotonic() - started
        size_mb = path.stat().st_size / 1e6
        log.info(
            "%s: %s rows in %.0f s (%.1f MB)", segment.label, f"{rows:,}", elapsed, size_mb
        )
        exported[segment.label] = rows
    log.info(
        "Done: %s rows exported in this run across %d segment(s)",
        f"{sum(exported.values()):,}", len(exported),
    )
    return exported


def _covered_range(path: Path) -> tuple[date, date]:
    metadata = pq.read_schema(path).metadata or {}
    return (
        date.fromisoformat(metadata[b"range_start"].decode()),
        date.fromisoformat(metadata[b"range_end"].decode()),
    )


def merge_segments(traces_dir: Path, out_path: Path | None = None) -> tuple[Path, int]:
    """Concatenate the monthly files into a single Parquet file.

    Files are copied batch by batch in chronological order, so memory use
    stays flat. Returns the output path and its row count. By default the
    file is written next to ``traces_dir`` and named after the covered range.
    """
    files = sorted(traces_dir.glob("traces_*.parquet"))
    if not files:
        raise SystemExit(f"No exported files found in {traces_dir}. Run the export first.")
    ranges = [_covered_range(path) for path in files]
    for (_, previous_end), (next_start, _), path in zip(ranges, ranges[1:], files[1:]):
        if next_start != previous_end:
            log.warning(
                "Gap before %s: previous file ends at %s, this one starts at %s",
                path.name, previous_end, next_start,
            )
    start, end = ranges[0][0], ranges[-1][1]
    if out_path is None:
        last_day = end - timedelta(days=1)
        out_path = traces_dir.parent / f"traces_{start}_{last_day}.parquet"
    schema = pq.read_schema(files[0]).with_metadata(
        {
            "range_start": start.isoformat(),
            "range_end": end.isoformat(),
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    row_count = 0
    try:
        with pq.ParquetWriter(tmp_path, schema, compression="zstd") as writer:
            for path in files:
                for batch in pq.ParquetFile(path).iter_batches():
                    table = pa.Table.from_batches([batch]).cast(schema)
                    writer.write_table(table)
                    row_count += table.num_rows
                log.info("Merged %s", path.name)
        os.replace(tmp_path, out_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    log.info(
        "Single file %s: %s rows (%.1f MB)",
        out_path, f"{row_count:,}", out_path.stat().st_size / 1e6,
    )
    return out_path, row_count
