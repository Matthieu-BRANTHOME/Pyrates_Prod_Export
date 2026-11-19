"""Tests of the export logic against a fake database connection."""

from datetime import date, datetime, timedelta

import pyarrow.parquet as pq
import pymysql
import pytest
from pymysql.constants import FIELD_TYPE

from pyrates_export.analysis import level_event_combinations
from pyrates_export.db import TRACE_COLUMNS
from pyrates_export.export import (
    Segment,
    export_segment,
    month_segments,
    run_export,
    segment_path,
)

@pytest.fixture(autouse=True)
def _view_name(monkeypatch):
    monkeypatch.setenv("MYSQL_TRACES_VIEW", "Traces_View")


TYPES = {
    "id": FIELD_TYPE.LONGLONG,
    "date": FIELD_TYPE.DATETIME,
    "level_id": FIELD_TYPE.LONG,
    "game_time": FIELD_TYPE.TIME,
    "progression": FIELD_TYPE.FLOAT,
    "duration": FIELD_TYPE.LONG,
    "code": FIELD_TYPE.BLOB,
}
DESCRIPTION = tuple(
    (name, TYPES.get(name, FIELD_TYPE.VAR_STRING), None, None, None, 0, True)
    for name in TRACE_COLUMNS
)

CODE = "for i in range(3):\n\twalk()\r\n    # trailing spaces  \n\n"


def make_row(i: int, when: datetime, **overrides):
    row = {
        "id": i,
        "date": when,
        "game_id": f"game-{i % 3}",
        "level_id": 1 + i % 2,
        "game_time": timedelta(seconds=90, milliseconds=500),
        "progression": 0.5,
        "action_name": "LAUNCHED",
        "object_name": "PROGRAM",
        "duration": None,
        "code": CODE,
        "value": None,
        "reason_name": None,
        "error_message": None,
    }
    row.update(overrides)
    return tuple(row[name] for name in TRACE_COLUMNS)


class FakeCursor:
    def __init__(self, rows, fail_after=None):
        self.rows, self.fail_after, self.pos = rows, fail_after, 0
        self.description = None

    def execute(self, query, params):
        start, end = params
        self.description = DESCRIPTION
        self.rows = [r for r in self.rows if start <= r[1].date() < end]

    def fetchmany(self, size):
        if self.fail_after is not None and self.pos >= self.fail_after:
            raise pymysql.err.OperationalError(2013, "Lost connection")
        chunk = self.rows[self.pos : self.pos + size]
        self.pos += len(chunk)
        return chunk

    def close(self):
        pass


class FakeDb:
    def __init__(self, rows, failures=0):
        self.rows, self.failures, self.connections = rows, failures, 0

    def __call__(self):
        self.connections += 1
        fail_after = 2 if self.connections <= self.failures else None
        db = self

        class Conn:
            def cursor(self):
                return FakeCursor(db.rows, fail_after)

            def close(self):
                pass

        return Conn()


def test_month_segments_handles_partial_months_and_year_end():
    segments = month_segments(date(2024, 11, 18), date(2025, 1, 5))
    assert segments == [
        Segment("2024-11", date(2024, 11, 18), date(2024, 12, 1)),
        Segment("2024-12", date(2024, 12, 1), date(2025, 1, 1)),
        Segment("2025-01", date(2025, 1, 1), date(2025, 1, 6)),
    ]


def test_export_preserves_code_and_types(tmp_path):
    rows = [make_row(i, datetime(2025, 3, 1 + i, 12, 0, 0)) for i in range(5)]
    segment = Segment("2025-03", date(2025, 3, 1), date(2025, 4, 1))
    count = export_segment(FakeDb(rows), segment, tmp_path, chunk_size=2)

    table = pq.read_table(segment_path(tmp_path, segment))
    assert count == table.num_rows == 5
    assert table.column_names == list(TRACE_COLUMNS)
    assert table["code"].to_pylist() == [CODE] * 5
    assert table["date"].to_pylist()[0] == datetime(2025, 3, 1, 12, 0, 0)
    assert table["game_time"].to_pylist()[0] == 90.5
    assert table["duration"].to_pylist() == [None] * 5
    assert str(table.schema.field("duration").type) == "int64"
    assert not list(tmp_path.glob("*.tmp"))


def test_run_export_skips_done_months_and_redoes_extended_ones(tmp_path):
    rows = [
        make_row(1, datetime(2025, 1, 10)),
        make_row(2, datetime(2025, 2, 10)),
        make_row(3, datetime(2025, 2, 20)),
    ]
    db = FakeDb(rows)
    first = run_export(db, date(2025, 1, 1), date(2025, 2, 15), tmp_path, 100)
    assert first == {"2025-01": 1, "2025-02": 1}

    # Same range again: nothing to do.
    assert run_export(db, date(2025, 1, 1), date(2025, 2, 15), tmp_path, 100) == {}

    # Longer range: January is kept, the partial February file is rewritten.
    third = run_export(db, date(2025, 1, 1), date(2025, 2, 28), tmp_path, 100)
    assert third == {"2025-02": 2}


def test_empty_month_is_written_and_marked_done(tmp_path):
    db = FakeDb([])
    assert run_export(db, date(2025, 1, 1), date(2025, 1, 31), tmp_path, 100) == {"2025-01": 0}
    assert run_export(db, date(2025, 1, 1), date(2025, 1, 31), tmp_path, 100) == {}


def test_lost_connection_is_retried_without_partial_file(tmp_path):
    rows = [make_row(i, datetime(2025, 3, 1, 0, i)) for i in range(6)]
    segment = Segment("2025-03", date(2025, 3, 1), date(2025, 4, 1))
    db = FakeDb(rows, failures=1)
    assert export_segment(db, segment, tmp_path, chunk_size=2, retry_delay=0) == 6
    assert db.connections == 2
    assert pq.read_metadata(segment_path(tmp_path, segment)).num_rows == 6


def test_persistent_failure_leaves_no_file(tmp_path):
    rows = [make_row(i, datetime(2025, 3, 1, 0, i)) for i in range(6)]
    segment = Segment("2025-03", date(2025, 3, 1), date(2025, 4, 1))
    with pytest.raises(pymysql.err.OperationalError):
        export_segment(FakeDb(rows, failures=99), segment, tmp_path, 2, retry_delay=0)
    assert not list(tmp_path.iterdir())


def test_level_event_combinations(tmp_path):
    rows = [make_row(i, datetime(2025, 1, 1 + i)) for i in range(4)]
    rows.append(make_row(9, datetime(2025, 2, 1), action_name="COMPLETED", object_name="LEVEL"))
    run_export(FakeDb(rows), date(2025, 1, 1), date(2025, 2, 28), tmp_path / "traces", 100)

    out_csv = tmp_path / "out" / "combos.csv"
    headers, result = level_event_combinations(tmp_path / "traces", out_csv)
    by_key = {row[:3]: row[3:5] for row in result}
    assert headers[:5] == ["level_id", "action_name", "object_name", "n_rows", "n_games"]
    assert by_key[(1, "LAUNCHED", "PROGRAM")] == (2, 2)
    assert by_key[(2, "COMPLETED", "LEVEL")] == (1, 1)
    assert out_csv.exists()


def test_merge_segments_produces_one_file_with_all_rows(tmp_path):
    from pyrates_export.export import merge_segments

    rows = [make_row(i, datetime(2025, 1 + i, 10)) for i in range(3)]
    traces = tmp_path / "traces"
    run_export(FakeDb(rows), date(2025, 1, 5), date(2025, 3, 20), traces, 100)

    out_path, count = merge_segments(traces)
    assert out_path == tmp_path / "traces_2025-01-05_2025-03-20.parquet"
    table = pq.read_table(out_path)
    assert count == table.num_rows == 3
    assert table["id"].to_pylist() == [0, 1, 2]
    assert table["code"].to_pylist() == [CODE] * 3
    assert table.schema.metadata[b"range_end"] == b"2025-03-21"
