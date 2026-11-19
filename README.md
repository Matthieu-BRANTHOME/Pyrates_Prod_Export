# Pyrates production trace export

Exports Pyrates interaction traces from the production MySQL database to
monthly Parquet files, for later analysis in Python.

- Source: a view that joins the traces with their action, object and reason
  names. Server, database and view names are settings, not part of the code.
- Output: one Parquet file per calendar month, `data/traces/traces_YYYY-MM.parquet`.
- Rows whose `game_id` is NULL or empty are excluded.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Then edit `.env` and fill in the server (`MYSQL_HOST`), the database
(`MYSQL_DATABASE`), the credentials (`MYSQL_USER`, `MYSQL_PASSWORD`) and the
view name (`MYSQL_TRACES_VIEW`, case-sensitive). The `.env` file is
git-ignored; none of these values must be written anywhere else.

Remote access to the database requires your public IP address to be
whitelisted on the hosting side. A connection error such as
`Access denied` or a timeout usually means the IP is not whitelisted.

## Usage

### 1. Check the connection

```bash
pyrates-export inspect
```

Prints the server version, the column types of the view, the view definition
and the query plan of a one-day export query. Nothing is exported.

### 2. Export the traces

```bash
pyrates-export export
```

The date range comes from `EXPORT_START` / `EXPORT_END` in `.env` (both
inclusive) and can be overridden. Both dates must be in the past: the current
day is refused because its traces are still being written.

```bash
pyrates-export export --start 2025-01-01 --end 2025-03-31
```

To try the pipeline on a small amount of data first, export a single day into
a separate directory:

```bash
pyrates-export export --start 2025-01-15 --end 2025-01-15 --out-dir data/smoke
```

How it works:

- The range is split by calendar month and each month is one query, read
  through an unbuffered (streamed) cursor in chunks of `CHUNK_SIZE` rows, so
  memory use stays flat whatever the size of the month.
- Each month is written to a `.tmp` file that is renamed only when the month
  is complete. An interrupted run never leaves a partial file behind.
- The export is resumable: a month whose file already covers the requested
  dates is skipped. A file that covers only part of the month (for example
  the last month of an earlier run) is exported again when the range is
  extended. Use `--force` to re-export everything.
- A lost connection is retried up to three times per month.
- Row counts per month are logged to the console and to `logs/export.log`.

Notes on the files:

- Column types follow the MySQL types. `TIME` columns, if any, are stored as
  seconds (float), because Parquet has no duration type.
- Text columns are stored as-is. In particular `code` keeps its newlines,
  tabs and trailing whitespace exactly.
- Row order inside a file is not guaranteed; sort by `date` and `id` when
  the order matters.
- Each file records the date range it covers in its Parquet metadata
  (`range_start` inclusive, `range_end` exclusive).

### 3. Get a single file

```bash
pyrates-export merge
```

Concatenates the monthly files, in chronological order, into one Parquet file
named after the covered range, for example
`data/traces_2024-11-18_2026-11-18.parquet`. The copy is streamed, so memory
use stays flat. The monthly files are kept: they are what makes the export
resumable, and they can be deleted by hand once the single file exists.

`pyrates-export export --merge` runs the export and the merge in one go.

### 4. List the level event combinations

```bash
pyrates-export combinations
```

Reads the exported files (not the database) and lists every distinct
`(level_id, action_name, object_name)` combination with its number of rows,
number of distinct games and first/last date seen. The table is printed and
written to `data/analysis/level_event_combinations.csv`.

This is the basis for choosing the event that reliably marks the completion
of a level. Selecting the games that passed level 1 is a separate step that
is not implemented yet.

## About the Parquet format

Parquet is an open file format (an Apache project) for storing tables of
data. It plays the same role as CSV, but it is binary and designed for
analysis. pandas, DuckDB, R, Spark and most data tools read it natively.

How it differs from CSV:

- **Typed**: each column has a type stored in the file (integer, date,
  text...). When read back, a date is a date and an empty value is a real
  NULL; nothing has to be re-parsed.
- **Columnar**: data is stored column by column rather than row by row. A tool
  can read only `game_id` and `level_id` without loading the `code` column,
  which is much faster on tens of millions of rows.
- **Compressed**: a Parquet file is typically 5 to 10 times smaller than the
  equivalent CSV.
- **No text ambiguity**: there is no separator or quoting to escape, so the
  newlines in the `code` column are preserved exactly.

The trade-off is that a Parquet file cannot be opened in a text editor or
directly in Excel. To get a CSV for another tool, convert it with pandas
(install pandas first with `pip install pandas`):

```python
import pandas as pd

traces = pd.read_parquet("data/traces/traces_2025-01.parquet")
traces.to_csv("traces_2025-01.csv", index=False)
```

## Reading the data

The examples below need pandas (`pip install pandas`) or DuckDB (already
installed with this project).

```python
import pandas as pd

traces = pd.read_parquet("data/traces/traces_2025-01.parquet")
```

```python
import duckdb

duckdb.sql("SELECT COUNT(*) FROM 'data/traces/traces_*.parquet'")
```

## Tests

```bash
pytest
```

The tests use a fake database connection and need no credentials.

## Data and repository

`data/`, `logs/`, `.env` and all `*.parquet` / `*.csv` files are git-ignored.
Data files must not be committed (size limit and privacy).
