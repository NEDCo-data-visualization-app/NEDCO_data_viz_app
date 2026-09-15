"""DuckDB access layer.

All dashboard queries go through this class. A single connection is shared and
guarded by a lock so the threaded development server cannot interleave calls
on it; long streaming reads use a dedicated cursor instead.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

import duckdb
import requests

from .metrics import Metrics

logger = logging.getLogger("volta")

Row = Dict[str, Any]

# Column names used by older exports of the same dataset (e.g. wkfile_shiny.csv),
# mapped to the names the dashboard expects. Matching is case-insensitive.
COLUMN_ALIASES = {
    "chargedate": "od_date",
    "charge_date": "od_date",
    "date": "od_date",
    "loc": "utility",
    "location": "utility",
    "res": "tariff_type",
    "res_mapped": "tariff_type",
    "kwh": "ocd_energy",
    "energy": "ocd_energy",
    "ghc": "ocd_cash_received",
    "cash": "ocd_cash_received",
    "cash_received": "ocd_cash_received",
    "paymoney": "ocd_paymoney",
    "pay_money": "ocd_paymoney",
}


CHUNK_BYTES = 48 * 1024 * 1024  # size of the slices a CSV upload is loaded in


def split_csv(path: Path, chunk_bytes: int = CHUNK_BYTES) -> List[Path]:
    """Split a CSV into slices of about ``chunk_bytes``, each with the header.

    Slices only start at a line boundary that is outside a quoted field, so a
    value containing a newline is never cut in half. A file smaller than one
    slice is used as is.
    """
    path = Path(path)
    if path.stat().st_size <= chunk_bytes:
        return [path]
    parts: List[Path] = []
    stem = f"{path.stem}.{os.getpid()}-{uuid.uuid4().hex[:8]}"
    with open(path, "rb") as src:
        header = src.readline()
        out = None
        written = 0
        in_quotes = False
        try:
            for line in src:
                if out is None or (written >= chunk_bytes and not in_quotes):
                    if out is not None:
                        out.close()
                    out = open(path.with_name(f"{stem}.part{len(parts):04d}{path.suffix}"), "wb")
                    parts.append(Path(out.name))
                    out.write(header)
                    written = 0
                out.write(line)
                written += len(line)
                if line.count(b'"') % 2:
                    in_quotes = not in_quotes
        finally:
            if out is not None:
                out.close()
    return parts


def _sql_literal(value: str) -> str:
    """Quote a Python string as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


class DataStore:
    def __init__(self, config: Mapping[str, Any], metrics: Metrics):
        self.config = config
        self.metrics = metrics
        self.table: str = config.get("PARQUET_PATH") or "merged_sales_customers_clean"
        self.date_col: str = config.get("DATE_COL", "od_date")
        self.date_fmt: str = config.get("DATE_FMT", "%d-%b-%y")

        self.db_path = Path(config["DB_PATH"])
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._con = duckdb.connect(database=str(self.db_path), read_only=False)
        self._apply_resource_limits()
        self._columns: Optional[List[str]] = None
        self._extent: Optional[Dict[str, Any]] = None
        logger.info("DuckDB opened at %s (table %s)", self.db_path, self.table)

    def _apply_resource_limits(self) -> None:
        """Keep DuckDB inside the container's memory: spill to disk instead of getting killed.

        DUCKDB_MEMORY_LIMIT (e.g. "256MB") caps DuckDB's buffer pool; large
        ingests and sorts then spill to a temp directory next to the database
        file, which sits on the persistent disk in hosted deployments.
        DUCKDB_THREADS bounds parallelism, which also bounds memory.
        """
        limit = self.config.get("DUCKDB_MEMORY_LIMIT")
        threads = self.config.get("DUCKDB_THREADS")
        temp_dir = self.db_path.parent / ".duckdb_tmp"
        try:
            temp_dir.mkdir(parents=True, exist_ok=True)
            self._con.execute(f"SET temp_directory = '{str(temp_dir).replace(chr(39), chr(39) * 2)}'")
            if limit:
                self._con.execute(f"SET memory_limit = '{str(limit).replace(chr(39), chr(39) * 2)}'")
            if threads:
                self._con.execute(f"SET threads = {int(threads)}")
            logger.info("DuckDB limits: memory=%s threads=%s temp=%s", limit or "default", threads or "default", temp_dir)
        except Exception as exc:  # noqa: BLE001 - limits are best effort
            logger.warning("Could not apply DuckDB resource limits: %s", exc)

    # ------------------------------------------------------------------ basics
    @property
    def table_sql(self) -> str:
        """Quoted table identifier for use in SQL."""
        return f'"{self.table}"'

    def table_exists(self) -> bool:
        with self._lock:
            return self._table_exists(self._con)

    def _table_exists(self, con) -> bool:
        row = con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = ?", [self.table]).fetchone()
        return row is not None

    def health(self) -> Dict[str, Any]:
        """Liveness information that never waits on a running query or ingest."""
        info: Dict[str, Any] = {"ok": True, "table": self.table, "busy": False, "rows": None, "cols": None}
        if not self._lock.acquire(blocking=False):
            info["busy"] = True  # an ingest or a long query holds the connection; the process is alive
            return info
        try:
            if self._table_exists(self._con):
                extent = self.data_extent()
                info["rows"] = extent.get("rows")
                info["cols"] = len(self.get_columns())
            else:
                info["rows"], info["cols"] = 0, 0
        finally:
            self._lock.release()
        return info

    def invalidate(self) -> None:
        self._columns = None
        self._extent = None

    def reopen(self) -> None:
        """Close and reopen the DuckDB connection.

        After a large load the connection keeps a buffer pool's worth of the
        freshly written blocks (about the memory limit plus metadata); on a
        small hosted instance that headroom is needed for the next page
        view. Reopening releases it.
        """
        with self._lock:
            try:
                self._con.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Closing DuckDB before reopening failed: %s", exc)
            self._con = duckdb.connect(database=str(self.db_path), read_only=False)
            self._apply_resource_limits()
            self.invalidate()

    def data_extent(self) -> Dict[str, Any]:
        """Row count and date span of the dataset, cached until the data changes."""
        if self._extent is None:
            extent: Dict[str, Any] = {"rows": 0, "date_min": None, "date_max": None, "date_max_label": ""}
            if self.date_col in self.get_columns():
                row = self.fetch_one(
                    f"SELECT COUNT(*) AS n, MIN({self.date_col}) AS dmin, MAX({self.date_col}) AS dmax FROM {self.table_sql}"
                )
                if row and row.get("n"):
                    extent.update(rows=int(row["n"]), date_min=row["dmin"], date_max=row["dmax"])
                    try:
                        extent["date_max_label"] = row["dmax"].strftime("%b %Y")
                    except AttributeError:
                        extent["date_max_label"] = str(row["dmax"])
            self._extent = extent
        return dict(self._extent)

    def get_columns(self) -> List[str]:
        if self._columns is None:
            if not self.table_exists():
                return []
            with self._lock:
                rows = self._con.execute(f"DESCRIBE {self.table_sql}").fetchall()
            self._columns = [r[0] for r in rows]
        return list(self._columns)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        with self._lock:
            self._con.execute(sql, list(params or []))

    def fetch_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Row]:
        rows = self.run_query(sql, params)
        return rows[0] if rows else None

    def run_query(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        fetch_all: bool = True,
    ) -> Union[List[Row], Iterator[Row]]:
        """Run a query and return rows as dicts (a list, or an iterator when fetch_all=False)."""
        try:
            with self._lock:
                cur = self._con.execute(sql, list(params or []))
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 - endpoints expect an empty result, not a 500
            logger.error("DuckDB query failed: %s\nSQL: %s", exc, sql)
            rows = []
        return rows if fetch_all else iter(rows)

    def stream_query(
        self, sql: str, params: Optional[Sequence[Any]] = None, chunk_size: int = 5000
    ) -> Iterator[Tuple[List[str], List[tuple]]]:
        """Yield (columns, chunk_of_tuples) using a dedicated cursor, for large exports."""
        with self._lock:
            cur = self._con.cursor()
        try:
            cur.execute(sql, list(params or []))
            cols = [c[0] for c in cur.description]
            first = True
            while True:
                chunk = cur.fetchmany(chunk_size)
                if not chunk and not first:
                    break
                yield cols, chunk  # an empty first chunk still carries the header
                first = False
                if not chunk:
                    break
        finally:
            cur.close()

    # ------------------------------------------------------------ aggregates
    def compute_stats(self, where_clause: str = "", sql_params: Optional[Sequence[Any]] = None) -> Dict[str, Dict[str, Union[float, str]]]:
        """Sum / mean / approximate median / min / max for every configured metric present in the table."""
        columns = set(self.get_columns())
        metrics = [m for m in self.metrics.keys() if m in columns]
        if not metrics:
            return {}

        parts = []
        for m in metrics:
            # approx_quantile (t-digest) runs in constant memory; an exact median
            # sorts every row and was the largest memory spike of a page view.
            parts.append(
                f"SUM({m}) AS sum_{m}, AVG({m}) AS avg_{m}, MIN({m}) AS min_{m}, MAX({m}) AS max_{m}, "
                f"approx_quantile({m}, 0.5) AS median_{m}"
            )
        sql = f"SELECT {', '.join(parts)} FROM {self.table_sql}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        row = self.fetch_one(sql, sql_params)
        if not row:
            return {}
        return {
            m: {
                "label": self.metrics.label(m),
                "sum": float(row.get(f"sum_{m}") or 0),
                "mean": float(row.get(f"avg_{m}") or 0),
                "median": float(row.get(f"median_{m}") or 0),
                "min": float(row.get(f"min_{m}") or 0),
                "max": float(row.get(f"max_{m}") or 0),
            }
            for m in metrics
        }

    def compute_summary(self, where_clause: str = "", sql_params: Optional[Sequence[Any]] = None) -> Dict[str, Union[int, str]]:
        empty = {"rows": 0, "cols": 0, "meters": 0, "locations": 0, "date_min": "", "date_max": ""}
        columns = self.get_columns()
        if not columns:
            return empty

        meters = "COUNT(DISTINCT meterid)" if "meterid" in columns else "0"
        locations = "COUNT(DISTINCT utility)" if "utility" in columns else "0"
        sql = f"""
            SELECT COUNT(*) AS n_rows, {meters} AS meters, {locations} AS locations,
                   MIN({self.date_col}) AS date_min, MAX({self.date_col}) AS date_max
            FROM {self.table_sql}
        """
        if where_clause:
            sql += f" WHERE {where_clause}"

        row = self.fetch_one(sql, sql_params)
        if not row:
            return empty
        return {
            "rows": int(row.get("n_rows") or 0),
            "cols": len(columns),
            "meters": int(row.get("meters") or 0),
            "locations": int(row.get("locations") or 0),
            "date_min": str(row.get("date_min") or ""),
            "date_max": str(row.get("date_max") or ""),
        }

    # ------------------------------------------------------------- ingestion
    def ingest_csv(self, path: Union[str, Path], replace: bool = False) -> int:
        """Load the rows of a CSV file into the dataset table and return how many were added.

        Columns are matched by name (case-insensitive) against the existing
        table and cast to its types; the date column also accepts DATE_FMT.
        Rows already present are skipped. If the table does not exist yet, or
        ``replace`` is set, the table is (re)created from the file, so new
        columns such as customer attributes come through.

        The file is loaded in slices of about CHUNK_BYTES each so that memory
        stays bounded whatever the file size (a 500 MB export on a 512 MB
        instance must not get the service killed). A replace builds a new
        table beside the old one and swaps them at the end, so readers keep
        the old data until the whole file is in, and a failed load leaves the
        dataset untouched.
        """
        path = Path(path)
        date_col = self.date_col.lower()
        # DDL statements cannot be prepared in DuckDB, so literals are inlined (escaped).
        raw_typed = f"read_csv_auto({_sql_literal(str(path))}, header=true)"
        raw = f"read_csv_auto({_sql_literal(str(path))}, header=true, all_varchar=true)"

        # A dedicated cursor (its own DuckDB connection) so that dashboard reads and
        # the /health probe keep working while a large file is being loaded.
        with self._lock:
            cur = self._con.cursor()
        chunks: List[Path] = []
        try:
            # Map canonical (lower-case) column name -> column name as it appears in the file.
            # A column already using the canonical name wins over an alias for it.
            raw_cols: Dict[str, str] = {}
            aliased: Dict[str, str] = {}
            for (name, *_rest) in cur.execute(f"DESCRIBE SELECT * FROM {raw}").fetchall():
                lower = name.lower()
                if lower in COLUMN_ALIASES:
                    aliased.setdefault(COLUMN_ALIASES[lower], name)
                else:
                    raw_cols[lower] = name
            for canonical, original in aliased.items():
                raw_cols.setdefault(canonical, original)
            exists = self._table_exists(cur) and not replace

            if exists:
                schema = cur.execute(f"DESCRIBE {self.table_sql}").fetchall()
                target = [(name, typ) for name, typ, *_ in schema if name.lower() in raw_cols]
            else:
                inferred = cur.execute(f"DESCRIBE SELECT * FROM {raw_typed}").fetchall()
                by_original = {orig: canon for canon, orig in raw_cols.items()}
                target = []
                for name, typ, *_ in inferred:
                    canonical = by_original.get(name)
                    if canonical is None:
                        continue  # an alias shadowed by a canonical column
                    target.append((canonical, "DATE" if canonical == date_col else typ))

            if not target:
                raise ValueError("The CSV has no columns in common with the dataset.")
            if date_col not in [n.lower() for n, _ in target]:
                raise ValueError(f"The CSV must contain a '{self.date_col}' column.")

            exprs = []
            for name, typ in target:
                src = f'"{raw_cols[name.lower()]}"'
                if name.lower() == date_col:
                    exprs.append(
                        f"COALESCE(TRY_CAST({src} AS DATE), "
                        f"TRY_CAST(try_strptime({src}, {_sql_literal(self.date_fmt)}) AS DATE)) AS \"{name}\""
                    )
                elif typ.upper().startswith("VARCHAR"):
                    exprs.append(f'{src} AS "{name}"')
                else:
                    exprs.append(f'TRY_CAST({src} AS {typ}) AS "{name}"')
            cols_sql = ", ".join(f'"{n}"' for n, _ in target)
            date_sql = f'"{next(n for n, _ in target if n.lower() == date_col)}"'

            def staged(chunk: Path) -> str:
                source = f"read_csv_auto({_sql_literal(str(chunk))}, header=true, all_varchar=true)"
                return f'SELECT * FROM (SELECT {", ".join(exprs)} FROM {source}) s WHERE s.{date_sql} IS NOT NULL'

            chunks = split_csv(path, CHUNK_BYTES)
            added = 0
            if exists:
                for chunk in chunks:
                    # Rows already present are skipped. Only the part of the table that
                    # overlaps the slice's date range can hold duplicates, which keeps
                    # the comparison small for a monthly update of a multi-year table.
                    sql = (
                        f"INSERT INTO {self.table_sql} ({cols_sql}) "
                        f"WITH incoming AS ({staged(chunk)}) "
                        f"SELECT * FROM incoming EXCEPT SELECT {cols_sql} FROM {self.table_sql} "
                        f"WHERE {date_sql} BETWEEN (SELECT MIN({date_sql}) FROM incoming) "
                        f"AND (SELECT MAX({date_sql}) FROM incoming)"
                    )
                    result = cur.execute(sql).fetchone()
                    added += int(result[0]) if result else 0
                    if chunk != path:
                        chunk.unlink(missing_ok=True)
            else:
                incoming = f'"{self.table}__incoming"'
                cur.execute(f"DROP TABLE IF EXISTS {incoming}")
                try:
                    cur.execute(f"CREATE TABLE {incoming} AS {staged(chunks[0])}")
                    if chunks[0] != path:
                        chunks[0].unlink(missing_ok=True)
                    for chunk in chunks[1:]:
                        cur.execute(f"INSERT INTO {incoming} ({cols_sql}) {staged(chunk)}")
                        if chunk != path:
                            chunk.unlink(missing_ok=True)
                    result = cur.execute(f"SELECT COUNT(*) FROM {incoming}").fetchone()
                    added = int(result[0]) if result else 0
                except Exception:
                    cur.execute(f"DROP TABLE IF EXISTS {incoming}")  # the current dataset stays as it was
                    raise
                # Swap the finished table in under the global lock so no reader
                # sees the dataset half-way through.
                with self._lock:
                    cur.execute("BEGIN")
                    cur.execute(f"DROP TABLE IF EXISTS {self.table_sql}")
                    cur.execute(f"ALTER TABLE {incoming} RENAME TO {self.table_sql}")
                    cur.execute("COMMIT")
                    self.invalidate()
            try:
                cur.execute("CHECKPOINT")  # flush the WAL so a restart does not replay a huge load
            except Exception as exc:  # noqa: BLE001 - only possible while other transactions run
                logger.info("Checkpoint deferred: %s", exc)
        finally:
            for chunk in chunks:
                if chunk != path:  # slices are ours to delete; the upload itself belongs to the caller
                    chunk.unlink(missing_ok=True)
            cur.close()

        self.reopen()  # release the buffers held for the load
        logger.info("Ingested %s rows from %s", added, path)
        return added

    def replace_from_parquet(self, path: Union[str, Path]) -> int:
        """Replace the dataset table with the contents of a parquet file."""
        with self._lock:
            self._con.execute(
                f"CREATE OR REPLACE TABLE {self.table_sql} AS SELECT * FROM read_parquet({_sql_literal(str(path))})"
            )
            row = self._con.execute(f"SELECT COUNT(*) FROM {self.table_sql}").fetchone()
        self.invalidate()
        return int(row[0]) if row else 0

    def try_internet_connection(self) -> Tuple[bool, str]:
        """Best-effort refresh of the dataset from BUCKET_URL.

        Returns (ok, human readable message). Without BUCKET_URL only a
        connectivity check is performed.
        """
        url = self.config.get("BUCKET_URL")
        key = self.config.get("SUPABASE_KEY")

        if not url:
            try:
                requests.head("https://www.google.com", timeout=5)
            except requests.RequestException:
                return False, "No internet connection detected. You can upload a CSV instead."
            return True, "Internet is available, but no remote data source (BUCKET_URL) is configured."

        headers = {"apikey": key, "Authorization": f"Bearer {key}"} if key else {}
        try:
            with requests.get(url, headers=headers, timeout=120, stream=True) as resp:
                resp.raise_for_status()
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        tmp.write(chunk)
                    tmp_path = Path(tmp.name)
        except requests.RequestException as exc:
            return False, f"Could not reach the remote data source: {exc}"

        try:
            rows = self.replace_from_parquet(tmp_path)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Remote dataset could not be loaded")
            return False, f"Downloaded file could not be loaded: {exc}"
        finally:
            tmp_path.unlink(missing_ok=True)
        return True, f"Dataset refreshed from the remote source ({rows:,} rows)."


__all__ = ["DataStore"]
