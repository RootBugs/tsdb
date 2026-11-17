"""TSDB: chunked append-only time-series storage with downsampling.

Public surface::

    db = TSDB("path/to/db", chunk_max_records=8, retention_ms=None)
    sid = db.create_series("cpu", {"host": "a"})
    db.append(sid, ts=1000, value=42.5)
    db.query(sid, start=0, end=2000)                 # raw points
    db.query(sid, 0, 2000, step=500, aggregate="avg")  # downsampled
    db.prune(now=5000)                               # retention

Documented policies (also in README):

* **Query range is half-open** ``[start, end)``: a point at exactly
  ``start`` is included, a point at exactly ``end`` is excluded.
* **Out-of-order appends are accepted.** Records are only appended to
  the active chunk; queries sort by ``(timestamp, append sequence)`` so
  results are always chronological and deterministic.  Late points never
  rewrite sealed chunks.
* **Step buckets** are aligned to the epoch: bucket ``k`` covers
  ``[k*step, (k+1)*step)``.  Empty buckets are omitted from the output;
  each emitted bucket carries ``count`` alongside the chosen aggregate.
* **Retention** deletes a *sealed* chunk only when its newest point is
  strictly older than ``cutoff = now - retention_ms``; the active chunk
  is never deleted, and a chunk straddling the cutoff is kept whole
  (chunks are pruned wholesale, never partially rewritten).
"""
from __future__ import annotations

import re
from pathlib import Path

from .chunk import Chunk, ChunkError, HEADER_SIZE, RECORD_SIZE
from .registry import Registry, RegistryError

AGGREGATES = ("min", "max", "avg", "sum", "count")
CHUNK_PREFIX = "chunk-"
CHUNK_SUFFIX = ".bin"
_SEQ_MOD = 0xFFFFFFFF

_CHUNK_RE = re.compile(r"^chunk-(\d{6})\.bin$")


class TSDBError(Exception):
    """Base class for store errors."""


class UnknownSeries(TSDBError):
    """The requested series id is not registered."""


class BadQuery(TSDBError):
    """Invalid query arguments (bad aggregate, bad range, ...)."""


class TSDB:
    """Append-only chunked time-series store."""

    def __init__(
        self,
        directory: str | Path,
        chunk_max_records: int = 256,
        retention_ms: int | None = None,
    ) -> None:
        if chunk_max_records < 1:
            raise ValueError("chunk_max_records must be >= 1")
        if retention_ms is not None and retention_ms <= 0:
            raise ValueError("retention_ms must be > 0 when given")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.chunk_dir = self.directory / "chunks"
        self.chunk_dir.mkdir(exist_ok=True)
        self.chunk_max_records = chunk_max_records
        self.retention_ms = retention_ms
        self.registry = Registry(self.directory)
        self._seq = 0
        self._chunks: list[Chunk] = []
        self._active: Chunk | None = None
        self._open_chunks()

    # -------------------------------------------------------- lifecycle
    def _open_chunks(self) -> None:
        paths = sorted(
            p for p in self.chunk_dir.glob(f"{CHUNK_PREFIX}*{CHUNK_SUFFIX}")
            if _CHUNK_RE.match(p.name)
        )
        for path in paths:
            try:
                chunk = Chunk(path)
            except ChunkError:
                continue  # corrupt/unrelated files are skipped, not fatal
            self._chunks.append(chunk)
        if self._chunks and self._chunks[-1].count < self.chunk_max_records:
            self._active = self._chunks[-1]
        else:
            self._active = None
        # seq == number of appends so far (u32); ties break by append order
        self._seq = sum(chunk.count for chunk in self._chunks)

    def _new_chunk(self) -> Chunk:
        seq = len(self._chunks) + 1
        path = self.chunk_dir / f"{CHUNK_PREFIX}{seq:06d}{CHUNK_SUFFIX}"
        while path.exists():
            seq += 1
            path = self.chunk_dir / f"{CHUNK_PREFIX}{seq:06d}{CHUNK_SUFFIX}"
        chunk = Chunk(path, create=True)
        self._chunks.append(chunk)
        self._active = chunk
        return chunk

    def close(self) -> None:
        for chunk in self._chunks:
            chunk.close()
        self._chunks.clear()
        self._active = None

    def __enter__(self) -> "TSDB":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---------------------------------------------------------- series
    def create_series(self, metric: str, tags: dict[str, str] | None = None) -> int:
        """Register (metric, tags) and return its stable id."""
        return self.registry.create(metric, tags)

    def find_series(self, metric: str, tags: dict[str, str] | None = None) -> int | None:
        return self.registry.find(metric, tags)

    def series_info(self, series_id: int) -> tuple[str, dict[str, str]]:
        try:
            return self.registry.info(series_id)
        except RegistryError as exc:
            raise UnknownSeries(str(exc)) from None

    def list_series(self) -> list[tuple[int, str, dict[str, str]]]:
        return self.registry.all()

    # ----------------------------------------------------------- append
    def append(self, series_id: int, ts: int, value: float) -> None:
        """Append one point; out-of-order timestamps are accepted."""
        if series_id not in self.registry:
            raise UnknownSeries(f"unknown series id {series_id}")
        if not isinstance(ts, int) or isinstance(ts, bool):
            raise TSDBError(f"timestamp must be an int, got {type(ts).__name__}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TSDBError(
                f"value must be numeric, got {type(value).__name__}"
            )
        if self._active is None or self._active.count >= self.chunk_max_records:
            self._new_chunk()
        self._seq += 1
        self._active.append(series_id, ts, float(value), self._seq)

    def append_many(self, series_id: int, points: list[tuple[int, float]]) -> None:
        for ts, value in points:
            self.append(series_id, ts, value)

    # ----------------------------------------------------------- query
    def query(
        self,
        series_id: int,
        start: int | None = None,
        end: int | None = None,
        step: int | None = None,
        aggregate: str = "avg",
    ) -> list[tuple[int, float]]:
        """Query one series over ``[start, end)``.

        ``step=None`` returns raw ``(ts, value)`` points sorted by time.
        With ``step`` the output is downsampled: ``(bucket_start, value)``
        where ``value`` is ``aggregate`` over the bucket's points, plus a
        parallel ``count`` available via :meth:`query_buckets`.
        """
        return [(ts, val) for ts, val, _n in self.query_buckets(
            series_id, start, end, step, aggregate
        )]

    def query_buckets(
        self,
        series_id: int,
        start: int | None = None,
        end: int | None = None,
        step: int | None = None,
        aggregate: str = "avg",
    ) -> list[tuple[int, float, int]]:
        """Like :meth:`query` but each row is ``(bucket, value, count)``.

        Raw mode returns one row per point with ``count == 1``.
        """
        if series_id not in self.registry:
            raise UnknownSeries(f"unknown series id {series_id}")
        if end is not None and start is not None and end < start:
            raise BadQuery(f"end ({end}) must be >= start ({start})")
        if step is not None:
            if not isinstance(step, int) or isinstance(step, bool) or step <= 0:
                raise BadQuery("step must be a positive integer")
            if aggregate not in AGGREGATES:
                raise BadQuery(
                    f"unknown aggregate {aggregate!r}; choose one of {AGGREGATES}"
                )

        points: list[tuple[int, float, int]] = []
        for chunk in self._chunks:
            if not chunk.overlaps(start, end):
                continue  # min/max index prunes non-intersecting chunks
            for _sid, ts, value, seq in chunk.read_range(start, end, series_id):
                points.append((ts, value, seq))
        points.sort(key=lambda p: (p[0], p[2]))

        if step is None:
            return [(ts, value, 1) for ts, value, _seq in points]

        buckets: dict[int, list[float]] = {}
        order: list[int] = []
        for ts, value, _seq in points:
            key = (ts // step) * step  # epoch-aligned, works for negatives
            if key not in buckets:
                buckets[key] = []
                order.append(key)
            buckets[key].append(value)
        rows = []
        for key in sorted(order):
            values = buckets[key]
            rows.append((key, _reduce(aggregate, values), len(values)))
        return rows

    # -------------------------------------------------------- retention
    def set_retention(self, retention_ms: int | None) -> None:
        if retention_ms is not None and retention_ms <= 0:
            raise ValueError("retention_ms must be > 0 when given")
        self.retention_ms = retention_ms

    def prune(self, now: int | None = None) -> list[str]:
        """Delete sealed chunks entirely older than the retention cutoff.

        Returns the deleted file names.  Requires ``retention_ms`` to be
        set; the active (still-being-written) chunk is never touched.
        """
        if self.retention_ms is None:
            raise TSDBError("no retention policy configured")
        if now is None:
            raise TSDBError("prune requires an explicit 'now' timestamp")
        cutoff = now - self.retention_ms
        deleted: list[str] = []
        for chunk in list(self._chunks):
            if chunk is self._active:
                continue
            if chunk.max_ts < cutoff:  # strictly older than the cutoff
                name = chunk.path.name
                chunk.close()
                chunk.path.unlink()
                self._chunks.remove(chunk)
                deleted.append(name)
        return deleted

    # ------------------------------------------------------------ stats
    def stats(self) -> dict:
        """Store statistics: series, chunks, records, index bounds."""
        chunk_rows = []
        for chunk in self._chunks:
            chunk_rows.append(
                {
                    "file": chunk.path.name,
                    "records": chunk.count,
                    "min_ts": chunk.min_ts if chunk.count else None,
                    "max_ts": chunk.max_ts if chunk.count else None,
                    "bytes": chunk.size_bytes(),
                    "active": chunk is self._active,
                }
            )
        total = sum(c["records"] for c in chunk_rows)
        return {
            "series": len(self.registry),
            "chunks": len(chunk_rows),
            "records": total,
            "chunk_max_records": self.chunk_max_records,
            "retention_ms": self.retention_ms,
            "header_bytes": HEADER_SIZE,
            "record_bytes": RECORD_SIZE,
            "chunk_files": chunk_rows,
        }


def _reduce(aggregate: str, values: list[float]) -> float:
    if aggregate == "min":
        return min(values)
    if aggregate == "max":
        return max(values)
    if aggregate == "sum":
        return sum(values)
    if aggregate == "count":
        return float(len(values))
    total = sum(values)
    return total / len(values)
