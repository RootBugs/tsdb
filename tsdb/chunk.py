"""Chunk files: fixed-record append-only segments with a min/max index.

File layout (all little-endian; documented verbatim in README)::

    header, 32 bytes:
        0   4s   magic  b"TSC1"
        4   I    version (1)
        8   Q    record_count
        16  q    min_ts   (i64)
        24  q    max_ts   (i64)
    records, record_count x 24 bytes:
        I   series_id  (u32)
        q   timestamp  (i64, caller-defined units — README uses ms)
        d   value      (f64)
        I   seq        (u32 append counter, breaks timestamp ties)

The header is rewritten in place on every append so the on-disk
``min_ts``/``max_ts`` index is always current; record bytes are only
ever appended.  At open time the store reads just the 32-byte header of
each chunk to build the in-memory index — queries open only chunks whose
``[min_ts, max_ts]`` intersects the query range.
"""
from __future__ import annotations

import struct
from pathlib import Path

MAGIC = b"TSC1"
VERSION = 1
# header: magic(4) version(4) count(8) min(8) max(8) = 32 bytes
HEADER = struct.Struct("<4sI Q q q")
RECORD = struct.Struct("<I q d I")  # series, ts, value, seq = 24 bytes
HEADER_SIZE = HEADER.size  # 32
RECORD_SIZE = RECORD.size  # 24

assert HEADER_SIZE == 32, HEADER_SIZE
assert RECORD_SIZE == 24, RECORD_SIZE


class ChunkError(Exception):
    """Corrupt or malformed chunk file."""


class Chunk:
    """A single chunk file: append records, read back by timestamp range."""

    def __init__(self, path: str | Path, create: bool = False) -> None:
        self.path = Path(path)
        self.count = 0
        self.min_ts = 0
        self.max_ts = 0
        self._fh = None
        if create and not self.path.exists():
            self._fh = open(self.path, "w+b")
            self._write_header()
        elif self.path.is_file():
            self._fh = open(self.path, "r+b")
            self._read_header()
        else:
            raise ChunkError(f"chunk not found: {self.path}")

    # ---------------------------------------------------------- header
    def _write_header(self) -> None:
        self._fh.seek(0)
        self._fh.write(
            HEADER.pack(MAGIC, VERSION, self.count, self.min_ts, self.max_ts)
        )
        self._fh.flush()

    def _read_header(self) -> None:
        self._fh.seek(0)
        raw = self._fh.read(HEADER_SIZE)
        if len(raw) < HEADER_SIZE:
            raise ChunkError(f"{self.path.name}: header truncated")
        magic, version, count, min_ts, max_ts = HEADER.unpack(raw)
        if magic != MAGIC:
            raise ChunkError(f"{self.path.name}: bad magic {magic!r}")
        if version != VERSION:
            raise ChunkError(f"{self.path.name}: unsupported version {version}")
        size = self.path.stat().st_size
        expected = HEADER_SIZE + count * RECORD_SIZE
        if size < expected:
            raise ChunkError(
                f"{self.path.name}: truncated ({size} < {expected} bytes)"
            )
        self.count = count
        self.min_ts = min_ts
        self.max_ts = max_ts

    # ----------------------------------------------------------- write
    def append(self, series_id: int, ts: int, value: float, seq: int) -> None:
        if self._fh is None:
            raise ChunkError("chunk is closed")
        self._fh.seek(HEADER_SIZE + self.count * RECORD_SIZE)
        self._fh.write(RECORD.pack(series_id, ts, float(value), seq & 0xFFFFFFFF))
        if self.count == 0:
            self.min_ts = self.max_ts = ts
        else:
            if ts < self.min_ts:
                self.min_ts = ts
            if ts > self.max_ts:
                self.max_ts = ts
        self.count += 1
        self._write_header()

    # ----------------------------------------------------------- read
    def read_all(self) -> list[tuple[int, int, float, int]]:
        """Every record as (series_id, ts, value, seq)."""
        return list(self._iter_records())

    def read_range(
        self, start: int | None = None, end: int | None = None,
        series_id: int | None = None,
    ) -> list[tuple[int, int, float, int]]:
        """Records with ``start <= ts < end`` (half-open), optionally for
        one series.  Out-of-order appends are returned sorted by
        (timestamp, append sequence)."""
        rows = [
            rec
            for rec in self._iter_records()
            if (start is None or rec[1] >= start)
            and (end is None or rec[1] < end)
            and (series_id is None or rec[0] == series_id)
        ]
        rows.sort(key=lambda r: (r[1], r[3]))
        return rows

    def _iter_records(self):
        if self.count == 0:
            return
        self._fh.seek(HEADER_SIZE)
        blob = self._fh.read(self.count * RECORD_SIZE)
        for i in range(self.count):
            yield RECORD.unpack_from(blob, i * RECORD_SIZE)

    def overlaps(self, start: int | None, end: int | None) -> bool:
        """True when the chunk's [min_ts, max_ts] intersects [start, end)."""
        if self.count == 0:
            return False
        if start is not None and self.max_ts < start:
            return False
        if end is not None and self.min_ts >= end:
            return False
        return True

    def size_bytes(self) -> int:
        return HEADER_SIZE + self.count * RECORD_SIZE

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "Chunk":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
