"""Series registry: metric name + tags -> stable integer id.

The registry lives in ``registry.json`` inside the database directory::

    {"next_id": 3,
     "series": [{"id": 1, "metric": "cpu", "tags": {"host": "a"}},
                {"id": 2, "metric": "cpu", "tags": {"host": "b"}}]}

Canonical identity: the metric name (lower-cased) plus the tag mapping
with tags sorted by key — ``{"b": "2", "a": "1"}`` and
``{"a": "1", "b": "2"}`` are the same series.  Ids are allocated from a
persisted counter, so ids are never reused after deletions.
"""
from __future__ import annotations

import json
from pathlib import Path

REGISTRY_FILE = "registry.json"


class RegistryError(Exception):
    """Invalid series definition or lookup."""


def canonical(metric: str, tags: dict[str, str] | None) -> tuple:
    """Normalize (metric, tags) into a hashable identity key."""
    if not isinstance(metric, str) or not metric.strip():
        raise RegistryError("metric name must be a non-empty string")
    norm_tags: dict[str, str] = {}
    for key, value in (tags or {}).items():
        if not isinstance(key, str) or not key:
            raise RegistryError("tag names must be non-empty strings")
        if not isinstance(value, str):
            raise RegistryError(
                f"tag {key!r} value must be a string, got {type(value).__name__}"
            )
        norm_tags[key] = value
    return (metric.lower(), tuple(sorted(norm_tags.items())))


class Registry:
    """Maps (metric, tags) <-> id, persisted as JSON."""

    def __init__(self, directory: str | Path) -> None:
        self.path = Path(directory) / REGISTRY_FILE
        self._by_key: dict[tuple, int] = {}
        self._by_id: dict[int, tuple[str, dict[str, str]]] = {}
        self.next_id = 1
        if self.path.is_file():
            self._load()

    # ---------------------------------------------------------- storage
    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RegistryError(f"corrupt registry: {exc}") from None
        self.next_id = int(payload.get("next_id", 1))
        for rec in payload.get("series", []):
            sid = int(rec["id"])
            metric = str(rec["metric"])
            tags = dict(rec.get("tags", {}))
            key = canonical(metric, tags)
            self._by_key[key] = sid
            self._by_id[sid] = (metric, tags)
            self.next_id = max(self.next_id, sid + 1)

    def save(self) -> None:
        payload = {
            "next_id": self.next_id,
            "series": [
                {"id": sid, "metric": metric, "tags": tags}
                for sid, (metric, tags) in sorted(self._by_id.items())
            ],
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    # ------------------------------------------------------------ API
    def create(self, metric: str, tags: dict[str, str] | None = None) -> int:
        """Register a series (idempotent: same identity -> same id)."""
        key = canonical(metric, tags)
        if key in self._by_key:
            return self._by_key[key]
        sid = self.next_id
        self.next_id += 1
        self._by_key[key] = sid
        self._by_id[sid] = (key[0], dict(key[1]))
        self.save()
        return sid

    def find(self, metric: str, tags: dict[str, str] | None = None) -> int | None:
        """Return the id for an existing series, or None."""
        return self._by_key.get(canonical(metric, tags))

    def info(self, series_id: int) -> tuple[str, dict[str, str]]:
        """Return (metric, tags) for an id; raises RegistryError if unknown."""
        if series_id not in self._by_id:
            raise RegistryError(f"unknown series id {series_id}")
        metric, tags = self._by_id[series_id]
        return metric, dict(tags)

    def all(self) -> list[tuple[int, str, dict[str, str]]]:
        """Every registered series as (id, metric, tags), id-sorted."""
        return [
            (sid, metric, dict(tags))
            for sid, (metric, tags) in sorted(self._by_id.items())
        ]

    def __contains__(self, series_id: int) -> bool:
        return series_id in self._by_id

    def __len__(self) -> int:
        return len(self._by_id)
