"""Observation identifiers (spec §4.3).

``observation_id`` is a ULID: 48 bits of millisecond timestamp in Crockford
base32, then 80 bits of entropy. Time-sortable, and — because the store keys
idempotency off this column (spec §3.2 rule 4) — it must be **derivable from
the record itself** for file-backed adapters. Re-reading the same CSV twice
must produce the same ids, or a re-pull would duplicate every row.

:func:`deterministic_ulid` therefore substitutes a content hash for the
random suffix: same natural key, same id, forever. Adapters pulling from a
live API where records carry their own unique keys use those keys as the
natural key; adapters over genuinely random-access data may still use
``ulid.new()`` from any ULID library instead.
"""

from __future__ import annotations

import hashlib

__all__ = ["CROCKFORD", "deterministic_ulid"]

#: Crockford base32 — excludes I, L, O, U to avoid transcription errors.
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_TIMESTAMP_CHARS = 10
_ENTROPY_CHARS = 16
_MAX_TIMESTAMP_MS = (1 << 48) - 1


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def deterministic_ulid(*, timestamp_ms: int, key: str) -> str:
    """A ULID whose entropy is ``sha256(key)`` rather than randomness.

    ``timestamp_ms`` should be the record's event time, so ids sort in event
    order. ``key`` must be the record's natural key — everything that makes
    the row distinct within its source (entity, date, signal, vintage).
    """
    if not 0 <= timestamp_ms <= _MAX_TIMESTAMP_MS:
        raise ValueError(f"timestamp_ms {timestamp_ms} does not fit in ULID's 48 bits")
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    entropy = int.from_bytes(digest[:10], "big")  # 80 bits
    return _encode(timestamp_ms, _TIMESTAMP_CHARS) + _encode(entropy, _ENTROPY_CHARS)
