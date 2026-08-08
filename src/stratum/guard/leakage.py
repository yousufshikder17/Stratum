"""The ingest leakage guard (spec §3.6, §4.4 rule 5).

This is the validation choke point between the adapter layer and everything
downstream: **no adapter output reaches the PIT store — and therefore no
factor engine or backtest — without passing through here.** The enforcement is
type-level: ``SignalStore.write`` accepts only :class:`ValidatedObservation`,
and the only way to obtain one is :meth:`LeakageGuard.validate`.

Because backfill is where lookahead bias is silently born, the guard enforces,
per the adapter's manifest:

- ``knowledge_time_basis = ingestion``: historical ``backfill()`` rows whose
  ``event_time`` is far in the past but whose ``knowledge_time`` is "now" are
  rejected — an adapter cannot claim it knew a 2019 value the moment it pulled
  it in 2026 unless it has a real historical knowledge stamp.
- ``publication`` / ``provider_vintage``: the adapter must supply a per-row
  source-provided stamp; provider-vintage rows without a ``vintage_id`` fail.
- Restatement-capable sources must write vintages (``vintage_id`` required);
  the append-only store rejects in-place updates.
- No observation may carry a ``knowledge_time`` in the future (beyond clock
  skew) — a future stamp is definitionally dishonest.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import final

from stratum.adapters.manifest import AdapterManifest, KnowledgeTimeBasis
from stratum.schema.observation import Observation
from stratum.schema.times import KnowledgeTime

__all__ = [
    "GuardPolicy",
    "IngestMode",
    "LeakageGuard",
    "LeakageViolation",
    "ValidatedObservation",
]


class IngestMode(Enum):
    """Which lifecycle method produced the row — the guard's rules differ
    (``knowledge_time = now`` is honest for POLL, a leakage bug for BACKFILL)."""

    BACKFILL = "backfill"
    POLL = "poll"


class LeakageViolation(Exception):
    """An observation failed point-in-time validation. Rejected, not stored."""

    def __init__(self, rule: str, message: str, observation: Observation) -> None:
        super().__init__(f"[{rule}] {message}")
        self.rule = rule
        self.observation = observation


class _Proof:
    """Module-private capability token; only LeakageGuard holds the instance."""

    __slots__ = ()


_PROOF = _Proof()


@final
class ValidatedObservation:
    """Proof that an observation passed the leakage guard.

    ``SignalStore.write`` accepts only this type, and its constructor demands
    the guard's private proof token — there is no honest way to construct one
    except :meth:`LeakageGuard.validate`. This is how "no adapter output
    crosses the guard without validation" is made structural rather than
    conventional.
    """

    __slots__ = ("_observation",)

    def __init__(self, observation: Observation, *, proof: _Proof | None = None) -> None:
        if proof is not _PROOF:
            raise TypeError(
                "ValidatedObservation cannot be constructed directly; "
                "obtain one from LeakageGuard.validate() (spec §3.6)"
            )
        self._observation = observation

    @property
    def observation(self) -> Observation:
        return self._observation

    def __repr__(self) -> str:
        o = self._observation
        return f"ValidatedObservation({o.observation_id!r}, {o.signal_type!r})"


@dataclass(frozen=True, kw_only=True)
class GuardPolicy:
    """Tunable thresholds. Defaults are conservative; loosening them is a
    deliberate, visible act (spec §6: turning a guard off is loud)."""

    #: Tolerated clock skew before a knowledge stamp counts as "in the future".
    max_clock_skew: timedelta = timedelta(minutes=5)
    #: A backfill knowledge stamp within this window of "now" is treated as a
    #: now-stamp (the classic backfill bug) when the event is historical.
    now_stamp_tolerance: timedelta = timedelta(hours=24)
    #: An event older than this counts as historical for the now-stamp rule.
    historical_event_age: timedelta = timedelta(days=7)


class LeakageGuard:
    """Validates adapter output against its manifest's PIT declaration."""

    def __init__(
        self,
        *,
        policy: GuardPolicy | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._policy = policy or GuardPolicy()
        self._now = now or (lambda: datetime.now(UTC))

    def validate(
        self,
        observation: Observation,
        *,
        manifest: AdapterManifest,
        mode: IngestMode,
    ) -> ValidatedObservation:
        """Return proof of validity, or raise :class:`LeakageViolation`."""
        policy = self._policy
        now = KnowledgeTime.at(self._now())
        kt = observation.knowledge_time

        # Rule: no future knowledge. A stamp beyond clock skew ahead of the
        # wall clock cannot be "the earliest moment this was actionable".
        skew_ns = int(policy.max_clock_skew.total_seconds() * 1_000_000_000)
        if kt.ns > now.ns + skew_ns:
            raise LeakageViolation(
                "future-knowledge",
                f"knowledge_time {kt.as_datetime().isoformat()} is in the future",
                observation,
            )

        # Rule (§3.6): the classic backfill bug. In BACKFILL, a historical
        # event stamped as knowable only-just-now means the adapter fabricated
        # knowledge_time instead of reconstructing it.
        basis = manifest.pit.knowledge_time_basis
        if mode is IngestMode.BACKFILL and basis is KnowledgeTimeBasis.INGESTION:
            event_age_ns = now.ns - observation.event_time.ns
            stamp_age_ns = now.ns - kt.ns
            historical_ns = int(policy.historical_event_age.total_seconds() * 1_000_000_000)
            tolerance_ns = int(policy.now_stamp_tolerance.total_seconds() * 1_000_000_000)
            if event_age_ns > historical_ns and stamp_age_ns < tolerance_ns:
                raise LeakageViolation(
                    "backfill-now-stamp",
                    "historical event_time with knowledge_time ~= now: the adapter "
                    "cannot claim it knew this value the moment it pulled it; it must "
                    "reconstruct a real historical knowledge stamp or refuse backfill "
                    "(manifest pit.knowledge_time_basis="
                    f"{manifest.pit.knowledge_time_basis.value!r})",
                    observation,
                )

        # Rule (§3.6): provider-vintage sources must carry the provider's
        # per-row stamp; a missing vintage means the stamp was not supplied.
        if basis is KnowledgeTimeBasis.PROVIDER_VINTAGE and not observation.vintage_id:
            raise LeakageViolation(
                "missing-provider-vintage",
                "manifest declares knowledge_time_basis=provider_vintage but the "
                "observation carries no vintage_id (source-provided stamp required)",
                observation,
            )

        # Rule (§3.6): restatement-capable sources write vintages, never
        # in-place updates. The store enforces append-only; the guard enforces
        # that every row is vintage-keyed so appends are well-defined.
        if manifest.pit.supports_restatement and not observation.vintage_id:
            raise LeakageViolation(
                "missing-vintage",
                "manifest declares supports_restatement=true; every row must carry a "
                "vintage_id so restatements append rather than overwrite",
                observation,
            )

        return ValidatedObservation(observation, proof=_PROOF)

    def validate_all(
        self,
        observations: Iterable[Observation],
        *,
        manifest: AdapterManifest,
        mode: IngestMode,
    ) -> Iterator[ValidatedObservation]:
        for observation in observations:
            yield self.validate(observation, manifest=manifest, mode=mode)
