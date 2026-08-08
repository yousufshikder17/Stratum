"""The leakage guard is the only path to a writable record (spec §3.6)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from stratum.adapters.manifest import KnowledgeTimeBasis
from stratum.guard.leakage import (
    IngestMode,
    LeakageGuard,
    LeakageViolation,
    ValidatedObservation,
)
from stratum.schema.times import EventTime, KnowledgeTime
from tests.conftest import make_manifest, make_observation

UTC = UTC
NOW = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)


def guard() -> LeakageGuard:
    return LeakageGuard(now=lambda: NOW)


def test_validated_observation_cannot_be_constructed_directly() -> None:
    with pytest.raises(TypeError, match=r"LeakageGuard\.validate"):
        ValidatedObservation(make_observation())


def test_honest_poll_row_passes() -> None:
    obs = make_observation(
        event_time=EventTime.at(NOW - timedelta(minutes=5)),
        knowledge_time=KnowledgeTime.at(NOW - timedelta(minutes=1)),
    )
    validated = guard().validate(obs, manifest=make_manifest(), mode=IngestMode.POLL)
    assert validated.observation is obs


def test_classic_backfill_bug_is_rejected() -> None:
    # A 2019 event whose knowledge_time is "now" during a 2026 backfill:
    # the adapter fabricated the stamp (spec §3.6, first bullet).
    obs = make_observation(
        event_time=EventTime.at(datetime(2019, 6, 1, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(NOW - timedelta(minutes=1)),
    )
    with pytest.raises(LeakageViolation, match="backfill-now-stamp"):
        guard().validate(obs, manifest=make_manifest(), mode=IngestMode.BACKFILL)


def test_backfill_with_reconstructed_historical_stamp_passes() -> None:
    obs = make_observation(
        event_time=EventTime.at(datetime(2019, 6, 1, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(datetime(2019, 6, 1, 15, 30, tzinfo=UTC)),
    )
    guard().validate(obs, manifest=make_manifest(), mode=IngestMode.BACKFILL)


def test_late_publication_for_old_event_passes() -> None:
    """A trustworthy publication stamp may legitimately be recent even when
    the event or reporting period is old, as with a late amendment."""
    manifest = make_manifest(basis=KnowledgeTimeBasis.PUBLICATION, supports_restatement=True)
    obs = make_observation(
        event_time=EventTime.at(datetime(2019, 6, 1, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(NOW - timedelta(minutes=1)),
        vintage_id="late-amendment",
    )
    guard().validate(obs, manifest=manifest, mode=IngestMode.BACKFILL)


def test_now_stamp_is_honest_in_poll_mode() -> None:
    obs = make_observation(
        event_time=EventTime.at(NOW - timedelta(days=30)),
        knowledge_time=KnowledgeTime.at(NOW),
    )
    guard().validate(obs, manifest=make_manifest(), mode=IngestMode.POLL)


def test_future_knowledge_time_is_always_rejected() -> None:
    obs = make_observation(
        event_time=EventTime.at(NOW),
        knowledge_time=KnowledgeTime.at(NOW + timedelta(hours=2)),
    )
    with pytest.raises(LeakageViolation, match="future-knowledge"):
        guard().validate(obs, manifest=make_manifest(), mode=IngestMode.POLL)


def test_provider_vintage_requires_per_row_stamp() -> None:
    manifest = make_manifest(basis=KnowledgeTimeBasis.PROVIDER_VINTAGE)
    obs = make_observation(
        event_time=EventTime.at(NOW - timedelta(days=1)),
        knowledge_time=KnowledgeTime.at(NOW - timedelta(hours=1)),
        vintage_id="",
    )
    with pytest.raises(LeakageViolation, match="missing-provider-vintage"):
        guard().validate(obs, manifest=manifest, mode=IngestMode.POLL)
    # With the provider stamp it passes.
    stamped = make_observation(
        event_time=obs.event_time,
        knowledge_time=obs.knowledge_time,
        vintage_id="pull-2026-07-17T11",
    )
    guard().validate(stamped, manifest=manifest, mode=IngestMode.POLL)


def test_restatement_capable_sources_must_vintage_every_row() -> None:
    manifest = make_manifest(basis=KnowledgeTimeBasis.PUBLICATION, supports_restatement=True)
    obs = make_observation(
        event_time=EventTime.at(NOW - timedelta(days=90)),
        knowledge_time=KnowledgeTime.at(NOW - timedelta(days=45)),
        vintage_id="",
    )
    with pytest.raises(LeakageViolation, match="missing-vintage"):
        guard().validate(obs, manifest=manifest, mode=IngestMode.BACKFILL)


def test_validate_all_streams_proofs() -> None:
    manifest = make_manifest()
    rows = [
        make_observation(
            observation_id=f"01J0000000000000000000TES{i}",
            event_time=EventTime.at(NOW - timedelta(minutes=10)),
            knowledge_time=KnowledgeTime.at(NOW - timedelta(minutes=5)),
        )
        for i in range(3)
    ]
    out = list(guard().validate_all(rows, manifest=manifest, mode=IngestMode.POLL))
    assert len(out) == 3
    assert all(isinstance(v, ValidatedObservation) for v in out)
