"""Snapshot read-back: research reads only the immutable snapshot (spec §2.3)."""

from datetime import UTC, datetime

import pytest

pytest.importorskip("duckdb")

from stratum.schema.payloads import ModelRef, SocialSentiment
from stratum.schema.times import AsOf, EventTime, KnowledgeTime
from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.interface import StoreError
from stratum.store.snapshot_store import SnapshotStore
from tests.test_store import FIRST, REVISED, validated


@pytest.fixture
def snap(tmp_path):
    store = DuckDBSignalStore(tmp_path / "stratum.duckdb")
    try:
        store.write(validated(FIRST, REVISED), run_id="run-1")
        ref = store.snapshot(
            as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)), target_dir=tmp_path / "snap"
        )
        yield ref
    finally:
        store.close()


def test_snapshot_read_back_matches_live_store(snap):
    live = DuckDBSignalStore(snap.path.parent / "stratum.duckdb")
    try:
        as_of = AsOf.at(datetime(2026, 6, 1, tzinfo=UTC))
        from_snap = list(SnapshotStore(snap.path).read(signal_type="social.sentiment", as_of=as_of))
        from_live = list(live.read(signal_type="social.sentiment", as_of=as_of))
    finally:
        live.close()
    assert [o.observation_id for o in from_snap] == [o.observation_id for o in from_live]
    assert from_snap[0].payload.score == pytest.approx(0.6)  # revised vintage wins


def test_snapshot_read_still_scopes_by_as_of(snap):
    # The snapshot holds rows up to its own as_of; an earlier read must still
    # hide the revision knowable 2026-05-08.
    seen = list(
        SnapshotStore(snap.path).read(
            signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 4, 15, tzinfo=UTC))
        )
    )
    assert len(seen) == 1
    assert seen[0].payload.score == pytest.approx(0.4)
    assert seen[0].vintage_id == "v-0403"


def test_content_hash_pin_mismatch_is_refused(snap):
    with pytest.raises(StoreError, match="expected_content_hash"):
        SnapshotStore(snap.path, expected_content_hash="pinned")


def test_as_snapshot_ref_carries_hash(snap):
    ref = SnapshotStore(snap.path).as_snapshot_ref()
    assert ref.content_hash == snap.content_hash


def test_write_is_refused(snap):
    observation = make_late_observation()
    guard_store = DuckDBSignalStore(snap.path.parent / "stratum.duckdb")
    try:
        record = validated(observation)[0]
    finally:
        guard_store.close()
    with pytest.raises(StoreError, match="immutable"):
        SnapshotStore(snap.path).write([record], run_id="run-x")


def make_late_observation():
    from tests.conftest import make_observation

    return make_observation(
        observation_id="01J0000000000000000000099",
        event_time=EventTime.at(datetime(2026, 6, 2, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(datetime(2026, 6, 2, tzinfo=UTC)),
        vintage_id="v-0602",
        payload=SocialSentiment(
            score=0.1,
            magnitude=0.5,
            model=ModelRef(model_id="vader", version="3.3.2"),
            sample_size=10,
        ),
    )


def test_missing_manifest_is_refused(tmp_path):
    with pytest.raises(StoreError, match="manifest"):
        SnapshotStore(tmp_path / "no-snap")
