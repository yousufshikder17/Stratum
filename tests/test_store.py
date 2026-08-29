"""DuckDB bitemporal signal store: PIT reads, append-only vintages (spec §4.8)."""

from datetime import UTC, date, datetime

import pytest

pytest.importorskip("duckdb")

from stratum.adapters.manifest import KnowledgeTimeBasis
from stratum.guard.leakage import IngestMode, LeakageGuard
from stratum.schema.payloads import MarketCorporateAction, ModelRef, SocialSentiment
from stratum.schema.times import AsOf, EventTime, KnowledgeTime
from stratum.store.duckdb_store import DuckDBSignalStore
from stratum.store.interface import StoreError
from tests.conftest import make_manifest, make_observation

GUARD = LeakageGuard(now=lambda: datetime(2026, 7, 1, tzinfo=UTC))
MANIFEST = make_manifest(
    basis=KnowledgeTimeBasis.PROVIDER_VINTAGE,
    supports_restatement=True,
)


def validated(*observations):
    return [GUARD.validate(o, manifest=MANIFEST, mode=IngestMode.BACKFILL) for o in observations]


def sentiment(obs_id, entity, knowledge, vintage, score):
    return make_observation(
        observation_id=obs_id,
        native_entity=entity,
        event_time=EventTime.at(datetime(2026, 3, 31, tzinfo=UTC)),
        knowledge_time=KnowledgeTime.at(knowledge),
        vintage_id=vintage,
        payload=SocialSentiment(
            score=score,
            magnitude=0.9,
            model=ModelRef(model_id="vader", version="3.3.2"),
            sample_size=100,
        ),
    )


FIRST = sentiment(
    "01J0000000000000000000001", "$AAA", datetime(2026, 4, 3, tzinfo=UTC), "v-0403", 0.4
)
REVISED = sentiment(
    "01J0000000000000000000002", "$AAA", datetime(2026, 5, 8, tzinfo=UTC), "v-0508", 0.6
)


@pytest.fixture
def store(tmp_path):
    s = DuckDBSignalStore(tmp_path / "stratum.duckdb")
    yield s
    s.close()


def test_pit_read_returns_latest_vintage_as_of(store):
    store.write(validated(FIRST, REVISED), run_id="run-1")

    # Between first release and revision: the first-release value.
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 4, 15, tzinfo=UTC)))
    )
    assert len(seen) == 1
    assert seen[0].payload.score == 0.4

    # After the revision: the revised vintage wins.
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)))
    )
    assert len(seen) == 1
    assert seen[0].payload.score == pytest.approx(0.6)
    assert seen[0].vintage_id == "v-0508"

    # Before anything was knowable: nothing is visible.
    assert (
        list(
            store.read(
                signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 4, 1, tzinfo=UTC))
            )
        )
        == []
    )


def test_write_is_idempotent(store):
    store.write(validated(FIRST), run_id="run-1")
    receipt = store.write(validated(FIRST), run_id="run-2")
    assert receipt.rows_written == 0
    assert receipt.rows_skipped == 1


def test_conflicting_content_under_same_id_is_rejected(store):
    store.write(validated(FIRST), run_id="run-1")
    impostor = sentiment(
        FIRST.observation_id, "$AAA", datetime(2026, 4, 3, tzinfo=UTC), "v-0403", 0.99
    )
    with pytest.raises(StoreError, match="different content"):
        store.write(validated(impostor), run_id="run-2")


def test_in_place_update_is_rejected(store):
    store.write(validated(FIRST), run_id="run-1")
    overwrite = sentiment(
        "01J0000000000000000000009", "$AAA", datetime(2026, 4, 4, tzinfo=UTC), "v-0403", 9.9
    )
    with pytest.raises(StoreError, match="in-place update"):
        store.write(validated(overwrite), run_id="run-1")


def test_snapshot_is_content_addressed_and_as_of_scoped(store, tmp_path):
    store.write(validated(FIRST, REVISED), run_id="run-1")
    early = store.snapshot(
        as_of=AsOf.at(datetime(2026, 4, 15, tzinfo=UTC)), target_dir=tmp_path / "snap-early"
    )
    late = store.snapshot(
        as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)), target_dir=tmp_path / "snap-late"
    )
    assert early.content_hash != late.content_hash
    assert (tmp_path / "snap-early" / "manifest.json").exists()
    manifest_text = (tmp_path / "snap-early" / "manifest.json").read_text()
    assert early.content_hash in manifest_text


def test_payload_round_trip_preserves_typed_fields(store):
    store.write(validated(FIRST), run_id="run-1")
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)))
    )
    payload = seen[0].payload
    assert payload.signal_type() == "social.sentiment"
    assert payload.sample_size == 100
    assert payload.model.model_id == "vader"


def test_payload_round_trip_preserves_dates(store):
    """Corporate actions, delistings and fundamentals all carry `date` fields.
    They must survive the JSON column as dates, not strings."""
    action = make_observation(
        observation_id="01J000000000000000000000A1",
        signal_type="market.corporate_action",
        native_entity="AAA",
        vintage_id="v-split",
        knowledge_time=KnowledgeTime.at(datetime(2026, 4, 3, tzinfo=UTC)),
        payload=MarketCorporateAction(
            action_type="split",
            ratio=2.0,
            ex_date=date(2026, 4, 20),
            record_date=date(2026, 4, 18),
            pay_date=date(2026, 4, 25),
        ),
    )
    store.write(validated(action), run_id="run-1")
    seen = list(
        store.read(
            signal_type="market.corporate_action", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC))
        )
    )
    payload = seen[0].payload
    assert payload.ex_date == date(2026, 4, 20)
    assert payload.pay_date == date(2026, 4, 25)
    assert payload.ratio == 2.0


def test_derived_by_round_trips(store):
    """`derived_by` is stored as "model@version"; ModelRef.parse reads it back."""
    scored = make_observation(
        observation_id="01J000000000000000000000B2",
        native_entity="$AAA",
        vintage_id="v-scored",
        knowledge_time=KnowledgeTime.at(datetime(2026, 4, 3, tzinfo=UTC)),
        derived_by=ModelRef(model_id="finbert", version="1.2"),
    )
    store.write(validated(scored), run_id="run-1")
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)))
    )
    assert seen[0].derived_by == ModelRef(model_id="finbert", version="1.2")


def test_batch_write_is_one_transaction(store):
    """A backfill submits thousands of rows; the store must take them as a
    batch rather than a transaction per observation."""
    batch = [
        sentiment(
            f"01J00000000000000000B{i:05d}",
            f"$E{i}",
            datetime(2026, 4, 3, tzinfo=UTC),
            f"v-{i}",
            0.1,
        )
        for i in range(250)
    ]
    receipt = store.write(validated(*batch), run_id="run-1")
    assert receipt.rows_written == 250
    assert receipt.rows_skipped == 0
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)))
    )
    assert len(seen) == 250


def test_batch_rolls_back_entirely_on_conflict(store):
    """Half-applied batches would leave the store in a state no run manifest
    describes. A rejected batch writes nothing."""
    store.write(validated(FIRST), run_id="run-1")
    conflicting = sentiment(
        "01J0000000000000000000ZZ", "$AAA", datetime(2026, 4, 4, tzinfo=UTC), "v-0403", 9.9
    )
    good = sentiment(
        "01J0000000000000000000YY", "$NEW", datetime(2026, 4, 4, tzinfo=UTC), "v-new", 0.2
    )
    with pytest.raises(StoreError, match="in-place update"):
        store.write(validated(good, conflicting), run_id="run-2")
    seen = list(
        store.read(signal_type="social.sentiment", as_of=AsOf.at(datetime(2026, 6, 1, tzinfo=UTC)))
    )
    assert {o.native_entity for o in seen} == {"$AAA"}


def test_duplicate_id_within_one_batch_is_skipped_when_identical(store):
    """Batching makes it possible to submit the same id twice before either
    reaches the table; the per-row path could never see this."""
    receipt = store.write(validated(FIRST, FIRST), run_id="run-1")
    assert receipt.rows_written == 1
    assert receipt.rows_skipped == 1


def test_duplicate_id_within_one_batch_with_different_content_raises(store):
    twin = sentiment(FIRST.observation_id, "$AAA", datetime(2026, 4, 3, tzinfo=UTC), "v-0403", 0.99)
    with pytest.raises(StoreError, match="twice in one batch"):
        store.write(validated(FIRST, twin), run_id="run-1")


def test_clashing_key_within_one_batch_raises(store):
    other = sentiment(
        "01J0000000000000000000XX", "$AAA", datetime(2026, 4, 5, tzinfo=UTC), "v-0403", 0.5
    )
    with pytest.raises(StoreError, match="twice in one batch"):
        store.write(validated(FIRST, other), run_id="run-1")


def test_empty_batch_is_a_no_op(store):
    receipt = store.write([], run_id="run-1")
    assert receipt.rows_written == 0
    assert receipt.rows_skipped == 0
