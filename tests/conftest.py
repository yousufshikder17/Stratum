"""Shared fixtures: observation factory + manifest factory."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from stratum.adapters.manifest import (
    AdapterFamily,
    AdapterManifest,
    Isolation,
    KnowledgeTimeBasis,
    PitDeclaration,
)
from stratum.schema.data_class import DataClass
from stratum.schema.observation import Observation
from stratum.schema.payloads import ModelRef, SocialSentiment
from stratum.schema.times import EventTime, KnowledgeTime

UTC = UTC


def make_observation(**overrides: Any) -> Observation:
    defaults: dict[str, Any] = {
        "observation_id": "01J0000000000000000000TEST",
        "signal_type": "social.sentiment",
        "run_id": "run-1",
        "source_id": "reddit",
        "adapter_id": "reddit_sentiment",
        "native_entity": "$TEST",
        "event_time": EventTime.at(datetime(2024, 3, 1, 15, 0, tzinfo=UTC)),
        "knowledge_time": KnowledgeTime.at(datetime(2024, 3, 1, 15, 0, tzinfo=UTC)),
        "data_class": DataClass.PUBLIC_TEXT,
        "license_tag": "reddit-api-tos",
        "payload": SocialSentiment(
            score=0.4,
            magnitude=0.9,
            model=ModelRef(model_id="vader", version="3.3.2"),
            sample_size=120,
        ),
    }
    defaults.update(overrides)
    return Observation(**defaults)


def make_manifest(
    *,
    basis: KnowledgeTimeBasis = KnowledgeTimeBasis.INGESTION,
    supports_restatement: bool = False,
) -> AdapterManifest:
    return AdapterManifest(
        id="test_adapter",
        family=AdapterFamily.SOURCE,
        version="0.1.0",
        schema_version="1",
        isolation=Isolation.INPROCESS,
        config_schema="config.schema.json",
        license_tag="test",
        frequency="daily",
        pit=PitDeclaration(
            knowledge_time_basis=basis,
            supports_restatement=supports_restatement,
        ),
    )


@pytest.fixture
def observation() -> Observation:
    return make_observation()
