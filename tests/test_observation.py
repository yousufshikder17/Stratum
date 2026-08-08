"""The envelope enforces point-in-time clock types at construction."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from stratum.schema.observation import Observation, PointInTimeRecord
from stratum.schema.payloads import GenericPayload
from stratum.schema.registry import SchemaRegistry, SchemaViolation
from stratum.schema.times import EventTime, KnowledgeTime
from tests.conftest import make_observation

UTC = UTC


def test_pointintimerecord_is_the_observation_envelope() -> None:
    assert PointInTimeRecord is Observation


def test_cannot_construct_without_knowledge_time() -> None:
    kwargs = {  # everything except knowledge_time
        k: v for k, v in vars_of_valid_observation().items() if k != "knowledge_time"
    }
    with pytest.raises(TypeError):
        Observation(**kwargs)


def test_event_time_cannot_be_passed_as_knowledge_time() -> None:
    et = EventTime.at(datetime(2024, 3, 1, tzinfo=UTC))
    assert not isinstance(et, KnowledgeTime)
    with pytest.raises(TypeError, match="knowledge_time"):
        make_observation(knowledge_time=et)


def test_frozen_envelope_rejects_mutation() -> None:
    obs = make_observation()
    with pytest.raises(AttributeError):
        obs.knowledge_time = KnowledgeTime.at(datetime(2020, 1, 1, tzinfo=UTC))  # type: ignore[misc]


def test_payload_signal_type_must_match_envelope() -> None:
    with pytest.raises(ValueError, match="relabeled"):
        make_observation(signal_type="fundamental.fact")  # payload says social.sentiment


def test_required_fields_must_be_nonempty() -> None:
    with pytest.raises(ValueError, match="license_tag"):
        make_observation(license_tag="")


def test_publication_lag_diagnostic() -> None:
    t0 = datetime(2024, 3, 1, tzinfo=UTC)
    obs = make_observation(
        event_time=EventTime.at(t0),
        knowledge_time=KnowledgeTime.at(t0 + timedelta(days=45)),
    )
    assert obs.publication_lag() == timedelta(days=45)


def test_registry_validates_known_payloads() -> None:
    registry = SchemaRegistry()
    registry.validate(make_observation())  # standard type + right payload: ok


def test_registry_rejects_wrong_payload_class_for_known_type() -> None:
    registry = SchemaRegistry()
    imposter = make_observation(payload=GenericPayload(type_name="social.sentiment"))
    with pytest.raises(SchemaViolation, match="requires payload"):
        registry.validate(imposter)


def test_registry_preserves_unknown_types_via_generic_payload() -> None:
    registry = SchemaRegistry()
    obs = make_observation(
        signal_type="vendor.custom_signal",
        payload=GenericPayload(type_name="vendor.custom_signal", fields={"x": 1}),
    )
    assert registry.validate(obs) is obs


def vars_of_valid_observation() -> dict[str, object]:
    obs = make_observation()
    return {
        "observation_id": obs.observation_id,
        "signal_type": obs.signal_type,
        "run_id": obs.run_id,
        "source_id": obs.source_id,
        "adapter_id": obs.adapter_id,
        "native_entity": obs.native_entity,
        "event_time": obs.event_time,
        "knowledge_time": obs.knowledge_time,
        "data_class": obs.data_class,
        "license_tag": obs.license_tag,
        "payload": obs.payload,
    }
