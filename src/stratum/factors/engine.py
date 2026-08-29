"""Factor engine (spec §5.4): signals -> cross-sectional exposures, PIT.

For each rebalance date ``t`` the engine queries the store at
``as_of = t - embargo`` and computes exposures only from rows with
``knowledge_time <= as_of``. There is **no API that reads the future**: every
store call is as_of-scoped, and the transform registry
(:mod:`stratum.factors.transforms`) has no whole-sample entry points.

Per input, each security contributes its latest-in-force value (greatest
``event_time`` knowable at ``as_of``). An input declaring a ``window``
(e.g. ``5d``) additionally supplies each entity's value one window earlier as
``params["base"]`` for the ``pct_change`` op — also PIT-scoped. Non-numeric
label columns (e.g. a sector-mapping signal) feed ``sector_neutralize``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from stratum.factors.definition import FactorDefinition, FactorInput
from stratum.factors.transforms import get_op
from stratum.schema.payloads import GenericPayload, Payload
from stratum.schema.times import AsOf
from stratum.store.interface import SignalStore, SnapshotRef

__all__ = ["DEFAULT_FIELDS", "ExposurePanel", "FactorEngine"]

#: Sensible numeric field per known signal type when an input declares none.
DEFAULT_FIELDS: dict[str, str] = {
    "market.bar": "close",
    "social.attention": "velocity",
    "social.sentiment": "score",
}


def _embargo_delta(text: str) -> timedelta:
    if text.endswith("d"):
        return timedelta(days=int(text[:-1]))
    raise ValueError(f"unsupported embargo {text!r} (expected e.g. '1d')")


def _as_of_for(day: date) -> AsOf:
    return AsOf.at(datetime(day.year, day.month, day.day, tzinfo=UTC))


def _numeric_value(payload: Any, field_name: str | None) -> float | None:
    """Extract a float from a payload; ``None`` when absent."""
    if isinstance(payload, GenericPayload):
        raw = payload.fields.get(field_name) if field_name else None
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return float(raw)
        return None
    if isinstance(payload, Payload) and field_name and hasattr(payload, field_name):
        raw = getattr(payload, field_name)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return float(raw)
    return None


def _label_value(payload: Any, field_name: str | None) -> str | None:
    if isinstance(payload, GenericPayload) and field_name:
        raw = payload.fields.get(field_name)
        return raw if isinstance(raw, str) else None
    return None


@dataclass(frozen=True, kw_only=True)
class ExposurePanel:
    """Cross-sectional exposures: rebalance date -> (security_id -> value),
    plus the coverage diagnostics that keep thin alt-data honest (spec §5.4)."""

    factor_id: str
    factor_version: str
    exposures: Mapping[date, Mapping[str, float]]
    coverage: Mapping[date, int]
    #: Content address of (definition hash, snapshot hash) for caching/repro.
    build_hash: str


class FactorEngine:
    def __init__(self, *, store: SignalStore, snapshot: SnapshotRef) -> None:
        self._store = store
        self._snapshot = snapshot

    # -- PIT reads -------------------------------------------------------------

    def _read_input(
        self, spec: FactorInput, as_of: AsOf
    ) -> tuple[dict[str, float], dict[str, str]]:
        """Latest-in-force numeric values (and string labels) per security,
        knowable at ``as_of`` only."""
        signal_type = spec.signal or spec.market
        assert signal_type is not None  # FactorInput enforces exactly-one-of
        default_field = DEFAULT_FIELDS.get(signal_type)
        latest_ns: dict[str, int] = {}
        numeric: dict[str, float] = {}
        labels: dict[str, str] = {}
        for obs in self._store.read(signal_type=signal_type, as_of=as_of):
            sid = obs.security_id or obs.native_entity
            ns = obs.event_time.ns
            if sid in latest_ns and ns < latest_ns[sid]:
                continue  # an older event than the one already in force
            field_name = spec.field or default_field
            value = _numeric_value(obs.payload, field_name)
            if value is not None:
                latest_ns[sid] = ns
                numeric[sid] = value
                continue
            label = _label_value(obs.payload, field_name)
            if label is not None:
                latest_ns[sid] = ns
                labels[sid] = label
        return numeric, labels

    # -- evaluation ------------------------------------------------------------

    def build(self, definition: FactorDefinition, *, rebalance_dates: list[date]) -> ExposurePanel:
        """Cross-sectional exposures per rebalance date, PIT-safe end to end."""
        embargo = _embargo_delta(definition.pit.embargo)
        needs_base = any(step.op == "pct_change" for step in definition.transform)
        exposures: dict[date, dict[str, float]] = {}
        coverage: dict[date, int] = {}

        for t in rebalance_dates:
            decision_day = t - embargo
            as_of = _as_of_for(decision_day)
            columns: list[dict[str, float]] = []
            sector_labels: dict[str, str] = {}
            base_values: dict[str, float] | None = None

            for spec in definition.inputs:
                numeric, labels = self._read_input(spec, as_of)
                columns.append(numeric)
                sector_labels.update(labels)
                if needs_base and spec.window:
                    prior_as_of = _as_of_for(decision_day - _embargo_delta(spec.window))
                    base_values, _ = self._read_input(spec, prior_as_of)

            numeric_sets = [set(c) for c in columns]
            if not numeric_sets:
                exposures[t] = {}
                coverage[t] = 0
                continue
            entities = set.intersection(*numeric_sets)
            if not entities:
                # Nothing knowable at this date: an honest EMPTY cross-section,
                # never a stale or partially-filled one.
                exposures[t] = {}
                coverage[t] = 0
                continue
            # A Mapping, not a dict: transforms return new mappings and
            # nothing here mutates the cross-section in place.
            xs: Mapping[str, float] = {sid: columns[0][sid] for sid in sorted(entities)}

            for step in definition.transform:
                params: dict[str, Any] = dict(step.params)
                if step.op == "pct_change":
                    if base_values is None:
                        raise ValueError(
                            f"factor {definition.id!r}: 'pct_change' needs an input "
                            "declaring a 'window' (e.g. window: 5d)"
                        )
                    params.setdefault("base", base_values)
                elif step.op == "sector_neutralize" and "sectors" not in params:
                    params["sectors"] = sector_labels
                xs = get_op(step.op)(xs, params)

            exposures[t] = dict(xs)
            coverage[t] = len(xs)

        definition_bytes = json.dumps(asdict(definition), sort_keys=True, default=str)
        definition_hash = hashlib.sha256(definition_bytes.encode()).hexdigest()
        build_hash = hashlib.sha256(
            f"{definition_hash}{self._snapshot.content_hash}".encode()
        ).hexdigest()
        return ExposurePanel(
            factor_id=definition.id,
            factor_version=definition.version,
            exposures=exposures,
            coverage=coverage,
            build_hash=build_hash,
        )
