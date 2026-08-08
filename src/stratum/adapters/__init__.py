"""The adapter layer (spec §3): the pluggable extension seam.

No source API leaks past this boundary — core code never imports ``praw``,
EDGAR SDKs, or provider types (spec §1, design pillars). Built-in adapters are
plugins registered through the same ``stratum.adapters`` entry-point group as
third-party ones.
"""

from stratum.adapters.base import (
    Adapter,
    Bar,
    CapabilityManifest,
    CorporateAction,
    CostBreakdown,
    CostModelAdapter,
    DelistingEvent,
    EntityHint,
    HealthState,
    HealthStatus,
    MarketDataAdapter,
    MarketSnapshot,
    OutputAdapter,
    ProposedFill,
    SignalValue,
    SourceAdapter,
    TimeWindow,
    UniverseAdapter,
    UniverseSpec,
)
from stratum.adapters.context import AdapterContext, ObservationSink
from stratum.adapters.discovery import discover, load_adapter
from stratum.adapters.manifest import (
    AdapterFamily,
    AdapterManifest,
    Isolation,
    KnowledgeTimeBasis,
    ManifestError,
    PitDeclaration,
    load_manifest,
)

__all__ = [
    "Adapter",
    "AdapterContext",
    "AdapterFamily",
    "AdapterManifest",
    "Bar",
    "CapabilityManifest",
    "CorporateAction",
    "CostBreakdown",
    "CostModelAdapter",
    "DelistingEvent",
    "EntityHint",
    "HealthState",
    "HealthStatus",
    "Isolation",
    "KnowledgeTimeBasis",
    "ManifestError",
    "MarketDataAdapter",
    "MarketSnapshot",
    "ObservationSink",
    "OutputAdapter",
    "PitDeclaration",
    "ProposedFill",
    "SignalValue",
    "SourceAdapter",
    "TimeWindow",
    "UniverseAdapter",
    "UniverseSpec",
    "discover",
    "load_adapter",
    "load_manifest",
]
