"""Adapter discovery via Python entry points (spec §3.1).

Any pip-installable package can ship an adapter by registering a class in the
``stratum.adapters`` entry-point group. Each adapter package must contain a
static ``manifest.toml`` next to its module so the core can list and validate
adapters from their manifests.

Scaffold note: :func:`discover` currently locates ``manifest.toml`` via
``importlib.resources``, which imports the adapter *package* (not the class).
Fully import-free listing (reading manifests out of distribution metadata) is
an MVP task; the public API here will not change.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from importlib.resources import as_file, files

from stratum.adapters.base import Adapter
from stratum.adapters.manifest import AdapterManifest, ManifestError, load_manifest

__all__ = ["ADAPTER_GROUP", "FACTOR_GROUP", "DiscoveredAdapter", "discover", "load_adapter"]

ADAPTER_GROUP = "stratum.adapters"
FACTOR_GROUP = "stratum.factors"


@dataclass(frozen=True, kw_only=True)
class DiscoveredAdapter:
    name: str
    entry_point: str  # "module:attr"
    manifest: AdapterManifest | None
    error: str | None = None  # populated when the manifest is missing/invalid


def _iter_entry_points() -> list[EntryPoint]:
    return list(entry_points(group=ADAPTER_GROUP))


def _read_manifest(ep: EntryPoint) -> AdapterManifest:
    package = ep.module
    module = importlib.import_module(package)
    if not hasattr(module, "__path__"):  # class lives in a plain module
        package = package.rsplit(".", 1)[0]
    resource = files(package).joinpath("manifest.toml")
    with as_file(resource) as path:
        return load_manifest(path)


def discover() -> list[DiscoveredAdapter]:
    """List installed adapters with their validated manifests.

    Manifest problems are surfaced per-adapter rather than raised, so one
    broken plugin cannot hide the rest.
    """
    found: list[DiscoveredAdapter] = []
    for ep in _iter_entry_points():
        try:
            manifest: AdapterManifest | None = _read_manifest(ep)
            error = None
        except (ManifestError, FileNotFoundError, ModuleNotFoundError) as exc:
            manifest, error = None, str(exc)
        found.append(
            DiscoveredAdapter(name=ep.name, entry_point=ep.value, manifest=manifest, error=error)
        )
    return sorted(found, key=lambda d: d.name)


def load_adapter(name: str) -> type[Adapter]:
    """Import and return the adapter class registered under ``name``."""
    for ep in _iter_entry_points():
        if ep.name == name:
            cls = ep.load()
            if not (isinstance(cls, type) and issubclass(cls, Adapter)):
                raise ManifestError(f"entry point {name!r} does not resolve to an Adapter class")
            return cls
    raise KeyError(f"no adapter registered under {name!r} in group {ADAPTER_GROUP!r}")
