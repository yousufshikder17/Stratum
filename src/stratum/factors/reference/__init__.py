"""Bundled declarative factor examples.

Definitions use the registered point-in-time transforms and computation engine.

Factor packs expose ``FACTOR_DIR``: a traversable directory of YAML factor
definitions.
"""

from importlib.resources import files

FACTOR_DIR = files(__package__)

__all__ = ["FACTOR_DIR"]
