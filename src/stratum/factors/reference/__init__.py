"""Bundled declarative factor examples.

Definitions validate, but their registered transforms and computation engine
are not implemented.

Factor packs expose ``FACTOR_DIR``: a traversable directory of YAML factor
definitions.
"""

from importlib.resources import files

FACTOR_DIR = files(__package__)

__all__ = ["FACTOR_DIR"]
