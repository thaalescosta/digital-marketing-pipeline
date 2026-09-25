"""Deterministic fake marketing data generator."""

from .api import generate, generate_backfill
from .dimensions import dimensions_as_of

__all__ = ["generate", "generate_backfill", "dimensions_as_of"]
__version__ = "0.1.0"