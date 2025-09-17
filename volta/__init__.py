"""Volta application package."""

from .config import Config  # noqa: F401
from .app import create_app  # noqa: F401

__all__ = ["Config", "create_app"]
