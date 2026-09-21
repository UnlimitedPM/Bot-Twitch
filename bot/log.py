"""Journalisation simple et homogene."""

from __future__ import annotations

import logging

_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"
_DATEFMT = "%d/%m %H:%M:%S"


def setup(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=_FORMAT,
        datefmt=_DATEFMT,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
