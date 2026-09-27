"""Journalisation simple et homogene.

Les horodatages sont ecrits dans le fuseau demande par ``TZ`` (par exemple
``Europe/Paris``) au lieu de l'heure systeme : dans un conteneur Docker l'heure
systeme est en UTC, ce qui decalait tous les logs de deux heures.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"
_DATEFMT = "%d/%m %H:%M:%S"

log = logging.getLogger("log")


class _Formatter(logging.Formatter):
    def __init__(self, fmt: str, datefmt: str, tz: tzinfo | None) -> None:
        super().__init__(fmt, datefmt)
        self._tz = tz

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        if self._tz is None:
            return super().formatTime(record, datefmt)
        moment = datetime.fromtimestamp(record.created, self._tz)
        return moment.strftime(datefmt or self.default_time_format)


def _zone_name(name: str | None) -> str:
    return (name if name is not None else (os.getenv("TZ") or "")).strip()


def resolve_timezone(name: str | None = None) -> tzinfo | None:
    """Fuseau demande par ``TZ`` ; ``None`` = heure systeme."""
    raw = _zone_name(name)
    if not raw or raw.upper() in {"LOCAL", "SYSTEM"}:
        return None
    try:
        return ZoneInfo(raw)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return None


def setup(level: str = "INFO", tz: str | None = None) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_Formatter(_FORMAT, _DATEFMT, resolve_timezone(tz)))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    raw = _zone_name(tz)
    if raw and resolve_timezone(tz) is None:
        log.warning(
            "Fuseau horaire \"%s\" introuvable (tzdata installe ?) : "
            "heure systeme utilisee.",
            raw,
        )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
