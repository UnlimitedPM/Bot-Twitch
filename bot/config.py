"""Chargement de la configuration depuis le fichier .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
TOKEN_FILE = DATA_DIR / "token.json"


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on", "oui"}


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


@dataclass(slots=True)
class Config:
    client_id: str
    client_secret: str
    streamer_login: str
    greeting: str
    greeting_enabled: bool
    greeting_max_age_minutes: int
    watch_quality: str
    poll_interval: int
    log_level: str
    discord_webhook: str | None
    timezone: str = ""
    token_file: Path = field(default=TOKEN_FILE)

    @classmethod
    def load(cls) -> "Config":
        load_dotenv(ROOT / ".env")

        client_id = (os.getenv("TWITCH_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("TWITCH_CLIENT_SECRET") or "").strip()
        streamer = (os.getenv("STREAMER_LOGIN") or "").strip().lstrip("@").lower()

        missing = [
            name
            for name, value in (
                ("TWITCH_CLIENT_ID", client_id),
                ("TWITCH_CLIENT_SECRET", client_secret),
                ("STREAMER_LOGIN", streamer),
            )
            if not value
        ]
        if missing:
            raise SystemExit(
                "Configuration incomplete, manque : "
                + ", ".join(missing)
                + f"\nCopie .env.example vers .env ({ROOT / '.env'}) et remplis les valeurs."
            )

        webhook = (os.getenv("DISCORD_WEBHOOK") or "").strip() or None

        return cls(
            client_id=client_id,
            client_secret=client_secret,
            streamer_login=streamer,
            greeting=os.getenv("GREETING", "Salut Asther ^^"),
            greeting_enabled=_bool("GREETING_ENABLED", True),
            greeting_max_age_minutes=max(0, _int("GREETING_MAX_AGE_MINUTES", 5)),
            watch_quality=(os.getenv("WATCH_QUALITY") or "audio_only").strip(),
            poll_interval=max(15, _int("POLL_INTERVAL", 20)),
            log_level=(os.getenv("LOG_LEVEL") or "INFO").strip().upper(),
            discord_webhook=webhook,
            timezone=(os.getenv("TZ") or "").strip(),
        )
