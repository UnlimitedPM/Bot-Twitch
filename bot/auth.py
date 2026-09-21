"""Gestion du token OAuth : stockage, validation, rafraichissement automatique."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from .log import get_logger

log = get_logger("auth")

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"

# Marge de securite : on rafraichit le token s'il expire dans moins de 10 min.
REFRESH_MARGIN = 600


class AuthError(RuntimeError):
    """Token impossible a obtenir (refresh token mort, scopes manquants...)."""


@dataclass(slots=True)
class Token:
    access_token: str
    refresh_token: str
    expires_at: float
    user_id: str
    login: str
    scopes: list[str]

    @property
    def expired(self) -> bool:
        return time.time() >= self.expires_at - REFRESH_MARGIN


class TokenStore:
    """Persiste le token sur disque pour ne pas refaire l'OAuth a chaque demarrage."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> Token | None:
        if not self.path.exists():
            return None
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return Token(
                access_token=raw["access_token"],
                refresh_token=raw["refresh_token"],
                expires_at=float(raw.get("expires_at", 0)),
                user_id=str(raw.get("user_id", "")),
                login=raw.get("login", ""),
                scopes=list(raw.get("scopes", [])),
            )
        except (OSError, ValueError, KeyError) as exc:
            log.warning("Token illisible (%s), on repart de zero.", exc)
            return None

    def save(self, token: Token) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "access_token": token.access_token,
            "refresh_token": token.refresh_token,
            "expires_at": token.expires_at,
            "user_id": token.user_id,
            "login": token.login,
            "scopes": token.scopes,
        }
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        try:
            self.path.chmod(0o600)
        except OSError:
            pass


class Authenticator:
    """Fournit un access token valide, en le rafraichissant au besoin."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        client_id: str,
        client_secret: str,
        store: TokenStore,
        bootstrap_refresh_token: str | None = None,
    ) -> None:
        self._session = session
        self._client_id = client_id
        self._client_secret = client_secret
        self._store = store
        self._token: Token | None = store.load()
        if bootstrap_refresh_token and not self._token:
            self._token = Token(
                access_token="",
                refresh_token=bootstrap_refresh_token,
                expires_at=0,
                user_id="",
                login="",
                scopes=[],
            )

    @property
    def token(self) -> Token:
        if self._token is None:
            raise AuthError(
                "Aucun token disponible. Lance d'abord : python get_token.py"
            )
        return self._token

    async def ensure_valid(self) -> str:
        """Retourne un access token utilisable, en le rafraichissant si besoin."""
        if self._token is None:
            raise AuthError(
                "Aucun token disponible. Lance d'abord : python get_token.py"
            )

        token = self._token
        if not token.expired and token.access_token:
            if not token.user_id or not token.login:
                # Token utilisable mais infos utilisateur absentes : on complete.
                await self._validate()
                self._store.save(self.token)
            return self.token.access_token

        await self._refresh()
        self._store.save(self.token)
        return self.token.access_token

    async def _refresh(self) -> None:
        assert self._token is not None
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._token.refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        try:
            async with self._session.post(TOKEN_URL, data=payload) as resp:
                body = await resp.json(content_type=None)
                if resp.status != 200:
                    raise AuthError(
                        f"Rafraichissement refuse ({resp.status}) : "
                        f"{body.get('message', body)} - relance python get_token.py"
                    )
        except aiohttp.ClientError as exc:
            raise AuthError(f"Impossible de joindre Twitch pour le refresh : {exc}") from exc

        self._token = Token(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token", self._token.refresh_token),
            expires_at=time.time() + int(body.get("expires_in", 3600)),
            user_id=self._token.user_id,
            login=self._token.login,
            scopes=self._token.scopes,
        )
        log.info("Token rafraichi (valide %s min).", body.get("expires_in", 0) // 60)
        await self._validate()

    async def _validate(self) -> None:
        """Verifie le token aupres de Twitch et complete user_id / login / scopes."""
        token = self.token
        headers = {"Authorization": f"OAuth {token.access_token}"}
        async with self._session.get(VALIDATE_URL, headers=headers) as resp:
            body = await resp.json(content_type=None)
            if resp.status != 200:
                raise AuthError(
                    f"Token invalide ({resp.status}) : {body.get('message', body)}"
                )

        self._token = Token(
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            expires_at=time.time() + int(body.get("expires_in", 3600)),
            user_id=str(body["user_id"]),
            login=body.get("login", ""),
            scopes=list(body.get("scopes", [])),
        )

    def invalidate(self) -> None:
        """Force le prochain ensure_valid() a rafraichir (utilise apres un 401)."""
        if self._token is not None:
            self._token.expires_at = 0

    async def validate_scopes(self, required: list[str]) -> None:
        """Echoue tot (et clairement) si un scope manque."""
        token = self.token
        missing = [scope for scope in required if scope not in token.scopes]
        if missing:
            raise AuthError(
                "Scopes manquants sur le token : "
                + ", ".join(missing)
                + "\nRelance python get_token.py pour regenerer un token complet."
            )
