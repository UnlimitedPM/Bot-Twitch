"""Client minimal de l'API Helix de Twitch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

import aiohttp

from .auth import Authenticator, AuthError
from .log import get_logger

log = get_logger("helix")

HELIX = "https://api.twitch.tv/helix"


class ApiError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Helix {status} : {message}")
        self.status = status
        self.message = message


@dataclass(slots=True)
class User:
    id: str
    login: str
    display_name: str


@dataclass(slots=True)
class Stream:
    id: str
    user_id: str
    user_login: str
    started_at: datetime
    title: str
    game: str
    viewer_count: int


class TwitchApi:
    def __init__(
        self, session: aiohttp.ClientSession, auth: Authenticator, client_id: str
    ) -> None:
        self._session = session
        self._auth = auth
        self._client_id = client_id

    async def _request(
        self, method: str, path: str, *, params: dict | None = None, json: dict | None = None
    ) -> dict:
        """Requete authentifiee, avec un retry unique apres refresh du token."""
        for attempt in (1, 2):
            token = await self._auth.ensure_valid()
            headers = {
                "Client-Id": self._client_id,
                "Authorization": f"Bearer {token}",
            }
            try:
                async with self._session.request(
                    method, f"{HELIX}{path}", headers=headers, params=params, json=json
                ) as resp:
                    if resp.status == 401 and attempt == 1:
                        log.warning("401 recu, rafraichissement du token puis nouvel essai.")
                        self._auth.invalidate()
                        continue
                    body = await resp.json(content_type=None) if resp.content_length else {}
                    if resp.status >= 400:
                        message = str(body.get("message", body)) if body else resp.reason
                        raise ApiError(resp.status, message)
                    return body or {}
            except aiohttp.ClientError as exc:
                raise ApiError(0, f"erreur reseau : {exc}") from exc
        raise ApiError(401, "authentification refusee")

    # ------------------------------------------------------------------ users

    async def get_user(self, login: str) -> User | None:
        body = await self._request("GET", "/users", params={"login": login})
        data = body.get("data") or []
        if not data:
            return None
        raw = data[0]
        return User(
            id=raw["id"],
            login=raw["login"],
            display_name=raw.get("display_name", raw["login"]),
        )

    # ---------------------------------------------------------------- streams

    async def get_stream(self, login: str) -> Stream | None:
        body = await self._request("GET", "/streams", params={"user_login": login})
        data = body.get("data") or []
        if not data:
            return None
        raw = data[0]
        return Stream(
            id=raw["id"],
            user_id=raw["user_id"],
            user_login=raw["user_login"],
            started_at=_parse_iso(raw["started_at"]),
            title=raw.get("title", ""),
            game=raw.get("game_name", ""),
            viewer_count=int(raw.get("viewer_count", 0)),
        )

    # --------------------------------------------------------------- eventsub

    async def subscribe_stream_online(
        self, broadcaster_id: str, session_id: str
    ) -> dict:
        return await self._request(
            "POST",
            "/eventsub/subscriptions",
            json={
                "type": "stream.online",
                "version": "1",
                "condition": {"broadcaster_user_id": broadcaster_id},
                "transport": {"method": "websocket", "session_id": session_id},
            },
        )

    async def subscribe_stream_offline(
        self, broadcaster_id: str, session_id: str
    ) -> dict:
        return await self._request(
            "POST",
            "/eventsub/subscriptions",
            json={
                "type": "stream.offline",
                "version": "1",
                "condition": {"broadcaster_user_id": broadcaster_id},
                "transport": {"method": "websocket", "session_id": session_id},
            },
        )

    async def list_subscriptions(self) -> list[dict]:
        body = await self._request("GET", "/eventsub/subscriptions")
        return body.get("data") or []

    async def delete_subscription(self, subscription_id: str) -> None:
        try:
            await self._request(
                "DELETE", "/eventsub/subscriptions", params={"id": subscription_id}
            )
        except ApiError as exc:
            log.debug("Suppression abonnement %s ignoree : %s", subscription_id, exc)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def retry_async(coro_factory, *, attempts: int = 3, delay: float = 5.0, what: str = ""):
    """Petit helper : retente une coroutine qui peut echouer (reseau)."""
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await coro_factory()
        except (ApiError, AuthError, aiohttp.ClientError) as exc:
            last = exc
            if attempt == attempts:
                break
            log.warning("%s a echoue (%s), nouvelle tentative dans %.0fs.", what or "Appel", exc, delay)
            await asyncio.sleep(delay)
    assert last is not None
    raise last
