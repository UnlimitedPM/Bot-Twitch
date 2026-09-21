"""Abonnement EventSub en WebSocket : detection du live quasi instantanee.

Flux Twitch :
1. on ouvre wss://eventsub.wss.twitch.tv/ws
2. Twitch envoie "session_welcome" avec un id de session
3. on cree un abonnement stream.online / stream.offline lie a cet id
4. Twitch pousse les notifications sur la meme connexion
5. si la connexion meurt, Twitch envoie "session_reconnect" (ou rien du tout) :
   on se reconnecte automatiquement
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable

import aiohttp

from .log import get_logger
from .twitch_api import ApiError, TwitchApi

log = get_logger("eventsub")

WEBSOCKET_URL = "wss://eventsub.wss.twitch.tv/ws"

OnEvent = Callable[[], Awaitable[None]]


class EventSubClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api: TwitchApi,
        broadcaster_id: str,
        on_online: OnEvent,
        on_offline: OnEvent,
    ) -> None:
        self._session = session
        self._api = api
        self._broadcaster_id = broadcaster_id
        self._on_online = on_online
        self._on_offline = on_offline
        self._connected = asyncio.Event()
        self._stop = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    async def wait_until_connected(self, timeout: float = 30.0) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Boucle principale : reste connecte tant que stop() n'est pas appele."""
        backoff = 1.0
        url = WEBSOCKET_URL
        while not self._stop.is_set():
            try:
                reconnect_url = await self._session_loop(url)
                backoff = 1.0
                if self._stop.is_set():
                    break
                url = reconnect_url or WEBSOCKET_URL
                log.info("Connexion EventSub renouvelee.")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - on veut survivre a tout
                log.warning("EventSub deconnecte (%s). Reconnexion dans %.0fs.", exc, backoff)
                await self._sleep_or_stop(backoff)
                backoff = min(backoff * 2, 60.0)
                url = WEBSOCKET_URL
        self._connected.clear()

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def _session_loop(self, url: str) -> str | None:
        """Une connexion : renvoie une eventuelle url de reconnexion."""
        async with self._session.ws_connect(url, heartbeat=None) as ws:
            session_id: str | None = None
            keepalive = 30.0
            last_message = time.monotonic()

            async def watchdog() -> None:
                """Si aucun message (keepalive inclus) n'arrive, la connexion est morte."""
                while True:
                    timeout = keepalive + 10
                    await asyncio.sleep(timeout)
                    if time.monotonic() - last_message > timeout:
                        log.warning("Plus de nouvelle de Twitch depuis %.0fs, on coupe.", timeout)
                        await ws.close()
                        return

            watchdog_task = asyncio.create_task(watchdog())
            try:
                while not self._stop.is_set():
                    msg = await ws.receive(timeout=None)
                    last_message = time.monotonic()

                    if msg.type is aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        metadata = data.get("metadata", {})
                        payload = data.get("payload", {})
                        kind = metadata.get("message_type")

                        if kind == "session_welcome":
                            session_id = payload["session"]["id"]
                            keepalive = float(
                                payload["session"].get("keepalive_timeout_seconds") or 30
                            )
                            await self._subscribe(session_id)
                            self._connected.set()
                            log.info("EventSub connecte (session %s).", session_id[:8])

                        elif kind == "session_keepalive":
                            log.debug("Keepalive recu.")

                        elif kind == "session_reconnect":
                            new_url = payload["session"]["reconnect_url"]
                            log.info("Twitch demande une reconnexion.")
                            return new_url

                        elif kind == "revocation":
                            status = payload.get("subscription", {}).get("status")
                            log.warning("Abonnement revoque par Twitch (%s).", status)

                        elif kind == "notification":
                            await self._dispatch(payload)

                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING,
                    ):
                        raise ConnectionError("connexion fermee par Twitch")

                    elif msg.type is aiohttp.WSMsgType.ERROR:
                        raise ConnectionError(f"erreur websocket : {ws.exception()}")
            finally:
                self._connected.clear()
                watchdog_task.cancel()
            return None

    async def _subscribe(self, session_id: str) -> None:
        for subscribe, label in (
            (self._api.subscribe_stream_online, "stream.online"),
            (self._api.subscribe_stream_offline, "stream.offline"),
        ):
            try:
                await subscribe(self._broadcaster_id, session_id)
                log.info("Abonne a %s.", label)
            except ApiError as exc:
                # 409 : l'abonnement existe deja pour cette session, ce n'est pas grave.
                if exc.status == 409:
                    log.debug("Abonnement %s deja actif.", label)
                else:
                    raise

    async def _dispatch(self, payload: dict) -> None:
        subscription_type = payload.get("subscription", {}).get("type")
        event = payload.get("event", {})
        log.info("Notification %s recue.", subscription_type)

        if subscription_type == "stream.online":
            log.info("LIVE detecte (demarre a %s).", event.get("started_at"))
            await self._on_online()
        elif subscription_type == "stream.offline":
            log.info("Fin du live.")
            await self._on_offline()
