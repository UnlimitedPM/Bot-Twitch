"""Connexion au chat Twitch (IRC) : sert a poster le message d'accueil.

Le chat est aussi une facon de rester "present" dans le channel pendant le live.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from .log import get_logger

log = get_logger("chat")

HOST = "irc.chat.twitch.tv"
PORT = 6697


class ChatClient:
    def __init__(
        self,
        login: str,
        token_provider: Callable[[], Awaitable[str]],
        channel: str,
    ) -> None:
        self._login = login.lower()
        self._channel = channel.lower().lstrip("#")
        self._token_provider = token_provider
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task | None = None
        self._welcome = asyncio.Event()
        self._auth_error: str | None = None

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def connect(self, timeout: float = 20.0) -> None:
        if self.connected:
            return
        await self.close()

        token = await self._token_provider()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HOST, PORT, ssl=True), timeout
        )
        self._reader = reader
        self._writer = writer
        self._welcome = asyncio.Event()
        self._auth_error = None

        self._reader_task = asyncio.create_task(self._pump())

        await self._write(
            "CAP REQ :twitch.tv/tags twitch.tv/commands\r\n"
            f"PASS oauth:{token}\r\n"
            f"NICK {self._login}\r\n"
        )

        # On attend le message de bienvenue (001) avant de faire quoi que ce soit.
        try:
            await asyncio.wait_for(self._welcome.wait(), timeout)
        except asyncio.TimeoutError as exc:
            await self.close()
            raise ConnectionError("pas de reponse du chat Twitch") from exc

        if self._auth_error:
            await self.close()
            raise ConnectionError(self._auth_error)

        await self._write(f"JOIN #{self._channel}\r\n")
        log.info("Connecte au chat de #%s en tant que %s.", self._channel, self._login)

    async def send(self, message: str) -> None:
        """Envoie un message dans le channel (reconnexion automatique si besoin)."""
        if not message.strip():
            return
        if len(message) > 450:
            log.warning("Message trop long, il sera tronque par Twitch.")

        for attempt in (1, 2):
            try:
                if not self.connected:
                    await self.connect()
                await self._write(f"PRIVMSG #{self._channel} :{message}\r\n")
                log.info("Message envoye : %s", message)
                return
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                log.warning("Envoi du message impossible (%s), essai %s/2.", exc, attempt)
                await self.close()
                await asyncio.sleep(2)

        raise ConnectionError("impossible d'envoyer le message dans le chat")

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._reader_task = None

        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is None:
            return
        try:
            writer.close()
            await writer.wait_closed()
        except (OSError, asyncio.TimeoutError):
            pass

    async def _write(self, payload: str) -> None:
        writer = self._writer
        if writer is None:
            raise ConnectionError("chat non connecte")
        writer.write(payload.encode("utf-8"))
        await writer.drain()

    async def _pump(self) -> None:
        """Lit le chat en fond pour repondre aux PING (sinon Twitch coupe)."""
        reader = self._reader
        assert reader is not None
        try:
            while True:
                line = await reader.readline()
                if not line:
                    raise ConnectionError("connexion IRC fermee")
                text = line.decode("utf-8", "replace").rstrip()
                if text.startswith("PING"):
                    await self._write("PONG :tmi.twitch.tv\r\n")
                elif " 001 " in text:
                    self._welcome.set()
                elif "Login authentication failed" in text or "Error logging in" in text:
                    self._auth_error = f"authentification du chat refusee : {text}"
                    self._welcome.set()
                elif " NOTICE " in text:
                    log.warning("Twitch : %s", text)
                else:
                    log.debug("chat: %s", text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("Connexion chat perdue (%s).", exc)
            self._writer = None
            self._reader = None
