"""Chef d'orchestre : detecte le live, garde la vue, dit bonjour.

Fonctionnement :
1. EventSub (WebSocket) previent Twitch -> nous, en quasi instantane, des
   evenements `stream.online` / `stream.offline`.
2. Un sondage Helix regulier sert de filet de securite si l'EventSub tombe.
3. Des qu'un live demarre, streamlink est lance en continue pour compter la vue.
4. Au tout debut du live, un message est envoye dans le chat.
"""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime, timezone

import aiohttp

from .auth import AuthError, Authenticator, TokenStore
from .chat import ChatClient
from .config import Config
from .eventsub import EventSubClient
from .log import get_logger, setup as setup_log
from .twitch_api import ApiError, Stream, TwitchApi
from .watcher import StreamWatcher

log = get_logger("bot")

REQUIRED_SCOPES = ["chat:read", "chat:edit"]

# Petit delai minimum avant d'envoyer le message d'accueil : la connexion au
# chat est deja prete (on la garde ouverte), donc presque rien a attendre.
GREET_DELAY = 1.0


class Bot:
    def __init__(
        self,
        cfg: Config,
        session: aiohttp.ClientSession,
        api: TwitchApi,
        auth: Authenticator,
        streamer_id: str,
        me_login: str,
    ) -> None:
        self._cfg = cfg
        self._session = session
        self._api = api
        self._auth = auth
        self._me = me_login

        self._watcher = StreamWatcher(
            cfg.streamer_login,
            cfg.watch_quality,
            is_live=self._is_stream_live,
            token_provider=auth.ensure_valid,
            on_problem=self._alert,
        )
        self._chat = ChatClient(
            me_login, token_provider=auth.ensure_valid, channel=cfg.streamer_login
        )
        self._eventsub = EventSubClient(
            session,
            api,
            streamer_id,
            on_online=self._on_online,
            on_offline=self._on_offline,
        )

        self._lock = asyncio.Lock()
        self._live = False
        self._greet_task: asyncio.Task | None = None

    # ------------------------------------------------------------------- etat

    @property
    def live(self) -> bool:
        return self._live

    async def run(self, stop_event: asyncio.Event) -> None:
        cfg = self._cfg
        log.info(
            "Surveillance de %s (sonde toutes les %ss, qualite streamlink : %s).",
            cfg.streamer_login,
            cfg.poll_interval,
            cfg.watch_quality,
        )

        await self._reconcile(initial=True)

        # On garde le chat connecte en permanence : le jour ou le live demarre,
        # le message part immediatement (pas de handshake a ce moment la).
        await self._ensure_chat()

        eventsub_task = asyncio.create_task(self._eventsub.run(), name="eventsub")
        if await self._eventsub.wait_until_connected(timeout=30):
            log.info("EventSub connecte : detection des lives quasi instantanee.")
        else:
            log.warning("EventSub non connecte, le sondage prend le relais.")

        tasks = [eventsub_task, asyncio.create_task(self._poll_loop(), name="poll")]
        stopper = asyncio.create_task(stop_event.wait(), name="stop")

        try:
            done, _ = await asyncio.wait(
                {*tasks, stopper}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is stopper:
                    log.info("Signal d'arret recu.")
                    continue
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    raise exc
        finally:
            for task in (*tasks, stopper):
                task.cancel()
            await asyncio.gather(*tasks, stopper, return_exceptions=True)
            self._eventsub.stop()

    async def shutdown(self) -> None:
        try:
            self._eventsub.stop()
            await self._go_offline()
            await self._chat.close()
        except Exception as exc:  # noqa: BLE001
            log.debug("Erreur pendant l'arret : %s", exc)
        log.info("Arrete proprement.")

    # ------------------------------------------------------------- evenements

    async def _on_online(self) -> None:
        stream: Stream | None
        try:
            stream = await self._api.get_stream(self._cfg.streamer_login)
        except (ApiError, AuthError) as exc:
            log.warning("Infos du live indisponibles (%s).", exc)
            stream = None
        await self._go_live(stream, force=True)

    async def _on_offline(self) -> None:
        await self._go_offline()

    async def _go_live(self, stream: Stream | None, *, force: bool) -> None:
        async with self._lock:
            if self._live:
                return
            self._live = True

        if stream is not None:
            log.info(
                "LIVE detecte : \"%s\" | %s | %s spectateurs | reaction en %.1fs",
                stream.title,
                stream.game or "?",
                stream.viewer_count,
                (datetime.now(timezone.utc) - stream.started_at).total_seconds(),
            )
        else:
            log.info("LIVE detecte.")

        # Le message part tout de suite (il est concu pour arriver au debut).
        if self._cfg.greeting_enabled:
            if self._should_greet(stream, force):
                self._greet_task = asyncio.create_task(self._greet(), name="greeting")
            else:
                log.info("Live deja commence, pas de message d'accueil.")
        else:
            log.info("Message d'accueil desactive.")

        try:
            await self._watcher.start()
            log.info("streamlink lance, en attente des premiers octets.")
        except (RuntimeError, OSError) as exc:
            log.error("Impossible de lancer streamlink : %s", exc)
            await self._alert(f"streamlink n'a pas demarre : {exc}")

    async def _go_offline(self) -> None:
        async with self._lock:
            if not self._live:
                return
            self._live = False

        log.info("Fin du live, on coupe le visionnage.")
        task = self._greet_task
        self._greet_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        # On garde volontairement le chat ouvert : comme ca le prochain
        # message d'accueil part sans attendre une reconnexion.
        await self._watcher.stop()

    async def _ensure_chat(self) -> None:
        """Garde une connexion chat prete (utile seulement pour le message)."""
        if not self._cfg.greeting_enabled or self._chat.connected:
            return
        try:
            await self._chat.connect()
        except (ConnectionError, OSError, AuthError, asyncio.TimeoutError) as exc:
            log.debug("Chat non connecte pour l'instant (%s).", exc)

    def _should_greet(self, stream: Stream | None, force: bool) -> bool:
        if force:
            return True
        if stream is None:
            return False
        max_age = self._cfg.greeting_max_age_minutes
        if max_age <= 0:
            return True
        age = datetime.now(timezone.utc) - stream.started_at
        return age.total_seconds() <= max_age * 60

    async def _greet(self) -> None:
        try:
            await asyncio.sleep(GREET_DELAY)
            await self._chat.send(self._cfg.greeting)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError, AuthError) as exc:
            log.warning("Message d'accueil non envoye : %s", exc)
            await self._alert(f"Message d'accueil non envoye : {exc}")

    # ---------------------------------------------------------------- sondage

    async def _poll_loop(self) -> None:
        interval = self._cfg.poll_interval
        while True:
            await asyncio.sleep(interval)
            await self._ensure_chat()
            try:
                stream = await self._api.get_stream(self._cfg.streamer_login)
            except AuthError as exc:
                log.error("Token Twitch inutilisable : %s", exc)
                await self._alert(f"Token Twitch inutilisable : {exc}")
                raise
            except ApiError as exc:
                log.warning("Sondage Helix en echec : %s", exc)
                continue

            if stream is not None and not self._live:
                log.info("Live detecte par sondage (EventSub muet ?).")
                await self._go_live(stream, force=False)
            elif stream is None and self._live:
                log.info("Live termine (vu par le sondage).")
                await self._go_offline()
            elif stream is not None:
                log.debug("Toujours en live (%s spectateurs).", stream.viewer_count)

    async def _reconcile(self, *, initial: bool = False) -> None:
        """Verifie ou on en est au demarrage (le live a pu commencer avant nous)."""
        try:
            stream = await self._api.get_stream(self._cfg.streamer_login)
        except (ApiError, AuthError) as exc:
            log.warning("Verification initiale impossible : %s", exc)
            return

        if stream is not None:
            log.info("Le live est deja en cours.")
            await self._go_live(stream, force=False)
        elif initial:
            log.info("%s n'est pas en live, on attend.", self._cfg.streamer_login)

    async def _is_stream_live(self) -> bool:
        try:
            return await self._api.get_stream(self._cfg.streamer_login) is not None
        except (ApiError, AuthError) as exc:
            log.warning("Verification du live impossible (%s), on garde le flux.", exc)
            return self._live

    # ---------------------------------------------------------------- alertes

    async def _alert(self, message: str) -> None:
        webhook = self._cfg.discord_webhook
        log.warning("%s", message)
        if not webhook:
            return
        payload = {"content": f"**Bot-Twitch** : {message}"[:1900]}
        try:
            async with self._session.post(webhook, json=payload) as resp:
                await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.debug("Alerte Discord non envoyee : %s", exc)


async def main() -> int:
    cfg = Config.load()
    setup_log(cfg.log_level, cfg.timezone)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError, ValueError):
            # Windows ne supporte pas add_signal_handler, Ctrl+C suffit.
            pass

    async with aiohttp.ClientSession() as session:
        store = TokenStore(cfg.token_file)
        auth = Authenticator(
            session,
            cfg.client_id,
            cfg.client_secret,
            store,
            bootstrap_refresh_token=(os.getenv("TWITCH_REFRESH_TOKEN") or "").strip()
            or None,
        )

        try:
            await auth.ensure_valid()
            await auth.validate_scopes(REQUIRED_SCOPES)
        except AuthError as exc:
            log.error("%s", exc)
            return 1

        me_login = auth.token.login
        if not me_login:
            log.error("Compte Twitch introuvable dans le token, relance get_token.py.")
            return 1

        api = TwitchApi(session, auth, cfg.client_id)
        streamer = await api.get_user(cfg.streamer_login)
        if streamer is None:
            log.error("Chaine \"%s\" introuvable sur Twitch.", cfg.streamer_login)
            return 1

        log.info(
            "Connecte en tant que %s, surveillance de %s (id %s).",
            me_login,
            streamer.display_name,
            streamer.id,
        )

        bot = Bot(cfg, session, api, auth, streamer.id, me_login)
        try:
            await bot.run(stop_event)
        except AuthError as exc:
            log.error("Arret : %s", exc)
            return 1
        finally:
            await bot.shutdown()

    return 0
