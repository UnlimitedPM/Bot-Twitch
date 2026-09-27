"""Maintien de la vue : telecharge le flux en continu tant que le live dure.

Un spectateur est compte par Twitch quand une session consomme le flux.
On lance donc streamlink (qualite configurable, "audio_only" par defaut, tres leger)
et on le surveille : s'il meurt alors que le live continue, on le relance.

La vue est prise avec ton compte : on passe son access token a streamlink via
``--twitch-api-header Authorization=Bearer <token>``. Attention au prefixe, en
``OAuth`` l'API interne de Twitch repond "The Authorization token is invalid" et
streamlink tourne alors a vide (0 octet). Si le token est refuse, on repasse
automatiquement en anonyme pour ne pas perdre la vue.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Awaitable, Callable

from .log import get_logger

log = get_logger("watcher")

# Fichier de config streamlink qui porte le token. En argument de commande, le
# token apparaitrait en clair dans "ps" / "docker top".
CONFIG_PATH = Path(tempfile.gettempdir()) / "streamlink-twitch.conf"

# Si aucun octet ne descend pendant ce delai alors que le live est en cours,
# on considere la connexion morte et on relance.
STALL_TIMEOUT = 120.0

# Lancements consecutifs sans le moindre octet avant de prevenir (et de
# fortement ralentir les relances) : signe d'un vrai probleme, pas d'un hoquet.
DEAD_RUNS_BEFORE_ALERT = 3
DEAD_RUNS_COOLDOWN = 300


class StreamWatcher:
    def __init__(
        self,
        login: str,
        quality: str,
        is_live: Callable[[], Awaitable[bool]],
        token_provider: Callable[[], Awaitable[str]] | None = None,
        on_problem: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._login = login
        self._quality = quality
        self._is_live = is_live
        self._token_provider = token_provider
        self._use_auth = token_provider is not None
        self._on_problem = on_problem
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._proc: asyncio.subprocess.Process | None = None
        self._last_data = 0.0
        self._total_bytes = 0

    @property
    def active(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.active:
            return
        if shutil.which("streamlink") is None:
            raise RuntimeError(
                "streamlink introuvable. Installe-le : pip install streamlink"
            )
        self._stop.clear()
        # Chaque live repart connecte au compte : on ne reste en anonyme que si le
        # token a vraiment ete refuse pendant le live precedent.
        self._use_auth = self._token_provider is not None
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stop.set()
        await self._kill()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        self._remove_config()

    # ------------------------------------------------------------------ interne

    async def _supervise(self) -> None:
        consecutive_failures = 0
        dead_runs = 0
        alerted = False
        while not self._stop.is_set():
            started = time.monotonic()
            stderr_tail: deque[str] = deque(maxlen=10)
            try:
                proc = await self._spawn()
            except Exception as exc:  # noqa: BLE001
                log.warning("Lancement de streamlink impossible : %s", exc)
                await self._sleep(15)
                continue

            self._proc = proc
            self._last_data = time.monotonic()
            self._total_bytes = 0
            stdout_task = asyncio.create_task(self._drain_stdout(proc))
            stderr_task = asyncio.create_task(self._drain_stderr(proc, stderr_tail))
            monitor = asyncio.create_task(self._stall_monitor(proc))

            log.info(
                "Vue demarree sur %s (qualite %s, %s).",
                self._login,
                self._quality,
                "connecte au compte" if self._use_auth else "anonyme",
            )
            await proc.wait()
            self._proc = None

            for task in (stdout_task, stderr_task, monitor):
                task.cancel()
            await asyncio.gather(stdout_task, stderr_task, monitor, return_exceptions=True)

            if self._stop.is_set():
                break

            duration = time.monotonic() - started
            megabytes = self._total_bytes / 1024 / 1024
            if self._total_bytes == 0:
                # Rien n'est descendu : on dit tout de suite pourquoi, sinon
                # l'echec reste invisible (les logs streamlink sont en DEBUG).
                log.warning(
                    "streamlink termine apres %.0fs sans le moindre octet "
                    "(qualite %s, code %s).",
                    duration,
                    self._quality,
                    proc.returncode,
                )
                if stderr_tail:
                    log.warning(
                        "Dernieres lignes streamlink : %s", " | ".join(stderr_tail)
                    )
                if self._use_auth:
                    # Le token est refuse (ou le flux bloque avant tout octet) :
                    # on prefere une vue anonyme a aucune vue du tout.
                    self._use_auth = False
                    self._remove_config()
                    await self._problem(
                        "Aucun octet recu avec le token du compte : on repasse en "
                        "anonyme pour ne pas perdre la vue."
                    )
            else:
                log.info(
                    "streamlink termine apres %.0fs (%.1f Mo recuperes, code %s).",
                    duration,
                    megabytes,
                    proc.returncode,
                )
                if stderr_tail:
                    log.debug("Dernieres lignes streamlink : %s", " | ".join(stderr_tail))

            if not await self._still_live():
                log.info("Le live est fini, on arrete la vue.")
                break

            if duration >= 30 and self._total_bytes > 0:
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            dead_runs = dead_runs + 1 if self._total_bytes == 0 else 0

            if dead_runs >= DEAD_RUNS_BEFORE_ALERT:
                if not alerted:
                    alerted = True
                    reason = stderr_tail[-1] if stderr_tail else f"code {proc.returncode}"
                    await self._problem(
                        f"streamlink ne recoit rien depuis {dead_runs} lancements "
                        f"({reason}). La vue n'est pas comptee."
                    )
            else:
                alerted = False

            delay = min(5 * consecutive_failures, 60)
            if dead_runs >= DEAD_RUNS_BEFORE_ALERT:
                # Inutile de marteler streamlink toutes les 2 minutes quand rien
                # ne passe : on espace les tentatives.
                delay = max(delay, DEAD_RUNS_COOLDOWN)
            log.warning("Live toujours en cours, relance de la vue dans %sd.", delay)
            await self._sleep(delay)

    async def _spawn(self) -> asyncio.subprocess.Process:
        # Le prefixe doit etre "Bearer" : en "OAuth", Twitch repond
        # "The Authorization token is invalid" et streamlink tourne a vide.
        # Pas de --retry-streams non plus : en cas d'erreur reelle streamlink doit
        # sortir vite (sinon il tourne a vide), c'est la supervision qui relance.
        cmd = ["streamlink", "--loglevel", "info"]
        if self._use_auth and self._token_provider is not None:
            token = await self._token_provider()
            self._write_config(token)
            cmd += ["--config", str(CONFIG_PATH)]
        cmd += [f"https://www.twitch.tv/{self._login}", self._quality, "--stdout"]
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def _write_config(self, token: str) -> None:
        """Ecrit la config streamlink avec le token (droits 600, nous seulement)."""
        descriptor = os.open(CONFIG_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(f"twitch-api-header=Authorization=Bearer {token}\n")

    def _remove_config(self) -> None:
        try:
            CONFIG_PATH.unlink(missing_ok=True)
        except OSError as exc:  # noqa: BLE001
            log.debug("Config streamlink non supprimee : %s", exc)

    async def _drain_stdout(self, proc: asyncio.subprocess.Process) -> None:
        """Consomme les octets du flux (c'est ca qui compte comme une vue)."""
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(64 * 1024)
            if not chunk:
                return
            if self._total_bytes == 0:
                log.info("Premiers octets recus : la vue est comptee.")
            self._last_data = time.monotonic()
            self._total_bytes += len(chunk)

    async def _drain_stderr(
        self, proc: asyncio.subprocess.Process, tail: deque[str]
    ) -> None:
        assert proc.stderr is not None
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            text = line.decode("utf-8", "replace").strip()
            if text:
                tail.append(text)
                log.debug("streamlink: %s", text)

    async def _stall_monitor(self, proc: asyncio.subprocess.Process) -> None:
        while True:
            await asyncio.sleep(30)
            if self._stop.is_set() or proc.returncode is not None:
                return
            silence = time.monotonic() - self._last_data
            if silence > STALL_TIMEOUT:
                log.warning("Flux bloque depuis %.0fs, on relance.", silence)
                proc.terminate()
                return

    async def _problem(self, message: str) -> None:
        """Erreur qui merite une alerte (Discord si configure)."""
        log.error("%s", message)
        if self._on_problem is None:
            return
        try:
            await self._on_problem(message)
        except Exception as exc:  # noqa: BLE001
            log.debug("Alerte non envoyee : %s", exc)

    async def _still_live(self) -> bool:
        try:
            return await self._is_live()
        except Exception as exc:  # noqa: BLE001
            log.warning("Verification du live impossible (%s), on continue.", exc)
            return True

    async def _kill(self) -> None:
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), 10)
        except asyncio.TimeoutError:
            proc.kill()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass
