"""Maintien de la vue : telecharge le flux en continu tant que le live dure.

Un spectateur est compte par Twitch quand une session consomme le flux.
On lance donc streamlink (qualite configurable, "audio_only" par defaut, tres leger)
et on le surveille : s'il meurt alors que le live continue, on le relance.
"""

from __future__ import annotations

import asyncio
import shutil
import time
from collections import deque
from typing import Awaitable, Callable

from .log import get_logger

log = get_logger("watcher")

# Si aucun octet ne descend pendant ce delai alors que le live est en cours,
# on considere la connexion morte et on relance.
STALL_TIMEOUT = 120.0


class StreamWatcher:
    def __init__(
        self,
        login: str,
        quality: str,
        token_provider: Callable[[], Awaitable[str]],
        is_live: Callable[[], Awaitable[bool]],
    ) -> None:
        self._login = login
        self._quality = quality
        self._token_provider = token_provider
        self._is_live = is_live
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

    # ------------------------------------------------------------------ interne

    async def _supervise(self) -> None:
        consecutive_failures = 0
        while not self._stop.is_set():
            started = time.monotonic()
            stderr_tail: deque[str] = deque(maxlen=10)
            try:
                proc = await self._spawn(stderr_tail)
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

            log.info("Vue demarree sur %s (qualite %s).", self._login, self._quality)
            await proc.wait()
            self._proc = None

            for task in (stdout_task, stderr_task, monitor):
                task.cancel()
            await asyncio.gather(stdout_task, stderr_task, monitor, return_exceptions=True)

            if self._stop.is_set():
                break

            duration = time.monotonic() - started
            megabytes = self._total_bytes / 1024 / 1024
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

            if duration >= 30:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
            delay = min(5 * consecutive_failures, 60)
            log.warning("Live toujours en cours, relance de la vue dans %sd.", delay)
            await self._sleep(delay)

    async def _spawn(self, stderr_tail: deque[str]) -> asyncio.subprocess.Process:
        token = await self._token_provider()
        cmd = [
            "streamlink",
            "--twitch-api-header",
            f"Authorization=OAuth {token}",
            "--loglevel",
            "warning",
            "--retry-streams",
            "2",
            f"https://www.twitch.tv/{self._login}",
            self._quality,
            "--stdout",
        ]
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def _drain_stdout(self, proc: asyncio.subprocess.Process) -> None:
        """Consomme les octets du flux (c'est ca qui compte comme une vue)."""
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(64 * 1024)
            if not chunk:
                return
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
