# SPDX-License-Identifier: AGPL-3.0-only
"""Pembaca: mengikuti siaran satu akun dan menaruh kejadiannya ke antrian lokal.

Siklusnya:

    menunggu live -> tersambung -> (pin, pin, ...) -> siaran berakhir -> menunggu live

Aturan sesi:

- Sesi dibuka saat tersambung, dengan referensi siaran dari sumbernya.
- Sesi ditutup saat host mengakhiri siaran, atau saat sambungan putus DAN
  sumbernya memastikan akun itu sudah tidak siaran. Putus saja bukan alasan
  menutup: host mungkin masih siaran dan kita yang kehilangan sambungan.
- Sesi TIDAK ditutup saat bridge dihentikan. Siaran yang sedang diikuti
  dicatat di antrian lokal, jadi bridge yang dinyalakan lagi melanjutkannya,
  atau menutupnya kalau siarannya ternyata sudah berakhir.

Aturan menyambung ulang: jeda mulai 5 detik dan berlipat sampai 60 detik;
setelah 10 kali gagal berturut-turut, satu percobaan tiap 10 menit. Tiap
percobaan bisa memakai satu permintaan tanda tangan, jadi angkanya dijaga
supaya akun yang bermasalah seharian pun tidak menghabiskan jatah bersama.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass

from .outbox import Outbox
from .source import Connected, Ended, LiveSource, NotLive, Pin, RateLimited, SourceError

_logger = logging.getLogger(__name__)

RECONNECT_START = 5.0
RECONNECT_MAX = 60.0
LONG_PAUSE_AFTER = 10
LONG_PAUSE = 600.0
# Batas atas menunggu atas permintaan penyedia tanda tangan.
RATE_LIMIT_MAX = 3600.0
# Kunci catatan "siaran yang sesinya sedang terbuka" di antrian lokal.
OPEN_ROOM = "open_room"


def reconnect_delay(failures: int) -> float:
    """Jeda sebelum percobaan berikutnya, setelah `failures` kali gagal berturut-turut."""
    if failures >= LONG_PAUSE_AFTER:
        return LONG_PAUSE
    return min(RECONNECT_START * 2 ** (max(failures, 1) - 1), RECONNECT_MAX)


@dataclass
class _Outcome:
    """Cara satu sambungan berakhir."""
    connected: bool = False
    ended: bool = False
    not_live: bool = False
    error: str | None = None
    retry_after: float = 0.0


class Reader:
    def __init__(
        self,
        source: LiveSource,
        outbox: Outbox,
        *,
        account: str,
        wake: Callable[[], None] | None = None,
        poll_seconds: float = 60.0,
    ):
        self._source = source
        self._outbox = outbox
        self._account = account
        self._wake = wake or (lambda: None)
        self._poll_seconds = poll_seconds
        self._waiting_logged = False

    async def run(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            live = await self._check_live()
            if live is not True:
                if live is False:
                    # Pasti tidak siaran: sesi yang masih tercatat terbuka
                    # (dari sambungan yang putus, atau dari sebelum bridge
                    # dihentikan) ditutup di sini.
                    self._close_room("siaran sudah berakhir")
                    failures = 0
                    if not self._waiting_logged:
                        _logger.info(
                            "Menunggu @%s siaran (diperiksa tiap %.0f detik).",
                            self._account, self._poll_seconds,
                        )
                        self._waiting_logged = True
                await self._pause(stop, self._poll_seconds)
                continue

            self._waiting_logged = False
            outcome = await self._follow(stop)
            if stop.is_set():
                break
            if outcome.ended or outcome.not_live:
                failures = 0
                await self._pause(stop, self._poll_seconds)
                continue

            # Putus di tengah, atau gagal menyambung. Sambungan yang sempat
            # berhasil memulai hitungan dari awal.
            failures = 1 if outcome.connected else failures + 1
            delay = max(reconnect_delay(failures), min(outcome.retry_after, RATE_LIMIT_MAX))
            if failures == LONG_PAUSE_AFTER:
                _logger.error(
                    "@%s: %s kali gagal menyambung berturut-turut. Selanjutnya dicoba "
                    "tiap %.0f menit. Selama itu pin tidak terbaca; pakai input manual.",
                    self._account, failures, LONG_PAUSE / 60,
                )
            _logger.warning(
                "@%s: %s. Dicoba lagi dalam %.0f detik (gagal ke-%s).",
                self._account, outcome.error or "sambungan putus", delay, failures,
            )
            await self._pause(stop, delay)

    async def _check_live(self) -> bool | None:
        """True/False kalau pasti; None kalau sumbernya tidak bisa memastikan."""
        try:
            return await self._source.is_live()
        except SourceError as exc:
            _logger.warning("@%s: status siaran tidak bisa dipastikan (%s).", self._account, exc)
            return None

    async def _follow(self, stop: asyncio.Event) -> _Outcome:
        """Ikuti satu sambungan sampai berakhir."""
        outcome = _Outcome()
        room: str | None = None
        try:
            # aclosing: sambungan diputus juga kalau pembaca berhenti di tengah.
            async with contextlib.aclosing(self._source.listen(stop)) as events:
                async for event in events:
                    if isinstance(event, Connected):
                        room = event.room_ref
                        outcome.connected = True
                        self._open_room(room)
                    elif isinstance(event, Pin) and room is not None:
                        self._queue_pin(room, event.row)
                    elif isinstance(event, Ended):
                        outcome.ended = True
                        self._close_room("host mengakhiri siaran")
        except NotLive:
            outcome.not_live = True
        except RateLimited as exc:
            outcome.error = f"diminta menunggu oleh penyedia tanda tangan ({exc})"
            outcome.retry_after = exc.retry_after
        except SourceError as exc:
            outcome.error = str(exc)
        return outcome

    # -- catatan ke antrian lokal -------------------------------------------

    def _open_room(self, room: str) -> None:
        previous = self._outbox.get_meta(OPEN_ROOM)
        if previous and previous != room:
            # Host memulai siaran baru; yang lama sudah pasti berakhir.
            self._outbox.enqueue_close(previous)
        # Catatan ditulis sebelum kirimannya. Kalau bridge mati di antaranya,
        # paling buruk ada "tutup sesi" untuk sesi yang belum pernah dibuka,
        # dan Odoo menjawab itu dengan tenang.
        self._outbox.set_meta(OPEN_ROOM, room)
        self._outbox.enqueue_open(room, self._account)
        self._wake()
        _logger.info("@%s: tersambung ke siaran %s.", self._account, room)

    def _queue_pin(self, room: str, row: dict) -> None:
        ref = row.get("external_message_ref", "tanpa ID")
        if self._outbox.enqueue_pin(room, row, self._account) is None:
            _logger.debug("@%s: pin %s sudah pernah masuk; dilewati.", self._account, ref)
            return
        self._wake()
        _logger.info("@%s: pin %s masuk antrian.", self._account, ref)

    def _close_room(self, reason: str) -> None:
        room = self._outbox.get_meta(OPEN_ROOM)
        if not room:
            return
        self._outbox.enqueue_close(room)
        self._outbox.set_meta(OPEN_ROOM, None)
        self._wake()
        _logger.info("@%s: sesi siaran %s ditutup (%s).", self._account, room, reason)

    @staticmethod
    async def _pause(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)
