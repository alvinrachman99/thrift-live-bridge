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
import time
from collections.abc import Callable
from dataclasses import dataclass

from .logs import labelled
from .outbox import Outbox, utc_now
from .source import Connected, Ended, LiveSource, NotLive, Pin, RateLimited, SourceError

_logger = logging.getLogger(__name__)

RECONNECT_START = 5.0
RECONNECT_MAX = 60.0
LONG_PAUSE_AFTER = 10
LONG_PAUSE = 600.0
# Batas atas menunggu atas permintaan penyedia tanda tangan.
RATE_LIMIT_MAX = 3600.0
# Setelah sekian kali berturut-turut keadaan siaran tidak terbaca, dicatat
# sebagai galat: selama itu siaran yang baru mulai tidak akan terlihat.
CHECK_ERROR_AFTER = 10
# Awalan kunci catatan "siaran yang sesinya sedang terbuka" di antrian lokal.
ROOM_PREFIX = "open_room:"


def room_key(account: str) -> str:
    """Kunci catatan itu untuk satu akun.

    Per akun, bukan per client: satu client boleh mengikuti lebih dari satu
    akun, dan siaran akun yang satu tidak boleh menutup sesi akun yang lain.
    """
    return ROOM_PREFIX + account.lstrip("@").strip().lower()


def duration_text(seconds: float) -> str:
    """Lama dalam kata, untuk log: '45 detik', '12 menit', '3 jam 5 menit'."""
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds} detik"
    hours, minutes = divmod(seconds // 60, 60)
    if not hours:
        return f"{minutes} menit"
    return f"{hours} jam {minutes} menit" if minutes else f"{hours} jam"


def _detail_suffix(detail: str | None) -> str:
    """Rincian galat untuk ditempel di akhir baris log; kosong kalau tidak ada."""
    return f" Rincian: {detail}" if detail else ""


def reconnect_delay(failures: int) -> float:
    """Jeda sebelum percobaan berikutnya, setelah `failures` kali gagal berturut-turut."""
    if failures >= LONG_PAUSE_AFTER:
        return LONG_PAUSE
    return min(RECONNECT_START * 2 ** (max(failures, 1) - 1), RECONNECT_MAX)


@dataclass
class Status:
    """Keadaan pembaca saat ini, untuk dilaporkan ke Odoo (heartbeat.py)."""
    state: str = "waiting"          # waiting / connected / retrying
    room: str | None = None         # siaran yang sesinya sedang terbuka
    failures: int = 0               # gagal menyambung berturut-turut
    message: str | None = None      # alasan singkat saat tidak sehat
    last_event: str | None = None   # waktu pin terakhir (UTC)


@dataclass
class _Outcome:
    """Cara satu sambungan berakhir."""
    connected: bool = False
    ended: bool = False
    not_live: bool = False
    error: str | None = None
    detail: str = ""                # galat aslinya, untuk log saja
    retry_after: float = 0.0


class Reader:
    def __init__(
        self,
        source: LiveSource,
        outbox: Outbox,
        *,
        account: str,
        wake: Callable[[], None] | None = None,
        on_status: Callable[[], None] | None = None,
        poll_seconds: float = 60.0,
        label: str | None = None,
    ):
        self._source = source
        self._outbox = outbox
        # '@' di depan dibuang di sini juga, supaya pemanggil mana pun
        # menghasilkan nama akun yang sama.
        self._account = account.lstrip("@").strip()
        self._room_key = room_key(account)
        self._on_status = on_status or (lambda: None)
        self.status = Status(room=outbox.get_meta(self._room_key))
        self._log = labelled(_logger, label)
        self._wake = wake or (lambda: None)
        self._poll_seconds = poll_seconds
        self._waiting_logged = False
        # Pemeriksaan keadaan siaran yang gagal berturut-turut, dan sejak kapan.
        self._check_failures = 0
        self._check_failed_since = 0.0

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
                    self._set_status("waiting")
                    if not self._waiting_logged:
                        self._log.info(
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
                self._set_status("waiting")
                await self._pause(stop, self._poll_seconds)
                continue

            # Putus di tengah, atau gagal menyambung. Sambungan yang sempat
            # berhasil memulai hitungan dari awal.
            failures = 1 if outcome.connected else failures + 1
            delay = max(reconnect_delay(failures), min(outcome.retry_after, RATE_LIMIT_MAX))
            self._set_status("retrying", failures=failures, message=outcome.error or "sambungan putus")
            if failures == LONG_PAUSE_AFTER:
                self._log.error(
                    "@%s: %s kali gagal menyambung berturut-turut. Selanjutnya dicoba "
                    "tiap %.0f menit. Selama itu pin tidak terbaca; pakai input manual.",
                    self._account, failures, LONG_PAUSE / 60,
                )
            self._log.warning(
                "@%s: %s. Dicoba lagi dalam %.0f detik (gagal ke-%s).%s",
                self._account, outcome.error or "sambungan putus", delay, failures,
                _detail_suffix(outcome.detail),
            )
            await self._pause(stop, delay)

    async def _check_live(self) -> bool | None:
        """True/False kalau pasti; None kalau sumbernya tidak bisa memastikan."""
        try:
            live = await self._source.is_live()
        except SourceError as exc:
            if not self._check_failures:
                self._check_failed_since = time.monotonic()
            self._check_failures += 1
            # Dicatat sekali per sebab, bukan tiap kali diperiksa: akun yang
            # salah ketik tidak boleh mengisi log satu baris per menit.
            repeated = (self.status.state, self.status.message) == ("retrying", str(exc))
            log = self._log.debug if repeated else self._log.warning
            log(
                "@%s: status siaran tidak bisa dipastikan (%s).%s",
                self._account, exc, _detail_suffix(exc.detail),
            )
            if self._check_failures == CHECK_ERROR_AFTER:
                self._log.error(
                    "@%s: status siaran tidak terbaca %s kali berturut-turut (%s). Selama "
                    "ini siaran yang baru mulai tidak akan terlihat; pakai input manual.",
                    self._account, self._check_failures,
                    duration_text(time.monotonic() - self._check_failed_since),
                )
            self._set_status("retrying", failures=self.status.failures, message=str(exc))
            return None
        if self._check_failures:
            # Tanpa baris ini lamanya bridge "buta" tidak bisa dihitung dari log.
            self._log.info(
                "@%s: status siaran terbaca lagi setelah %s kali gagal berturut-turut (%s).",
                self._account, self._check_failures,
                duration_text(time.monotonic() - self._check_failed_since),
            )
            self._check_failures = 0
        return live

    def _set_status(self, state: str, *, failures: int = 0, message: str | None = None) -> None:
        """Perbarui keadaan; pelapor dibangunkan hanya kalau ada yang berubah."""
        room = self._outbox.get_meta(self._room_key)
        before = (self.status.state, self.status.room, self.status.failures, self.status.message)
        self.status.state = state
        self.status.room = room
        self.status.failures = failures
        self.status.message = message
        if before != (state, room, failures, message):
            self._on_status()

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
            outcome.error = str(exc)
            outcome.detail = exc.detail
            outcome.retry_after = exc.retry_after
        except SourceError as exc:
            outcome.error = str(exc)
            outcome.detail = exc.detail
        return outcome

    # -- catatan ke antrian lokal -------------------------------------------

    def _open_room(self, room: str) -> None:
        previous = self._outbox.get_meta(self._room_key)
        if previous and previous != room:
            # Host memulai siaran baru; yang lama sudah pasti berakhir.
            self._outbox.enqueue_close(previous)
        # Catatan ditulis sebelum kirimannya. Kalau bridge mati di antaranya,
        # paling buruk ada "tutup sesi" untuk sesi yang belum pernah dibuka,
        # dan Odoo menjawab itu dengan tenang.
        self._outbox.set_meta(self._room_key, room)
        self._outbox.enqueue_open(room, self._account)
        self._wake()
        self._set_status("connected")
        self._log.info("@%s: tersambung ke siaran %s.", self._account, room)

    def _queue_pin(self, room: str, row: dict) -> None:
        ref = row.get("external_message_ref", "tanpa ID")
        if self._outbox.enqueue_pin(room, row, self._account) is None:
            self._log.debug("@%s: pin %s sudah pernah masuk; dilewati.", self._account, ref)
            return
        self._wake()
        # Ikut laporan berikutnya; satu pin tidak perlu laporan tersendiri.
        self.status.last_event = row.get("date") or utc_now()
        self._log.info("@%s: pin %s masuk antrian.", self._account, ref)

    def _close_room(self, reason: str) -> None:
        room = self._outbox.get_meta(self._room_key)
        if not room:
            return
        self._outbox.enqueue_close(room)
        self._outbox.set_meta(self._room_key, None)
        self._wake()
        self._log.info("@%s: sesi siaran %s ditutup (%s).", self._account, room, reason)

    @staticmethod
    async def _pause(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)
