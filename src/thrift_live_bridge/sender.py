# SPDX-License-Identifier: AGPL-3.0-only
"""Pengirim: memindahkan kiriman dari antrian lokal ke Odoo.

Aturannya sengaja sederhana:

- Urutan dijaga. Kiriman tertua dikirim dulu, dan selama ia belum sampai,
  yang di belakangnya menunggu.
- Kiriman baru ditandai sampai SETELAH Odoo menjawab 200. Kalau bridge mati
  di antara keduanya, kiriman itu terkirim dua kali — dan Odoo melewati yang
  kedua, karena pin dikenali dari ID pesannya. Jadi: bisa terlambat, tidak
  hilang, tidak dobel.
- Kiriman yang isinya ditolak Odoo berulang kali disisihkan, supaya satu
  kiriman rusak tidak menyumbat seluruh antrian. Ia tidak dihapus.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .odoo import OdooDoor, Rejected, RetryLater
from .outbox import Outbox

_logger = logging.getLogger(__name__)

METHODS = {"open": "open_session", "push": "push", "close": "close_session"}

# Berapa kali isi kiriman boleh ditolak sebelum disisihkan. Lebih dari satu,
# karena penolakan pertama bisa akibat dua kiriman kembar yang berpapasan.
MAX_REJECTIONS = 3
# Jeda coba-ulang saat Odoo tidak bisa dicapai: berlipat dua sampai batasnya.
BACKOFF_START = 1.0
BACKOFF_MAX = 60.0
# Jeda memeriksa antrian saat tidak ada masalah.
POLL_INTERVAL = 1.0
# Seberapa sering kiriman lama yang sudah sampai dibersihkan.
PURGE_INTERVAL = 3600.0


@dataclass
class Round:
    """Hasil satu putaran kirim."""
    sent: int = 0
    rejected: int = 0
    # Terisi kalau putaran berhenti karena kiriman terdepan belum bisa sampai.
    blocked: str | None = None


class Sender:
    def __init__(self, outbox: Outbox, door: OdooDoor, *, keep_sent_days: int | None = None):
        self._outbox = outbox
        self._door = door
        # None = kiriman yang sudah sampai tidak pernah dibersihkan.
        self._keep_sent_days = keep_sent_days
        self._wake = asyncio.Event()
        self._was_blocked = False

    def wake(self) -> None:
        """Beri tahu pengirim bahwa ada kiriman baru, tanpa menunggu jedanya."""
        self._wake.set()

    async def send_pending(self) -> Round:
        """Kirim sampai antrian kosong, atau sampai kiriman terdepan tertahan."""
        result = Round()
        while True:
            item = self._outbox.next_pending()
            if item is None:
                return result
            try:
                await self._door.call(METHODS[item.kind], item.payload)
            except Rejected as exc:
                if item.rejections + 1 >= MAX_REJECTIONS:
                    self._outbox.mark_rejected(item.id, str(exc))
                    result.rejected += 1
                    _logger.error(
                        "Kiriman #%s (%s) disisihkan setelah %s kali ditolak: %s",
                        item.id, item.kind, MAX_REJECTIONS, exc,
                    )
                    continue
                self._outbox.mark_retry(item.id, str(exc), rejection=True)
                result.blocked = str(exc)
                return result
            except RetryLater as exc:
                self._outbox.mark_retry(item.id, str(exc))
                result.blocked = str(exc)
                return result
            self._outbox.mark_sent(item.id)
            result.sent += 1

    async def run(self, stop: asyncio.Event) -> None:
        """Kirim terus sampai `stop` dinyalakan."""
        delay = BACKOFF_START
        next_purge = 0.0
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            if self._keep_sent_days is not None and loop.time() >= next_purge:
                self._outbox.purge_sent(self._keep_sent_days)
                next_purge = loop.time() + PURGE_INTERVAL
            result = await self.send_pending()
            self._report(result)
            if result.blocked:
                # Selama tertahan, kiriman baru tidak mempercepat coba-ulang:
                # Odoo yang sedang mati tidak perlu diketuk lebih sering.
                await self._sleep(stop, delay, wakeable=False)
                delay = min(delay * 2, BACKOFF_MAX)
            else:
                delay = BACKOFF_START
                await self._sleep(stop, POLL_INTERVAL, wakeable=True)

    def _report(self, result: Round) -> None:
        """Catat perubahan keadaan saja, bukan setiap percobaan."""
        if result.blocked and not self._was_blocked:
            pending = self._outbox.counts()["pending"]
            _logger.warning("Tertahan: %s. %s kiriman menunggu.", result.blocked, pending)
        elif not result.blocked and self._was_blocked:
            _logger.info("Tersambung lagi. %s kiriman tertunda terkirim.", result.sent)
        elif result.sent:
            _logger.info("%s kiriman terkirim.", result.sent)
        self._was_blocked = bool(result.blocked)

    async def _sleep(self, stop: asyncio.Event, seconds: float, *, wakeable: bool) -> None:
        waiters = [asyncio.ensure_future(stop.wait())]
        if wakeable:
            waiters.append(asyncio.ensure_future(self._wake.wait()))
        try:
            await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
            self._wake.clear()
