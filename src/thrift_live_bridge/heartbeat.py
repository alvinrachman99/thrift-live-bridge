# SPDX-License-Identifier: AGPL-3.0-only
"""Laporan berkala ke Odoo: bridge masih hidup, dan sedang apa.

Odoo menampilkannya di menu Status Bot, supaya admin bisa membedakan "host
belum menyematkan apa-apa" dari "bot-nya terputus".

Laporan ini sengaja TIDAK lewat antrian lokal. Kalau Odoo tidak bisa
dihubungi, laporannya dilewati: dari sisi Odoo bridge memang tidak terdengar,
dan laporan basi yang dikirim belakangan justru menyesatkan.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from .logs import labelled
from .odoo import DoorError, OdooDoor

_logger = logging.getLogger(__name__)

# Odoo menganggap bridge tidak terdengar setelah 3 menit tanpa laporan.
HEARTBEAT_SECONDS = 60.0
# Perubahan keadaan dilaporkan segera, tapi tidak lebih rapat dari ini.
MIN_GAP = 2.0


class Heartbeat:
    def __init__(
        self,
        door: OdooDoor,
        account: str,
        get_status: Callable,
        *,
        label: str | None = None,
    ):
        self._door = door
        self._account = account
        self._get_status = get_status
        self._log = labelled(_logger, label)
        self._wake = asyncio.Event()
        self._failing = False

    def wake(self) -> None:
        """Keadaan berubah: laporkan tanpa menunggu jadwal berikutnya."""
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.report()
            except Exception:  # noqa: BLE001 - laporan tidak boleh menghentikan pembacaan
                self._log.exception("Galat tak terduga saat melapor status.")
            if await self._sleep(stop, HEARTBEAT_SECONDS):
                # Dibangunkan lebih awal: beri jeda supaya perubahan beruntun
                # menjadi satu laporan.
                await self._sleep(stop, MIN_GAP, wakeable=False)

    async def report(self) -> bool:
        status = self._get_status()
        params = {
            "external_account_ref": self._account,
            "state": status.state,
            "failures": status.failures,
        }
        for key, value in (
            ("external_ref", status.room),
            ("last_event", status.last_event),
            ("message", status.message),
        ):
            if value:
                params[key] = value
        try:
            await self._door.call("heartbeat", params)
        except DoorError as exc:
            if not self._failing:
                self._log.warning("Laporan status belum sampai ke Odoo (%s).", exc)
            self._failing = True
            return False
        if self._failing:
            self._log.info("Laporan status sampai lagi ke Odoo.")
        self._failing = False
        return True

    async def _sleep(self, stop: asyncio.Event, seconds: float, *, wakeable: bool = True) -> bool:
        """Tunggu; mengembalikan True kalau dibangunkan sebelum waktunya."""
        waiters = [asyncio.ensure_future(stop.wait())]
        if wakeable:
            waiters.append(asyncio.ensure_future(self._wake.wait()))
        try:
            await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
            return wakeable and self._wake.is_set() and not stop.is_set()
        finally:
            for waiter in waiters:
                waiter.cancel()
            if wakeable:
                self._wake.clear()
