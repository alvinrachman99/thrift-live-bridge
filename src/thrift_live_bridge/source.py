# SPDX-License-Identifier: AGPL-3.0-only
"""Bentuk baku sumber siaran, tanpa menyebut TikTok.

Pembaca (reader.py) hanya mengenal isi file ini. Semua yang tahu soal TikTok
ada di tiktok.py; mengganti sumber data berarti menulis satu kelas baru yang
memenuhi `LiveSource`, tanpa menyentuh pembaca, antrian, maupun pengirim.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Connected:
    """Tersambung ke satu siaran. `room_ref` membedakan siaran yang satu dari yang lain."""
    room_ref: str


@dataclass(frozen=True)
class Pin:
    """Satu komentar disematkan. `row` sudah berbentuk baris untuk pintu Odoo."""
    row: dict


@dataclass(frozen=True)
class Ended:
    """Host mengakhiri siaran."""


SourceEvent = Connected | Pin | Ended


class SourceError(Exception):
    """Gagal menyambung, sambungan putus, atau keadaan siaran tidak bisa dipastikan.

    Pesannya singkat dan sampai ke layar admin. `detail` adalah galat aslinya
    dalam satu baris, untuk log saja: yang mengurus server perlu tahu apakah
    "jaringan bermasalah" itu DNS, sambungan yang ditolak, atau hal lain.
    """

    def __init__(self, message: str, detail: str = ""):
        super().__init__(message)
        self.detail = detail


class NotLive(SourceError):
    """Akunnya sedang tidak siaran."""


class RateLimited(SourceError):
    """Penyedia tanda tangan meminta kita menunggu."""

    def __init__(self, message: str, retry_after: float, detail: str = ""):
        super().__init__(message, detail)
        self.retry_after = retry_after


class LiveSource(Protocol):
    async def is_live(self) -> bool:
        """Sedang siaran atau tidak. SourceError kalau tidak bisa dipastikan."""
        ...

    def listen(self, stop: asyncio.Event) -> AsyncIterator[SourceEvent]:
        """Sambung dan keluarkan kejadiannya sampai sambungan berakhir.

        Berakhir tanpa galat = sambungan ditutup. Berakhir dengan SourceError
        = gagal menyambung atau putus di tengah. Selalu diawali `Connected`.
        """
        ...

    async def close(self) -> None:
        ...
