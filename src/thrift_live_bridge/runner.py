# SPDX-License-Identifier: AGPL-3.0-only
"""Menjalankan banyak client dalam satu proses, tanpa saling mengganggu.

Tiap client punya antrian, pengirim, pembaca, dan klien HTTP sendiri. Yang
dipakai bersama hanya key sign server.

- Client yang setelannya belum lengkap dilewati, bukan menggagalkan yang lain.
- Client yang berhenti karena galat tak terduga dinyalakan lagi sendiri,
  dengan jeda yang makin panjang. Client lain tidak ikut berhenti.
- Tanda berhenti (Ctrl+C, `docker stop`) menghentikan semuanya dengan rapi.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Iterable

from .logs import labelled
from .odoo import OdooDoor, new_http_client
from .outbox import Outbox
from .reader import ROOM_PREFIX, Reader, room_key
from .sender import Sender
from .settings import SIGN_KEY_ENV, ClientSettings, Settings, SettingsError

_logger = logging.getLogger(__name__)

# Jeda antar-client saat mulai, supaya semuanya tidak mengetuk TikTok dan sign
# server pada detik yang sama.
START_STAGGER = 1.0
# Jeda sebelum client yang berhenti karena galat dinyalakan lagi.
RESTART_START = 5.0
RESTART_MAX = 60.0
# Client yang sempat berjalan selama ini dianggap sehat lagi.
HEALTHY_AFTER = 300.0


def close_unfollowed(outbox: Outbox, accounts: Iterable[str], label: str) -> int:
    """Tutup sesi milik akun yang tidak lagi diikuti client ini.

    Tanpa ini, sesi akun yang dihapus dari setelan saat siarannya masih
    tercatat terbuka akan menggantung selamanya: tidak ada lagi pembaca yang
    akan menutupnya.
    """
    followed = {room_key(account) for account in accounts}
    closed = 0
    for key, room in outbox.meta_items(ROOM_PREFIX).items():
        if key in followed:
            continue
        outbox.enqueue_close(room)
        outbox.set_meta(key, None)
        closed += 1
        labelled(_logger, label).info(
            "Sesi siaran %s ditutup: akunnya tidak lagi diikuti.", room,
        )
    return closed


async def run_client(
    client: ClientSettings,
    account: str,
    settings: Settings,
    stop: asyncio.Event,
    *,
    source_factory: Callable,
    http_factory: Callable = new_http_client,
) -> None:
    """Satu client sampai `stop` menyala. Melempar galat kalau berhenti karena hal lain."""
    unit_stop = asyncio.Event()
    with Outbox(client.queue_path) as outbox:
        close_unfollowed(outbox, [account], client.code)
        async with http_factory() as http:
            sender = Sender(
                outbox, OdooDoor(http, client),
                keep_sent_days=settings.keep_sent_days, label=client.code,
            )
            source = source_factory(account, settings.sign_api_key or None)
            reader = Reader(
                source, outbox, account=account, wake=sender.wake,
                poll_seconds=settings.poll_seconds, label=client.code,
            )
            tasks = [
                asyncio.create_task(sender.run(unit_stop)),
                asyncio.create_task(reader.run(unit_stop)),
            ]
            stopper = asyncio.create_task(stop.wait())
            try:
                await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
            finally:
                # Apa pun sebabnya, pasangan pembaca-pengirim berhenti bersama.
                unit_stop.set()
                stopper.cancel()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await source.close()
    for result in results:
        if isinstance(result, BaseException):
            raise result
    if not stop.is_set():
        raise RuntimeError("pembaca atau pengirim berhenti tanpa diminta")


async def supervise(
    label: str, run: Callable, stop: asyncio.Event, *, start_delay: float = 0.0,
) -> None:
    """Jalankan `run()` terus; kalau berhenti karena galat, nyalakan lagi."""
    log = labelled(_logger, label)
    loop = asyncio.get_running_loop()
    await _pause(stop, start_delay)
    delay = RESTART_START
    while not stop.is_set():
        started = loop.time()
        try:
            await run()
        except Exception:  # noqa: BLE001 - satu client tidak boleh menjatuhkan yang lain
            if stop.is_set():
                return
            if loop.time() - started >= HEALTHY_AFTER:
                delay = RESTART_START
            log.exception("Berhenti karena galat tak terduga. Dinyalakan lagi dalam %.0f detik.", delay)
            await _pause(stop, delay)
            delay = min(delay * 2, RESTART_MAX)


def ready_clients(
    clients: Iterable[ClientSettings], tiktok: str | None = None,
) -> list[tuple[ClientSettings, str]]:
    """Client yang setelannya lengkap, berpasangan dengan akun yang diikutinya."""
    clients = list(clients)
    ready = []
    for client in clients:
        account = (tiktok or client.tiktok or "").lstrip("@").strip()
        try:
            client.require_api_key()
            if not account:
                raise SettingsError(
                    f"Akun TikTok untuk client '{client.code}' belum diisi: tambahkan "
                    f"`tiktok = \"...\"` di file setelan, atau pakai --tiktok."
                )
        except SettingsError as exc:
            if len(clients) == 1:
                raise
            labelled(_logger, client.code).error("Dilewati: %s", exc)
            continue
        ready.append((client, account))
    if not ready:
        raise SettingsError("Tidak ada client yang setelannya lengkap; lihat pesan di atas.")
    return ready


async def run_all(
    settings: Settings,
    clients: Iterable[ClientSettings],
    stop: asyncio.Event,
    *,
    tiktok: str | None = None,
    source_factory: Callable | None = None,
    http_factory: Callable = new_http_client,
) -> None:
    ready = ready_clients(clients, tiktok)
    if source_factory is None:
        # Diimpor di sini: memuat library TikTok makan waktu, dan perintah
        # lain tidak membutuhkannya.
        from .tiktok import TikTokSource
        source_factory = TikTokSource
    if not settings.sign_api_key:
        _logger.warning(
            "Variabel %s kosong: menyambung tanpa key sign server, dengan batas yang lebih rendah.",
            SIGN_KEY_ENV,
        )
    for client, account in ready:
        labelled(_logger, client.code).info("Mengikuti @%s.", account)

    def unit(client: ClientSettings, account: str) -> Callable:
        return lambda: run_client(
            client, account, settings, stop,
            source_factory=source_factory, http_factory=http_factory,
        )

    await asyncio.gather(*(
        supervise(client.code, unit(client, account), stop, start_delay=index * START_STAGGER)
        for index, (client, account) in enumerate(ready)
    ))


async def _pause(stop: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        return
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
