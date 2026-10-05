# SPDX-License-Identifier: AGPL-3.0-only
"""Ping ke layanan pemantau: tanda bahwa proses bridge masih hidup.

Dikirim dari putaran utama, jadi ping yang berhenti berarti prosesnya mati
atau macet. Yang diberi tahu adalah pengelola server, bukan client; keadaan
tiap client dilaporkan ke Odoo oleh heartbeat.py.

Alamat ping bersifat rahasia (siapa pun yang tahu bisa memalsukan tanda
hidup), jadi tidak pernah ditulis ke log.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable

import httpx

_logger = logging.getLogger(__name__)

PING_SECONDS = 60.0


def new_ping_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=10.0, follow_redirects=False, trust_env=False)


async def ping_forever(
    url: str, stop: asyncio.Event, *, http_factory: Callable = new_ping_client,
) -> None:
    failing = False
    async with http_factory() as http:
        while not stop.is_set():
            try:
                response = await http.get(url)
                problem = None if response.status_code < 400 else f"jawaban {response.status_code}"
            except httpx.HTTPError as exc:
                problem = type(exc).__name__
            if problem and not failing:
                _logger.warning("Ping pemantau gagal (%s). Dicoba lagi tiap %.0f detik.", problem, PING_SECONDS)
            elif failing and not problem:
                _logger.info("Ping pemantau berhasil lagi.")
            failing = bool(problem)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=PING_SECONDS)
