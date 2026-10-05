# SPDX-License-Identifier: AGPL-3.0-only
"""Pemanggil pintu masuk antrian di Odoo.

Pintu itu tiga method pada model `thrift.bot`, dipanggil lewat JSON-2:

    POST <odoo_url>/json/2/thrift.bot/<method>

Jawabannya dibagi tiga, dan pembagian inilah yang menentukan pin hilang atau
tidak:

- 200          -> sampai.
- 422          -> Odoo menolak ISI kiriman. Mengulang tidak mengubah jawabannya.
- selain itu   -> belum sampai, coba lagi nanti. Termasuk Odoo mati, jaringan
                  putus, timeout, dan salah setelan (key ditolak, database
                  tidak ditemukan). Kirimannya sendiri tidak salah, jadi tidak
                  boleh dibuang.
"""
from __future__ import annotations

import httpx

from .settings import ClientSettings

DOOR_MODEL = "thrift.bot"
ERROR_TEXT_MAX = 200


class DoorError(Exception):
    pass


class RetryLater(DoorError):
    """Belum sampai; kirimannya tetap di antrian dan dicoba lagi."""


class Rejected(DoorError):
    """Odoo menolak isi kiriman ini."""


def new_http_client() -> httpx.AsyncClient:
    """Klien HTTP untuk Odoo.

    trust_env=False: variabel proxy di environment (yang mungkin dipasang
    untuk sambungan ke TikTok) tidak boleh ikut dipakai ke Odoo. Kalau ikut,
    API key melewati proxy itu.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        follow_redirects=False,
        trust_env=False,
    )


def _error_text(response: httpx.Response) -> str:
    """Pesan singkat dari jawaban galat. Jejak `debug` kiriman Odoo dibuang."""
    try:
        body = response.json()
    except ValueError:
        body = None
    message = body.get("message") if isinstance(body, dict) else None
    if not isinstance(message, str) or not message.strip():
        message = response.reason_phrase or "tanpa keterangan"
    return message.strip().splitlines()[0][:ERROR_TEXT_MAX]


class OdooDoor:
    def __init__(self, http: httpx.AsyncClient, client: ClientSettings):
        self._http = http
        self._url = f"{client.odoo_url}/json/2/{DOOR_MODEL}/"
        # Header ini satu-satunya tempat API key berada. Ia tidak pernah
        # dimasukkan ke log maupun ke pesan galat.
        self._headers = {"Authorization": f"bearer {client.api_key}"}
        if client.odoo_host:
            self._headers["Host"] = client.odoo_host
        if client.odoo_db:
            self._headers["X-Odoo-Database"] = client.odoo_db

    async def call(self, method: str, params: dict) -> object:
        try:
            response = await self._http.post(
                self._url + method, json=params, headers=self._headers,
            )
        except httpx.TimeoutException:
            raise RetryLater("Odoo tidak menjawab tepat waktu") from None
        except httpx.TransportError as exc:
            raise RetryLater(f"Odoo tidak bisa dihubungi ({type(exc).__name__})") from None

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                raise RetryLater("jawaban Odoo bukan JSON") from None
        text = f"{response.status_code}: {_error_text(response)}"
        if response.status_code == 422:
            raise Rejected(text)
        raise RetryLater(text)
