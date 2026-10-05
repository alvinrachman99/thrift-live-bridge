# SPDX-License-Identifier: AGPL-3.0-only
"""Sumber siaran TikTok LIVE, lewat library TikTokLive.

Semua yang tahu soal TikTok ada di file ini: cara menyambung, dan cara
mengubah event pin menjadi baris baku (`pin_to_row`).

Tiga hal dari library yang membentuk file ini:

- Library tidak menyambung ulang sendiri. Tiap sambungan butuh tanda tangan
  baru dari sign server, jadi kapan mencoba lagi diputuskan pembaca.
- Event tidak diserialisasi utuh (`to_dict()` gagal pada event sungguhan);
  field diambil satu per satu.
- Sambungan diakhiri lewat `disconnect()`. Membatalkan task library merusak
  event loop yang sedang berjalan.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

from TikTokLive import TikTokLiveClient
from TikTokLive.client.errors import SignatureRateLimitError, UserOfflineError
from TikTokLive.client.web.web_settings import WebDefaults
from TikTokLive.events import ConnectEvent, LiveEndEvent, RoomPinEvent

from .outbox import TIME_FORMAT
from .source import Connected, Ended, NotLive, Pin, RateLimited, SourceError, SourceEvent

_logger = logging.getLogger(__name__)

# PinMessageActionType: 1 = disematkan, 2 = dilepas.
ACTION_PIN = 1
ERROR_TEXT_MAX = 200
# Batas cuplikan event yang gagal dibaca, saat dicatat ke log.
DUMP_MAX = 4000
DEFAULT_RETRY_AFTER = 60.0


def _as_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _clean(value) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def _pin_moment(pin_time: int, received_at: datetime) -> str:
    """Waktu pin (UTC). Angka yang tidak masuk akal diganti waktu terima."""
    if pin_time > 0:
        # Di spike `pin_time` berupa milidetik; angka kecil dianggap detik.
        seconds = pin_time / 1000 if pin_time > 10 ** 11 else float(pin_time)
        try:
            moment = datetime.fromtimestamp(seconds, timezone.utc)
        except (OverflowError, OSError, ValueError):
            moment = None
        if moment and received_at - timedelta(days=2) <= moment <= received_at + timedelta(minutes=5):
            return moment.strftime(TIME_FORMAT)
    return received_at.strftime(TIME_FORMAT)


def pin_to_row(event, received_at: datetime | None = None) -> dict | None:
    """Ubah event pin menjadi baris untuk pintu Odoo.

    Mengembalikan None untuk yang bukan pin komentar: pin yang dilepas, dan
    pin berisi hadiah, like, atau pesan lain.

    Yang dibawa hanya yang dipakai. Event aslinya memuat profil lengkap
    penonton (bio, foto, lencana, statistik); semua itu sengaja ditinggal,
    termasuk dari `raw_payload`.
    """
    if _as_int(getattr(event, "action", None)) != ACTION_PIN:
        return None
    chat = getattr(event, "chat_message", None)
    if chat is None:
        return None
    user = getattr(chat, "user", None)

    pin_msg_id = _as_int(getattr(event, "pin_msg_id", None))
    pin_time = _as_int(getattr(event, "pin_time", None))
    user_id = _as_int(getattr(user, "id", None)) or _as_int(getattr(user, "id_str", None))
    handle = _clean(getattr(user, "display_id", None))
    nickname = _clean(getattr(user, "nickname", None))
    comment = _clean(getattr(chat, "content", None))

    row = {"date": _pin_moment(pin_time, received_at or datetime.now(timezone.utc))}
    # ID-nya 19 digit: dikirim sebagai teks, bukan angka.
    for key, value in (
        ("external_message_ref", str(pin_msg_id) if pin_msg_id else None),
        ("external_user_ref", str(user_id) if user_id else None),
        ("handle", handle),
        ("nickname", nickname),
        ("comment", comment),
    ):
        if value:
            row[key] = value

    raw = {
        "source": "tiktok",
        "method": _clean(getattr(event, "method", None)),
        "pin_msg_id": row.get("external_message_ref"),
        "pin_time": pin_time or None,
        "display_duration": _as_int(getattr(event, "display_duration", None)) or None,
        "operator_id": str(_as_int(getattr(getattr(event, "operator", None), "id", None)) or "") or None,
        "user_id": row.get("external_user_ref"),
        "display_id": handle,
        "nickname": nickname,
        "content": comment,
    }
    row["raw_payload"] = json.dumps(
        {key: value for key, value in raw.items() if value is not None},
        ensure_ascii=False, separators=(",", ":"),
    )
    return row


def is_complete(row: dict) -> bool:
    """Baris punya ID pin, ID akun, dan sebuah nama — seperti semua pin di spike."""
    return bool(
        row.get("external_message_ref") and row.get("external_user_ref")
        and (row.get("nickname") or row.get("handle"))
    )


def _dump(event) -> str:
    """Cuplikan event untuk log, dipakai hanya saat event tidak terbaca utuh."""
    try:
        return base64.b64encode(bytes(event)).decode()[:DUMP_MAX]
    except Exception:  # noqa: BLE001 - apa pun galatnya, log tetap harus tertulis
        return repr(event)[:DUMP_MAX]


# Alasan singkat untuk galat yang sudah dikenal. Teks ini sampai ke layar
# Status Bot di Odoo, jadi ditulis untuk admin; nama galatnya tetap disertakan
# untuk yang mengurus server. Kuncinya nama kelas, dicari sampai ke induknya.
_REASONS = {
    "UserNotFoundError": "akun tidak ditemukan atau belum pernah siaran",
    "WebcastBlockedError": "sambungan ditolak TikTok",
    "SignatureRateLimitError": "penyedia tanda tangan meminta menunggu",
    "SignAPIError": "penyedia tanda tangan gagal",
    "ConnectionClosed": "sambungan terputus",
    "TransportError": "jaringan bermasalah",
    "KeyError": "jawaban TikTok tidak dikenali",
    "TypeError": "jawaban TikTok tidak dikenali",
}


def _brief(exc: BaseException) -> str:
    name = type(exc).__name__
    for cls in type(exc).__mro__:
        if cls.__name__ in _REASONS:
            return f"{_REASONS[cls.__name__]} ({name})"
    text = (str(exc).strip().splitlines() or [""])[0]
    return f"{name}: {text}"[:ERROR_TEXT_MAX].rstrip(": ")


class TikTokSource:
    """Satu akun TikTok yang diikuti siarannya."""

    def __init__(self, unique_id: str, sign_api_key: str | None = None, *, client=None):
        if sign_api_key:
            # Dibaca library saat klien dibuat; berlaku untuk semua akun.
            WebDefaults.tiktok_sign_api_key = sign_api_key
        # `client` hanya diisi oleh uji.
        self._client = client or TikTokLiveClient(unique_id=f"@{unique_id.lstrip('@')}")
        self._queue: asyncio.Queue[SourceEvent] = asyncio.Queue()
        # Penangan sengaja bukan coroutine: library menjadwalkan coroutine
        # sebagai task terpisah, dan urutan kejadian bisa tertukar.
        self._client.add_listener(ConnectEvent, self._on_connect)
        self._client.add_listener(RoomPinEvent, self._on_pin)
        self._client.add_listener(LiveEndEvent, self._on_end)

    # Galat di dalam penangan akan merambat ke dalam library dan memutus
    # sambungan, jadi ketiganya menelan galatnya sendiri.

    def _on_connect(self, event) -> None:
        try:
            self._queue.put_nowait(Connected(str(event.room_id)))
        except Exception:  # noqa: BLE001
            _logger.exception("Event tersambung tidak terbaca.")

    def _on_pin(self, event) -> None:
        try:
            row = pin_to_row(event)
            if row is None:
                return
            if not is_complete(row):
                _logger.warning(
                    "Pin %s tidak terbaca utuh; tetap dimasukkan ke antrian. Event (base64): %s",
                    row.get("external_message_ref", "tanpa ID"), _dump(event),
                )
            self._queue.put_nowait(Pin(row))
        except Exception:  # noqa: BLE001
            _logger.exception("Event pin tidak terbaca. Event (base64): %s", _dump(event))

    def _on_end(self, event) -> None:
        self._queue.put_nowait(Ended())

    async def is_live(self) -> bool:
        try:
            return bool(await self._client.is_live())
        except Exception as exc:  # noqa: BLE001 - library melempar banyak jenis galat
            raise SourceError(_brief(exc)) from None

    async def listen(self, stop: asyncio.Event) -> AsyncIterator[SourceEvent]:
        while not self._queue.empty():      # sisa sambungan sebelumnya
            self._queue.get_nowait()
        try:
            task = await self._client.start()
        except UserOfflineError:
            raise NotLive("sedang tidak siaran") from None
        except SignatureRateLimitError as exc:
            try:
                retry_after = float(exc.retry_after)
            except Exception:  # noqa: BLE001 - header-nya bisa tidak ada
                retry_after = DEFAULT_RETRY_AFTER
            raise RateLimited(_brief(exc), retry_after) from None
        except Exception as exc:  # noqa: BLE001
            raise SourceError(_brief(exc)) from None

        stopping = asyncio.ensure_future(stop.wait())
        try:
            while True:
                getter = asyncio.ensure_future(self._queue.get())
                done, _ = await asyncio.wait(
                    {getter, task, stopping}, return_when=asyncio.FIRST_COMPLETED,
                )
                if getter in done:
                    yield getter.result()
                    continue
                getter.cancel()
                if stopping in done:
                    return
                # Sambungan sudah berakhir: keluarkan dulu yang masih tersisa.
                while not self._queue.empty():
                    yield self._queue.get_nowait()
                if not task.cancelled() and task.exception() is not None:
                    raise SourceError(_brief(task.exception()))
                return
        finally:
            stopping.cancel()
            await self._disconnect()

    async def _disconnect(self) -> None:
        try:
            await self._client.disconnect()
        except Exception as exc:  # noqa: BLE001
            _logger.debug("Galat saat memutus sambungan diabaikan: %s", _brief(exc))

    async def close(self) -> None:
        await self._disconnect()
        try:
            # Bukan client.close(): method itu menjalankan event loop sendiri
            # dan gagal kalau loop-nya sedang berjalan.
            await self._client.web.close()
        except Exception as exc:  # noqa: BLE001
            _logger.debug("Galat saat menutup klien diabaikan: %s", _brief(exc))
