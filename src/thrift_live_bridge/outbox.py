# SPDX-License-Identifier: AGPL-3.0-only
"""Antrian lokal: kiriman untuk Odoo yang belum tentu sampai.

Setiap kejadian dari siaran (sesi dibuka, komentar disematkan, sesi ditutup)
ditulis ke sini DULU, baru dikirim. Kalau Odoo mati, jaringan putus, atau
bridge dihentikan di tengah siaran, kirimannya menunggu di file ini dan
dikirim saat keadaan pulih.

Satu file SQLite per client. Isinya memuat data penonton siaran (nama
tampilan, username), jadi filenya hanya bisa dibaca pemiliknya, dan kiriman
yang sudah sampai dihapus setelah beberapa hari.
"""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('open', 'push', 'close')),
    payload     TEXT NOT NULL,
    message_ref TEXT,
    state       TEXT NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'sent', 'rejected')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    rejections  INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TEXT NOT NULL,
    sent_at     TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS outbox_message_ref
    ON outbox (message_ref) WHERE message_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS outbox_state ON outbox (state, id);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def utc_now() -> str:
    """Waktu sekarang dalam UTC, dalam bentuk yang diterima Odoo."""
    return datetime.now(timezone.utc).strftime(TIME_FORMAT)


@dataclass(frozen=True)
class Item:
    """Satu kiriman yang menunggu: nama method pintu dan argumennya."""
    id: int
    kind: str
    payload: dict
    attempts: int
    rejections: int
    created_at: str
    last_error: str | None


class Outbox:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            # Dibuat dulu dengan izin 0600; file -wal dan -shm buatan SQLite
            # mengikuti izin file induknya.
            os.close(os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600))
        self._db = sqlite3.connect(self.path, timeout=5.0)
        # WAL: perintah `fake` dan pengirim boleh membuka file yang sama dari
        # dua proses. FULL: kiriman yang sudah ditulis tidak hilang walau
        # listrik mati sesaat sesudahnya.
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.executescript(_SCHEMA)

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> Outbox:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # -- menaruh kiriman ---------------------------------------------------

    def _insert(self, kind: str, payload: dict, message_ref: str | None = None) -> int | None:
        with self._db:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO outbox (kind, payload, message_ref, created_at) "
                "VALUES (?, ?, ?, ?)",
                (kind, json.dumps(payload, ensure_ascii=False), message_ref, utc_now()),
            )
        return cursor.lastrowid if cursor.rowcount else None

    def enqueue_open(self, external_ref: str, account_ref: str | None = None) -> int:
        payload = {"external_ref": external_ref}
        if account_ref:
            payload["external_account_ref"] = account_ref
        return self._insert("open", payload)

    def enqueue_close(self, external_ref: str) -> int:
        return self._insert("close", {"external_ref": external_ref})

    def enqueue_pin(
        self, external_ref: str, row: dict, account_ref: str | None = None,
    ) -> int | None:
        """Taruh satu pin. Mengembalikan None kalau pin itu sudah pernah masuk.

        Pin dikenali dari `external_message_ref`. Saat bridge menyambung lagi,
        TikTok mengirim ulang pin yang sedang terpasang; tanpa penolak ini pin
        yang sama antre dua kali. Pin tanpa ID pesan selalu diterima, karena
        tidak ada yang bisa dibandingkan.
        """
        message_ref = str(row.get("external_message_ref") or "").strip() or None
        payload = {"external_ref": external_ref, "rows": [row]}
        if account_ref:
            payload["external_account_ref"] = account_ref
        return self._insert("push", payload, message_ref)

    # -- mengirim ----------------------------------------------------------

    def next_pending(self) -> Item | None:
        """Kiriman tertua yang belum sampai. Urutan dijaga: yang lama dulu."""
        row = self._db.execute(
            "SELECT id, kind, payload, attempts, rejections, created_at, last_error FROM outbox "
            "WHERE state = 'pending' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return Item(
            id=row[0], kind=row[1], payload=json.loads(row[2]),
            attempts=row[3], rejections=row[4], created_at=row[5], last_error=row[6],
        )

    def mark_sent(self, item_id: int) -> None:
        with self._db:
            self._db.execute(
                "UPDATE outbox SET state = 'sent', sent_at = ?, last_error = NULL WHERE id = ?",
                (utc_now(), item_id),
            )

    def mark_retry(self, item_id: int, error: str, *, rejection: bool = False) -> None:
        """Belum sampai; tetap di depan antrian dan dicoba lagi nanti.

        `rejection` membedakan "Odoo menolak isinya" dari "Odoo tidak bisa
        dicapai". Hanya yang pertama yang dihitung menuju penyisihan: kiriman
        yang menunggu Odoo hidup selama sejam tidak boleh disisihkan karena
        itu.
        """
        with self._db:
            self._db.execute(
                "UPDATE outbox SET attempts = attempts + 1, "
                "rejections = rejections + ?, last_error = ? WHERE id = ?",
                (1 if rejection else 0, error, item_id),
            )

    def mark_rejected(self, item_id: int, error: str) -> None:
        """Disisihkan supaya tidak menyumbat antrian. Tidak dihapus."""
        with self._db:
            self._db.execute(
                "UPDATE outbox SET state = 'rejected', attempts = attempts + 1, "
                "rejections = rejections + 1, last_error = ? WHERE id = ?",
                (error, item_id),
            )

    def requeue_rejected(self) -> int:
        """Kembalikan kiriman yang disisihkan ke antrian, di urutan aslinya."""
        with self._db:
            cursor = self._db.execute(
                "UPDATE outbox SET state = 'pending', attempts = 0, rejections = 0 "
                "WHERE state = 'rejected'"
            )
        return cursor.rowcount

    # -- catatan kecil -----------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        """Catatan yang harus bertahan saat bridge dihentikan dan dinyalakan lagi."""
        row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def meta_items(self, prefix: str) -> dict[str, str]:
        """Semua catatan yang kuncinya berawalan `prefix`."""
        rows = self._db.execute(
            "SELECT key, value FROM meta WHERE substr(key, 1, ?) = ? ORDER BY key",
            (len(prefix), prefix),
        )
        return dict(rows.fetchall())

    def set_meta(self, key: str, value: str | None) -> None:
        with self._db:
            if value is None:
                self._db.execute("DELETE FROM meta WHERE key = ?", (key,))
            else:
                self._db.execute(
                    "INSERT INTO meta (key, value) VALUES (?, ?) "
                    "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )

    # -- melihat dan merawat -----------------------------------------------

    def counts(self) -> dict[str, int]:
        result = {"pending": 0, "sent": 0, "rejected": 0}
        for state, count in self._db.execute("SELECT state, count(*) FROM outbox GROUP BY state"):
            result[state] = count
        return result

    def purge_sent(self, keep_days: int) -> int:
        """Hapus kiriman yang sudah sampai lebih dari `keep_days` hari lalu.

        Yang belum sampai dan yang disisihkan tidak pernah dihapus di sini.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime(TIME_FORMAT)
        with self._db:
            cursor = self._db.execute(
                "DELETE FROM outbox WHERE state = 'sent' AND sent_at < ?", (cutoff,),
            )
        return cursor.rowcount
