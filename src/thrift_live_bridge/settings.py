# SPDX-License-Identifier: AGPL-3.0-only
"""Setelan bridge: siapa client-nya dan ke mana kirimannya.

File setelan (TOML) tidak memuat rahasia. API key dibaca dari environment;
file hanya menyebut NAMA variabelnya. Dengan begitu file setelan boleh dibaca
siapa pun yang mengurus server, sedangkan key tinggal di satu tempat.
"""
from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_SETTINGS_FILE = "clients.toml"
DEFAULT_DATA_DIR = "data"
DEFAULT_KEEP_SENT_DAYS = 3

# Kode client dipakai sebagai nama file antrian, jadi hurufnya dibatasi.
_CODE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class SettingsError(Exception):
    """Setelan tidak lengkap atau tidak terbaca. Pesannya untuk manusia."""


@dataclass(frozen=True)
class ClientSettings:
    code: str
    odoo_url: str
    queue_path: Path
    api_key_env: str
    # repr=False: key tidak ikut tercetak kalau objek ini masuk log atau
    # pesan galat.
    api_key: str = field(default="", repr=False)
    # Header Host: dipakai saat Odoo memilih database dari nama host, tetapi
    # dipanggil lewat alamat lain (misalnya dari dalam server).
    odoo_host: str | None = None
    # Header X-Odoo-Database.
    odoo_db: str | None = None

    def require_api_key(self) -> None:
        if not self.api_key:
            raise SettingsError(
                f"API key untuk client '{self.code}' belum ada: isi variabel "
                f"environment {self.api_key_env}."
            )


@dataclass(frozen=True)
class Settings:
    clients: tuple[ClientSettings, ...]
    keep_sent_days: int = DEFAULT_KEEP_SENT_DAYS

    def client(self, code: str | None = None) -> ClientSettings:
        """Client dengan kode itu; tanpa kode, satu-satunya client yang ada."""
        if code is None:
            if len(self.clients) == 1:
                return self.clients[0]
            known = ", ".join(c.code for c in self.clients)
            raise SettingsError(f"Ada lebih dari satu client ({known}); sebut salah satu dengan --client.")
        for client in self.clients:
            if client.code == code:
                return client
        known = ", ".join(c.code for c in self.clients)
        raise SettingsError(f"Client '{code}' tidak ada di file setelan. Yang ada: {known}.")


def default_key_env(code: str) -> str:
    return "BRIDGE_KEY_" + re.sub(r"[^A-Za-z0-9]", "_", code).upper()


def _text(table: Mapping, key: str, where: str, *, required: bool = False) -> str | None:
    value = table.get(key)
    if value is None or value == "":
        if required:
            raise SettingsError(f"{where}: '{key}' wajib diisi.")
        return None
    if not isinstance(value, str):
        raise SettingsError(f"{where}: '{key}' harus berupa teks.")
    return value.strip()


def load_settings(
    path: str | os.PathLike = DEFAULT_SETTINGS_FILE,
    environ: Mapping[str, str] = os.environ,
) -> Settings:
    path = Path(path)
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError:
        raise SettingsError(
            f"File setelan '{path}' tidak ada. Salin clients.example.toml "
            f"menjadi clients.toml lalu sesuaikan."
        ) from None
    except tomllib.TOMLDecodeError as exc:
        raise SettingsError(f"File setelan '{path}' tidak terbaca: {exc}") from None

    data_dir = Path(_text(raw, "data_dir", str(path)) or DEFAULT_DATA_DIR)
    if not data_dir.is_absolute():
        # Relatif terhadap file setelan, bukan terhadap folder tempat perintah
        # dijalankan: antrian tidak boleh pindah hanya karena `cd`.
        data_dir = path.resolve().parent / data_dir

    keep_sent_days = raw.get("keep_sent_days", DEFAULT_KEEP_SENT_DAYS)
    if not isinstance(keep_sent_days, int) or isinstance(keep_sent_days, bool) or keep_sent_days < 0:
        raise SettingsError(f"{path}: 'keep_sent_days' harus bilangan bulat 0 atau lebih.")

    entries = raw.get("client")
    if not isinstance(entries, list) or not entries:
        raise SettingsError(f"{path}: belum ada client. Tambahkan minimal satu blok [[client]].")

    clients: list[ClientSettings] = []
    seen: set[str] = set()
    for number, entry in enumerate(entries, start=1):
        where = f"{path}, client ke-{number}"
        if not isinstance(entry, dict):
            raise SettingsError(f"{where}: bentuknya salah.")
        code = _text(entry, "code", where, required=True)
        if not _CODE_PATTERN.match(code):
            raise SettingsError(f"{where}: 'code' hanya boleh huruf, angka, '_' dan '-'.")
        if code in seen:
            raise SettingsError(f"{where}: kode '{code}' dipakai dua kali.")
        seen.add(code)

        odoo_url = _text(entry, "odoo_url", where, required=True).rstrip("/")
        if not odoo_url.startswith(("http://", "https://")):
            raise SettingsError(f"{where}: 'odoo_url' harus diawali http:// atau https://.")

        api_key_env = _text(entry, "api_key_env", where) or default_key_env(code)
        clients.append(ClientSettings(
            code=code,
            odoo_url=odoo_url,
            queue_path=data_dir / f"{code}.sqlite",
            api_key_env=api_key_env,
            api_key=(environ.get(api_key_env) or "").strip(),
            odoo_host=_text(entry, "odoo_host", where),
            odoo_db=_text(entry, "odoo_db", where),
        ))
    return Settings(clients=tuple(clients), keep_sent_days=keep_sent_days)
