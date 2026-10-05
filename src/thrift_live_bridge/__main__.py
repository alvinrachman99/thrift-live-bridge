# SPDX-License-Identifier: AGPL-3.0-only
"""Baris perintah bridge.

    python -m thrift_live_bridge status
    python -m thrift_live_bridge send [--once]
    python -m thrift_live_bridge requeue
    python -m thrift_live_bridge fake open|pin|close ...

Perintah `fake` menaruh kiriman ke antrian lokal seolah-olah datang dari
siaran. Gunanya menguji antrian dan pengirim tanpa TikTok.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

from .odoo import OdooDoor, new_http_client
from .outbox import Outbox, utc_now
from .sender import Sender
from .settings import DEFAULT_SETTINGS_FILE, ClientSettings, Settings, SettingsError, load_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="thrift-live-bridge",
        description="Jembatan TikTok LIVE ke antrian order di Odoo.",
    )
    parser.add_argument(
        "--settings", default=os.environ.get("BRIDGE_SETTINGS", DEFAULT_SETTINGS_FILE),
        help="file setelan (bawaan: clients.toml)",
    )
    parser.add_argument("--client", help="kode client; boleh dikosongkan kalau hanya ada satu")
    commands = parser.add_subparsers(dest="command", required=True, metavar="perintah")

    commands.add_parser("status", help="lihat isi antrian lokal")

    send = commands.add_parser("send", help="kirim isi antrian lokal ke Odoo")
    send.add_argument(
        "--once", action="store_true",
        help="kirim yang ada sekarang lalu berhenti; tanpa ini berjalan terus",
    )

    commands.add_parser("requeue", help="kembalikan kiriman yang disisihkan ke antrian")

    fake = commands.add_parser("fake", help="taruh kiriman buatan ke antrian (untuk menguji)")
    fakes = fake.add_subparsers(dest="fake_command", required=True, metavar="jenis")

    fake_open = fakes.add_parser("open", help="siaran dimulai")
    fake_open.add_argument("--ref", required=True, help="referensi siaran")
    fake_open.add_argument("--account", help="akun pemilik siaran")

    fake_pin = fakes.add_parser("pin", help="komentar disematkan")
    fake_pin.add_argument("--ref", required=True, help="referensi siaran")
    fake_pin.add_argument("--nickname", required=True, help="nama tampilan")
    fake_pin.add_argument("--comment", help="teks komentar")
    fake_pin.add_argument("--handle", help="username")
    fake_pin.add_argument("--user-ref", help="ID akun penonton")
    fake_pin.add_argument("--message-ref", help="ID pin; kalau kosong dibuatkan yang unik")
    fake_pin.add_argument("--account", help="akun pemilik siaran")

    fake_close = fakes.add_parser("close", help="siaran berakhir")
    fake_close.add_argument("--ref", required=True, help="referensi siaran")
    return parser


def cmd_status(client: ClientSettings) -> int:
    with Outbox(client.queue_path) as outbox:
        counts = outbox.counts()
        head = outbox.next_pending()
    print(f"Client   : {client.code}")
    print(f"Antrian  : {client.queue_path}")
    print(f"Menunggu : {counts['pending']}")
    print(f"Terkirim : {counts['sent']}")
    print(f"Ditolak  : {counts['rejected']}")
    if head is not None:
        print(f"Tertua menunggu sejak {head.created_at} UTC, {head.attempts} kali dicoba")
        if head.last_error:
            print(f"Galat terakhir: {head.last_error}")
    return 0


def cmd_requeue(client: ClientSettings) -> int:
    with Outbox(client.queue_path) as outbox:
        count = outbox.requeue_rejected()
    print(f"{count} kiriman dikembalikan ke antrian.")
    return 0


def cmd_fake(client: ClientSettings, args: argparse.Namespace) -> int:
    with Outbox(client.queue_path) as outbox:
        if args.fake_command == "open":
            outbox.enqueue_open(args.ref, args.account)
            print(f"Masuk antrian: buka sesi {args.ref}.")
        elif args.fake_command == "close":
            outbox.enqueue_close(args.ref)
            print(f"Masuk antrian: tutup sesi {args.ref}.")
        else:
            message_ref = args.message_ref or f"fake-{time.time_ns()}"
            row = {
                "external_message_ref": message_ref,
                "nickname": args.nickname,
                "date": utc_now(),
            }
            for key, value in (
                ("comment", args.comment), ("handle", args.handle),
                ("external_user_ref", args.user_ref),
            ):
                if value:
                    row[key] = value
            if outbox.enqueue_pin(args.ref, row, args.account) is None:
                print(f"Pin {message_ref} sudah pernah masuk antrian; dilewati.")
            else:
                print(f"Masuk antrian: pin {message_ref} untuk sesi {args.ref}.")
    return 0


async def cmd_send(client: ClientSettings, settings: Settings, *, once: bool) -> int:
    client.require_api_key()
    with Outbox(client.queue_path) as outbox:
        async with new_http_client() as http:
            sender = Sender(
                outbox, OdooDoor(http, client), keep_sent_days=settings.keep_sent_days,
            )
            if once:
                result = await sender.send_pending()
                print(f"Terkirim: {result.sent}. Disisihkan: {result.rejected}.")
                if result.blocked:
                    pending = outbox.counts()["pending"]
                    print(f"Tertahan: {result.blocked}. {pending} kiriman masih menunggu.")
                    return 1
                return 0
            logging.getLogger(__name__).info(
                "Pengirim berjalan untuk client %s. Ctrl+C untuk berhenti.", client.code,
            )
            await sender.run(asyncio.Event())
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S",
    )
    # httpx mencatat setiap permintaan di tingkat INFO; itu terlalu ramai.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        settings = load_settings(args.settings)
        client = settings.client(args.client)
        if args.command == "status":
            return cmd_status(client)
        if args.command == "requeue":
            return cmd_requeue(client)
        if args.command == "fake":
            return cmd_fake(client, args)
        return asyncio.run(cmd_send(client, settings, once=args.once))
    except SettingsError as exc:
        print(f"Setelan: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nBerhenti.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
