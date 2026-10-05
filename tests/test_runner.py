# SPDX-License-Identifier: AGPL-3.0-only
"""Banyak client dalam satu proses: masing-masing jalan sendiri."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

from thrift_live_bridge import runner
from thrift_live_bridge.outbox import Outbox
from thrift_live_bridge.reader import room_key
from thrift_live_bridge.settings import SettingsError, load_settings
from thrift_live_bridge.source import Connected, Pin

TWO_CLIENTS = """
[[client]]
code = "aaaa"
odoo_url = "http://caddy:8080"
odoo_host = "aaaa.contoh.id"
tiktok = "toko.a"

[[client]]
code = "bbbb"
odoo_url = "http://caddy:8080"
odoo_host = "bbbb.contoh.id"
tiktok = "toko.b"
"""
KEYS = {"BRIDGE_KEY_AAAA": "kunci-a", "BRIDGE_KEY_BBBB": "kunci-b", "EULER_API_KEY": "euler"}


class Sources:
    """Pabrik sumber berskenario: tiap akun punya siarannya sendiri."""

    def __init__(self):
        self.created = []
        self.crash = {}         # akun -> berapa kali `is_live` meledak dengan galat tak terduga

    def __call__(self, account, sign_api_key=None):
        source = _Source(self, account)
        self.created.append(source)
        return source


class _Source:
    def __init__(self, owner, account):
        self.owner = owner
        self.account = account
        self.closed = False

    async def is_live(self):
        if self.owner.crash.get(self.account, 0) > 0:
            self.owner.crash[self.account] -= 1
            raise RuntimeError("galat yang tidak diperkirakan")
        return True

    async def listen(self, stop):
        yield Connected(f"ruang-{self.account}")
        yield Pin({"external_message_ref": f"pin-{self.account}", "nickname": "Penonton"})
        await stop.wait()

    async def close(self):
        self.closed = True


class RunnerCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.folder = Path(self._tmp.name)
        self.received = []      # (host, key, method, isi)
        self.sources = Sources()
        for name in ("START_STAGGER", "RESTART_START", "RESTART_MAX"):
            patcher = mock.patch.object(runner, name, 0.01)
            patcher.start()
            self.addCleanup(patcher.stop)

    def settings(self, text=TWO_CLIENTS, environ=KEYS):
        path = self.folder / "clients.toml"
        path.write_text(text, encoding="utf-8")
        return load_settings(path, environ)

    def http_client(self):
        def handler(request):
            self.received.append((
                request.headers["host"], request.headers["authorization"],
                request.url.path.rsplit("/", 1)[1], json.loads(request.content),
            ))
            return httpx.Response(200, json={})
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def run_until(self, settings, condition, clients=None, **options):
        stop = asyncio.Event()
        task = asyncio.create_task(runner.run_all(
            settings, clients or settings.clients, stop,
            source_factory=self.sources, http_factory=self.http_client, **options,
        ))
        for _ in range(800):
            if condition() or task.done():
                break
            await asyncio.sleep(0.005)
        stop.set()
        await asyncio.wait_for(task, 3.0)

    def calls_for(self, host):
        return [(method, body["external_ref"]) for h, _, method, body in self.received if h == host]


class TestIsolation(RunnerCase):
    async def test_each_client_gets_only_its_own_broadcast(self):
        await self.run_until(self.settings(), lambda: len(self.received) >= 4)

        self.assertEqual(self.calls_for("aaaa.contoh.id"),
                         [("open_session", "ruang-toko.a"), ("push", "ruang-toko.a")])
        self.assertEqual(self.calls_for("bbbb.contoh.id"),
                         [("open_session", "ruang-toko.b"), ("push", "ruang-toko.b")])
        # Key client A tidak pernah terbawa ke Odoo client B, dan sebaliknya.
        self.assertEqual({(host, key) for host, key, _, _ in self.received}, {
            ("aaaa.contoh.id", "bearer kunci-a"), ("bbbb.contoh.id", "bearer kunci-b"),
        })
        self.assertTrue(all(source.closed for source in self.sources.created))

    async def test_each_client_has_its_own_queue_file(self):
        settings = self.settings()
        await self.run_until(settings, lambda: len(self.received) >= 4)
        for client, account in zip(settings.clients, ("toko.a", "toko.b")):
            with Outbox(client.queue_path) as outbox:
                self.assertEqual(outbox.counts()["sent"], 2)
                self.assertEqual(outbox.get_meta(room_key(account)), f"ruang-{account}")

    async def test_incomplete_client_is_skipped_others_run(self):
        settings = self.settings(environ={"BRIDGE_KEY_BBBB": "kunci-b"})
        with self.assertLogs("thrift_live_bridge.runner", level="ERROR") as logs:
            await self.run_until(settings, lambda: len(self.received) >= 2)
        self.assertEqual({host for host, *_ in self.received}, {"bbbb.contoh.id"})
        self.assertTrue(any("[aaaa] Dilewati" in line and "BRIDGE_KEY_AAAA" in line for line in logs.output))

    async def test_nothing_runs_when_no_client_is_complete(self):
        settings = self.settings(environ={})
        with self.assertLogs("thrift_live_bridge.runner", level="ERROR"):
            with self.assertRaises(SettingsError):
                await self.run_until(settings, lambda: True)
        self.assertEqual(self.received, [])


class TestSupervision(RunnerCase):
    async def test_crashing_client_restarts_and_the_other_never_stops(self):
        self.sources.crash = {"toko.a": 2}
        with self.assertLogs("thrift_live_bridge.runner", level="ERROR") as logs:
            await self.run_until(self.settings(), lambda: len(self.received) >= 4)

        self.assertEqual(self.calls_for("bbbb.contoh.id"),
                         [("open_session", "ruang-toko.b"), ("push", "ruang-toko.b")])
        self.assertEqual(self.calls_for("aaaa.contoh.id"),
                         [("open_session", "ruang-toko.a"), ("push", "ruang-toko.a")])
        crashes = [line for line in logs.output if "[aaaa] Berhenti karena galat tak terduga" in line]
        self.assertEqual(len(crashes), 2)
        # Tiga kali dibuat untuk client A (dua gagal, satu berhasil), sekali untuk B.
        self.assertEqual([s.account for s in self.sources.created].count("toko.a"), 3)
        self.assertEqual([s.account for s in self.sources.created].count("toko.b"), 1)
        self.assertTrue(all(source.closed for source in self.sources.created))


class TestUnfollowedAccount(RunnerCase):
    async def test_session_of_an_account_no_longer_followed_is_closed(self):
        settings = self.settings()
        first = settings.clients[0]
        with Outbox(first.queue_path) as outbox:
            outbox.set_meta(room_key("akun.lama"), "ruang-lama")
            outbox.set_meta(room_key("toko.a"), "ruang-toko.a")

        await self.run_until(settings, lambda: len(self.received) >= 3, clients=[first])
        self.assertEqual(self.calls_for("aaaa.contoh.id"), [
            ("close_session", "ruang-lama"),
            ("open_session", "ruang-toko.a"), ("push", "ruang-toko.a"),
        ])
        with Outbox(first.queue_path) as outbox:
            self.assertIsNone(outbox.get_meta(room_key("akun.lama")))


if __name__ == "__main__":
    unittest.main()
