# SPDX-License-Identifier: AGPL-3.0-only
"""Perintah `run`: pembaca, pengirim, dan cara berhentinya, dirangkai jadi satu."""
import asyncio
import json
import os
import signal
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

from thrift_live_bridge import __main__ as cli
from thrift_live_bridge.outbox import Outbox
from thrift_live_bridge.settings import SettingsError, load_settings
from thrift_live_bridge.source import Connected, Pin


class ScriptedSource:
    """Pengganti TikTokSource: satu siaran yang tetap hidup sampai bridge dihentikan."""
    created = []

    def __init__(self, unique_id, sign_api_key=None):
        self.unique_id = unique_id
        self.sign_api_key = sign_api_key
        self.closed = False
        ScriptedSource.created.append(self)

    async def is_live(self):
        return True

    async def listen(self, stop):
        yield Connected("7412345678901234567")
        yield Pin({"external_message_ref": "P1", "nickname": "Budi", "comment": "35"})
        await stop.wait()

    async def close(self):
        self.closed = True


class TestRun(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.folder = Path(self._tmp.name)
        (self.folder / "clients.toml").write_text(
            '[[client]]\ncode = "uji"\nodoo_url = "http://odoo.test"\ntiktok = "@toko"\n',
            encoding="utf-8",
        )
        self.received = []
        ScriptedSource.created = []

    def settings(self, **environ):
        return load_settings(self.folder / "clients.toml", {"BRIDGE_KEY_UJI": "kunci", **environ})

    def handler(self, request):
        self.received.append((request.url.path.rsplit("/", 1)[1], json.loads(request.content)))
        return httpx.Response(200, json={})

    async def run_until(self, settings, condition, tiktok=None):
        def http_client():
            return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

        async def interrupt_when_ready():
            for _ in range(600):
                if condition():
                    break
                await asyncio.sleep(0.005)
            os.kill(os.getpid(), signal.SIGINT)     # sama dengan menekan Ctrl+C

        with mock.patch.object(cli, "new_http_client", http_client), \
                mock.patch("thrift_live_bridge.tiktok.TikTokSource", ScriptedSource):
            stopper = asyncio.create_task(interrupt_when_ready())
            code = await asyncio.wait_for(cli.cmd_run(settings.client(), settings, tiktok=tiktok), 5.0)
            await stopper
        return code

    async def test_broadcast_reaches_odoo_and_ctrl_c_stops_cleanly(self):
        settings = self.settings(EULER_API_KEY="kunci-euler")
        code = await self.run_until(settings, lambda: len(self.received) >= 2)

        self.assertEqual(code, 0)
        self.assertEqual([name for name, _ in self.received], ["open_session", "push"])
        self.assertEqual(self.received[0][1], {
            "external_ref": "7412345678901234567", "external_account_ref": "toko",
        })
        self.assertEqual(self.received[1][1]["rows"][0]["external_message_ref"], "P1")

        source = ScriptedSource.created[0]
        self.assertEqual((source.unique_id, source.sign_api_key), ("toko", "kunci-euler"))
        self.assertTrue(source.closed)
        # Berhenti di tengah siaran: sesinya tidak ditutup, dan tetap tercatat.
        with Outbox(settings.client().queue_path) as outbox:
            self.assertEqual(outbox.get_meta("open_room"), "7412345678901234567")
            self.assertEqual(outbox.counts(), {"pending": 0, "sent": 2, "rejected": 0})

    async def test_account_can_be_overridden_for_testing(self):
        settings = self.settings()
        with self.assertLogs("thrift_live_bridge.__main__", level="WARNING") as logs:
            await self.run_until(settings, lambda: len(self.received) >= 2, tiktok="@akun.lain")
        self.assertEqual(ScriptedSource.created[0].unique_id, "akun.lain")
        self.assertIsNone(ScriptedSource.created[0].sign_api_key)
        self.assertTrue(any("EULER_API_KEY" in line for line in logs.output))

    async def test_missing_account_or_key_is_a_settings_error(self):
        (self.folder / "clients.toml").write_text(
            '[[client]]\ncode = "uji"\nodoo_url = "http://odoo.test"\n', encoding="utf-8")
        settings = self.settings()
        with self.assertRaises(SettingsError) as caught:
            await cli.cmd_run(settings.client(), settings, tiktok=None)
        self.assertIn("--tiktok", str(caught.exception))

        no_key = load_settings(self.folder / "clients.toml", {})
        with self.assertRaises(SettingsError) as caught:
            await cli.cmd_run(no_key.client(), no_key, tiktok="toko")
        self.assertIn("BRIDGE_KEY_UJI", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
