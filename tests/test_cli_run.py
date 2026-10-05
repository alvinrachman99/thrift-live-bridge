# SPDX-License-Identifier: AGPL-3.0-only
"""Perintah `run`: pembaca, pengirim, dan cara berhentinya, dirangkai jadi satu."""
import asyncio
import functools
import json
import os
import signal
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

from thrift_live_bridge import __main__ as cli
from thrift_live_bridge import runner
from thrift_live_bridge.outbox import Outbox
from thrift_live_bridge.reader import room_key
from thrift_live_bridge.settings import SettingsError, load_settings
from thrift_live_bridge.source import Connected, Pin

ROOM = "7412345678901234567"


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
        yield Connected(ROOM)
        yield Pin({"external_message_ref": "P1", "nickname": "Budi", "comment": "35"})
        await stop.wait()

    async def close(self):
        self.closed = True


class TestRun(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.folder = Path(self._tmp.name)
        self.write('[[client]]\ncode = "uji"\nodoo_url = "http://odoo.test"\ntiktok = "@toko"\n')
        self.received = []
        ScriptedSource.created = []

    def write(self, text):
        (self.folder / "clients.toml").write_text(text, encoding="utf-8")

    def settings(self, **environ):
        return load_settings(self.folder / "clients.toml", {"BRIDGE_KEY_UJI": "kunci", **environ})

    def handler(self, request):
        method = request.url.path.rsplit("/", 1)[1]
        if method != "heartbeat":
            self.received.append((method, json.loads(request.content)))
        return httpx.Response(200, json={})

    async def run_until(self, settings, condition, **options):
        def http_client():
            return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

        async def interrupt_when_ready():
            for _ in range(600):
                if condition():
                    break
                await asyncio.sleep(0.005)
            os.kill(os.getpid(), signal.SIGINT)     # sama dengan menekan Ctrl+C

        run_all = functools.partial(
            runner.run_all, source_factory=ScriptedSource, http_factory=http_client,
        )
        options = {"client_code": None, "tiktok": None, **options}
        with mock.patch.object(cli, "run_all", run_all):
            stopper = asyncio.create_task(interrupt_when_ready())
            code = await asyncio.wait_for(cli.cmd_run(settings, **options), 5.0)
            await stopper
        return code

    async def test_broadcast_reaches_odoo_and_ctrl_c_stops_cleanly(self):
        settings = self.settings(EULER_API_KEY="kunci-euler")
        code = await self.run_until(settings, lambda: len(self.received) >= 2)

        self.assertEqual(code, 0)
        self.assertEqual([name for name, _ in self.received], ["open_session", "push"])
        self.assertEqual(self.received[0][1], {"external_ref": ROOM, "external_account_ref": "toko"})
        self.assertEqual(self.received[1][1]["rows"][0]["external_message_ref"], "P1")

        source = ScriptedSource.created[0]
        self.assertEqual((source.unique_id, source.sign_api_key), ("toko", "kunci-euler"))
        self.assertTrue(source.closed)
        # Berhenti di tengah siaran: sesinya tidak ditutup, dan tetap tercatat.
        with Outbox(settings.client().queue_path) as outbox:
            self.assertEqual(outbox.get_meta(room_key("toko")), ROOM)
            self.assertEqual(outbox.counts(), {"pending": 0, "sent": 2, "rejected": 0})

    async def test_account_can_be_overridden_for_testing(self):
        settings = self.settings()
        with self.assertLogs("thrift_live_bridge.runner", level="WARNING") as logs:
            await self.run_until(settings, lambda: len(self.received) >= 2, tiktok="@akun.lain")
        self.assertEqual(ScriptedSource.created[0].unique_id, "akun.lain")
        self.assertIsNone(ScriptedSource.created[0].sign_api_key)
        self.assertTrue(any("EULER_API_KEY" in line for line in logs.output))

    async def test_missing_account_or_key_is_a_settings_error(self):
        self.write('[[client]]\ncode = "uji"\nodoo_url = "http://odoo.test"\n')
        with self.assertRaises(SettingsError) as caught:
            await cli.cmd_run(self.settings(), client_code=None, tiktok=None)
        self.assertIn("--tiktok", str(caught.exception))

        no_key = load_settings(self.folder / "clients.toml", {})
        with self.assertRaises(SettingsError) as caught:
            await cli.cmd_run(no_key, client_code=None, tiktok="toko")
        self.assertIn("BRIDGE_KEY_UJI", str(caught.exception))

    async def test_account_override_needs_exactly_one_client(self):
        self.write(
            '[[client]]\ncode = "a"\nodoo_url = "http://odoo.test"\n'
            '[[client]]\ncode = "b"\nodoo_url = "http://odoo.test"\n'
        )
        settings = load_settings(self.folder / "clients.toml", {})
        with self.assertRaises(SettingsError) as caught:
            await cli.cmd_run(settings, client_code=None, tiktok="toko")
        self.assertIn("--client", str(caught.exception))


class TestStatus(unittest.TestCase):
    def test_status_lists_every_client(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "clients.toml"
            path.write_text(
                '[[client]]\ncode = "a"\nodoo_url = "http://odoo.test"\n'
                '[[client]]\ncode = "b"\nodoo_url = "http://odoo.test"\n', encoding="utf-8")
            with Outbox(Path(tmp) / "data" / "b.sqlite") as outbox:
                outbox.enqueue_open("R1")
            with mock.patch("builtins.print") as printed:
                self.assertEqual(cli.main(["--settings", str(path), "status"]), 0)
            lines = [call.args[0] if call.args else "" for call in printed.call_args_list]
            self.assertIn("Client   : a", lines)
            self.assertIn("Client   : b", lines)
            self.assertEqual([line for line in lines if line.startswith("Menunggu")],
                             ["Menunggu : 0", "Menunggu : 1"])

    def test_other_commands_need_a_client_when_there_are_several(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "clients.toml"
            path.write_text(
                '[[client]]\ncode = "a"\nodoo_url = "http://odoo.test"\n'
                '[[client]]\ncode = "b"\nodoo_url = "http://odoo.test"\n', encoding="utf-8")
            with mock.patch("builtins.print"):
                self.assertEqual(cli.main(["--settings", str(path), "fake", "open", "--ref", "R"]), 2)
                self.assertEqual(
                    cli.main(["--settings", str(path), "--client", "b", "fake", "open", "--ref", "R"]), 0)


if __name__ == "__main__":
    unittest.main()
