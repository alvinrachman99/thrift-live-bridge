# SPDX-License-Identifier: AGPL-3.0-only
import asyncio
import unittest
from unittest import mock

import httpx

from thrift_live_bridge import ping as ping_module
from thrift_live_bridge.ping import ping_forever

URL = "https://pemantau.contoh/ping/sangat-rahasia"


class PingCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.requests = []
        self.answers = []       # jawaban berikutnya: kode status, atau galat
        patcher = mock.patch.object(ping_module, "PING_SECONDS", 0.02)
        patcher.start()
        self.addCleanup(patcher.stop)

    def http_client(self):
        def handler(request):
            self.requests.append((request.method, str(request.url)))
            answer = self.answers.pop(0) if self.answers else 200
            if isinstance(answer, Exception):
                raise answer
            return httpx.Response(answer, text="OK")
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def run_for(self, seconds):
        stop = asyncio.Event()
        task = asyncio.create_task(ping_forever(URL, stop, http_factory=self.http_client))
        await asyncio.sleep(seconds)
        stop.set()
        await asyncio.wait_for(task, 1.0)


class TestPing(PingCase):
    async def test_pings_repeatedly(self):
        await self.run_for(0.07)
        self.assertGreaterEqual(len(self.requests), 3)
        self.assertEqual(set(self.requests), {("GET", URL)})

    async def test_failure_is_tolerated_and_address_never_logged(self):
        self.answers = [httpx.ConnectError("tidak tersambung"), 503, 200]
        with self.assertLogs("thrift_live_bridge.ping", level="INFO") as logs:
            await self.run_for(0.09)
        self.assertGreaterEqual(len(self.requests), 3)
        text = "\n".join(logs.output)
        self.assertIn("Ping pemantau gagal (ConnectError)", text)
        self.assertIn("berhasil lagi", text)
        self.assertEqual(text.count("gagal"), 1)        # sekali per rentetan gagal
        self.assertNotIn("sangat-rahasia", text)
        self.assertNotIn("pemantau.contoh", text)

    async def test_stop_ends_promptly(self):
        with mock.patch.object(ping_module, "PING_SECONDS", 30.0):
            await self.run_for(0.01)
        self.assertEqual(len(self.requests), 1)


if __name__ == "__main__":
    unittest.main()
