# SPDX-License-Identifier: AGPL-3.0-only
import asyncio
import unittest
from unittest import mock

from thrift_live_bridge import heartbeat as heartbeat_module
from thrift_live_bridge.heartbeat import Heartbeat
from thrift_live_bridge.odoo import Rejected, RetryLater
from thrift_live_bridge.reader import Status


class FakeDoor:
    def __init__(self):
        self.calls = []
        self.fail = None        # galat yang dilempar tiap panggilan

    async def call(self, method, params):
        self.calls.append((method, dict(params)))
        if self.fail is not None:
            raise self.fail
        return True


class HeartbeatCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.door = FakeDoor()
        self.status = Status()
        self.heartbeat = Heartbeat(self.door, "toko", lambda: self.status, label="uji")

    def fast(self, interval=0.02, gap=0.0):
        for name, value in (("HEARTBEAT_SECONDS", interval), ("MIN_GAP", gap)):
            patcher = mock.patch.object(heartbeat_module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def run_for(self, seconds, during=None):
        stop = asyncio.Event()
        task = asyncio.create_task(self.heartbeat.run(stop))
        if during is not None:
            await during()
        await asyncio.sleep(seconds)
        stop.set()
        await asyncio.wait_for(task, 1.0)


class TestReport(HeartbeatCase):
    async def test_waiting_report_carries_only_what_exists(self):
        self.assertTrue(await self.heartbeat.report())
        self.assertEqual(self.door.calls, [
            ("heartbeat", {"external_account_ref": "toko", "state": "waiting", "failures": 0}),
        ])

    async def test_full_report(self):
        self.status = Status(
            state="retrying", room="7412345678901234567", failures=3,
            message="WebcastBlockedError: ditolak", last_event="2026-10-05 12:00:00",
        )
        await self.heartbeat.report()
        self.assertEqual(self.door.calls[0][1], {
            "external_account_ref": "toko", "state": "retrying", "failures": 3,
            "external_ref": "7412345678901234567", "last_event": "2026-10-05 12:00:00",
            "message": "WebcastBlockedError: ditolak",
        })

    async def test_failure_is_swallowed_and_logged_once_per_streak(self):
        self.door.fail = RetryLater("Odoo tidak bisa dihubungi (ConnectError)")
        with self.assertLogs("thrift_live_bridge.heartbeat", level="INFO") as logs:
            self.assertFalse(await self.heartbeat.report())
            self.assertFalse(await self.heartbeat.report())
            self.door.fail = None
            self.assertTrue(await self.heartbeat.report())
        self.assertEqual(len(logs.output), 2)
        self.assertIn("[uji] Laporan status belum sampai", logs.output[0])
        self.assertIn("sampai lagi", logs.output[1])

    async def test_rejection_by_odoo_is_also_swallowed(self):
        self.door.fail = Rejected("422: Status laporan tidak dikenal")
        with self.assertLogs("thrift_live_bridge.heartbeat", level="WARNING"):
            self.assertFalse(await self.heartbeat.report())


class TestLoop(HeartbeatCase):
    async def test_reports_at_start_and_then_on_schedule(self):
        self.fast(interval=0.03)
        await self.run_for(0.1)
        self.assertGreaterEqual(len(self.door.calls), 3)
        self.assertLessEqual(len(self.door.calls), 5)

    async def test_change_is_reported_without_waiting_for_the_schedule(self):
        self.fast(interval=30.0)

        async def change():
            await asyncio.sleep(0.02)
            self.status = Status(state="connected", room="R1")
            self.heartbeat.wake()

        await self.run_for(0.05, during=change)
        self.assertEqual([params["state"] for _, params in self.door.calls], ["waiting", "connected"])

    async def test_burst_of_changes_becomes_few_reports(self):
        self.fast(interval=30.0, gap=0.05)

        async def burst():
            for _ in range(10):
                self.heartbeat.wake()
                await asyncio.sleep(0.002)

        await self.run_for(0.12, during=burst)
        self.assertLessEqual(len(self.door.calls), 4)

    async def test_unexpected_error_does_not_stop_the_loop(self):
        self.fast(interval=0.02)
        self.door.fail = RuntimeError("galat yang tidak diperkirakan")
        with self.assertLogs("thrift_live_bridge.heartbeat", level="ERROR"):
            await self.run_for(0.07)
        self.assertGreaterEqual(len(self.door.calls), 2)

    async def test_stop_ends_the_loop_promptly(self):
        self.fast(interval=30.0)
        await self.run_for(0.01)
        self.assertEqual(len(self.door.calls), 1)


if __name__ == "__main__":
    unittest.main()
