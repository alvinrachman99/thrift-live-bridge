# SPDX-License-Identifier: AGPL-3.0-only
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from thrift_live_bridge import sender as sender_module
from thrift_live_bridge.odoo import Rejected, RetryLater
from thrift_live_bridge.outbox import Outbox
from thrift_live_bridge.sender import MAX_REJECTIONS, Sender


def pin(ref):
    return {"external_message_ref": ref, "nickname": f"Penonton {ref}"}


class FakeOdoo:
    """Meniru pintu Odoo, termasuk sifatnya melewati pin kembar."""

    def __init__(self):
        self.down = False
        self.received = []          # (method, params) yang sampai di Odoo
        self.rows = []              # ID pin yang tersimpan, tanpa kembaran
        self.reject = set()         # ID pin yang isinya ditolak
        self.lose_answers = 0       # sampai di Odoo, tapi jawabannya hilang di jalan

    async def call(self, method, params):
        if self.down:
            raise RetryLater("Odoo tidak bisa dihubungi (ConnectError)")
        refs = [row["external_message_ref"] for row in params.get("rows", [])]
        if any(ref in self.reject for ref in refs):
            raise Rejected("422: isi kiriman salah")
        self.received.append((method, params))
        for ref in refs:
            if ref not in self.rows:
                self.rows.append(ref)
        if self.lose_answers:
            self.lose_answers -= 1
            raise RetryLater("Odoo tidak menjawab tepat waktu")
        return {"created": len(refs), "duplicate": 0}


class SenderCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.outbox = Outbox(Path(self._tmp.name) / "uji.sqlite")
        self.addCleanup(self.outbox.close)
        self.odoo = FakeOdoo()
        self.sender = Sender(self.outbox, self.odoo)

    def queue_session(self, ref="R1", pins=("P1", "P2")):
        self.outbox.enqueue_open(ref, "toko")
        for message_ref in pins:
            self.outbox.enqueue_pin(ref, pin(message_ref))
        self.outbox.enqueue_close(ref)


class TestDelivery(SenderCase):
    async def test_everything_arrives_in_order(self):
        self.queue_session()
        result = await self.sender.send_pending()
        self.assertEqual((result.sent, result.rejected, result.blocked), (4, 0, None))
        self.assertEqual(
            [method for method, _ in self.odoo.received],
            ["open_session", "push", "push", "close_session"],
        )
        self.assertEqual(self.odoo.rows, ["P1", "P2"])
        self.assertEqual(self.outbox.counts(), {"pending": 0, "sent": 4, "rejected": 0})

    async def test_nothing_is_lost_while_odoo_is_down(self):
        self.queue_session()
        self.odoo.down = True
        for _ in range(3):
            result = await self.sender.send_pending()
            self.assertEqual(result.sent, 0)
            self.assertEqual(result.blocked, "Odoo tidak bisa dihubungi (ConnectError)")
        self.assertEqual(self.outbox.counts()["pending"], 4)
        self.assertEqual(self.outbox.next_pending().attempts, 3)

        self.odoo.down = False
        result = await self.sender.send_pending()
        self.assertEqual((result.sent, result.blocked), (4, None))
        self.assertEqual(self.odoo.rows, ["P1", "P2"])

    async def test_item_behind_a_stuck_one_waits(self):
        self.queue_session()
        self.odoo.down = True
        await self.sender.send_pending()
        self.assertEqual(self.odoo.received, [])

    async def test_lost_answer_does_not_duplicate_the_pin(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.odoo.lose_answers = 1
        result = await self.sender.send_pending()
        self.assertEqual(result.sent, 0)
        self.assertEqual(self.outbox.counts()["pending"], 1)

        result = await self.sender.send_pending()
        self.assertEqual(result.sent, 1)
        # Odoo menerimanya dua kali, tapi yang tersimpan tetap satu.
        self.assertEqual(len(self.odoo.received), 2)
        self.assertEqual(self.odoo.rows, ["P1"])


class TestRejection(SenderCase):
    async def test_bad_item_is_set_aside_and_queue_moves_on(self):
        self.queue_session(pins=("P1", "RUSAK", "P3"))
        self.odoo.reject = {"RUSAK"}
        rounds = [await self.sender.send_pending() for _ in range(MAX_REJECTIONS)]
        self.assertEqual([r.sent for r in rounds], [2, 0, 2])
        self.assertEqual([bool(r.blocked) for r in rounds], [True, True, False])
        self.assertEqual(rounds[-1].rejected, 1)
        self.assertEqual(self.odoo.rows, ["P1", "P3"])
        self.assertEqual(self.outbox.counts(), {"pending": 0, "sent": 4, "rejected": 1})

    async def test_downtime_does_not_count_as_rejection(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.odoo.down = True
        for _ in range(MAX_REJECTIONS + 5):
            await self.sender.send_pending()
        self.odoo.down = False
        self.odoo.reject = {"P1"}
        await self.sender.send_pending()
        item = self.outbox.next_pending()
        self.assertIsNotNone(item, "kiriman tidak boleh disisihkan hanya karena Odoo sempat mati")
        self.assertEqual((item.attempts, item.rejections), (MAX_REJECTIONS + 6, 1))


class TestRunLoop(SenderCase):
    def setUp(self):
        super().setUp()
        for name in ("BACKOFF_START", "BACKOFF_MAX", "POLL_INTERVAL"):
            patcher = mock.patch.object(sender_module, name, 0.01)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def wait_until(self, condition, seconds=2.0):
        deadline = asyncio.get_running_loop().time() + seconds
        while not condition():
            if asyncio.get_running_loop().time() > deadline:
                self.fail("keadaan yang ditunggu tidak tercapai")
            await asyncio.sleep(0.005)

    async def test_recovers_by_itself_when_odoo_comes_back(self):
        self.queue_session()
        self.odoo.down = True
        stop = asyncio.Event()
        task = asyncio.create_task(self.sender.run(stop))
        await self.wait_until(lambda: self.outbox.next_pending().attempts >= 3)
        self.assertEqual(self.odoo.received, [])

        self.odoo.down = False
        await self.wait_until(lambda: self.outbox.counts()["pending"] == 0)
        stop.set()
        await asyncio.wait_for(task, 1.0)
        self.assertEqual(self.odoo.rows, ["P1", "P2"])

    async def test_new_item_is_sent_while_running(self):
        stop = asyncio.Event()
        task = asyncio.create_task(self.sender.run(stop))
        await asyncio.sleep(0.02)
        self.outbox.enqueue_pin("R1", pin("P9"))
        self.sender.wake()
        await self.wait_until(lambda: self.odoo.rows == ["P9"])
        stop.set()
        await asyncio.wait_for(task, 1.0)

    async def test_stop_ends_the_loop_promptly(self):
        with mock.patch.object(sender_module, "POLL_INTERVAL", 30.0):
            stop = asyncio.Event()
            task = asyncio.create_task(self.sender.run(stop))
            await asyncio.sleep(0.02)
            stop.set()
            await asyncio.wait_for(task, 1.0)


if __name__ == "__main__":
    unittest.main()
