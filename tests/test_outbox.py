# SPDX-License-Identifier: AGPL-3.0-only
import stat
import tempfile
import unittest
from pathlib import Path

from thrift_live_bridge.outbox import Outbox


def pin(ref, nickname="Budi"):
    return {"external_message_ref": ref, "nickname": nickname}


class OutboxCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "antrian" / "uji.sqlite"
        self.outbox = Outbox(self.path)
        self.addCleanup(self.outbox.close)

    def drain(self):
        """Ambil semua kiriman menunggu, sambil menandainya sampai."""
        kinds = []
        while (item := self.outbox.next_pending()) is not None:
            kinds.append((item.kind, item.payload))
            self.outbox.mark_sent(item.id)
        return kinds


class TestOrder(OutboxCase):
    def test_oldest_first(self):
        self.outbox.enqueue_open("R1", "toko")
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.outbox.enqueue_pin("R1", pin("P2"))
        self.outbox.enqueue_close("R1")
        sent = self.drain()
        self.assertEqual([kind for kind, _ in sent], ["open", "push", "push", "close"])
        self.assertEqual(sent[0][1], {"external_ref": "R1", "external_account_ref": "toko"})
        self.assertEqual(sent[1][1]["rows"], [pin("P1")])
        self.assertEqual(sent[3][1], {"external_ref": "R1"})

    def test_unsent_item_stays_in_front(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.outbox.enqueue_pin("R1", pin("P2"))
        first = self.outbox.next_pending()
        self.outbox.mark_retry(first.id, "Odoo mati")
        again = self.outbox.next_pending()
        self.assertEqual(again.id, first.id)
        self.assertEqual(again.attempts, 1)
        self.assertEqual(again.rejections, 0)
        self.assertEqual(again.last_error, "Odoo mati")


class TestDuplicates(OutboxCase):
    def test_same_pin_is_queued_once(self):
        self.assertIsNotNone(self.outbox.enqueue_pin("R1", pin("P1")))
        self.assertIsNone(self.outbox.enqueue_pin("R1", pin("P1")))
        self.assertEqual(self.outbox.counts()["pending"], 1)

    def test_pin_already_sent_is_still_refused(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.drain()
        self.assertIsNone(self.outbox.enqueue_pin("R1", pin("P1")))
        self.assertEqual(self.outbox.counts(), {"pending": 0, "sent": 1, "rejected": 0})

    def test_pin_without_message_ref_is_always_queued(self):
        self.outbox.enqueue_pin("R1", {"nickname": "Budi"})
        self.outbox.enqueue_pin("R1", {"nickname": "Budi"})
        self.assertEqual(self.outbox.counts()["pending"], 2)

    def test_open_and_close_are_never_refused(self):
        self.outbox.enqueue_open("R1")
        self.outbox.enqueue_open("R1")
        self.outbox.enqueue_close("R1")
        self.outbox.enqueue_close("R1")
        self.assertEqual(self.outbox.counts()["pending"], 4)


class TestSurvival(OutboxCase):
    def test_pending_survives_restart(self):
        self.outbox.enqueue_pin("R1", pin("P1", "Siti 🌸"))
        self.outbox.close()
        reopened = Outbox(self.path)
        self.addCleanup(reopened.close)
        item = reopened.next_pending()
        self.assertEqual(item.payload["rows"][0]["nickname"], "Siti 🌸")

    def test_file_is_private(self):
        mode = stat.S_IMODE(self.path.stat().st_mode)
        self.assertEqual(mode, 0o600)


class TestMeta(OutboxCase):
    def test_set_get_replace_clear(self):
        self.assertIsNone(self.outbox.get_meta("open_room"))
        self.outbox.set_meta("open_room", "R1")
        self.outbox.set_meta("open_room", "R2")
        self.assertEqual(self.outbox.get_meta("open_room"), "R2")
        self.outbox.set_meta("open_room", None)
        self.assertIsNone(self.outbox.get_meta("open_room"))

    def test_survives_restart(self):
        self.outbox.set_meta("open_room", "7412345678901234567")
        self.outbox.close()
        reopened = Outbox(self.path)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.get_meta("open_room"), "7412345678901234567")

    def test_queue_file_from_before_meta_existed_still_opens(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        with self.outbox._db:
            self.outbox._db.execute("DROP TABLE meta")
        self.outbox.close()
        reopened = Outbox(self.path)
        self.addCleanup(reopened.close)
        self.assertIsNone(reopened.get_meta("open_room"))
        self.assertEqual(reopened.counts()["pending"], 1)


class TestRejected(OutboxCase):
    def test_rejected_is_set_aside_not_deleted(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.outbox.enqueue_pin("R1", pin("P2"))
        first = self.outbox.next_pending()
        self.outbox.mark_rejected(first.id, "422: isi salah")
        self.assertEqual(self.outbox.next_pending().payload["rows"], [pin("P2")])
        self.assertEqual(self.outbox.counts(), {"pending": 1, "sent": 0, "rejected": 1})

    def test_requeue_restores_original_order(self):
        self.outbox.enqueue_pin("R1", pin("P1"))
        self.outbox.enqueue_pin("R1", pin("P2"))
        self.outbox.mark_rejected(self.outbox.next_pending().id, "422")
        self.assertEqual(self.outbox.requeue_rejected(), 1)
        item = self.outbox.next_pending()
        self.assertEqual(item.payload["rows"], [pin("P1")])
        self.assertEqual((item.attempts, item.rejections), (0, 0))


class TestPurge(OutboxCase):
    def test_only_old_sent_items_are_deleted(self):
        self.outbox.enqueue_pin("R1", pin("lama"))
        self.outbox.enqueue_pin("R1", pin("baru"))
        self.outbox.enqueue_pin("R1", pin("menunggu"))
        self.outbox.enqueue_pin("R1", pin("ditolak"))
        for _ in range(2):
            self.outbox.mark_sent(self.outbox.next_pending().id)
        self.outbox.mark_rejected(self.outbox._db.execute(
            "SELECT id FROM outbox WHERE message_ref = 'ditolak'").fetchone()[0], "422")
        with self.outbox._db:
            self.outbox._db.execute(
                "UPDATE outbox SET sent_at = '2020-01-01 00:00:00' WHERE message_ref = 'lama'")
        self.assertEqual(self.outbox.purge_sent(keep_days=3), 1)
        self.assertEqual(self.outbox.counts(), {"pending": 1, "sent": 1, "rejected": 1})


if __name__ == "__main__":
    unittest.main()
