# SPDX-License-Identifier: AGPL-3.0-only
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from thrift_live_bridge.outbox import Outbox
from thrift_live_bridge.reader import LONG_PAUSE, OPEN_ROOM, Reader, reconnect_delay
from thrift_live_bridge.sender import Sender
from thrift_live_bridge.source import Connected, Ended, NotLive, Pin, RateLimited, SourceError

POLL = 60.0


def pin(ref):
    return Pin({"external_message_ref": ref, "nickname": f"Penonton {ref}"})


class FakeSource:
    """Sumber siaran berskenario: jawaban `is_live` dan isi tiap sambungan."""

    def __init__(self, live=(), connections=()):
        self.live = list(live)                  # True / False / Exception; habis = False
        self.connections = list(connections)    # daftar event; Exception = putus dengan galat
        self.listens = 0

    async def is_live(self):
        answer = self.live.pop(0) if self.live else False
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def listen(self, stop):
        self.listens += 1
        script = self.connections.pop(0)
        for item in script:
            if isinstance(item, Exception):
                raise item
            if item is HOLD:
                await stop.wait()
                return
            yield item

    async def close(self):
        pass


HOLD = object()     # sambungan tetap hidup sampai bridge dihentikan


class ReaderCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.outbox = Outbox(Path(self._tmp.name) / "uji.sqlite")
        self.addCleanup(self.outbox.close)
        self.pauses = []
        self.wakes = 0

    def wake(self):
        self.wakes += 1

    async def run_reader(self, source, pauses):
        """Jalankan pembaca sampai ia sudah berhenti sejenak sebanyak `pauses` kali."""
        stop = asyncio.Event()

        async def fake_pause(_stop, seconds):
            self.pauses.append(seconds)
            if len(self.pauses) >= pauses:
                stop.set()

        reader = Reader(source, self.outbox, account="toko", wake=self.wake, poll_seconds=POLL)
        with mock.patch.object(Reader, "_pause", staticmethod(fake_pause)):
            await asyncio.wait_for(reader.run(stop), 2.0)

    def queued(self):
        """Isi antrian sebagai daftar ringkas, sambil mengosongkannya."""
        result = []
        while (item := self.outbox.next_pending()) is not None:
            if item.kind == "push":
                result.append(("pin", item.payload["external_ref"],
                               item.payload["rows"][0]["external_message_ref"]))
            else:
                result.append((item.kind, item.payload["external_ref"]))
            self.outbox.mark_sent(item.id)
        return result


class TestOneBroadcast(ReaderCase):
    async def test_open_pins_close(self):
        source = FakeSource(live=[True], connections=[
            [Connected("R1"), pin("P1"), pin("P2"), Ended()],
        ])
        await self.run_reader(source, pauses=1)
        self.assertEqual(self.queued(), [
            ("open", "R1"), ("pin", "R1", "P1"), ("pin", "R1", "P2"), ("close", "R1"),
        ])
        self.assertEqual(self.pauses, [POLL])
        self.assertIsNone(self.outbox.get_meta(OPEN_ROOM))
        self.assertEqual(self.wakes, 4)

    async def test_account_is_sent_with_open_and_pins(self):
        source = FakeSource(live=[True], connections=[[Connected("R1"), pin("P1"), Ended()]])
        await self.run_reader(source, pauses=1)
        first = self.outbox.next_pending()
        self.assertEqual(first.payload, {"external_ref": "R1", "external_account_ref": "toko"})

    async def test_not_live_just_waits(self):
        source = FakeSource(live=[False, False, False])
        await self.run_reader(source, pauses=3)
        self.assertEqual(self.pauses, [POLL, POLL, POLL])
        self.assertEqual(source.listens, 0)
        self.assertEqual(self.queued(), [])

    async def test_offline_between_check_and_connect(self):
        source = FakeSource(live=[True], connections=[[NotLive("sedang tidak siaran")]])
        await self.run_reader(source, pauses=1)
        self.assertEqual(self.pauses, [POLL])
        self.assertEqual(self.queued(), [])


class TestDrops(ReaderCase):
    async def test_reconnect_to_same_broadcast_keeps_one_session(self):
        source = FakeSource(live=[True, True], connections=[
            [Connected("R1"), pin("P1"), SourceError("ConnectionClosedError")],
            [Connected("R1"), pin("P1"), pin("P2"), Ended()],
        ])
        await self.run_reader(source, pauses=2)
        self.assertEqual(self.queued(), [
            ("open", "R1"), ("pin", "R1", "P1"),
            ("open", "R1"), ("pin", "R1", "P2"), ("close", "R1"),
        ])
        self.assertEqual(self.pauses, [5.0, POLL])

    async def test_drop_while_host_still_live_does_not_close(self):
        source = FakeSource(live=[True], connections=[[Connected("R1"), pin("P1")]])
        await self.run_reader(source, pauses=1)
        self.assertEqual(self.queued(), [("open", "R1"), ("pin", "R1", "P1")])
        self.assertEqual(self.outbox.get_meta(OPEN_ROOM), "R1")

    async def test_drop_then_host_offline_closes(self):
        source = FakeSource(live=[True, False], connections=[[Connected("R1"), pin("P1")]])
        await self.run_reader(source, pauses=2)
        self.assertEqual(self.queued(), [("open", "R1"), ("pin", "R1", "P1"), ("close", "R1")])
        self.assertEqual(self.pauses, [5.0, POLL])

    async def test_unknown_status_never_closes(self):
        source = FakeSource(
            live=[True, SourceError("KeyError: data"), SourceError("KeyError: data"), True],
            connections=[[Connected("R1")], [Connected("R1"), Ended()]],
        )
        with self.assertLogs("thrift_live_bridge.reader", level="WARNING"):
            await self.run_reader(source, pauses=4)
        self.assertEqual(self.queued(), [("open", "R1"), ("open", "R1"), ("close", "R1")])
        self.assertEqual(self.pauses, [5.0, POLL, POLL, POLL])

    async def test_new_broadcast_closes_the_old_one_first(self):
        source = FakeSource(live=[True, True], connections=[
            [Connected("R1"), pin("P1")],
            [Connected("R2"), pin("P2"), Ended()],
        ])
        await self.run_reader(source, pauses=2)
        self.assertEqual(self.queued(), [
            ("open", "R1"), ("pin", "R1", "P1"),
            ("close", "R1"), ("open", "R2"), ("pin", "R2", "P2"), ("close", "R2"),
        ])


class TestReconnectRule(ReaderCase):
    def test_delay_table(self):
        self.assertEqual(
            [reconnect_delay(n) for n in range(1, 13)],
            [5, 10, 20, 40, 60, 60, 60, 60, 60, LONG_PAUSE, LONG_PAUSE, LONG_PAUSE],
        )

    async def test_repeated_failures_slow_down_and_never_stop(self):
        attempts = 13
        source = FakeSource(
            live=[True] * attempts,
            connections=[[SourceError("WebcastBlockedError")] for _ in range(attempts)],
        )
        with self.assertLogs("thrift_live_bridge.reader", level="ERROR") as logs:
            await self.run_reader(source, pauses=attempts)
        self.assertEqual(self.pauses, [5, 10, 20, 40, 60, 60, 60, 60, 60, 600, 600, 600, 600])
        self.assertEqual(source.listens, attempts)
        self.assertEqual(self.queued(), [])
        self.assertEqual(sum("10 kali gagal" in line for line in logs.output), 1)

    async def test_success_resets_the_count(self):
        source = FakeSource(live=[True] * 4, connections=[
            [SourceError("gagal")], [SourceError("gagal")],
            [Connected("R1"), SourceError("putus")],
            [SourceError("gagal")],
        ])
        await self.run_reader(source, pauses=4)
        self.assertEqual(self.pauses, [5.0, 10.0, 5.0, 10.0])

    async def test_rate_limit_wait_is_respected_and_capped(self):
        source = FakeSource(live=[True, True], connections=[
            [RateLimited("429", 120.0)], [RateLimited("429", 999_999.0)],
        ])
        await self.run_reader(source, pauses=2)
        self.assertEqual(self.pauses, [120.0, 3600.0])


class TestStopAndRestart(ReaderCase):
    async def follow_then_stop(self):
        source = FakeSource(live=[True], connections=[[Connected("R1"), pin("P1"), HOLD]])
        stop = asyncio.Event()
        reader = Reader(source, self.outbox, account="toko", poll_seconds=POLL)
        task = asyncio.create_task(reader.run(stop))
        for _ in range(200):
            if self.outbox.counts()["pending"] == 2:
                break
            await asyncio.sleep(0.005)
        stop.set()
        await asyncio.wait_for(task, 1.0)

    async def test_stopping_does_not_close_the_session(self):
        await self.follow_then_stop()
        self.assertEqual(self.queued(), [("open", "R1"), ("pin", "R1", "P1")])
        self.assertEqual(self.outbox.get_meta(OPEN_ROOM), "R1")

    async def test_restart_after_broadcast_ended_closes_it(self):
        await self.follow_then_stop()
        self.queued()
        await self.run_reader(FakeSource(live=[False]), pauses=1)
        self.assertEqual(self.queued(), [("close", "R1")])
        self.assertIsNone(self.outbox.get_meta(OPEN_ROOM))

    async def test_restart_while_still_live_continues_the_session(self):
        await self.follow_then_stop()
        self.queued()
        source = FakeSource(live=[True], connections=[[Connected("R1"), pin("P1"), pin("P2"), Ended()]])
        await self.run_reader(source, pauses=1)
        self.assertEqual(self.queued(), [("open", "R1"), ("pin", "R1", "P2"), ("close", "R1")])


class FakeOdoo:
    def __init__(self):
        self.calls = []

    async def call(self, method, params):
        self.calls.append((method, params["external_ref"]))
        return {}


class TestWithSender(ReaderCase):
    async def test_broadcast_reaches_odoo_in_order(self):
        odoo = FakeOdoo()
        sender = Sender(self.outbox, odoo)
        source = FakeSource(live=[True], connections=[
            [Connected("R1"), pin("P1"), pin("P2"), Ended()],
        ])
        stop = asyncio.Event()
        reader = Reader(source, self.outbox, account="toko", wake=sender.wake, poll_seconds=POLL)
        tasks = [asyncio.create_task(sender.run(stop)), asyncio.create_task(reader.run(stop))]
        for _ in range(400):
            if len(odoo.calls) == 4:
                break
            await asyncio.sleep(0.005)
        stop.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 2.0)
        self.assertEqual(odoo.calls, [
            ("open_session", "R1"), ("push", "R1"), ("push", "R1"), ("close_session", "R1"),
        ])


if __name__ == "__main__":
    unittest.main()
