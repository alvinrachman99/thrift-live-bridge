# SPDX-License-Identifier: AGPL-3.0-only
import asyncio
import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
from TikTokLive.client.errors import SignatureRateLimitError, UserOfflineError, WebcastBlockedError
from TikTokLive.events import ConnectEvent, LiveEndEvent, RoomPinEvent
from TikTokLive.events.proto_events import WebcastChatMessage
from TikTokLiveProto.v3.webcast.im import PinMessageActionType
from TikTokLiveProto.v3.webcast.model.base.user import User

from thrift_live_bridge.source import Connected, Ended, NotLive, Pin, RateLimited, SourceError
from thrift_live_bridge.tiktok import TikTokSource, is_complete, pin_to_row

NOW = datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)
NOW_MS = int(NOW.timestamp() * 1000)
PIN_ID = 7412345678901234567
USER_ID = 7234567890123456789


def pin_event(**overrides):
    values = dict(
        action=PinMessageActionType.PIN,
        pin_msg_id=PIN_ID,
        pin_time=NOW_MS - 2000,
        display_duration=60,
        method="WebcastChatMessage",
        chat_message=WebcastChatMessage(
            content=" kemeja flanel 35 ",
            user=User(
                id=USER_ID, nickname="Budi 🧢", display_id="budi_88",
                sec_uid="MS4wLjABAAAA-rahasia", bio_description="bio yang tidak dipakai",
            ),
        ),
        operator=User(id=6900000000000000001),
    )
    values.update(overrides)
    return RoomPinEvent(**values)


class TestPinToRow(unittest.TestCase):
    def test_comment_pin(self):
        row = pin_to_row(pin_event(), NOW)
        self.assertEqual(row["external_message_ref"], str(PIN_ID))
        self.assertEqual(row["external_user_ref"], str(USER_ID))
        self.assertEqual(row["handle"], "budi_88")
        self.assertEqual(row["nickname"], "Budi 🧢")
        self.assertEqual(row["comment"], "kemeja flanel 35")
        self.assertEqual(row["date"], "2026-10-05 11:59:58")
        self.assertTrue(is_complete(row))

    def test_ids_are_text_not_numbers(self):
        row = pin_to_row(pin_event(), NOW)
        self.assertIsInstance(row["external_message_ref"], str)
        self.assertIsInstance(row["external_user_ref"], str)
        self.assertEqual(len(row["external_user_ref"]), 19)

    def test_survives_a_round_trip_through_the_wire_format(self):
        parsed = RoomPinEvent().parse(bytes(pin_event()))
        self.assertEqual(pin_to_row(parsed, NOW), pin_to_row(pin_event(), NOW))

    def test_unpin_is_ignored(self):
        cancel = RoomPinEvent(action=PinMessageActionType.PIN_CANCEL, pin_msg_id=PIN_ID)
        self.assertIsNone(pin_to_row(cancel, NOW))

    def test_unknown_action_is_ignored(self):
        self.assertIsNone(pin_to_row(pin_event(action=PinMessageActionType.UN_KOWN), NOW))

    def test_pin_that_is_not_a_comment_is_ignored(self):
        self.assertIsNone(pin_to_row(pin_event(chat_message=None), NOW))


class TestOnlyWhatIsUsed(unittest.TestCase):
    def test_raw_payload_is_small_and_has_no_profile_data(self):
        row = pin_to_row(pin_event(), NOW)
        raw = json.loads(row["raw_payload"])
        self.assertEqual(raw, {
            "source": "tiktok",
            "method": "WebcastChatMessage",
            "pin_msg_id": str(PIN_ID),
            "pin_time": NOW_MS - 2000,
            "display_duration": 60,
            "operator_id": "6900000000000000001",
            "user_id": str(USER_ID),
            "display_id": "budi_88",
            "nickname": "Budi 🧢",
            "content": "kemeja flanel 35",
        })
        self.assertLess(len(row["raw_payload"].encode()), 400)

    def test_bio_and_sec_uid_never_leave(self):
        text = json.dumps(pin_to_row(pin_event(), NOW), ensure_ascii=False)
        self.assertNotIn("bio yang tidak dipakai", text)
        self.assertNotIn("MS4wLjABAAAA", text)

    def test_only_known_keys(self):
        self.assertEqual(set(pin_to_row(pin_event(), NOW)), {
            "date", "external_message_ref", "external_user_ref", "handle", "nickname",
            "comment", "raw_payload",
        })


class TestOddEvents(unittest.TestCase):
    def test_incomplete_pin_is_kept_and_flagged(self):
        event = pin_event(pin_msg_id=0, chat_message=WebcastChatMessage(content="35"))
        row = pin_to_row(event, NOW)
        self.assertEqual(row["comment"], "35")
        self.assertNotIn("external_message_ref", row)
        self.assertNotIn("nickname", row)
        self.assertFalse(is_complete(row))

    def test_implausible_time_falls_back_to_receive_time(self):
        for pin_time in (0, -5, 1, NOW_MS + 86_400_000, NOW_MS - 10 * 86_400_000, 10 ** 30):
            with self.subTest(pin_time=pin_time):
                event = SimpleNamespace(
                    action=1, pin_msg_id=1, pin_time=pin_time,
                    chat_message=SimpleNamespace(content="x", user=None),
                )
                self.assertEqual(pin_to_row(event, NOW)["date"], "2026-10-05 12:00:00")

    def test_time_in_seconds_is_understood(self):
        event = pin_event(pin_time=int(NOW.timestamp()) - 30)
        self.assertEqual(pin_to_row(event, NOW)["date"], "2026-10-05 11:59:30")

    def test_plain_object_with_missing_fields(self):
        self.assertIsNone(pin_to_row(SimpleNamespace(), NOW))
        row = pin_to_row(SimpleNamespace(action=1, chat_message=SimpleNamespace()), NOW)
        self.assertEqual(set(row), {"date", "raw_payload"})
        self.assertEqual(json.loads(row["raw_payload"]), {"source": "tiktok"})


class FakeClient:
    """Meniru TikTokLiveClient sebatas yang dipakai TikTokSource."""

    def __init__(self):
        self.handlers = {}
        self.live = True
        self.start_error = None
        self.script = []            # event yang dipancarkan setelah tersambung
        self.fail_with = None       # galat yang mengakhiri sambungan
        self.hold = False           # sambungan tetap hidup sampai disconnect()
        self.starts = 0
        self.disconnects = 0
        self.web_closed = False
        self.web = SimpleNamespace(close=self._close_web)
        self._release = asyncio.Event()
        self._task = None

    def add_listener(self, event, f):
        self.handlers[event] = f

    async def is_live(self):
        if isinstance(self.live, Exception):
            raise self.live
        return self.live

    async def start(self):
        self.starts += 1
        if self.start_error is not None:
            raise self.start_error
        self._release.clear()
        self._task = asyncio.ensure_future(self._connection())
        return self._task

    async def _connection(self):
        for event in self.script:
            self.handlers[type(event)](event)
            await asyncio.sleep(0)
        if self.hold:
            await self._release.wait()
        if self.fail_with is not None:
            raise self.fail_with

    async def disconnect(self):
        self.disconnects += 1
        self._release.set()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)

    async def _close_web(self):
        self.web_closed = True


class SourceCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = FakeClient()
        self.source = TikTokSource("@toko", client=self.client)
        self.stop = asyncio.Event()

    async def collect(self):
        return [event async for event in self.source.listen(self.stop)]


class TestSourceEvents(SourceCase):
    async def test_events_come_out_in_order(self):
        self.client.script = [
            ConnectEvent(unique_id="toko", room_id=PIN_ID + 1),
            pin_event(pin_msg_id=1),
            RoomPinEvent(action=PinMessageActionType.PIN_CANCEL, pin_msg_id=1),
            pin_event(pin_msg_id=2),
            LiveEndEvent(),
        ]
        events = await self.collect()
        self.assertEqual(events[0], Connected(str(PIN_ID + 1)))
        self.assertEqual([type(e) for e in events], [Connected, Pin, Pin, Ended])
        self.assertEqual([e.row["external_message_ref"] for e in events[1:3]], ["1", "2"])
        self.assertGreaterEqual(self.client.disconnects, 1)

    async def test_broken_event_does_not_break_the_connection(self):
        self.client.script = [
            ConnectEvent(unique_id="toko", room_id=5),
            pin_event(pin_msg_id=1),
            pin_event(pin_msg_id=2),
        ]
        original = self.client.handlers[RoomPinEvent]
        calls = []

        def flaky(event):
            calls.append(event)
            if len(calls) == 1:
                # Paksa galat di dalam penangan untuk pin pertama.
                return original(SimpleNamespace(action=1, chat_message=object(), pin_time="rusak"))
            return original(event)

        self.client.handlers[RoomPinEvent] = flaky
        with self.assertLogs("thrift_live_bridge.tiktok", level="WARNING"):
            events = await self.collect()
        self.assertEqual(type(events[0]), Connected)
        self.assertEqual(events[-1].row["external_message_ref"], "2")

    async def test_leftovers_from_previous_connection_are_dropped(self):
        self.source._queue.put_nowait(Pin({"external_message_ref": "basi"}))
        self.client.script = [ConnectEvent(unique_id="toko", room_id=5)]
        self.assertEqual(await self.collect(), [Connected("5")])


class TestSourceFailures(SourceCase):
    async def test_offline(self):
        self.client.start_error = UserOfflineError()
        with self.assertRaises(NotLive):
            await self.collect()

    async def test_rate_limited(self):
        response = httpx.Response(429, headers={"RateLimit-Remaining": "42"})
        self.client.start_error = SignatureRateLimitError(
            None, "Too many connections started, try again in %s seconds.", response=response,
        )
        with self.assertRaises(RateLimited) as caught:
            await self.collect()
        self.assertEqual(caught.exception.retry_after, 42.0)
        self.assertEqual(
            str(caught.exception), "penyedia tanda tangan meminta menunggu (SignatureRateLimitError)")

    async def test_rate_limited_without_header_still_waits(self):
        error = SignatureRateLimitError(
            None, "try again in %s seconds.",
            response=httpx.Response(429, headers={"RateLimit-Remaining": "7"}),
        )
        error.__dict__.pop("retry_after", None)
        error._response = httpx.Response(429)
        self.client.start_error = error
        with self.assertRaises(RateLimited) as caught:
            await self.collect()
        self.assertEqual(caught.exception.retry_after, 60.0)

    async def test_blocked_at_connect(self):
        self.client.start_error = WebcastBlockedError("WebSocket rejected by TikTok")
        with self.assertRaises(SourceError) as caught:
            await self.collect()
        self.assertNotIsInstance(caught.exception, (NotLive, RateLimited))
        self.assertEqual(str(caught.exception), "sambungan ditolak TikTok (WebcastBlockedError)")

    async def test_drop_after_events_delivers_them_first(self):
        self.client.script = [ConnectEvent(unique_id="toko", room_id=5), pin_event(pin_msg_id=1)]
        self.client.fail_with = ConnectionError("no close frame received\nbaris kedua")
        got = []
        with self.assertRaises(SourceError) as caught:
            async for event in self.source.listen(self.stop):
                got.append(event)
        self.assertEqual([type(e) for e in got], [Connected, Pin])
        self.assertEqual(str(caught.exception), "ConnectionError: no close frame received")

    async def test_known_failures_get_a_reason_an_admin_can_read(self):
        from TikTokLive.client.errors import UserNotFoundError
        cases = [
            (UserNotFoundError("toko", "The requested user 'toko' is not capable of going LIVE ..."),
             "akun tidak ditemukan atau belum pernah siaran (UserNotFoundError)"),
            (httpx.ConnectError("gagal"), "jaringan bermasalah (ConnectError)"),
            (KeyError("data"), "jawaban TikTok tidak dikenali (KeyError)"),
        ]
        for error, expected in cases:
            with self.subTest(error=type(error).__name__):
                self.client.live = error
                with self.assertRaises(SourceError) as caught:
                    await self.source.is_live()
                self.assertEqual(str(caught.exception), expected)

    async def test_is_live_failure_is_a_source_error(self):
        self.client.live = KeyError("data")
        with self.assertRaises(SourceError):
            await self.source.is_live()
        self.client.live = False
        self.assertIs(await self.source.is_live(), False)


class TestSourceStop(SourceCase):
    async def test_stop_disconnects_cleanly(self):
        self.client.script = [ConnectEvent(unique_id="toko", room_id=5)]
        self.client.hold = True
        got = []

        async def consume():
            async for event in self.source.listen(self.stop):
                got.append(event)

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)
        self.assertEqual(got, [Connected("5")])
        self.assertFalse(task.done())
        self.stop.set()
        await asyncio.wait_for(task, 1.0)
        self.assertEqual(self.client.disconnects, 1)

    async def test_close_shuts_the_http_client_without_client_close(self):
        await self.source.close()
        self.assertTrue(self.client.web_closed)


if __name__ == "__main__":
    unittest.main()
