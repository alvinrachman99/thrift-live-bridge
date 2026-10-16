# SPDX-License-Identifier: AGPL-3.0-only
import json
import unittest
from pathlib import Path

import httpx

from thrift_live_bridge.odoo import OdooDoor, Rejected, RetryLater
from thrift_live_bridge.settings import ClientSettings

KEY = "kunci-rahasia-uji"


def client_settings(**extra):
    return ClientSettings(
        code="uji", odoo_url="http://odoo.test:8069", queue_path=Path("uji.sqlite"),
        api_key_env="BRIDGE_KEY_UJI", api_key=KEY, **extra,
    )


def door_with(handler, **extra):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return http, OdooDoor(http, client_settings(**extra))


class TestRequest(unittest.IsolatedAsyncioTestCase):
    async def test_url_headers_and_body(self):
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            seen["headers"] = request.headers
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"id": 4, "name": "THS/1", "state": "open"})

        http, door = door_with(handler, odoo_host="a5tr.contoh.id", odoo_db="a5tr")
        async with http:
            answer = await door.call("open_session", {"external_ref": "R1"})

        self.assertEqual(answer, {"id": 4, "name": "THS/1", "state": "open"})
        self.assertEqual(seen["url"], "http://odoo.test:8069/json/2/thrift.bot/open_session")
        self.assertEqual(seen["headers"]["authorization"], f"bearer {KEY}")
        self.assertEqual(seen["headers"]["host"], "a5tr.contoh.id")
        self.assertEqual(seen["headers"]["x-odoo-database"], "a5tr")
        self.assertEqual(seen["headers"]["content-type"], "application/json")
        self.assertEqual(seen["body"], {"external_ref": "R1"})

    async def test_optional_headers_absent_by_default(self):
        seen = {}

        def handler(request):
            seen["headers"] = request.headers
            return httpx.Response(200, json=False)

        http, door = door_with(handler)
        async with http:
            self.assertIs(await door.call("close_session", {"external_ref": "X"}), False)
        self.assertEqual(seen["headers"]["host"], "odoo.test:8069")
        self.assertNotIn("x-odoo-database", seen["headers"])


class TestAnswers(unittest.IsolatedAsyncioTestCase):
    async def call_with(self, handler):
        http, door = door_with(handler)
        async with http:
            return await door.call("push", {"external_ref": "R1", "rows": []})

    async def test_422_is_rejected_without_debug_trace(self):
        def handler(request):
            return httpx.Response(422, json={
                "name": "odoo.exceptions.UserError",
                "message": "`rows` harus berupa daftar baris.\nbaris kedua",
                "debug": "Traceback (most recent call last): ... sangat panjang",
            })

        with self.assertRaises(Rejected) as caught:
            await self.call_with(handler)
        self.assertEqual(str(caught.exception), "422: `rows` harus berupa daftar baris.")

    async def test_other_statuses_are_retried(self):
        for status in (401, 403, 404, 409, 429, 500, 502, 503):
            with self.subTest(status=status):
                def handler(request, status=status):
                    return httpx.Response(status, json={"message": "tidak bisa"})

                with self.assertRaises(RetryLater) as caught:
                    await self.call_with(handler)
                self.assertTrue(str(caught.exception).startswith(f"{status}: "))

    async def test_html_error_page_is_retried(self):
        def handler(request):
            return httpx.Response(502, text="<html>Bad Gateway</html>")

        with self.assertRaises(RetryLater) as caught:
            await self.call_with(handler)
        self.assertEqual(str(caught.exception), "502: Bad Gateway")

    async def test_unreachable_is_retried(self):
        def handler(request):
            raise httpx.ConnectError("connection refused", request=request)

        with self.assertRaises(RetryLater) as caught:
            await self.call_with(handler)
        self.assertEqual(str(caught.exception), "Odoo tidak bisa dihubungi (ConnectError)")

    async def test_timeout_is_retried(self):
        def handler(request):
            raise httpx.ReadTimeout("lama", request=request)

        with self.assertRaises(RetryLater) as caught:
            await self.call_with(handler)
        self.assertEqual(str(caught.exception), "Odoo tidak menjawab tepat waktu")

    async def test_200_that_is_not_json_is_retried(self):
        def handler(request):
            return httpx.Response(200, text="<html>halaman login</html>")

        with self.assertRaises(RetryLater):
            await self.call_with(handler)


class TestSecret(unittest.IsolatedAsyncioTestCase):
    async def test_key_never_appears_in_errors_or_repr(self):
        def handler(request):
            return httpx.Response(401, json={"message": "Invalid apikey"})

        http, door = door_with(handler)
        async with http:
            with self.assertRaises(RetryLater) as caught:
                await door.call("push", {"external_ref": "R1", "rows": []})
        self.assertNotIn(KEY, str(caught.exception))
        self.assertNotIn(KEY, repr(caught.exception))
        self.assertNotIn(KEY, repr(client_settings()))


if __name__ == "__main__":
    unittest.main()
