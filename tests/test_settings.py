# SPDX-License-Identifier: AGPL-3.0-only
import tempfile
import unittest
from pathlib import Path

from thrift_live_bridge.settings import SettingsError, default_key_env, load_settings

ONE_CLIENT = """
[[client]]
code = "b47v"
odoo_url = "http://caddy:8080/"
odoo_host = "b47v.contoh.id"
odoo_db = "b47v"
"""


class SettingsCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.folder = Path(self._tmp.name)

    def write(self, text, name="clients.toml"):
        path = self.folder / name
        path.write_text(text, encoding="utf-8")
        return path

    def assert_error(self, text, fragment, environ=None):
        with self.assertRaises(SettingsError) as caught:
            load_settings(self.write(text), environ or {})
        self.assertIn(fragment, str(caught.exception))


class TestLoad(SettingsCase):
    def test_one_client(self):
        settings = load_settings(self.write(ONE_CLIENT), {"BRIDGE_KEY_B47V": " rahasia \n"})
        client = settings.client()
        self.assertEqual(client.code, "b47v")
        self.assertEqual(client.odoo_url, "http://caddy:8080")
        self.assertEqual(client.odoo_host, "b47v.contoh.id")
        self.assertEqual(client.odoo_db, "b47v")
        self.assertEqual(client.api_key, "rahasia")
        self.assertEqual(settings.keep_sent_days, 3)

    def test_queue_lives_next_to_settings_file_not_cwd(self):
        client = load_settings(self.write(ONE_CLIENT), {}).client()
        self.assertEqual(client.queue_path, self.folder.resolve() / "data" / "b47v.sqlite")

    def test_custom_key_variable_and_data_dir(self):
        text = 'data_dir = "/var/lib/bridge"\nkeep_sent_days = 0\n' + ONE_CLIENT + 'api_key_env = "KUNCI"\n'
        settings = load_settings(self.write(text), {"KUNCI": "abc", "BRIDGE_KEY_B47V": "bukan ini"})
        client = settings.client("b47v")
        self.assertEqual(client.api_key, "abc")
        self.assertEqual(client.queue_path, Path("/var/lib/bridge/b47v.sqlite"))
        self.assertEqual(settings.keep_sent_days, 0)

    def test_missing_key_is_only_an_error_when_needed(self):
        client = load_settings(self.write(ONE_CLIENT), {}).client()
        self.assertEqual(client.api_key, "")
        with self.assertRaises(SettingsError) as caught:
            client.require_api_key()
        self.assertIn("BRIDGE_KEY_B47V", str(caught.exception))

    def test_key_is_not_in_repr(self):
        client = load_settings(self.write(ONE_CLIENT), {"BRIDGE_KEY_B47V": "rahasia-sekali"}).client()
        self.assertNotIn("rahasia-sekali", repr(client))

    def test_default_key_variable_name(self):
        self.assertEqual(default_key_env("thrift_tes8"), "BRIDGE_KEY_THRIFT_TES8")
        self.assertEqual(default_key_env("k7-m2"), "BRIDGE_KEY_K7_M2")


class TestLiveSettings(SettingsCase):
    def test_defaults(self):
        settings = load_settings(self.write(ONE_CLIENT), {})
        self.assertEqual(settings.poll_seconds, 60)
        self.assertEqual(settings.sign_api_key, "")
        self.assertIsNone(settings.client().tiktok)

    def test_account_without_at_sign(self):
        settings = load_settings(self.write(ONE_CLIENT + 'tiktok = " @Toko.Thrift "\n'), {})
        self.assertEqual(settings.client().tiktok, "Toko.Thrift")

    def test_poll_seconds(self):
        settings = load_settings(self.write("poll_seconds = 30\n" + ONE_CLIENT), {})
        self.assertEqual(settings.poll_seconds, 30)
        self.assert_error("poll_seconds = 1\n" + ONE_CLIENT, "poll_seconds")
        self.assert_error('poll_seconds = "60"\n' + ONE_CLIENT, "poll_seconds")

    def test_sign_key_comes_from_environment_and_stays_out_of_repr(self):
        settings = load_settings(self.write(ONE_CLIENT), {"EULER_API_KEY": " kunci-euler \n"})
        self.assertEqual(settings.sign_api_key, "kunci-euler")
        self.assertNotIn("kunci-euler", repr(settings))


class TestMonitorSetting(SettingsCase):
    def test_ping_address_comes_from_environment_and_stays_out_of_repr(self):
        url = "https://pemantau.contoh/ping/sangat-rahasia"
        settings = load_settings(self.write(ONE_CLIENT), {"BRIDGE_HEALTHCHECK_URL": f" {url}\n"})
        self.assertEqual(settings.healthcheck_url, url)
        self.assertNotIn("sangat-rahasia", repr(settings))
        self.assertEqual(load_settings(self.write(ONE_CLIENT), {}).healthcheck_url, "")

    def test_ping_address_must_be_a_url(self):
        with self.assertRaises(SettingsError) as caught:
            load_settings(self.write(ONE_CLIENT), {"BRIDGE_HEALTHCHECK_URL": "sangat-rahasia"})
        self.assertIn("BRIDGE_HEALTHCHECK_URL", str(caught.exception))
        self.assertNotIn("sangat-rahasia", str(caught.exception))


class TestSeveralClients(SettingsCase):
    TWO = ONE_CLIENT + '\n[[client]]\ncode = "k7m2"\nodoo_url = "http://caddy:8080"\n'

    def test_must_name_one(self):
        settings = load_settings(self.write(self.TWO), {})
        self.assertEqual(settings.client("k7m2").code, "k7m2")
        with self.assertRaises(SettingsError) as caught:
            settings.client()
        self.assertIn("b47v, k7m2", str(caught.exception))

    def test_unknown_code(self):
        settings = load_settings(self.write(self.TWO), {})
        with self.assertRaises(SettingsError):
            settings.client("zzzz")


class TestMistakes(SettingsCase):
    def test_missing_file(self):
        with self.assertRaises(SettingsError) as caught:
            load_settings(self.folder / "tidak-ada.toml", {})
        self.assertIn("clients.example.toml", str(caught.exception))

    def test_broken_toml(self):
        self.assert_error("[[client]\ncode = ", "tidak terbaca")

    def test_no_client(self):
        self.assert_error('data_dir = "data"\n', "belum ada client")

    def test_missing_url(self):
        self.assert_error('[[client]]\ncode = "a"\n', "'odoo_url' wajib diisi")

    def test_url_without_scheme(self):
        self.assert_error('[[client]]\ncode = "a"\nodoo_url = "localhost:8069"\n', "http://")

    def test_unsafe_code(self):
        self.assert_error('[[client]]\ncode = "../etc"\nodoo_url = "http://x"\n', "'code' hanya boleh")

    def test_duplicate_code(self):
        self.assert_error(ONE_CLIENT + ONE_CLIENT, "dipakai dua kali")

    def test_bad_keep_days(self):
        self.assert_error("keep_sent_days = -1\n" + ONE_CLIENT, "keep_sent_days")


if __name__ == "__main__":
    unittest.main()
