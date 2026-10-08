# SPDX-License-Identifier: AGPL-3.0-only
"""File kunci harus sejalan dengan pyproject.toml, dan tiap barisnya bersidik jari."""
import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def locked():
    """Isi requirements.lock sebagai {nama: (versi, jumlah sidik jari)}."""
    entries = {}
    name = None
    for line in (ROOT / "requirements.lock").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_.-]+)==(\S+)", line)
        if match:
            name = match.group(1).lower().replace("_", "-")
            entries[name] = [match.group(2), 0]
        elif name and "--hash=sha256:" in line:
            entries[name][1] += 1
    return entries


class TestLock(unittest.TestCase):
    def test_direct_dependencies_are_locked_at_the_same_version(self):
        with (ROOT / "pyproject.toml").open("rb") as handle:
            declared = tomllib.load(handle)["project"]["dependencies"]
        lock = locked()
        for requirement in declared:
            name, _, version = requirement.partition("==")
            self.assertTrue(version, f"{requirement}: versinya harus dikunci dengan ==")
            key = name.strip().lower().replace("_", "-")
            self.assertIn(key, lock, f"{name} belum ada di requirements.lock")
            self.assertEqual(lock[key][0], version.strip(), f"{name}: versi di file kunci berbeda")

    def test_every_entry_has_a_fingerprint(self):
        lock = locked()
        self.assertGreater(len(lock), 10)
        missing = [name for name, (_, hashes) in lock.items() if not hashes]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
