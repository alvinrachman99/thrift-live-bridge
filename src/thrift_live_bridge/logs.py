# SPDX-License-Identifier: AGPL-3.0-only
"""Penanda client di baris log.

Dengan banyak client dalam satu proses, baris log tanpa penanda tidak bisa
ditelusuri ke pemiliknya.
"""
from __future__ import annotations

import logging


class _Labelled(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['label']}] {msg}", kwargs


def labelled(logger: logging.Logger, label: str | None):
    """Logger yang mengawali tiap baris dengan `[label]`; tanpa label, apa adanya."""
    return _Labelled(logger, {"label": label}) if label else logger
