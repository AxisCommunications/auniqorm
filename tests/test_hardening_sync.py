# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Hardening of the sync runner's background loop."""

from __future__ import annotations

import asyncio
import threading
import time

from auniqorm.sync import _SyncRunner


class TestSyncRunnerStart:
    def test_concurrent_first_calls_start_one_loop(self, monkeypatch):
        created = []
        real = asyncio.new_event_loop

        def slow_new_event_loop():
            time.sleep(0.05)  # widen the race window
            loop = real()
            created.append(loop)
            return loop

        monkeypatch.setattr(asyncio, "new_event_loop", slow_new_event_loop)
        runner = _SyncRunner()
        barrier = threading.Barrier(8)

        def first_call():
            barrier.wait()
            runner._start()

        threads = [threading.Thread(target=first_call) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        try:
            assert len(created) == 1
        finally:
            for loop in created:
                loop.call_soon_threadsafe(loop.stop)
