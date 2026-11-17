# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Hardening of the sync runner's background loop."""

from __future__ import annotations

import asyncio
import contextvars
import multiprocessing
import threading
import time

import pytest

from auniqorm.sync import _SyncRunner, run


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


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="requires fork"
)
@pytest.mark.parametrize("operation", ["run", "iter"])
def test_sync_runner_after_fork(operation):
    marker = contextvars.ContextVar("fork_marker", default="clean")

    async def read_marker():
        return marker.get()

    async def set_marker():
        marker.set("parent")

    async def iter_marker():
        yield marker.get()

    run(set_marker())
    runner = _SyncRunner.get()

    def check_child():
        if operation == "run":
            assert run(read_marker()) == "clean"
        else:
            assert list(_SyncRunner.get().iter(iter_marker())) == ["clean"]
        assert _SyncRunner.get() is not runner

    process = multiprocessing.get_context("fork").Process(target=check_child)
    with _SyncRunner._lock, runner._start_lock:
        process.start()
    try:
        process.join(timeout=5)
        assert not process.is_alive(), "forked child hung using the sync runner"
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        process.close()

    assert _SyncRunner.get() is runner
    assert run(read_marker()) == "parent"
