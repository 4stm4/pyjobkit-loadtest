import asyncio
import os
import time
from collections import deque
from typing import Iterable, Tuple

from pyjobkit import Engine, Worker
from pyjobkit.backends.sql import SQLBackend
from pyjobkit.executors import SubprocessExecutor
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class Metrics:
    """Thread-safe-ish metrics storage used across coroutines."""

    processed: int = 0
    enqueued: int = 0
    last_processed: int = 0
    last_time: float = time.time()
    rps_history: deque = deque(maxlen=60)  # последние 60 секунд


metrics = Metrics()


class InstrumentedSubprocessExecutor(SubprocessExecutor):
    """Executor that increments processed metrics after successful runs."""

    async def execute(self, job):
        result = await super().execute(job)
        metrics.processed += 1
        return result


async def enqueuer(engine: Engine, rate: int):
    """Постоянно ставит в очередь задачи с заданной скоростью."""

    interval = 1.0 / rate
    while True:
        await engine.enqueue(
            kind="subprocess",
            payload={"cmd": "echo . && sleep 0.005"}  # лёгкая нагрузка
        )
        metrics.enqueued += 1
        await asyncio.sleep(interval)


async def metrics_updater():
    """Каждую секунду считает RPS и сохраняет в историю."""

    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        delta = metrics.processed - metrics.last_processed
        rps = delta / max(now - metrics.last_time, 1e-6)

        metrics.rps_history.append(round(rps, 2))
        metrics.last_processed = metrics.processed
        metrics.last_time = now


async def start_benchmark(
    dsn: str, rate: int, concurrency: int
) -> Tuple[Engine, Iterable[asyncio.Task]]:
    """Create engine, workers and background tasks for the benchmark."""

    sql_engine: AsyncEngine = create_async_engine(dsn)
    backend = SQLBackend(sql_engine, lease_ttl_s=60)
    engine = Engine(backend=backend, executors=[InstrumentedSubprocessExecutor()])

    tasks = (
        asyncio.create_task(enqueuer(engine, rate)),
        asyncio.create_task(Worker(engine, max_concurrency=concurrency).run()),
        asyncio.create_task(metrics_updater()),
    )

    return engine, tasks


async def stop_tasks(tasks: Iterable[asyncio.Task]):
    """Cancel background tasks gracefully."""

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    dsn = os.getenv("DSN")
    if not dsn:
        raise RuntimeError("DSN environment variable is required")

    rate = int(os.getenv("ENQUEUE_RATE", "100"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))

    _engine, tasks = await start_benchmark(dsn, rate, concurrency)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Для запуска через uvicorn в веб-режиме — не запускаем main()
    pass
