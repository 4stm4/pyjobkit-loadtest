import asyncio
import os
import time
from collections import deque
from typing import Iterable, Tuple
from pathlib import Path

from pyjobkit import Engine, Worker
from pyjobkit.backends.sql import SQLBackend
from pyjobkit.executors import SubprocessExecutor
from sqlalchemy.engine import make_url
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    JSON,
    MetaData,
    Text,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.sql.schema import Table


class Metrics:
    """Thread-safe-ish metrics storage used across coroutines."""

    processed: int = 0
    enqueued: int = 0
    last_processed: int = 0
    last_time: float = time.time()
    rps_history: deque = deque(maxlen=60)  # последние 60 секунд


metrics = Metrics()


metadata = MetaData()

jobkit_jobs = Table(
    "jobkit_jobs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("kind", Text, nullable=False),
    Column("payload", JSON().with_variant(Text(), "sqlite"), nullable=False),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column(
        "created_at",
        DateTime(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("attempts", Integer, server_default=text("0")),
)

Index(
    "idx_jobkit_pending",
    jobkit_jobs.c.status,
    jobkit_jobs.c.lease_expires_at,
    sqlite_where=text(
        "status = 'pending' AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)"
    ),
    postgresql_where=text(
        "status = 'pending' AND (lease_expires_at IS NULL OR lease_expires_at < now())"
    ),
)


async def ensure_schema(sql_engine: AsyncEngine):
    """Create required tables for the SQL backend if they are missing."""

    async with sql_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


def ensure_sqlite_directory(dsn: str) -> None:
    """Create the parent directory for a SQLite database if needed."""

    url = make_url(dsn)
    if url.get_backend_name() != "sqlite":
        return

    database = url.database
    if not database or database == ":memory:":
        return

    db_path = Path(database).expanduser()
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)


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

    ensure_sqlite_directory(dsn)
    sql_engine: AsyncEngine = create_async_engine(dsn)
    await ensure_schema(sql_engine)
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
