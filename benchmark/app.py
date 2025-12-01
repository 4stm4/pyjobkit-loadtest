import asyncio
import logging
import os
import time
from collections import deque
from pathlib import Path
from typing import Iterable, Tuple

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


logger = logging.getLogger(__name__)


class Metrics:
    """Thread-safe-ish metrics storage used across coroutines."""

    processed: int = 0
    enqueued: int = 0
    last_processed: int = 0
    last_time: float = time.time()
    rps_history: deque = deque(maxlen=60)  # последние 60 секунд


DEFAULT_DSN = "sqlite+aiosqlite:///./jobkit.db"

metrics = Metrics()


metadata = MetaData()

job_tasks = Table(
    "job_tasks",
    metadata,
    Column("id", Text, primary_key=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column(
        "scheduled_for",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", Text, nullable=False, server_default=text("'queued'")),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("max_attempts", Integer, nullable=False, server_default=text("25")),
    Column("priority", Integer, nullable=False, server_default=text("100")),
    Column("kind", Text, nullable=False),
    Column("payload", JSON().with_variant(Text(), "sqlite"), nullable=False),
    Column("result", JSON().with_variant(Text(), "sqlite")),
    Column("idempotency_key", Text, unique=True),
    Column("cancel_requested", Integer, nullable=False, server_default=text("0")),
    Column("leased_by", Text),
    Column("lease_until", DateTime(timezone=True)),
    Column("version", Integer, nullable=False, server_default=text("1")),
    Column("timeout_s", Integer, nullable=False, server_default=text("300")),
)

Index(
    "idx_job_tasks_queue",
    job_tasks.c.status,
    job_tasks.c.scheduled_for,
    job_tasks.c.lease_until,
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


def load_settings() -> Tuple[str, int, int]:
    """Read configuration from the environment with safe defaults."""

    dsn = os.getenv("DSN")
    if not dsn:
        logging.warning(
            "DSN environment variable is not set; falling back to %s", DEFAULT_DSN
        )
        dsn = DEFAULT_DSN

    rate = int(os.getenv("ENQUEUE_RATE", "100"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))

    return dsn, rate, concurrency


async def enqueuer(engine: Engine, rate: int):
    """Постоянно ставит в очередь задачи с заданной скоростью."""

    interval = 1.0 / rate
    default_timeout_s = 300  # to avoid NULL timeout values when enqueuing
    while True:
        try:
            await engine.enqueue(
                kind="subprocess",
                payload={"cmd": "echo . && sleep 0.005"},  # лёгкая нагрузка
                timeout_s=default_timeout_s,
            )
            metrics.enqueued += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Не удалось поставить задачу в очередь, повтор через секунду")
            await asyncio.sleep(1.0)
            continue

        await asyncio.sleep(interval)


async def worker_runner(engine: Engine, concurrency: int):
    """Вспомогательный раннер, перезапускающий воркера при сбоях."""

    worker = Worker(engine, max_concurrency=concurrency)
    while True:
        try:
            await worker.run()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Воркер упал, пробуем перезапустить через секунду")
            await asyncio.sleep(1.0)


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
        asyncio.create_task(worker_runner(engine, concurrency)),
        asyncio.create_task(metrics_updater()),
    )

    return engine, tasks


async def stop_tasks(tasks: Iterable[asyncio.Task]):
    """Cancel background tasks gracefully."""

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    dsn, rate, concurrency = load_settings()

    _engine, tasks = await start_benchmark(dsn, rate, concurrency)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Для запуска через uvicorn в веб-режиме — не запускаем main()
    pass
