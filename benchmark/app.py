import asyncio
import time
import os
from collections import deque
from pyjobkit import Engine, Worker
from pyjobkit.backends.sql import SQLBackend
from pyjobkit.executors import SubprocessExecutor
from sqlalchemy.ext.asyncio import create_async_engine

# Глобальные метрики (обновляются из нескольких тасков)
class Metrics:
    processed = 0
    last_processed = 0
    last_time = time.time()
    rps_history = deque(maxlen=60)  # последние 60 секунд

metrics = Metrics()

async def enqueuer(engine: Engine, rate: int):
    """Постоянно ставит в очередь задачи с заданной скоростью"""
    interval = 1.0 / rate
    while True:
        await engine.enqueue(
            kind="subprocess",
            payload={"cmd": "echo . && sleep 0.005"}  # лёгкая нагрузка
        )
        await asyncio.sleep(interval)

async def metrics_updater():
    """Каждую секунду считает RPS и сохраняет в историю"""
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        delta = metrics.processed - metrics.last_processed
        rps = delta / (now - metrics.last_time)
        
        metrics.rps_history.append(round(rps, 2))
        metrics.last_processed = metrics.processed
        metrics.last_time = now

async def main():
    dsn = os.getenv("DSN")
    rate = int(os.getenv("ENQUEUE_RATE", "100"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))

    sql_engine = create_async_engine(dsn)
    backend = SQLBackend(sql_engine, lease_ttl_s=60)
    engine = Engine(backend=backend, executors=[SubprocessExecutor()])

    # Запускаем три постоянных задачи
    await asyncio.gather(
        enqueuer(engine, rate),
        Worker(engine, max_concurrency=concurrency).run(),
        metrics_updater()
    )

if __name__ == "__main__":
    # Для запуска через uvicorn в веб-режиме — не запускаем main()
    pass
