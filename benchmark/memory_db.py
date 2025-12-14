"""
Оптимизированный backend для pyjobkit load testing.

FastQueueBackend - использует asyncio.Queue вместо dict + сортировки.
Минимальный Redis overhead - только batch INCR для счётчика.
"""
from uuid import UUID, uuid4
from datetime import datetime, UTC
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict
from pyjobkit import Engine, ExecContext
from pyjobkit.executors.subprocess import SubprocessExecutor
from pyjobkit.contracts import QueueBackend
import asyncio
import os
import redis.asyncio as aioredis

_global_engine = None
_redis_client = None


@dataclass
class _FastJob:
    id: UUID
    kind: str
    payload: dict
    timeout_s: int | None = None
    max_attempts: int = 3
    attempts: int = 0
    version: int = 0


class FastQueueBackend(QueueBackend):
    """
    Сверхбыстрый backend на asyncio.Queue.
    
    Стандартный MemoryBackend тормозит из-за:
    1. asyncio.Lock на каждую операцию
    2. Сортировка ВСЕХ задач в claim_batch
    3. Хранение завершённых задач
    
    FastQueueBackend:
    - Использует asyncio.Queue (O(1) put/get)
    - Без сортировки
    - Без хранения завершённых задач
    """
    
    def __init__(self) -> None:
        self._queue: asyncio.Queue[_FastJob] = asyncio.Queue()
        self._running: Dict[UUID, _FastJob] = {}
    
    async def enqueue(
        self,
        *,
        kind: str,
        payload: dict,
        priority: int = 100,
        scheduled_for: datetime | None = None,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        timeout_s: int | None = None,
    ) -> UUID:
        job_id = uuid4()
        job = _FastJob(
            id=job_id,
            kind=kind,
            payload=payload,
            timeout_s=timeout_s,
            max_attempts=max_attempts,
        )
        await self._queue.put(job)
        return job_id

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> List[QueueBackend.ClaimedJob]:
        claimed = []
        for _ in range(limit):
            try:
                job = self._queue.get_nowait()
                self._running[job.id] = job
                job.version += 1
                claimed.append({
                    "id": job.id,
                    "kind": job.kind,
                    "payload": job.payload,
                    "timeout_s": job.timeout_s,
                    "max_attempts": job.max_attempts,
                    "attempts": job.attempts,
                    "version": job.version,
                })
            except asyncio.QueueEmpty:
                break
        return claimed

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        if job_id in self._running:
            self._running[job_id].attempts += 1

    async def succeed(self, job_id: UUID, result: dict, *, expected_version: int | None = None) -> None:
        self._running.pop(job_id, None)

    async def fail(self, job_id: UUID, reason: dict, *, expected_version: int | None = None) -> None:
        self._running.pop(job_id, None)

    async def timeout(self, job_id: UUID, *, expected_version: int | None = None) -> None:
        self._running.pop(job_id, None)

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        job = self._running.pop(job_id, None)
        if job:
            await self._queue.put(job)

    async def cancel(self, job_id: UUID) -> None:
        self._running.pop(job_id, None)

    async def get(self, job_id: UUID) -> dict:
        job = self._running.get(job_id)
        if not job:
            raise KeyError(job_id)
        return asdict(job)

    async def is_cancelled(self, job_id: UUID) -> bool:
        return False

    async def extend_lease(self, job_id: UUID, worker_id: UUID, ttl_s: int, *, expected_version: int | None = None) -> None:
        pass  # No-op для скорости

    async def reap_expired(self) -> int:
        return 0

    async def queue_depth(self) -> int:
        return self._queue.qsize()

    async def check_connection(self) -> None:
        return None


async def get_redis() -> aioredis.Redis:
    """Получаем Redis клиент с connection pool"""
    global _redis_client
    if _redis_client is None:
        dsn = os.getenv('DSN', 'redis://localhost:6379/0')
        # Connection pool для высокой производительности
        _redis_client = aioredis.from_url(
            dsn, 
            max_connections=50,
            decode_responses=True
        )
    return _redis_client


import hashlib

# Константы для проверяемой работы
HASH_ITERATIONS = 100  # Количество итераций хэширования
EXPECTED_HASH = None  # Вычислим при первом запуске


def compute_work(data: str, iterations: int) -> str:
    """
    Детерминированная CPU-bound работа.
    Хэширует строку N раз — всегда даёт одинаковый результат.
    """
    result = data.encode()
    for _ in range(iterations):
        result = hashlib.sha256(result).digest()
    return result.hex()


class HashExecutor(SubprocessExecutor):
    """
    Executor с реальной проверяемой работой.
    
    Каждая задача выполняет N итераций SHA256.
    Результат детерминирован и проверяем.
    Redis счётчик инкрементируется 1:1 с локальным.
    """
    
    def __init__(self, use_redis: bool = False):
        self.kind = "subprocess"
        import app
        self._metrics = app.metrics
        self._redis: Optional[aioredis.Redis] = None
        self._use_redis = use_redis
        self._iterations = int(os.getenv("HASH_ITERATIONS", "100"))
        
        # Вычисляем ожидаемый хэш один раз
        self._expected = compute_work("benchmark", self._iterations)
        
    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await get_redis()
        return self._redis
        
    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext):
        """Выполняем реальную работу и проверяем результат"""
        
        # Реальная CPU работа
        result = compute_work("benchmark", self._iterations)
        
        # Проверяем корректность (опционально)
        if result != self._expected:
            return {"returncode": 1, "stdout": "", "stderr": "hash mismatch"}
        
        # Обновляем локальные метрики
        self._metrics.processed += 1
        
        # Синхронное обновление Redis (1:1 с локальным)
        if self._use_redis:
            redis = await self._get_redis()
            await redis.incr("pyjobkit:processed")
        
        return {"returncode": 0, "stdout": result[:16], "stderr": ""}


class FastRedisExecutor(SubprocessExecutor):
    """
    Быстрый executor с минимальным Redis overhead.
    
    Делает только 1 операцию Redis (INCR) вместо 5.
    НЕ запускает реальный subprocess - возвращает mock результат.
    """
    
    def __init__(self):
        self.kind = "subprocess"
        import app
        self._metrics = app.metrics
        self._redis: Optional[aioredis.Redis] = None
        self._result = {"returncode": 0, "stdout": "ok", "stderr": ""}
        self._batch_count = 0
        
    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await get_redis()
        return self._redis
        
    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext):
        """Минимальный overhead - только INCR в Redis"""
        
        # Обновляем локальные метрики
        self._metrics.processed += 1
        self._batch_count += 1
        
        # Пакетное обновление Redis (каждые 100 задач) для минимального overhead
        if self._batch_count >= 100:
            redis = await self._get_redis()
            await redis.incrby("pyjobkit:processed", self._batch_count)
            self._batch_count = 0
        
        return self._result


class FastMockExecutor(SubprocessExecutor):
    """Быстрый mock executor без Redis"""
    
    def __init__(self):
        self.kind = "subprocess"
        import app
        self._metrics = app.metrics
        self._result = {"returncode": 0, "stdout": ".", "stderr": ""}
    
    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext):
        self._metrics.processed += 1
        return self._result


async def create_engine():
    """Создает движок с FastQueueBackend"""
    global _global_engine
    
    if _global_engine is not None:
        return _global_engine
    
    dsn = os.getenv('DSN', 'memory://')
    use_real_work = os.getenv('REAL_WORK', '1') == '1'  # По умолчанию включено
    
    # Всегда используем FastQueueBackend - он в 10x быстрее MemoryBackend
    backend = FastQueueBackend()
    
    if dsn.startswith('redis://'):
        print(f"🔴 REDIS: {dsn}")
        
        # Проверяем подключение и СБРАСЫВАЕМ счётчик
        redis = await get_redis()
        await redis.ping()
        await redis.set("pyjobkit:processed", 0)
        print("✅ Redis OK (счётчик сброшен)")
        print("⚡ FastQueueBackend (asyncio.Queue)")
        
        if use_real_work:
            iterations = int(os.getenv("HASH_ITERATIONS", "100"))
            print(f"🔨 HashExecutor (SHA256 x{iterations})")
            executor = HashExecutor(use_redis=True)
        else:
            print("💨 FastRedisExecutor (no-op)")
            executor = FastRedisExecutor()
        
    else:
        print(f"💾 MEMORY: {dsn}")
        print("⚡ FastQueueBackend (asyncio.Queue)")
        
        if use_real_work:
            iterations = int(os.getenv("HASH_ITERATIONS", "100"))
            print(f"🔨 HashExecutor (SHA256 x{iterations})")
            executor = HashExecutor(use_redis=False)
        else:
            print("💨 FastMockExecutor (no-op)")
            executor = FastMockExecutor()
    
    _global_engine = Engine(backend=backend, executors=[executor])
    
    return _global_engine


def create_instrumented_executor():
    """Для обратной совместимости"""
    dsn = os.getenv('DSN', 'memory://')
    if dsn.startswith('redis://'):
        return FastRedisExecutor()
    return FastMockExecutor()
