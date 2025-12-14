# pyjobkit-loadtest

Load testing benchmark for [pyjobkit](https://github.com/4stm4/pyjobkit) with real-time web dashboard.

## Features

- 📊 **Web dashboard** with RPS, CPU, and RAM charts
- 🚀 **High performance** — up to 6000+ RPS with FastQueueBackend
- 🔴 **Redis integration** — task counter persistence
- ⚡ **Optimized executor** — batch INCR every 100 tasks
- 📈 **Resource monitoring** — real-time CPU and RAM tracking

## Quick Start

### With Docker Compose (recommended)

```bash
docker compose up --build
```

Open the dashboard: [http://localhost:8888](http://localhost:8888)

### Locally

1. Install dependencies:

```bash
cd pyjobkit-loadtest
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn psutil redis pyjobkit jinja2
```

2. Start Redis:

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

3. Run the benchmark:

```bash
cd benchmark
DSN=redis://localhost:6379/0 ENQUEUE_RATE=500 CONCURRENCY=8 python web.py
```

4. Open [http://localhost:8888](http://localhost:8888)

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DSN` | `memory://` | Redis URL or `memory://` for no-Redis mode |
| `ENQUEUE_RATE` | `200` | Task enqueue rate per second |
| `CONCURRENCY` | `8` | Number of parallel workers |

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Enqueuer   │───▶│ MemoryQueue │───▶│   Worker    │
│ (rate/sec)  │    │  (pyjobkit) │    │(concurrency)│
└─────────────┘    └─────────────┘    └──────┬──────┘
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │    Redis    │
                                      │ (INCR/100)  │
                                      └─────────────┘
```

- **FastQueueBackend** — asyncio.Queue with O(1) operations (10x faster than MemoryBackend)
- **FastRedisExecutor** — optimized executor with batch INCR
- **Metrics** — RPS, CPU%, RAM collection in deque (60 seconds)

## Performance

| Mode | RPS | CPU | RAM |
|------|-----|-----|-----|
| FastQueueBackend | ~6000-7000 | 85-97% | 67-70 MB |
| MemoryBackend (legacy) | ~350-500 | 20-30% | 60-85 MB |

## Screenshot

![Dashboard](docs/screenshot.png)

## Requirements

- Python 3.11+
- pyjobkit 0.2.0
- Redis 7+ (optional)
