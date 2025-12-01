# pyjobkit-loadtest

This repository contains a lightweight load testing benchmark for pyjobkit. The Docker Compose configuration runs a Python 3.11 container and clones the application into `/app` inside the container before installing dependencies.

## Running the benchmark

1. From the repository root, start the stack:

   ```bash
   docker compose up --build
   ```

2. On startup the container installs `git`, clones [https://github.com/4stm4/pyjobkit-loadtest](https://github.com/4stm4/pyjobkit-loadtest) into `/app`, installs dependencies from `/app/benchmark/requirements.txt` and then serves the benchmark app via Uvicorn on port `7777`.

3. Open the benchmark UI at [http://localhost:7777](http://localhost:7777).

If you want to run the app locally without Docker, the service will default to a
SQLite database at `./jobkit.db` when the `DSN` environment variable is not set
(a warning is logged). `ENQUEUE_RATE` and `CONCURRENCY` keep their defaults of
`100` and `8` unless you override them in the environment.

## Common pitfalls

- Ensure the container has network access to GitHub so it can clone the repository during startup.
- If you see `Could not open requirements file: [Errno 2] No such file or directory: 'benchmark/requirements.txt'`, verify the clone succeeded by checking `ls /app` inside the running container.
- If the logs show `Version mismatch on finish for job ... (expected_version=0)`, it means the worker tried to mark a task as finished with an outdated version number. `pyjobkit` increments the job version when leasing tasks for processing, so a mismatch indicates the in-memory job still had the pre-lease version. The load test wraps the subprocess executor to bump the local version (both `version` and `expected_version`) before finishing the job, which prevents this warning in current builds.
- The dashboard displays `Текущий RPS: 0.0` until the worker processes at least one task. If RPS stays at zero for more than a few seconds, check the worker logs for crashes and verify that the database DSN is reachable. Когда воркер падает, на дэшборде отображается последняя ошибка — это прямой подсказчик, почему обработка не идёт.
