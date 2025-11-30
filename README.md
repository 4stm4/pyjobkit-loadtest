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
