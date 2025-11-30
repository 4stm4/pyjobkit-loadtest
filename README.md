# pyjobkit-loadtest

This repository contains a lightweight load testing benchmark for pyjobkit. The Docker Compose configuration runs a Python 3.11 container and binds the current working directory into `/app` inside the container.

## Running the benchmark

1. From the repository root, start the stack:

   ```bash
   docker compose up --build
   ```

2. The container installs dependencies from `/app/benchmark/requirements.txt` and then serves the benchmark app via Uvicorn on port `7777`.

3. Open the benchmark UI at [http://localhost:7777](http://localhost:7777).

## Common pitfalls

- Ensure you launch Docker Compose from the repository root so the `.:/app` volume mount can provide the `benchmark/requirements.txt` file inside the container.
- If you see `Could not open requirements file: [Errno 2] No such file or directory: 'benchmark/requirements.txt'`, verify the repository was mounted into `/app` by checking `ls /app` inside the running container.
