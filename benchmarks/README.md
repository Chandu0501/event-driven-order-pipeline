# Reproducible performance test

This workload measures the `POST /api/v1/orders` boundary; it does not claim a
production capacity figure. Results depend on CPU, memory, Docker limits, Kafka
configuration, and database state.

Start the stack and run a conservative baseline:

```bash
docker compose up --build -d
k6 run benchmarks/create-orders.js
```

Tune the workload without editing the script:

```bash
RATE=100 DURATION=2m VUS=50 MAX_VUS=200 k6 run benchmarks/create-orders.js
```

When publishing a result, record the commit SHA, machine specifications, exact
command, p50/p95/p99 latency, request rate, error rate, and relevant container
resource limits. Commit raw k6 output separately so every performance claim is
traceable to evidence.
