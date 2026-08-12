# Benchmarks

Numbers measured on a single machine against a single broker node. Reproduce
with the built-in `bench` command:

```powershell
dotnet run --project demo/HttpBroker.Demo -- bench `
  --url http://127.0.0.1:8123 --topic bench `
  --messages 1000000 --size 64 --batch 500 --workers 8
```

## Methodology (the honest part)

- **Harness:** the demo `bench` command. Workers split one shared message
  budget via `Interlocked`; each worker POSTs batches and records per-batch
  latency. Reports aggregate throughput plus p50/p99 batch latency.
- **Durability:** `buffered` (the default) unless noted — batch flush ~50 ms,
  per-batch buffer flush to the OS.
- **Replication:** none. Single node, single process, local disk.
- **Load:** 64-byte payloads, JSON over HTTP/2 (h2c) from a local .NET client.

> **Why this is not an apples-to-apples "faster than Kafka" claim:** Kafka's
> published numbers assume replication + fsync + multi-node clusters and are
> throughput-optimized via batching. This broker's claims are single-node,
> single-replica, buffered. The honest summary: *no quorum round-trips, no
> batching-for-latency, sub-100 µs batch latency, and a broker you can debug
> with curl* — not "beats Kafka everywhere".

## Results (Debug build, consumer-grade laptop, 2026)

| run | throughput | latency (per batch) |
|---|---|---|
| 1,000,000 msgs × 64 B, batch 500, 8 workers | **231,650 msg/s** | p50 29 µs · p99 110 µs |
| 200,000 msgs × 64 B, batch 100, 8 workers | **100,771 msg/s** | p50 59 µs · p99 196 µs |
| 20,000 msgs × 64 B, batch 1, 1 worker | 3,053 msg/s | p50 290 µs · p99 976 µs (per message) |

Reading the table:

- **Throughput scales with batch size** — batch 500 moves ~2.3× the messages
  per second of batch 100. That is the amortized HTTP + append + flush path.
- **Batch-1 latency** is the honest per-message story: ~290 µs median, ~1 ms
  p99 end-to-end (HTTP round trip + JSON + append + flush). Single-message
  throughput is connection/serialization-bound, not broker-bound.
- p99 ≈ 3–7× p50 on batch paths — tail is clean because there is no GC-driven
  stall cascade and no quorum to wait on.

## Consumer path (at-least-once resume, live run)

Two sequential runs against the same topic and group, killed mid-stream:

```
run 1: replay 0 → 14,000, committed every 2,000 (journal: 2000..14000), killed
run 2: resumed at exactly 14,000 → consumed to 20,000 (journal: 16000..20000)
```

Result: **every message consumed exactly once across the two runs**, with the
offset journal as the audit trail. ~3,500 msg/s consumed through the HTTP/2
stream including a commit round-trip every 2,000 messages.

## What would move these numbers

- **Release build** (`dotnet run -c Release`): expect a significant constant
  factor on the batch-1 path (JIT tiering, no Debug bounds overhead).
- **Binary codec** (roadmap): removes JSON serialization from the hot path.
- **Server GC tuning** and `TieredPGO`: standard .NET levers for latency.
- The known hot spot is `Segment.ReadRecord`, which opens a fresh file handle
  per record — correct, cached by the OS, and the first optimization target.
