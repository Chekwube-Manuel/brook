# HttpBroker

**An HTTP-native message broker — fast because it's simple, safe because its
failure modes are explicit.**

Written in C#/.NET 10. Single node. HTTP/2 (h2c) end-to-end. Per-topic
durability knobs. At-least-once consumer groups with a provable
no-gap/no-duplicate handoff between log replay and live push.

```
Producer ──POST /v1/topics/{topic}/messages──▶  Router
                                                    │
Consumer ◀──GET  /v1/topics/{topic}/stream───  append-only segment log
             (HTTP/2 server-stream,           (disk, Kafka-style segments,
              replay-then-push)                per-topic durability mode)
                                                    │
                                            ┌───────┴────────┐
                                            │ offset store   │  PUT /v1/groups/{g}/topics/{t}/offset
                                            │ (consumer grps)│  at-least-once
                                            └────────────────┘
```

## Why this shape

- **HTTP/2 native.** One multiplexed connection carries every produce and
  consume stream. `.NET` clients negotiate h2c automatically; `curl
  --http2-prior-knowledge` works too. The whole broker is debuggable as plain
  HTTP.
- **Replay-then-push consume.** Join a topic at any offset, get the gap
  replayed from the log, then get live push on the same open connection. The
  handoff is exact — tested, not promised.
- **Durability is a per-topic knob.** `none` (page cache), `buffered`
  (batch flush ~50ms — the fast default), or `fsync` (durable per batch).
- **Backpressure without data loss.** A slow consumer trips a bounded-channel
  overflow → the broker aborts that stream → the consumer replays from its
  committed offset. At-least-once made physical.
- **Multi-language by construction.** JSON over HTTP/2; the C# client is a
  reference implementation, and Go/Node clients are straightforward against
  the same endpoints.

## Quickstart

```powershell
# 1. serve
dotnet run --project demo/HttpBroker.Demo -- serve --urls http://127.0.0.1:8123 --data ./data

# 2. produce 10k messages
dotnet run --project demo/HttpBroker.Demo -- produce --url http://127.0.0.1:8123 --topic demo --count 10000

# 3. consume (replay, print, commit every 100 — at-least-once)
dotnet run --project demo/HttpBroker.Demo -- consume --url http://127.0.0.1:8123 --topic demo --group g1

# 4. benchmark
dotnet run --project demo/HttpBroker.Demo -- bench --url http://127.0.0.1:8123 --topic bench --messages 1000000 --size 64 --batch 500 --workers 8

# 5. tests
dotnet test
```

## Reference numbers (Debug build, single node, buffered)

| run | result |
|---|---|
| 1,000,000 × 64 B, batch 500, 8 workers | **231,650 msg/s** — batch p50 29 µs · p99 110 µs |
| 200,000 × 64 B, batch 100, 8 workers | **100,771 msg/s** — batch p50 59 µs · p99 196 µs |
| 20,000 × 64 B, batch 1 (per-message latency) | p50 290 µs · p99 976 µs |
| at-least-once resume | killed at 14,000 → resumed at 14,000 → finished 20,000, exactly once |

Honest framing: single node, no replication, buffered durability — the
"faster than Kafka" claim is *no quorum round-trips, sub-100 µs batch latency,
curl-debuggable*, not "beats a replicated fsync'd cluster". See
[benchmarks](docs/benchmarks.md).

## Documentation

| doc | what it covers |
|---|---|
| [docs/architecture.md](docs/architecture.md) | components, storage format, data flow, the no-gap/no-dup argument, concurrency model |
| [docs/api.md](docs/api.md) | full endpoint reference with examples, offset semantics, error codes |
| [docs/design.md](docs/design.md) | every design decision and the rejected alternatives |
| [docs/benchmarks.md](docs/benchmarks.md) | methodology, the honest numbers, how to reproduce |
| [docs/roadmap.md](docs/roadmap.md) | v1 limitations and the M1–M5 milestones (perf, binary codec, partitioning, replication, ops) |

## Project layout

```
src/HttpBroker.Core     storage engine: SegmentLog, OffsetStore, BrokerEngine, models
src/HttpBroker.Server   Kestrel host + HTTP/2 endpoints
src/HttpBroker.Client   C# producer/consumer client library
demo/HttpBroker.Demo    serve / produce / consume / bench CLI
tests/HttpBroker.Tests  20 tests: storage, engine semantics, real HTTP end-to-end
docs/                   the documentation above
```
