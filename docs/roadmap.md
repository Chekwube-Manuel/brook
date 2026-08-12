# Roadmap and Limitations

Honest about what v1 does *not* do, and where the project goes next. The seams
are deliberately in place: the architecture was written so these milestones are
additions, not rewrites.

## v1 limitations (by design)

| limitation | why it's fine for now | what unlocks it |
|---|---|---|
| Single node, no replication | Replication is the expensive 20% that buys durability-at-scale; v1 targets a single fast node | file-per-topic layout; closed segments are immutable and copyable |
| No partitioning — one ordered log per topic | Total ordering is easy to reason about and test | partitioning milestone below |
| Payloads are UTF-8 text on the wire | JSON keeps every language + curl working | binary codec milestone below |
| No exactly-once | at-least-once + idempotent consumers is the right cost/benefit for most workloads | transactional producer milestone (probably never — see design.md) |
| No authn/z, no TLS | proxy / service mesh territory | plain HTTP layer; middleware drops in |
| No metrics endpoint | Prometheus scrape can already read `/v1/topics` | metrics milestone below |

## Milestones, in the order they should happen

### M1 — Release build + performance pass
- CI build with `-c Release`, Server GC + `TieredPGO`.
- Optimize `Segment.ReadRecord`: reuse a read handle per segment instead of
  opening one per record; batch replay reads into the OS page cache.
- Re-run benchmarks; publish Release numbers.

### M2 — Binary codec (optional wire format)
- `POST /v1/topics/{t}/messages` accepts `application/octet-stream` with
  length-prefixed records; the stream endpoint offers the same framing.
- Negotiated via `Content-Type` / `Accept`, JSON stays the default for
  debuggability. Benchmark JSON vs binary on the batch-1 path.

### M3 — Partitioning (parallelism + per-key ordering)
- Topics get `partitions: N`. A partition key (hash) routes each message to a
  partition; ordering is guaranteed per key, throughput scales across
  partitions.
- Offsets become `(partition, offset)` pairs — the wire protocol and offset
  store grow to carry the partition dimension.
- Consumer groups get rebalancing: partition ownership split across consumers
  in a group (Kafka-style assignment).

### M4 — Replication and HA
- Leader-per-partition with followers tailing the closed segments + active
  segment replication.
- `acks=all` style durability option (`min.insync.replicas`).
- Leader election on node failure; consumers reconnect to the new leader.
- This is where single-node numbers stop being the whole story — benchmarks
  should then be measured against replicated Kafka with `acks=all`.

### M5 — Operations hardening
- `GET /metrics` (Prometheus): per-topic offsets, consumer lag, bytes/s,
  p50/p99 produce latency.
- Graceful topic delete (`DELETE /v1/topics/{topic}`), export/import for
  backup, retention by segment-age already present.
- Config file + TLS listener options for fronted deployments.

## Non-goals (until a reason appears)

- Exactly-once semantics (cost > benefit; idempotent consumers suffice).
- Multi-protocol gateway (AMQP/STOMP) — one protocol, done well.
- Full Kafka wire compatibility — the point is the HTTP-native model.

## Contributing / experiments worth trying

- A **Go** or **Node** client against the same endpoints (the wire contract in
  [api.md](./api.md) is the spec) — the multi-language claim should be proven.
- A slow-consumer soak test: bounded channel + overflow resets under sustained
  produce, verifying the at-least-once recovery loop end to end.
- `fsync` durability vs `buffered` vs `none` on the same workload — the knob
  should be measurable, not just documented.
