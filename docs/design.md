# Design Decisions

Every non-obvious choice in this broker, with the reasoning. The through-line:
**a broker that is fast because it is simple, and safe because its failure modes
are explicit.**

## 1. HTTP/2 (h2c) as the wire protocol

The original brief was "HTTP-native, faster than Kafka". HTTP/2 was chosen over
a custom TCP protocol and over HTTP/1.1:

- **One multiplexed connection** carries many concurrent produce and consume
  streams. HTTP/1.1 would need a connection per poll or a connection pool —
  request churn that eats latency and throughput.
- **Server streaming with native backpressure.** HTTP/2 flow control and
  cancellation map 1:1 onto our consume model. The browser-era tooling
  (`curl`, proxies, load balancers) works against it.
- **Debuggability.** The whole broker is inspectable with `curl --http2-prior-knowledge`.

Trade-offs accepted: HTTP header/parsing overhead per request (mitigated by
batch produce), and h2c requires clients to opt in (`.NET` auto-negotiates;
curl needs the flag). We deliberately serve **HTTP/2-only**: no silent downgrade
to HTTP/1.1, no dual-protocol edge cases.

## 2. Durability as a per-topic knob

Kafka's latency story is dominated by fsync and replication. We don't fight
that — we make it a choice per topic:

| mode | behavior | survives process crash | survives power loss |
|---|---|---|---|
| `none` | OS page cache only | yes | no (may lose recent) |
| `buffered` | batch flush ~50 ms + per-batch buffer flush | yes | ≤50 ms window |
| `fsync` | fsync per produce batch | yes | yes |

`buffered` is the default and the "faster than Kafka" sweet spot: sub-100 µs
batch latency on a single node, with a small and *known* loss window. Topics
that must never lose a message opt into `fsync`; topics that are replayable
snapshots use `none`.

## 3. Replay-then-push with a provable handoff

Consumers get `[requested, end)` replayed from the log, then push delivery.
Registration and end-offset capture happen under the same lock appends use, so
the boundary is exact: a message is either in the replay window or on the
channel — never both, never neither. See
[architecture.md](./architecture.md) for the ordering argument.

## 4. Bounded fan-out and the overflow contract

A live consumer is a 4096-slot bounded channel. When a slow consumer fills it,
the broker refuses to drop data — it flags overflow and aborts that stream.
The consumer reconnects from its committed offset and replays.

This makes the at-least-once contract physical instead of aspirational, and it
means **a slow consumer hurts only itself**, not the producers or other
consumers. (Compare: Kafka solves the same problem with consumer lag — you just
don't get the reset for free.)

## 5. At-least-once, not exactly-once

Exactly-once in a distributed system requires transactional producers and
idempotent consumers — complexity most workloads don't justify. We ship
at-least-once: commit offsets *after* processing, and make handlers idempotent
(offsets make natural idempotency keys). The overflow contract is the
enforcement mechanism.

## 6. JSON on the wire

JSON is not the fastest encoding. It buys: every language already has a client
(the C# one is a reference implementation), curl works, debugging is trivial.
The codec is a documented seam — a binary framing (length-prefixed) is a
drop-in change behind the same endpoints, listed in the [roadmap](./roadmap.md).

## 7. Single node first, replication later

Replication multiplies complexity (quorum writes, leader election, catch-up)
before it adds user value at this stage. The storage design anticipates it:
everything is a file per topic, offsets are absolute, segments are immutable
once closed — the natural unit for copying to replicas. Milestone is in the
[roadmap](./roadmap.md).

## 8. Offsets are absolute and sequential

No per-partition complexity in v1: one ordered log per topic, offsets
`0, 1, 2, ...`. Ordering is total, which makes "no gaps, no duplicates" easy to
reason about and to test. Partitioning (per-key ordering + parallelism) is the
natural follow-up when a single log becomes the bottleneck.

## Decisions that did NOT make the cut (and why)

| idea | verdict |
|---|---|
| TLS by default | h2c keeps the demo curl-able and the config minimal; TLS belongs in front (proxy/service mesh) |
| At-most-once mode | tempting for `none` durability; skipped — at-least-once covers the use cases and stays predictable |
| Topic-level authn/z | out of scope for v1; the HTTP layer makes a future auth middleware trivial |
| WebSocket transport | HTTP/2 streaming supersedes it without the WS handshake cost |
