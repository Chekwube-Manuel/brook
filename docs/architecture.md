# Architecture

Brook is a single-node, HTTP-native message broker. This document describes
the components, the data flow, and the concurrency model that guarantees ordering
without gaps or duplicates.

## Components

```
┌────────────────────────────── Brook.Server (Kestrel, HTTP/2 h2c) ──────────────────────────────┐
│  /v1/topics/{t}/messages   POST    produce batch ──▶  BrokerEngine.Produce                         │
│  /v1/topics/{t}/stream     GET     consume        ◀──  BrokerEngine.Subscribe + ReadReplayAsync    │
│  /v1/groups/...            PUT/GET commit/read offsets ─▶ OffsetStore                             │
│  /v1/topics/{t}            PUT     configure     ──▶  TopicConfigIO (topic.json)                  │
└──────────────────────────────────────────┬────────────────────────────────────────────────────────┘
                                            │
┌──────────────────────────────────────────▼───────────────────────────── Brook.Core ──────────┐
│  BrokerEngine                                                          │                          │
│   ├─ TopicState registry (one per topic)                              │                          │
│   │    ├─ TopicConfig  (durability, segment size, retention)          │                          │
│   │    ├─ SegmentLog   (append-only on-disk log)                      │                          │
│   │    └─ fan-out list (ConsumerSubscription per live stream)         │                          │
│   └─ OffsetStore       (consumer-group offsets, journal + snapshot)   │                          │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Storage: append-only segment log

Each topic is a directory under `data/topics/<topic>/`:

```
data/
├── offsets.log              # consumer-group offsets (append-only journal)
└── topics/
    └── <topic>/
        ├── topic.json       # durability, segment size, retention config
        ├── seg-0.log        # first segment, starts at offset 0
        ├── seg-1000.log     # next segment, starts at offset 1000
        └── ...
```

**Record layout** (little-endian):

```
┌──────────────┬──────────────────┬─────────────────┐
│ int32 length │ int64 timestamp  │ payload bytes   │
│ (payload)    │ (unix epoch ms)  │ (length bytes)  │
└──────────────┴──────────────────┴─────────────────┘
```

- Offsets are **implicit**: a segment covering offsets `[start, end)` is named
  `seg-{start}.log`, and record *i* inside it has offset `start + i`.
- On open, the segment scans the file once and builds an in-memory position
  index (`positions[]`), so random reads by offset are O(1) after the scan.
- The **active** segment is always the last one. Retention only ever deletes
  *closed* segments, never the active one.
- Rollover happens when the active segment exceeds `SegmentMaxBytes`.

## Data flow: produce

1. `POST /v1/topics/{topic}/messages` with a JSON batch.
2. `BrokerEngine.Produce` acquires the topic's **fan-out lock**.
3. `SegmentLog.Append` writes the batch to the active segment, assigns
   sequential offsets, and (per durability mode) flushes/fsyncs.
4. If any consumer streams are subscribed, each appended record is read back
   and pushed into every subscriber's bounded channel.
5. The lock is released. The response carries `firstOffset` / `lastOffset`.

The fan-out lock serializes appends *and* subscription registration, which is
what makes the replay→channel handoff gap-free (see below).

## Data flow: consume (replay-then-push)

A consumer opens `GET /v1/topics/{topic}/stream?group=...&offset=...`. The
broker responds with an NDJSON stream and then:

```
Phase 1 — replay:
    register subscription under the fan-out lock
    capture endOffset E  (next offset after anything appended so far)
    replay [requested, E) straight from the segment log

Phase 2 — push:
    drain the subscription's channel; every message appended after
    subscription is delivered here, in append order
```

**Why this has no gaps and no duplicates:**

- Register + capture happen atomically under the same lock appends use.
- Anything appended *before* registration has offset `< E` → comes back in the
  log replay.
- Anything appended *after* registration is delivered to the channel (the
  subscription existed before that append) → comes back on the push phase.

A message can never land in both phases (replay is `[requested, E)` only) and
can never land in neither (the lock ordering above). The handoff is exact.

## Backpressure: bounded fan-out and the overflow contract

- Every subscription has a **bounded channel** (4096 slots).
- On produce, the broker does a non-blocking `TryWrite` into each channel
  while holding the fan-out lock. This keeps append ordering and channel
  ordering identical.
- If a slow consumer lets the channel fill, `TryWrite` fails → the broker sets
  the subscription's overflow flag **instead of dropping the message**.
- The HTTP layer detects the flag and **aborts the stream**. The client
  reconnects from its last committed offset, which replays the missed data.

This is the at-least-once contract made physical: data is never silently lost,
and the slowest consumer pays only its own cost (its stream resets and rewinds).

## Concurrency model

| Shared state | Guard |
|---|---|
| Segment list, `nextOffset`, position indexes | per-topic `_lock` in `SegmentLog` |
| Topic registry (`_topics`) | `ConcurrentDictionary` + `_createLock` for creation |
| Subscription list, produce, registration | per-topic `FanoutLock` in `TopicState` |
| Committed offsets | `ConcurrentDictionary` + `_ioLock` for journal writes |

Two locks per topic (log lock + fan-out lock) keep append ordering and fan-out
ordering consistent while allowing replay reads to proceed without blocking
appends (reads open a fresh file handle per record).

## Process lifecycle

```
start  →  BrokerEngine ctor:
             open offsets.log (replay journal into memory)
             scan data/topics/ and reopen every topic + segment log
             start background loops:
               - flush loop per topic (50ms) for buffered durability
               - retention sweep every 30s
serve  →  Kestrel accepts HTTP/2 (h2c) requests
stop   →  cancel background loops
          flush every segment to disk (graceful shutdown)
          close offset journal
```

Data survives restart: topics, segments, and committed offsets are all on disk
and re-opened in order on boot.
