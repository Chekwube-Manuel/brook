# API Reference

The wire format is JSON over HTTP/2 (cleartext h2c). The stream endpoint emits
NDJSON (one JSON object per line). Base URL defaults to `http://127.0.0.1:8123`.

> **HTTP/2 note:** the broker is HTTP/2-only on the wire. .NET clients negotiate
> it automatically. `curl` needs `--http2-prior-knowledge`.

## Conventions

- **Topic / group names:** letters, digits, `.`, `_`, `-`; max 128 chars.
- **Offsets:** zero-based, strictly sequential per topic. A committed offset
  means "the next offset to consume".
- **Errors:** JSON body `{"error": "message"}` with a meaningful status code:
  `400` bad request, `404` unknown topic, `410` expired offset, `500` broker error.

## Produce

```
POST /v1/topics/{topic}/messages
```

Body — a single message object or an array of them:

```json
{ "payload": "hello world" }
[ { "payload": "one" }, { "payload": "two" } ]
```

Response:

```json
{
  "topic": "demo",
  "firstOffset": 0,
  "lastOffset": 1,
  "count": 2,
  "latencyUs": 42.5,
  "durability": "Buffered"
}
```

## Consume (server-streaming, NDJSON)

```
GET /v1/topics/{topic}/stream?group=<group>&offset=<offset>
```

- `group` — resume from this group's committed offset (optional; mutually
  exclusive with explicit `offset`).
- `offset` — resume from an explicit offset (optional; defaults to `0`).
- The broker replies `200 application/x-ndjson` and keeps the connection open:

```json
{"offset":0,"timestamp":1723400000000,"payload":"hello world"}
{"offset":1,"timestamp":1723400000001,"payload":"next"}
```

- The stream replays `[offset, end)` from the log, then pushes new arrivals as
  they happen. The connection stays open until the client leaves or the broker
  aborts it for backpressure (see below).

### Expired offset

If retention has deleted the requested offset, the broker returns:

```
HTTP/1.1 410 Gone
X-Oldest-Offset: 42
{"error": "Offset 0 expired; rewind to 42."}
```

Clients should rewind to `X-Oldest-Offset` and start from there.

### Stream reset (slow consumer)

If the consumer falls too far behind (its 4096-slot channel fills), the broker
**aborts the connection** rather than drop data. The client reconnects from its
last committed offset and replays. At-least-once in action.

## Consumer groups

```
PUT /v1/groups/{group}/topics/{topic}/offset
```

Commit the next offset to consume. **Commit after processing**, so a crash
between processing and committing is what creates duplicates — make consumers
idempotent.

```json
{ "offset": 500 }
```

```
GET /v1/groups/{group}/topics/{topic}/offset   →  { "group": "g1", "topic": "demo", "offset": 500 }
```

## Admin

### Configure a topic (create-or-update)

```
PUT /v1/topics/{topic}
```

```json
{
  "durability": "buffered",
  "segmentMaxBytes": 67108864,
  "retention": { "maxBytes": 268435456, "maxAgeSeconds": 604800 }
}
```

| field | values | default |
|---|---|---|
| `durability` | `none` · `buffered` · `fsync` | `buffered` |
| `segmentMaxBytes` | bytes before rolling to a new segment | `67108864` (64 MB) |
| `retention.maxBytes` | delete oldest closed segments past this total | `268435456` (256 MB) |
| `retention.maxAgeSeconds` | delete closed segments older than this | unset (keep by age) |

### List and describe

```
GET /v1/topics
→ [ { "name": "demo", "durability": "Buffered", "oldestOffset": 0,
      "endOffset": 20000, "sizeBytes": 1024000 } ]

GET /v1/topics/{topic}
→ { "topic": "demo", "durability": "Buffered", "retention": {...},
    "oldestOffset": 0, "endOffset": 20000, "sizeBytes": 1024000 }
```

### Manual retention sweep

```
POST /v1/admin/sweep   →  { "swept": true }
```

Retention also runs automatically every 30s; this forces an immediate pass
(e.g., after shrinking `maxBytes`).

### Health

```
GET /healthz   →  ok
```

## Client library (C#)

```csharp
using var broker = new BrokerClient("http://127.0.0.1:8123");

// produce a batch
var result = await broker.ProduceAsync("demo", ["hello", "world"]);
// result.FirstOffset, result.LastOffset, result.Count

// consume, committing every N messages (at-least-once)
await using var stream = await broker.OpenStreamAsync("demo", group: "g1");
while (true)
{
    var msg = await stream.NextAsync();
    if (msg is null) break;
    await ProcessAsync(msg);                       // idempotent processing
    await broker.CommitOffsetAsync("g1", "demo", msg.Offset + 1);
}

// the same endpoints are reachable from any language over HTTP/2.
```
