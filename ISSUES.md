# Performance issues and fixes for Brook

This document lists the performance issues discovered in the codebase, the impact, and the small, safe fixes applied in the branch `fix/stream-flush-and-issues-docs`.

## Summary

1. Per-record FileStream open on reads (high)
2. Flush performed while holding the global log lock (high)
3. Per-message Response.FlushAsync on streaming paths (medium)
4. JSON DOM parsing for large produce batches (medium — noted, not fully changed in this patch)
5. Index rebuild performance (not fully changed in this patch)

Each item below includes the location, impact, and what was changed in this branch.

---

### 1) Per-record FileStream open on reads
- Where: src/Brook.Core/Log/Segment.cs::ReadRecord
- Problem: every record read opened a new FileStream which causes open/close syscalls per message.
- Impact: high syscall overhead and worse p99 latency when streaming many messages.
- Fix applied: keep a dedicated read-only FileStream per Segment and reuse it for reads. Protect concurrent reads of that stream with a lightweight _readLock. As a safety fallback, if the pooled read stream is not present, a temporary stream is opened.
- Files changed: src/Brook.Core/Log/Segment.cs

### 2) Flush called while holding the global log lock
- Where: src/Brook.Core/Log/SegmentLog.cs::Append
- Problem: Append held the global _lock while calling Segment.Flush (which can call fsync) and thus serialized producers during slow disk operations.
- Impact: producers blocked during flushes and throughput dropped under contention.
- Fix applied: do not perform disk flushes while holding the _lock. The Append implementation now collects segments that need flushing and performs their Flush(...) calls after releasing the lock. The active segment is also flushed outside the lock according to the durability mode.
- Files changed: src/Brook.Core/Log/SegmentLog.cs

### 3) Per-message response flush in streaming paths
- Where: src/Brook.Server/Endpoints.cs::WriteNdjsonLineAsync and WriteBinaryFrameAsync
- Problem: previously every message triggered Response.Body.FlushAsync, which prevents write batching.
- Impact: increased syscalls and decreased throughput on streaming consumers.
- Fix applied: removed per-message FlushAsync. The response writes remain but rely on the underlying pipeline and OS socket buffering to amortize writes. If necessary a periodic flush or PipeWriter-based batching can be added later.
- Files changed: src/Brook.Server/Endpoints.cs

### 4) JSON DOM parsing for produce batches
- Where: src/Brook.Server/Endpoints.cs::DecodeJsonBatch
- Problem: uses JsonDocument.ParseAsync which builds a full DOM and is allocation-heavy for large batches.
- Impact: higher memory usage and GC pressure for big producer batches.
- Fix: noted in ISSUES and planned as a follow-up; not changed in this commit to keep the patch small. Suggested follow-up: use Utf8JsonReader streaming parsing.

### 5) Index rebuild scan time
- Where: src/Brook.Core/Log/Segment.cs::ScanIndex
- Problem: scanning large segments on open can be slow. Suggested improvements: persist small sidecar index files or memory-map the file.
- Fix: not implemented in this patch; added as an item for follow-up.

---

## How I validated
- Ran repository unit tests locally (dotnet test) as part of the development flow. (If CI runs the tests, check the branch's CI run for passing tests.)
- The changes were kept minimal and conservative to avoid functional regressions.

## Follow-ups
- Replace DecodeJsonBatch with a streaming Utf8JsonReader-based parser.
- Consider persisting segment indexes to sidecar files to avoid long scan times at startup.
- Optionally adopt MemoryMappedFile for readers for even lower-latency random reads.
- Add microbenchmarks to CI for the hot paths we changed (append latency, read p99, streaming throughput).


