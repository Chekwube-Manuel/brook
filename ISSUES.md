# Performance issues and fixes for Brook

This document lists the performance issues discovered in the codebase, the impact, and the small, safe fixes applied in the branch `fix/stream-flush-and-issues-docs`.

## Summary

1. Per-record FileStream open on reads (high) — FIXED
2. Flush performed while holding the global log lock (high) — FIXED
3. Per-message Response.FlushAsync on streaming paths (medium) — FIXED
4. JSON DOM parsing for large produce batches (medium) — FIXED (streaming parser)
5. Index rebuild performance (not fully changed in this patch) — TODO (see below)

Each item below includes the location, impact, and what was changed in this branch.

---

### 1) Per-record FileStream open on reads
- Where: src/Brook.Core/Log/Segment.cs::ReadRecord
- Problem: every record read opened a new FileStream which causes open/close syscalls per message.
- Impact: high syscall overhead and worse p99 latency when streaming many messages.
- Fix applied: keep a dedicated read-only FileStream per Segment and a small read lock instead of opening a new FileStream per record.
- Files changed: src/Brook.Core/Log/Segment.cs

### 2) Flush called while holding the global log lock
- Where: src/Brook.Core/Log/SegmentLog.cs::Append
- Problem: Append held the global _lock while calling Segment.Flush (which can call fsync) and thus serialized producers during slow disk operations.
- Impact: producers blocked during flushes and throughput dropped under contention.
- Fix applied: do not perform disk flushes while holding the _lock. The Append implementation now collects segments to flush and performs their Flush(...) calls after releasing the lock.
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
- Fix applied: replaced JSON DOM parsing with a streaming parse using JsonSerializer.DeserializeAsyncEnumerable<JsonElement>, which iterates elements from the request stream without building a full DOM. This reduces peak memory and GC pressure for large arrays of messages.
- Files changed: src/Brook.Server/Endpoints.cs

### 5) Index rebuild scan time
- Where: src/Brook.Core/Log/Segment.cs::ScanIndex
- Problem: scanning large segments on open can be slow. Suggested improvements: persist small sidecar index files or memory-map the file.
- Status: NOT YET IMPLEMENTED in this branch. Planned follow-up: write a small sidecar index (seg-{start}.idx) when appending and read it on open; fall back to scan if index file missing.

---

## How I validated
- Ran repository unit tests locally (dotnet test) as part of the development flow. (If CI runs the tests, check the branch's CI run for passing tests.)
- The changes were kept minimal and conservative to avoid functional regressions.

## Follow-ups
- Persist segment indexes to sidecar files to avoid long scan times at startup (next step).
- Consider MemoryMappedFile for readers for even lower-latency random reads.
- Add microbenchmarks to CI for the hot paths we changed (append latency, read p99, streaming throughput).


