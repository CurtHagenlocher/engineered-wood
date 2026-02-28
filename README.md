# EngineeredWood

A C# library for reading Parquet files as Arrow buffers, optimized for cloud storage.

## What

EngineeredWood aims to provide a fast, modern Parquet reader for .NET that treats cloud storage as a first-class target rather than an afterthought. The traditional `Stream` abstraction — with its single position cursor — is a poor fit for Parquet's columnar layout, where a reader needs to fetch many disjoint byte ranges concurrently. Every "seek + read" on cloud storage becomes a separate HTTP range request with significant latency.

This library replaces `Stream` with an offset-based random access interface (`IRandomAccessFile`) that supports:

- **Concurrent reads** with no shared position cursor
- **Batch range requests** that can be coalesced to minimize round trips
- **Pooled buffer management** to reduce GC pressure
- **Pluggable backends** — local files via `RandomAccess` and Azure Blob Storage via range requests, with the same interface

## Why

There are two motivations for this project, and they're equally important:

**1. A better Parquet reader for .NET.** Existing .NET Parquet libraries were not designed around the access patterns that matter for cloud-native analytics — batched range reads, concurrent column chunk fetches, and zero-copy buffer management. EngineeredWood is being built from the ground up with these patterns in mind, drawing inspiration from [arrow-rs](https://github.com/apache/arrow-rs).

**2. An experiment in agentic coding.** This project is being built collaboratively with an AI coding agent (Claude Code). Every file, test, and design decision has been produced through human-AI pair programming. It's a real-world test of how far agentic coding can go on a nontrivial systems library — not a toy demo, but a genuine attempt to build something useful while exploring a new way of writing software.

## Status

Early development. The I/O abstraction layer is implemented and tested:

- `IRandomAccessFile` — core interface for offset-based reads
- `LocalRandomAccessFile` — local files via .NET's `RandomAccess` API
- `AzureBlobRandomAccessFile` — Azure Blob Storage with throttled concurrency
- `CoalescingFileReader` — decorator that merges nearby byte ranges
- `BufferAllocator` / `PooledBufferAllocator` — `ArrayPool`-backed buffer management

Next up: Parquet metadata parsing and column chunk reading.

## Building

```
dotnet build
dotnet test
```

Requires .NET 10.

## License

TBD
