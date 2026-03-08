# EngineeredWood

A .NET library for reading and writing Apache Parquet files as Apache Arrow `RecordBatch` objects, optimized for cloud storage.

## What

EngineeredWood is a fast, modern Parquet implementation for .NET that treats cloud storage as a first-class target rather than an afterthought. The traditional `Stream` abstraction — with its single position cursor — is a poor fit for Parquet's columnar layout, where a reader needs to fetch many disjoint byte ranges concurrently. Every "seek + read" on cloud storage becomes a separate HTTP request with significant latency.

This library replaces `Stream` with an offset-based I/O layer that supports:

- **Concurrent reads** with no shared position cursor
- **Batch range requests** that can be coalesced to minimize round trips
- **Pooled buffer management** to reduce GC pressure
- **Pluggable backends** — local files and Azure Blob Storage, with the same interface

## Why

There are two motivations for this project, and they're equally important:

**1. A better Parquet library for .NET.** Existing .NET Parquet libraries were not designed around the access patterns that matter for cloud-native analytics — batched range reads, concurrent column chunk fetches, and zero-copy buffer management. EngineeredWood is built from the ground up with these patterns in mind, drawing inspiration from [arrow-rs](https://github.com/apache/arrow-rs).

**2. An experiment in agentic coding.** This project is being built collaboratively with an AI coding agent (Claude Code). Every file, test, and design decision has been produced through human-AI pair programming. It's a real-world test of how far agentic coding can go on a nontrivial systems library — not a toy demo, but a genuine attempt to build something useful while exploring a new way of writing software.

## Features

### Reading

- Full Parquet footer and metadata parsing (custom Thrift Compact Protocol codec)
- Parallel column I/O with configurable concurrency strategies
- Column projection by name (including dotted paths for nested columns)
- Streaming via `IAsyncEnumerable<RecordBatch>` (`ReadAllAsync`)
- Three BYTE_ARRAY output modes: standard (32-bit offsets), view types (inline short strings), large offsets (64-bit, removes 2 GB limit)

### Writing

- Arrow `RecordBatch` → Parquet with parallel column encoding
- V2 data pages by default with type-aware encodings
- Analyze-before-write dictionary encoding (20% cardinality threshold)
- Auto-splitting of large batches into multiple row groups
- Per-column compression and encoding overrides
- Column statistics (min/max/null_count) with binary truncation

### Types

- **Flat columns**: all physical types (Boolean, Int32, Int64, Int96, Float, Double, ByteArray, FixedLenByteArray)
- **Nested columns**: Struct (optional/required), List (3-level standard, 2-level legacy, bare repeated), Map
- **Deeply nested**: list-of-list, map-of-map, list-of-map, etc.
- **Decimal**: INT32→Decimal32, INT64→Decimal64, FLBA→Decimal128/256 with big-endian↔little-endian conversion
- **Temporal**: Timestamp (millis/micros/nanos), Date, Time

### Encodings

| Encoding | Read | Write |
|---|---|---|
| PLAIN | yes | yes |
| RLE_DICTIONARY / PLAIN_DICTIONARY | yes | yes |
| DELTA_BINARY_PACKED | yes | yes |
| DELTA_LENGTH_BYTE_ARRAY | yes | yes |
| DELTA_BYTE_ARRAY | yes | yes |
| BYTE_STREAM_SPLIT | yes | yes |
| RLE (levels) | yes | yes |
| BIT_PACKED (deprecated, levels) | yes | — |

### Compression

| Codec | Library |
|---|---|
| Snappy (default) | [Snappier](https://github.com/brantburnett/Snappier) — pure managed |
| Zstd | [ZstdSharp](https://github.com/oleg-st/ZstdSharp) — pure managed |
| LZ4 / LZ4_RAW | [K4os.Compression.LZ4](https://github.com/MiloszKrajewski/K4os.Compression.LZ4) |
| Gzip | System.IO.Compression |
| Brotli | System.IO.Compression |
| Uncompressed | — |

### I/O backends

| Backend | Read | Write |
|---|---|---|
| Local files | `LocalRandomAccessFile` | `LocalSequentialFile` |
| Azure Blob Storage | `AzureBlobRandomAccessFile` | `AzureBlobSequentialFile` |

`CoalescingFileReader` is a decorator that merges nearby byte ranges to reduce I/O round trips — particularly useful on cloud storage.

## Usage

### Reading

```csharp
await using var file = new LocalRandomAccessFile("data.parquet");
await using var reader = new ParquetFileReader(file);

// Read all row groups
await foreach (var batch in reader.ReadAllAsync())
{
    // batch is an Apache.Arrow.RecordBatch
}

// Or read a specific row group with column projection
var batch = await reader.ReadRowGroupAsync(0, columnNames: ["id", "name"]);
```

### Writing

```csharp
await using var file = new LocalSequentialFile("output.parquet");
await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
{
    Compression = CompressionCodec.Zstd,
    DataPageVersion = DataPageVersion.V2,
});

await writer.WriteRowGroupAsync(recordBatch);
await writer.CloseAsync();
```

## Building

```
dotnet build
dotnet test
```

Requires .NET 10.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed guide to the source code, implementation choices, and internal structure.

## License

TBD
