# EngineeredWood

A .NET library for reading and writing columnar file formats — **Apache Parquet**, **Apache ORC**, and **Apache Avro** — as Apache Arrow `RecordBatch` objects, optimized for cloud storage.

## What

EngineeredWood is a fast, modern implementation of columnar file formats for .NET that treats cloud storage as a first-class target rather than an afterthought. The traditional `Stream` abstraction — with its single position cursor — is a poor fit for columnar layouts, where a reader needs to fetch many disjoint byte ranges concurrently. Every "seek + read" on cloud storage becomes a separate HTTP request with significant latency.

This library replaces `Stream` with an offset-based I/O layer that supports:

- **Concurrent reads** with no shared position cursor
- **Batch range requests** that can be coalesced to minimize round trips
- **Pooled buffer management** to reduce GC pressure
- **Pluggable backends** — local files and Azure Blob Storage, with the same interface

## Why

There are two motivations for this project, and they're equally important:

**1. Better columnar file format libraries for .NET.** Existing .NET Parquet and ORC libraries were not designed around the access patterns that matter for cloud-native analytics — batched range reads, concurrent column chunk fetches, and zero-copy buffer management. EngineeredWood is built from the ground up with these patterns in mind, drawing inspiration from [arrow-rs](https://github.com/apache/arrow-rs).

**2. An experiment in agentic coding.** This project is being built collaboratively with an AI coding agent (Claude Code). Every file, test, and design decision has been produced through human-AI pair programming. It's a real-world test of how far agentic coding can go on a nontrivial systems library — not a toy demo, but a genuine attempt to build something useful while exploring a new way of writing software.

## Project structure

```
src/
  EngineeredWood.Core/          Shared abstractions: I/O, compression, Arrow helpers
  EngineeredWood.Parquet/       Parquet reader and writer
  EngineeredWood.Orc/           ORC reader and writer
  EngineeredWood.Avro/          Avro reader and writer
  EngineeredWood.Azure/         Azure Blob Storage backends
test/
  EngineeredWood.Parquet.Tests/         xUnit tests for Parquet
  EngineeredWood.Parquet.Benchmarks/    BenchmarkDotNet suites for Parquet
  EngineeredWood.Parquet.Compatibility/ 92-file cross-tool validation
  EngineeredWood.Orc.Tests/             xUnit tests for ORC
  EngineeredWood.Orc.Benchmarks/        BenchmarkDotNet suites for ORC
  EngineeredWood.Avro.Tests/            xUnit tests for Avro
  EngineeredWood.Avro.Benchmarks/       BenchmarkDotNet suites for Avro
```

## Features — Parquet

### Reading

- Full footer and metadata parsing (custom Thrift Compact Protocol codec)
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

## Features — ORC

### Reading

- Full ORC file parsing (postscript, footer, stripe metadata via Protobuf)
- Selective column I/O with stream coalescing
- Column projection by name
- Row indexing support (10K row stride)
- Streaming via `IAsyncEnumerable<RecordBatch>`

### Writing

- Arrow `RecordBatch` → ORC with auto stripe management (64 MB default)
- RLE v2 encoding for integers, Dictionary v2 for strings
- Configurable string dictionary threshold (40K unique values default)
- Row index generation (10K row stride, configurable)
- Per-stripe and file-level column statistics

### Types

All 19 ORC types are supported for both reading and writing:
- **Integer**: Boolean, Byte, Short, Int, Long
- **Floating point**: Float, Double
- **String/Binary**: String, Varchar, Char, Binary
- **Temporal**: Date, Timestamp, TimestampInstant (UTC)
- **Decimal**: Decimal128 (configurable precision/scale)
- **Complex**: Struct, List, Map, Union

### Encodings

| Encoding | Read | Write |
|---|---|---|
| DIRECT | yes | yes |
| DIRECT_V2 (RLE v2) | yes | yes |
| DICTIONARY | yes | — |
| DICTIONARY_V2 | yes | yes |

## Features — Avro

### Reading

- Object Container File (OCF) reading with sync and async APIs
- Fluent builder API (`AvroReaderBuilder`)
- Schema evolution: field matching by name/alias, type promotion, default value insertion
- Field projection by index or skip-by-name
- Streaming via `IAsyncEnumerable<RecordBatch>`

### Writing

- Arrow `RecordBatch` → Avro OCF with codec selection
- Sync and async writers
- Explicit Avro schema or auto-inferred from Arrow schema

### Streaming (non-OCF)

- Push-based decoder for framed messages (`AvroDecoder`)
- Row-level encoder with per-message framing (`AvroEncoder`)
- Wire formats: Single Object Encoding (SOE), Confluent Schema Registry, Apicurio Registry, raw binary
- Schema fingerprinting: CRC-64-AVRO (Rabin), MD5, SHA-256
- `SchemaStore` for schema-by-fingerprint lookup

### Types

- **Primitives**: null, boolean, int, long, float, double, bytes, string
- **Named**: record (→ StructArray), enum (→ DictionaryArray), fixed (→ FixedSizeBinaryArray)
- **Complex**: array (→ ListArray), map (→ MapArray), union (nullable → nullable field, general → DenseUnionArray)
- **Logical types**: date, time-millis/micros, timestamp-millis/micros/nanos (UTC and local), decimal (bytes and fixed with precision/scale → Decimal128), uuid

### Compression

| Codec | Read | Write |
|---|---|---|
| null (uncompressed) | yes | yes |
| deflate | yes | yes |
| snappy (with CRC32C) | yes | yes |
| zstandard | yes | yes |
| lz4 | yes | yes |

## Shared infrastructure

### Compression

| Codec | Library | Parquet | ORC | Avro |
|---|---|---|---|---|
| Snappy | [Snappier](https://github.com/brantburnett/Snappier) — pure managed | yes (default) | yes | yes |
| Zstd | [ZstdSharp](https://github.com/oleg-st/ZstdSharp) — pure managed | yes | yes (default) | yes |
| LZ4 / LZ4_RAW | [K4os.Compression.LZ4](https://github.com/MiloszKrajewski/K4os.Compression.LZ4) | yes | yes | yes |
| Gzip / Zlib | System.IO.Compression | yes | yes | — |
| Brotli | System.IO.Compression | yes | — | — |
| Deflate | System.IO.Compression | — | yes | yes |
| Uncompressed | — | yes | yes | yes |

### I/O backends

| Backend | Read | Write |
|---|---|---|
| Local files | `LocalRandomAccessFile` | `LocalSequentialFile` |
| Azure Blob Storage | `AzureBlobRandomAccessFile` | `AzureBlobSequentialFile` |

`CoalescingFileReader` is a decorator that merges nearby byte ranges to reduce I/O round trips — particularly useful on cloud storage.

## Usage

### Parquet — Reading

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

### Parquet — Writing

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

### ORC — Reading

```csharp
await using var reader = await OrcReader.OpenAsync("data.orc");

var rowReader = reader.CreateRowReader();
await foreach (var batch in rowReader)
{
    // batch is an Apache.Arrow.RecordBatch
}
```

### ORC — Writing

```csharp
await using var writer = OrcWriter.Create("output.orc", arrowSchema, new OrcWriterOptions
{
    Compression = CompressionKind.Zstd,
});

await writer.WriteBatchAsync(recordBatch);
await writer.CloseAsync();
```

### Avro — Reading

```csharp
using var stream = File.OpenRead("data.avro");
using var reader = new AvroReaderBuilder()
    .WithBatchSize(4096)
    .Build(stream);

foreach (var batch in reader)
{
    // batch is an Apache.Arrow.RecordBatch
}
```

### Avro — Writing

```csharp
using var stream = File.Create("output.avro");
using var writer = new AvroWriterBuilder(arrowSchema)
    .WithCompression(AvroCodec.Snappy)
    .Build(stream);

writer.Write(recordBatch);
writer.Finish();
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

Licensed under the [Apache License, Version 2.0](LICENSE).
