# Known Issues and Limitations

## Concatenated Gzip Members on .NET Framework

**Status:** Open — workaround in place, fix requires third-party Gzip library  
**Affected targets:** `netstandard2.0` when consumed by .NET Framework 4.x  
**Not affected:** .NET 8+, .NET Core 3.0+

### Problem

.NET Framework's `System.IO.Compression.GZipStream` does not correctly
handle Gzip streams containing multiple concatenated members as permitted
by RFC 1952. After decompressing the first member, it stops reading and
over-reads the underlying stream, making subsequent members inaccessible.

Some Parquet writers — notably Apache parquet-mr — produce Gzip-compressed
data pages with multiple concatenated Gzip members. When reading such files
on .NET Framework, the decompressed output is incomplete or corrupt.

**Normal Gzip-compressed Parquet files (single member per page) work
correctly on all platforms.** This issue only affects the concatenated-member
edge case.

### Scope

- The test file `parquet-testing/data/concatenated_gzip_members.parquet`
  (produced by parquet-mr) triggers this issue.
- The test `GzipCompressed_ReadsTestFile` is skipped on netstandard2.0/net472
  via `#if !NET8_0_OR_GREATER` in `ReadRowGroupTests.cs`.
- The round-trip Gzip test (`GzipCompressed_RoundTrip`) passes on all
  platforms because our own writer produces single-member pages.

### Code location

- Decompressor: `src/EngineeredWood.Core/Compression/Decompressor.cs`,
  `DecompressGzip` method, `#else` branch.
- Test skip: `test/EngineeredWood.Parquet.Tests/Parquet/Data/ReadRowGroupTests.cs`,
  `GzipCompressed_ReadsTestFile`.

### Possible fixes

1. **Use SharpZipLib or DotNetZip** on netstandard2.0 — both correctly
   handle concatenated members. This is the same pattern used for Brotli
   (BrotliSharpLib on netstandard2.0, built-in on .NET 6+).

2. **Manual member boundary detection** — scan for Gzip magic bytes
   (`1F 8B`) in the source data and decompress each member separately.
   This is fragile (the magic bytes could appear in compressed data) and
   not recommended.

3. **Accept the limitation** — concatenated Gzip members in Parquet pages
   are rare. Most modern writers use Snappy or Zstd. Parquet-mr is the
   primary producer of this pattern, and only for certain configurations.
