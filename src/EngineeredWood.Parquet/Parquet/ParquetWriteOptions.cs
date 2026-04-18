// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;

namespace EngineeredWood.Parquet;

/// <summary>
/// Parquet data page version.
/// </summary>
public enum DataPageVersion
{
    /// <summary>Data page V1: levels and values are concatenated and compressed together.</summary>
    V1 = 1,

    /// <summary>Data page V2: levels are stored separately (uncompressed); only values are compressed.</summary>
    V2 = 2,
}

/// <summary>
/// Controls the non-dictionary fallback encoding for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns
/// when using V2 data pages.
/// </summary>
public enum ByteArrayEncoding
{
    /// <summary>
    /// DELTA_LENGTH_BYTE_ARRAY: delta-encodes value lengths, concatenates raw bytes.
    /// Good general-purpose encoding for variable-length data. This is the default.
    /// </summary>
    DeltaLengthByteArray,

    /// <summary>
    /// DELTA_BYTE_ARRAY: delta-encodes prefix lengths and suffix lengths, then stores suffixes.
    /// Most effective for sorted or prefix-heavy data (URLs, file paths, dictionary-like keys).
    /// </summary>
    DeltaByteArray,
}

/// <summary>
/// Options that control how Arrow data is written to Parquet files.
/// </summary>
public sealed record ParquetWriteOptions
{
    /// <summary>Default options with sensible defaults for general-purpose use.</summary>
    public static readonly ParquetWriteOptions Default = new();

    /// <summary>
    /// Compression codec applied to data pages. Default is <see cref="CompressionCodec.Snappy"/>.
    /// </summary>
    public CompressionCodec Compression { get; init; } = CompressionCodec.Snappy;

    /// <summary>
    /// Data page version. Default is <see cref="DataPageVersion.V2"/>.
    /// </summary>
    public DataPageVersion DataPageVersion { get; init; } = DataPageVersion.V2;

    /// <summary>
    /// Target uncompressed size of a data page in bytes. Default is 1 MB.
    /// </summary>
    public int DataPageSize { get; init; } = 1024 * 1024;

    /// <summary>
    /// Maximum byte size of a dictionary page before dictionary encoding is abandoned
    /// for that column. Default is 1 MB.
    /// </summary>
    public int DictionaryPageSizeLimit { get; init; } = 1024 * 1024;

    /// <summary>
    /// Whether dictionary encoding is enabled. When enabled, columns are analyzed
    /// before writing and dictionary encoding is used if the cardinality is sufficiently low.
    /// Default is <see langword="true"/>.
    /// </summary>
    public bool DictionaryEnabled { get; init; } = true;

    /// <summary>
    /// Maximum number of rows per row group. Default is 1,000,000.
    /// </summary>
    public int RowGroupMaxRows { get; init; } = 1_000_000;

    /// <summary>
    /// Maximum uncompressed byte size per row group. Default is 128 MB.
    /// </summary>
    public long RowGroupMaxBytes { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Non-dictionary fallback encoding for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns
    /// when using V2 data pages. Default is <see cref="ByteArrayEncoding.DeltaLengthByteArray"/>.
    /// Set to <see cref="ByteArrayEncoding.DeltaByteArray"/> for sorted or prefix-heavy data.
    /// </summary>
    public ByteArrayEncoding ByteArrayEncoding { get; init; } = ByteArrayEncoding.DeltaLengthByteArray;

    /// <summary>
    /// Per-column compression codec overrides, keyed by dotted column path (e.g. "col1" or "struct1.field1").
    /// Columns not listed use <see cref="Compression"/>.
    /// </summary>
    public IReadOnlyDictionary<string, CompressionCodec>? ColumnCodecs { get; init; }

    /// <summary>
    /// Per-column encoding overrides for BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY columns,
    /// keyed by dotted column path. Columns not listed use <see cref="ByteArrayEncoding"/>.
    /// </summary>
    public IReadOnlyDictionary<string, ByteArrayEncoding>? ColumnEncodings { get; init; }

    /// <summary>
    /// Application identifier written to the file footer's <c>created_by</c> field.
    /// </summary>
    public string CreatedBy { get; init; } = "EngineeredWood";

    /// <summary>
    /// Optional key-value metadata to include in the file footer.
    /// </summary>
    public IReadOnlyList<Metadata.KeyValue>? KeyValueMetadata { get; init; }

    /// <summary>
    /// Whether to compute and write CRC-32C checksums for each data and dictionary page.
    /// When enabled, each page header includes a <c>crc</c> field covering the compressed
    /// page data (excluding the header itself). Default is <see langword="false"/>.
    /// </summary>
    public bool PageChecksumEnabled { get; init; }

    /// <summary>
    /// Column names (dotted paths) for which Bloom filters should be written.
    /// <c>null</c> (the default) disables Bloom filter writing for all columns.
    /// Use a <see cref="HashSet{T}"/> for efficient lookup.
    /// </summary>
    public IReadOnlyCollection<string>? BloomFilterColumns { get; init; }

    /// <summary>
    /// Target false positive probability for Bloom filters. Default is 0.05 (5%).
    /// Lower values produce larger filters with fewer false positives.
    /// </summary>
    public double BloomFilterFpp { get; init; } = 0.05;

    /// <summary>
    /// Maximum Bloom filter size in bytes per column per row group. Default is 1 MB.
    /// </summary>
    public int BloomFilterMaxBytes { get; init; } = 1024 * 1024;

    /// <summary>
    /// Whether to omit the <c>path_in_schema</c> field from each column chunk's
    /// metadata. The Parquet spec currently describes the field as required, but
    /// the row group columns are matched to the schema by ordinal position, so the
    /// field is redundant — and an in-progress spec change makes it optional.
    /// Omitting it shrinks the file footer, especially for wide schemas with deep
    /// nesting. Default is <see langword="true"/>.
    /// </summary>
    public bool OmitPathInSchema { get; init; } = true;

    /// <summary>
    /// Returns whether the given column should have a Bloom filter.
    /// </summary>
    internal bool HasBloomFilter(IReadOnlyList<string> pathInSchema)
    {
        if (BloomFilterColumns == null) return false;
        var dottedPath = string.Join(".", pathInSchema);
        // Use efficient Contains for HashSet, linear scan for other collections.
        if (BloomFilterColumns is HashSet<string> hs)
            return hs.Contains(dottedPath);
        foreach (var col in BloomFilterColumns)
            if (col == dottedPath) return true;
        return false;
    }

    /// <summary>
    /// Resolves the compression codec for a column, checking per-column overrides first.
    /// </summary>
    internal CompressionCodec GetCodec(IReadOnlyList<string> pathInSchema) =>
        ColumnCodecs != null && ColumnCodecs.TryGetValue(string.Join(".", pathInSchema), out var codec)
            ? codec
            : Compression;

    /// <summary>
    /// Resolves the byte-array encoding for a column, checking per-column overrides first.
    /// </summary>
    internal ByteArrayEncoding GetByteArrayEncoding(IReadOnlyList<string> pathInSchema) =>
        ColumnEncodings != null && ColumnEncodings.TryGetValue(string.Join(".", pathInSchema), out var enc)
            ? enc
            : ByteArrayEncoding;
}
