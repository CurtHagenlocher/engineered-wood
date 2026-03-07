namespace EngineeredWood.Parquet;

/// <summary>
/// Options controlling how Parquet files are written.
/// </summary>
public sealed class ParquetWriteOptions
{
    /// <summary>Default write options.</summary>
    public static readonly ParquetWriteOptions Default = new();

    /// <summary>Compression codec. Default: Snappy.</summary>
    public CompressionCodec Codec { get; init; } = CompressionCodec.Snappy;

    /// <summary>Default encoding for data values. Default: Plain.</summary>
    public Encoding Encoding { get; init; } = Encoding.Plain;

    /// <summary>Target data page size in bytes. Default: 1 MB.</summary>
    public int TargetPageSize { get; init; } = 1 * 1024 * 1024;

    /// <summary>Maximum dictionary page size in bytes. Default: 1 MB.</summary>
    public int MaxDictionarySize { get; init; } = 1 * 1024 * 1024;

    /// <summary>Target row group size in rows. Default: 1,000,000.</summary>
    public int RowGroupSizeRows { get; init; } = 1_000_000;

    /// <summary>Target row group size in bytes. Default: 128 MB.</summary>
    public long RowGroupSizeBytes { get; init; } = 128 * 1024 * 1024L;

    /// <summary>Per-column compression codec overrides. Key is the column path (dot-separated).</summary>
    public IReadOnlyDictionary<string, CompressionCodec>? ColumnCodecs { get; init; }

    /// <summary>Per-column encoding overrides. Key is the column path (dot-separated).</summary>
    public IReadOnlyDictionary<string, Encoding>? ColumnEncodings { get; init; }

    /// <summary>Parquet format version. Default: 2.</summary>
    public int Version { get; init; } = 2;

    /// <summary>Data page version: V1 or V2. Default: V2.</summary>
    public DataPageVersion DataPageVersion { get; init; } = DataPageVersion.V2;

    /// <summary>Created-by identifier written to the file footer.</summary>
    public string CreatedBy { get; init; } = "EngineeredWood";

    /// <summary>Encoding strategy for automatic encoding selection. Default: Adaptive.</summary>
    public EncodingStrategy EncodingStrategy { get; init; } = EncodingStrategy.Adaptive;

    /// <summary>
    /// Gets the effective compression codec for a column.
    /// </summary>
    internal CompressionCodec GetCodecForColumn(string columnPath)
    {
        if (ColumnCodecs != null && ColumnCodecs.TryGetValue(columnPath, out var codec))
            return codec;
        return Codec;
    }

    /// <summary>
    /// Gets the effective encoding for a column.
    /// </summary>
    internal Encoding GetEncodingForColumn(string columnPath)
    {
        if (ColumnEncodings != null && ColumnEncodings.TryGetValue(columnPath, out var encoding))
            return encoding;
        return Encoding;
    }
}
