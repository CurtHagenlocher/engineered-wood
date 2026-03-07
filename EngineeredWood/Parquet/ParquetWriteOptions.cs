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
/// Options that control how Arrow data is written to Parquet files.
/// </summary>
public sealed class ParquetWriteOptions
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
    /// Application identifier written to the file footer's <c>created_by</c> field.
    /// </summary>
    public string CreatedBy { get; init; } = "EngineeredWood";

    /// <summary>
    /// Optional key-value metadata to include in the file footer.
    /// </summary>
    public IReadOnlyList<Metadata.KeyValue>? KeyValueMetadata { get; init; }
}
