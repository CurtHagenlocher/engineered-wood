namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Metadata for a single column chunk.
/// </summary>
public sealed class ColumnMetaData
{
    /// <summary>Physical type of the column.</summary>
    public required PhysicalType Type { get; init; }

    /// <summary>Encodings used in this column chunk.</summary>
    public required IReadOnlyList<Encoding> Encodings { get; init; }

    /// <summary>Dot-separated path in the schema.</summary>
    public required IReadOnlyList<string> PathInSchema { get; init; }

    /// <summary>Compression codec used.</summary>
    public required CompressionCodec Codec { get; init; }

    /// <summary>Total number of values (including nulls) in this column chunk.</summary>
    public required long NumValues { get; init; }

    /// <summary>Total uncompressed byte size of all pages in this column chunk.</summary>
    public required long TotalUncompressedSize { get; init; }

    /// <summary>Total compressed byte size of all pages in this column chunk.</summary>
    public required long TotalCompressedSize { get; init; }

    /// <summary>Byte offset of the first data page in the file.</summary>
    public required long DataPageOffset { get; init; }

    /// <summary>Byte offset of the index page, if any.</summary>
    public long? IndexPageOffset { get; init; }

    /// <summary>Byte offset of the dictionary page, if any.</summary>
    public long? DictionaryPageOffset { get; init; }

    /// <summary>Column chunk statistics.</summary>
    public Statistics? Statistics { get; init; }

    /// <summary>Encoding statistics per page type.</summary>
    public IReadOnlyList<KeyValue>? KeyValueMetadata { get; init; }
}
