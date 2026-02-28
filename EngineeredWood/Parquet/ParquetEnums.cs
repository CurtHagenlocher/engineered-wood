namespace EngineeredWood.Parquet;

/// <summary>
/// Physical (primitive) types defined in the Parquet format specification.
/// </summary>
public enum PhysicalType
{
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
}

/// <summary>
/// Encodings supported by the Parquet format.
/// </summary>
public enum Encoding
{
    Plain = 0,
    PlainDictionary = 2,
    Rle = 3,
    BitPacked = 4,
    DeltaBinaryPacked = 5,
    DeltaLengthByteArray = 6,
    DeltaByteArray = 7,
    RleDictionary = 8,
    ByteStreamSplit = 9,
}

/// <summary>
/// Compression codecs supported by the Parquet format.
/// </summary>
public enum CompressionCodec
{
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lzo = 3,
    Brotli = 4,
    Lz4 = 5,
    Zstd = 6,
    Lz4Raw = 7,
}

/// <summary>
/// Converted types (deprecated in favor of LogicalType, but still present in many files).
/// </summary>
public enum ConvertedType
{
    Utf8 = 0,
    Map = 1,
    MapKeyValue = 2,
    List = 3,
    Enum = 4,
    Decimal = 5,
    Date = 6,
    TimeMillis = 7,
    TimeMicros = 8,
    TimestampMillis = 9,
    TimestampMicros = 10,
    Uint8 = 11,
    Uint16 = 12,
    Uint32 = 13,
    Uint64 = 14,
    Int8 = 15,
    Int16 = 16,
    Int32 = 17,
    Int64 = 18,
    Json = 19,
    Bson = 20,
    Interval = 21,
}

/// <summary>
/// Repetition type for schema fields.
/// </summary>
public enum FieldRepetitionType
{
    Required = 0,
    Optional = 1,
    Repeated = 2,
}

/// <summary>
/// Types of pages within a column chunk.
/// </summary>
public enum PageType
{
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
}

/// <summary>
/// Sort order for column index boundary values.
/// </summary>
public enum BoundaryOrder
{
    Unordered = 0,
    Ascending = 1,
    Descending = 2,
}
