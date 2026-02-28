namespace EngineeredWood.Parquet;

/// <summary>
/// Exception thrown when a Parquet file is malformed or contains unexpected data.
/// </summary>
public sealed class ParquetFormatException : Exception
{
    public ParquetFormatException(string message) : base(message) { }

    public ParquetFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
