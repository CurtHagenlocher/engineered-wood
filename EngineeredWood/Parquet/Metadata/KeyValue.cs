namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// A key-value pair from the file metadata.
/// </summary>
public readonly record struct KeyValue(string Key, string? Value);
