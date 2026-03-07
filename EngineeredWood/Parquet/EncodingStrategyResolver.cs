namespace EngineeredWood.Parquet;

/// <summary>
/// Resolves the encoding to use for a column based on the chosen strategy and physical type.
/// </summary>
internal static class EncodingStrategyResolver
{
    /// <summary>
    /// Resolves the effective encoding for a column given the strategy and options.
    /// Returns the encoding to pass to ColumnWriter.
    /// </summary>
    public static Encoding Resolve(
        EncodingStrategy strategy,
        Encoding explicitEncoding,
        PhysicalType physicalType)
    {
        if (strategy == EncodingStrategy.None)
            return explicitEncoding;

        // Booleans don't benefit from dictionary — always use Plain
        if (physicalType == PhysicalType.Boolean)
            return Encoding.Plain;

        // Both Adaptive and Aggressive start with dictionary encoding.
        // ColumnWriter already handles dictionary-too-large fallback to Plain.
        return Encoding.PlainDictionary;
    }

    /// <summary>
    /// Gets the type-appropriate fallback encoding when dictionary is too large.
    /// Used by ColumnWriter when the strategy is Adaptive or Aggressive.
    /// </summary>
    public static Encoding GetFallbackEncoding(EncodingStrategy strategy, PhysicalType physicalType)
    {
        if (strategy == EncodingStrategy.None)
            return Encoding.Plain;

        return physicalType switch
        {
            PhysicalType.Int32 => Encoding.DeltaBinaryPacked,
            PhysicalType.Int64 => Encoding.DeltaBinaryPacked,
            PhysicalType.Float => Encoding.ByteStreamSplit,
            PhysicalType.Double => Encoding.ByteStreamSplit,
            PhysicalType.ByteArray => Encoding.DeltaLengthByteArray,
            PhysicalType.FixedLenByteArray => Encoding.DeltaByteArray,
            _ => Encoding.Plain,
        };
    }

    /// <summary>
    /// For Aggressive strategy: maximum dictionary byte size as a fraction of estimated
    /// uncompressed column data size. If dictionary exceeds this ratio, abandon early.
    /// </summary>
    public static int GetMaxDictionarySize(EncodingStrategy strategy, int defaultMaxSize)
    {
        if (strategy == EncodingStrategy.Aggressive)
            return defaultMaxSize / 2; // Tighter threshold for aggressive
        return defaultMaxSize;
    }
}
