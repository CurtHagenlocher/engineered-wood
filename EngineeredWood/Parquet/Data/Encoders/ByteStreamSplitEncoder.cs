namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes fixed-width values using BYTE_STREAM_SPLIT encoding.
/// Splits each N-byte value into N byte streams, one per byte position.
/// Format: [stream_0: byte 0 of all values] [stream_1: byte 1 of all values] ...
/// </summary>
internal static class ByteStreamSplitEncoder
{
    /// <summary>
    /// Encodes fixed-width values by byte-stream splitting.
    /// </summary>
    /// <param name="values">Flat byte data: count * typeWidth bytes in native layout.</param>
    /// <param name="count">Number of values.</param>
    /// <param name="typeWidth">Width of each value in bytes (4 for float/int32, 8 for double/int64).</param>
    public static byte[] Encode(ReadOnlySpan<byte> values, int count, int typeWidth)
    {
        if (count == 0) return [];

        int totalBytes = count * typeWidth;
        if (values.Length != totalBytes)
            throw new ArgumentException(
                $"Expected {totalBytes} bytes for {count} values of width {typeWidth}, got {values.Length}.");

        var result = new byte[totalBytes];

        // Stream s gets byte s of every value
        for (int s = 0; s < typeWidth; s++)
        {
            int destOffset = s * count;
            for (int i = 0; i < count; i++)
                result[destOffset + i] = values[i * typeWidth + s];
        }

        return result;
    }

    /// <summary>Encodes Float values using byte-stream split.</summary>
    public static byte[] EncodeFloat(ReadOnlySpan<float> values)
    {
        var bytes = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 4), values[i]);
        return Encode(bytes, values.Length, 4);
    }

    /// <summary>Encodes Double values using byte-stream split.</summary>
    public static byte[] EncodeDouble(ReadOnlySpan<double> values)
    {
        var bytes = new byte[values.Length * 8];
        for (int i = 0; i < values.Length; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 8), values[i]);
        return Encode(bytes, values.Length, 8);
    }

    /// <summary>Encodes Int32 values using byte-stream split.</summary>
    public static byte[] EncodeInt32(ReadOnlySpan<int> values)
    {
        var bytes = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 4), values[i]);
        return Encode(bytes, values.Length, 4);
    }

    /// <summary>Encodes Int64 values using byte-stream split.</summary>
    public static byte[] EncodeInt64(ReadOnlySpan<long> values)
    {
        var bytes = new byte[values.Length * 8];
        for (int i = 0; i < values.Length; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 8), values[i]);
        return Encode(bytes, values.Length, 8);
    }
}
