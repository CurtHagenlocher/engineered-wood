using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes values using PLAIN encoding for all Parquet physical types.
/// </summary>
internal static class PlainEncoder
{
    /// <summary>
    /// Encodes boolean values as bit-packed bytes (LSB first).
    /// </summary>
    public static byte[] EncodeBoolean(ReadOnlySpan<bool> values)
    {
        int byteCount = (values.Length + 7) / 8;
        var result = new byte[byteCount];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i])
                result[i >> 3] |= (byte)(1 << (i & 7));
        }
        return result;
    }

    /// <summary>
    /// Encodes Int32 values as contiguous 4-byte little-endian.
    /// </summary>
    public static byte[] EncodeInt32(ReadOnlySpan<int> values)
    {
        var result = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(i * 4), values[i]);
        return result;
    }

    /// <summary>
    /// Encodes Int64 values as contiguous 8-byte little-endian.
    /// </summary>
    public static byte[] EncodeInt64(ReadOnlySpan<long> values)
    {
        var result = new byte[values.Length * 8];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(i * 8), values[i]);
        return result;
    }

    /// <summary>
    /// Encodes Float values as contiguous 4-byte IEEE 754 little-endian.
    /// </summary>
    public static byte[] EncodeFloat(ReadOnlySpan<float> values)
    {
        var result = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(result.AsSpan(i * 4), values[i]);
        return result;
    }

    /// <summary>
    /// Encodes Double values as contiguous 8-byte IEEE 754 little-endian.
    /// </summary>
    public static byte[] EncodeDouble(ReadOnlySpan<double> values)
    {
        var result = new byte[values.Length * 8];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteDoubleLittleEndian(result.AsSpan(i * 8), values[i]);
        return result;
    }

    /// <summary>
    /// Encodes FixedLenByteArray values as contiguous bytes (count * typeLength).
    /// </summary>
    public static byte[] EncodeFixedLenByteArray(ReadOnlySpan<byte> flatValues, int count, int typeLength)
    {
        if (flatValues.Length != count * typeLength)
            throw new ArgumentException(
                $"Expected {count * typeLength} bytes for {count} FLBA values of length {typeLength}, got {flatValues.Length}.");
        return flatValues.ToArray();
    }

    /// <summary>
    /// Encodes ByteArray values as length-prefixed sequences: [4-byte LE length][data]...
    /// </summary>
    public static byte[] EncodeByteArray(ReadOnlySpan<byte> flatData, ReadOnlySpan<int> offsets)
    {
        int count = offsets.Length - 1;
        int totalSize = count * 4 + flatData.Length;
        var result = new byte[totalSize];
        int pos = 0;

        for (int i = 0; i < count; i++)
        {
            int length = offsets[i + 1] - offsets[i];
            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), length);
            pos += 4;
            flatData.Slice(offsets[i], length).CopyTo(result.AsSpan(pos));
            pos += length;
        }

        return result;
    }

    /// <summary>
    /// Encodes Int96 values (12 bytes each) as contiguous bytes.
    /// </summary>
    public static byte[] EncodeInt96(ReadOnlySpan<byte> flatValues, int count)
    {
        if (flatValues.Length != count * 12)
            throw new ArgumentException(
                $"Expected {count * 12} bytes for {count} INT96 values, got {flatValues.Length}.");
        return flatValues.ToArray();
    }
}
