using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes PLAIN-encoded values for each Parquet physical type.
/// </summary>
internal static class PlainDecoder
{
    /// <summary>
    /// Decodes <paramref name="count"/> boolean values from the PLAIN encoding.
    /// Booleans are bit-packed: 1 bit per value, LSB first.
    /// </summary>
    public static void DecodeBooleans(ReadOnlySpan<byte> data, Span<bool> destination, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            if (byteIndex >= data.Length)
                throw new ParquetFormatException("Unexpected end of PLAIN boolean data.");
            destination[i] = ((data[byteIndex] >> bitIndex) & 1) == 1;
        }
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Int32 values from the PLAIN encoding.
    /// </summary>
    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, int>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Int32 data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Int64 values from the PLAIN encoding.
    /// </summary>
    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, long>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Int64 data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Float values from the PLAIN encoding.
    /// </summary>
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, float>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Float data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Double values from the PLAIN encoding.
    /// </summary>
    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, double>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Double data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> INT96 values from the PLAIN encoding.
    /// Each value is 12 bytes.
    /// </summary>
    public static void DecodeInt96s(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        int totalBytes = count * 12;
        if (data.Length < totalBytes)
            throw new ParquetFormatException("Unexpected end of PLAIN Int96 data.");
        data.Slice(0, totalBytes).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> FIXED_LEN_BYTE_ARRAY values from the PLAIN encoding.
    /// </summary>
    public static void DecodeFixedLenByteArrays(
        ReadOnlySpan<byte> data, Span<byte> destination, int count, int typeLength)
    {
        int totalBytes = count * typeLength;
        if (data.Length < totalBytes)
            throw new ParquetFormatException("Unexpected end of PLAIN FixedLenByteArray data.");
        data.Slice(0, totalBytes).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> BYTE_ARRAY values from the PLAIN encoding.
    /// Each value is prefixed with a 4-byte little-endian length.
    /// Returns offsets and data as separate arrays for use with Arrow binary/string arrays.
    /// </summary>
    /// <returns>The number of bytes consumed from the source data.</returns>
    public static int DecodeByteArrays(
        ReadOnlySpan<byte> data,
        Span<int> offsets,
        out byte[] values,
        int count)
    {
        // First pass: compute total data size
        int pos = 0;
        int totalDataSize = 0;
        for (int i = 0; i < count; i++)
        {
            if (pos + 4 > data.Length)
                throw new ParquetFormatException("Unexpected end of PLAIN ByteArray data.");
            int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
            pos += 4 + len;
            totalDataSize += len;
        }

        values = new byte[totalDataSize];

        // Second pass: copy data and build offsets
        pos = 0;
        int dataPos = 0;
        offsets[0] = 0;
        for (int i = 0; i < count; i++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
            pos += 4;
            data.Slice(pos, len).CopyTo(values.AsSpan(dataPos));
            pos += len;
            dataPos += len;
            offsets[i + 1] = dataPos;
        }

        return pos;
    }
}
