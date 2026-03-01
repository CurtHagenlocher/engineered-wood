using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes BYTE_STREAM_SPLIT encoded values.
/// </summary>
/// <remarks>
/// The encoding splits each value's bytes across interleaved streams for better compression.
/// For N values of W-byte width, the data is W consecutive streams of N bytes each:
/// stream 0 has byte 0 of every value, stream 1 has byte 1 of every value, etc.
/// </remarks>
internal static class ByteStreamSplitDecoder
{
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
    {
        Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(float));
    }

    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
    {
        Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(double));
    }

    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
    {
        Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(int));
    }

    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
    {
        Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(long));
    }

    public static void DecodeFixedLenByteArrays(ReadOnlySpan<byte> data, Span<byte> destination, int count, int typeLength)
    {
        Unsplit(data, destination, count, typeLength);
    }

    private static void Unsplit(ReadOnlySpan<byte> data, Span<byte> destination, int count, int width)
    {
        for (int stream = 0; stream < width; stream++)
        {
            var streamData = data.Slice(stream * count, count);
            for (int i = 0; i < count; i++)
                destination[i * width + stream] = streamData[i];
        }
    }
}
