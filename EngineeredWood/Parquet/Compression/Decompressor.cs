using Snappier;
using ZstdDecompressor = ZstdSharp.Decompressor;

namespace EngineeredWood.Parquet.Compression;

/// <summary>
/// Dispatches decompression based on the Parquet compression codec.
/// </summary>
internal static class Decompressor
{
    [ThreadStatic]
    private static ZstdDecompressor? t_zstd;

    /// <summary>
    /// Decompresses <paramref name="source"/> into <paramref name="destination"/>.
    /// For <see cref="CompressionCodec.Uncompressed"/>, the source is copied directly.
    /// </summary>
    /// <returns>The number of bytes written to <paramref name="destination"/>.</returns>
    public static int Decompress(
        CompressionCodec codec,
        ReadOnlySpan<byte> source,
        Span<byte> destination)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => DecompressUncompressed(source, destination),
            CompressionCodec.Snappy => DecompressSnappy(source, destination),
            CompressionCodec.Zstd => DecompressZstd(source, destination),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported."),
        };
    }

    private static int DecompressUncompressed(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        source.CopyTo(destination);
        return source.Length;
    }

    private static int DecompressSnappy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return Snappy.Decompress(source, destination);
    }

    private static int DecompressZstd(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var zstd = t_zstd ??= new ZstdDecompressor();
        return zstd.Unwrap(source, destination);
    }

    /// <summary>
    /// Returns the decompressed length for Snappy-compressed data.
    /// For uncompressed data, returns the source length.
    /// </summary>
    public static int GetDecompressedLength(CompressionCodec codec, ReadOnlySpan<byte> source)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => source.Length,
            CompressionCodec.Snappy => Snappy.GetUncompressedLength(source),
            CompressionCodec.Zstd => checked((int)ZstdDecompressor.GetDecompressedSize(source)),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported."),
        };
    }
}
