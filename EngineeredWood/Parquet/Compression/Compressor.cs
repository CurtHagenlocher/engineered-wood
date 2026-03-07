using System.IO.Compression;
using K4os.Compression.LZ4;
using Snappier;
using ZstdCompressor = ZstdSharp.Compressor;

namespace EngineeredWood.Parquet.Compression;

/// <summary>
/// Dispatches compression based on the Parquet compression codec.
/// Mirror of <see cref="Decompressor"/>.
/// </summary>
internal static class Compressor
{
    [ThreadStatic]
    private static ZstdCompressor? t_zstd;

    /// <summary>
    /// Returns the maximum compressed size for a given codec and input length.
    /// Used to pre-allocate output buffers.
    /// </summary>
    public static int GetMaxCompressedLength(CompressionCodec codec, int inputLength)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => inputLength,
            CompressionCodec.Snappy => Snappy.GetMaxCompressedLength(inputLength),
            CompressionCodec.Gzip => inputLength + 64, // conservative overhead estimate
            CompressionCodec.Brotli => BrotliEncoder.GetMaxCompressedLength(inputLength),
            CompressionCodec.Lz4Raw => LZ4Codec.MaximumOutputSize(inputLength),
            CompressionCodec.Zstd => ZstdSharp.Compressor.GetCompressBound(inputLength),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported for writing."),
        };
    }

    /// <summary>
    /// Compresses <paramref name="source"/> into <paramref name="destination"/>.
    /// </summary>
    /// <returns>The number of bytes written to <paramref name="destination"/>.</returns>
    public static int Compress(
        CompressionCodec codec,
        ReadOnlySpan<byte> source,
        Span<byte> destination)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => CompressUncompressed(source, destination),
            CompressionCodec.Snappy => CompressSnappy(source, destination),
            CompressionCodec.Gzip => CompressGzip(source, destination),
            CompressionCodec.Brotli => CompressBrotli(source, destination),
            CompressionCodec.Lz4Raw => CompressLz4Raw(source, destination),
            CompressionCodec.Zstd => CompressZstd(source, destination),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported for writing."),
        };
    }

    private static int CompressUncompressed(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        source.CopyTo(destination);
        return source.Length;
    }

    private static int CompressSnappy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return Snappy.Compress(source, destination);
    }

    private static unsafe int CompressGzip(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        // Use a MemoryStream to collect compressed output, then copy to destination.
        using var outputStream = new MemoryStream();
        using (var gzip = new GZipStream(outputStream, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(source);
        }

        int compressedLength = checked((int)outputStream.Length);
        outputStream.GetBuffer().AsSpan(0, compressedLength).CopyTo(destination);
        return compressedLength;
    }

    private static int CompressBrotli(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (!BrotliEncoder.TryCompress(source, destination, out int bytesWritten))
            throw new InvalidOperationException("Brotli compression failed.");
        return bytesWritten;
    }

    private static int CompressLz4Raw(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int encoded = LZ4Codec.Encode(source, destination);
        if (encoded < 0)
            throw new InvalidOperationException("LZ4 compression failed.");
        return encoded;
    }

    private static int CompressZstd(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var zstd = t_zstd ??= new ZstdCompressor();
        return zstd.Wrap(source, destination);
    }
}
