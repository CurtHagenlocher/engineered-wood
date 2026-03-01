using System.Buffers.Binary;
using System.IO.Compression;
using K4os.Compression.LZ4;
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
            CompressionCodec.Gzip => DecompressGzip(source, destination),
            CompressionCodec.Brotli => DecompressBrotli(source, destination),
            CompressionCodec.Lz4 => DecompressHadoopLz4(source, destination),
            CompressionCodec.Zstd => DecompressZstd(source, destination),
            CompressionCodec.Lz4Raw => DecompressLz4Raw(source, destination),
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

    private static unsafe int DecompressGzip(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        fixed (byte* ptr = source)
        {
            using var sourceStream = new UnmanagedMemoryStream(ptr, source.Length);
            using var gzip = new GZipStream(sourceStream, CompressionMode.Decompress);
            int totalRead = 0;
            while (totalRead < destination.Length)
            {
                int read = gzip.Read(destination.Slice(totalRead));
                if (read == 0) break;
                totalRead += read;
            }
            return totalRead;
        }
    }

    private static int DecompressBrotli(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (!BrotliDecoder.TryDecompress(source, destination, out int bytesWritten))
            throw new InvalidDataException("Brotli decompression failed.");
        return bytesWritten;
    }

    private static int DecompressLz4Raw(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int decoded = LZ4Codec.Decode(source, destination);
        if (decoded < 0)
            throw new InvalidDataException("LZ4 decompression failed.");
        return decoded;
    }

    /// <summary>
    /// Decompresses data using the deprecated Hadoop LZ4 framing format.
    /// Each frame: 4-byte big-endian decompressed size + 4-byte big-endian compressed size + data.
    /// Falls back to raw LZ4 if the framing doesn't validate.
    /// </summary>
    private static int DecompressHadoopLz4(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (TryDecompressHadoopFramed(source, destination, out int written))
            return written;

        // Fallback: treat as raw LZ4 block (some writers label raw LZ4 as Hadoop LZ4)
        return DecompressLz4Raw(source, destination);
    }

    private static bool TryDecompressHadoopFramed(
        ReadOnlySpan<byte> source, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        if (source.Length < 8)
            return false;

        // Read first frame header — validate before committing to Hadoop framing
        uint rawDecomp = BinaryPrimitives.ReadUInt32BigEndian(source);
        uint rawComp = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(4));

        if (rawDecomp == 0 || rawComp == 0 ||
            rawDecomp > (uint)destination.Length ||
            rawComp > (uint)(source.Length - 8))
            return false;

        int srcOffset = 0;
        int dstOffset = 0;

        while (srcOffset + 8 <= source.Length)
        {
            uint decompSize = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(srcOffset));
            uint compSize = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(srcOffset + 4));
            srcOffset += 8;

            if (compSize == 0 || decompSize == 0 ||
                srcOffset + (int)compSize > source.Length ||
                dstOffset + (int)decompSize > destination.Length)
                return false;

            int decoded = LZ4Codec.Decode(
                source.Slice(srcOffset, (int)compSize),
                destination.Slice(dstOffset, (int)decompSize));

            if (decoded != (int)decompSize)
                return false; // frame decode mismatch — not valid Hadoop framing

            srcOffset += (int)compSize;
            dstOffset += (int)decompSize;
        }

        bytesWritten = dstOffset;
        return bytesWritten > 0;
    }

    private static int DecompressZstd(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var zstd = t_zstd ??= new ZstdDecompressor();
        return zstd.Unwrap(source, destination);
    }

    /// <summary>
    /// Returns the decompressed length for compressed data.
    /// For uncompressed data, returns the source length.
    /// </summary>
    public static int GetDecompressedLength(CompressionCodec codec, ReadOnlySpan<byte> source)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => source.Length,
            CompressionCodec.Snappy => Snappy.GetUncompressedLength(source),
            CompressionCodec.Zstd => checked((int)ZstdDecompressor.GetDecompressedSize(source)),
            // Gzip, Brotli, LZ4: decompressed size comes from Parquet page headers
            _ => throw new NotSupportedException(
                $"GetDecompressedLength is not supported for codec '{codec}'. " +
                "Use the uncompressed page size from the page header instead."),
        };
    }
}
