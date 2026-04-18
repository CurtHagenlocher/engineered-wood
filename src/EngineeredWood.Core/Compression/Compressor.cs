// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Compression;
using K4os.Compression.LZ4;
using Snappier;
using ZstdCompressor = ZstdSharp.Compressor;

namespace EngineeredWood.Compression;

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
#if NET6_0_OR_GREATER
            CompressionCodec.Brotli => BrotliEncoder.GetMaxCompressedLength(inputLength),
#else
            CompressionCodec.Brotli => inputLength + 64, // conservative estimate for BrotliSharpLib
#endif
            CompressionCodec.Lz4 => LZ4Codec.MaximumOutputSize(inputLength),
            CompressionCodec.Deflate => inputLength + 64,
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
            CompressionCodec.Lz4 => CompressLz4(source, destination),
            CompressionCodec.Deflate => CompressDeflate(source, destination),
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

    private static int CompressGzip(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        // Use a MemoryStream to collect compressed output, then copy to destination.
        using var outputStream = new MemoryStream();
        using (var gzip = new GZipStream(outputStream, CompressionLevel.Fastest, leaveOpen: true))
        {
#if NET6_0_OR_GREATER
            gzip.Write(source);
#else
            byte[] array = source.ToArray();
            gzip.Write(array, 0, array.Length);
#endif
        }

        int compressedLength = checked((int)outputStream.Length);
        outputStream.GetBuffer().AsSpan(0, compressedLength).CopyTo(destination);
        return compressedLength;
    }

    private static int CompressBrotli(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        if (!BrotliEncoder.TryCompress(source, destination, out int bytesWritten))
            throw new InvalidOperationException("Brotli compression failed.");
        return bytesWritten;
#else
        byte[] compressed = BrotliSharpLib.Brotli.CompressBuffer(source.ToArray(), 0, source.Length);
        compressed.AsSpan().CopyTo(destination);
        return compressed.Length;
#endif
    }

    private static int CompressLz4(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int encoded = LZ4Codec.Encode(source, destination);
        if (encoded < 0)
            throw new InvalidOperationException("LZ4 compression failed.");
        return encoded;
    }

    private static int CompressDeflate(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        using var outputStream = new MemoryStream();
        using (var deflate = new DeflateStream(outputStream, CompressionLevel.Fastest, leaveOpen: true))
        {
#if NET6_0_OR_GREATER
            deflate.Write(source);
#else
            byte[] array = source.ToArray();
            deflate.Write(array, 0, array.Length);
#endif
        }

        int compressedLength = checked((int)outputStream.Length);
        outputStream.GetBuffer().AsSpan(0, compressedLength).CopyTo(destination);
        return compressedLength;
    }

    private static int CompressZstd(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var zstd = t_zstd ??= new ZstdCompressor();
        return zstd.Wrap(source, destination);
    }
}
