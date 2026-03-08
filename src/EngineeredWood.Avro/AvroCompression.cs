using System.Buffers.Binary;
using System.IO.Hashing;
using EngineeredWood.Compression;

namespace EngineeredWood.Avro;

/// <summary>
/// Bridges Avro compression codecs to the shared Core compressor/decompressor.
/// Handles Avro-specific framing (e.g. Snappy with trailing CRC32C).
/// </summary>
internal static class AvroCompression
{
    public static byte[] Compress(AvroCodec codec, ReadOnlySpan<byte> data)
    {
        if (codec == AvroCodec.Null)
            return data.ToArray();

        if (codec == AvroCodec.Snappy)
            return CompressSnappy(data);

        if (codec == AvroCodec.Lz4)
            return CompressLz4(data);

        var coreCodec = ToCoreCodec(codec);
        int maxLen = Compressor.GetMaxCompressedLength(coreCodec, data.Length);
        var output = new byte[maxLen];
        int written = Compressor.Compress(coreCodec, data, output);
        return output.AsSpan(0, written).ToArray();
    }

    public static byte[] Decompress(AvroCodec codec, ReadOnlySpan<byte> data, int uncompressedSizeHint)
    {
        if (codec == AvroCodec.Null)
            return data.ToArray();

        if (codec == AvroCodec.Snappy)
            return DecompressSnappy(data);

        if (codec == AvroCodec.Lz4)
            return DecompressLz4(data);

        if (codec == AvroCodec.Zstandard)
            return DecompressZstd(data);

        // Deflate: unknown uncompressed size, use stream-based decompression
        return DecompressStream(ToCoreCodec(codec), data);
    }

    private static byte[] DecompressStream(CompressionCodec codec, ReadOnlySpan<byte> data)
    {
        using var sourceStream = new MemoryStream(data.ToArray());
        using var decompressionStream = new System.IO.Compression.DeflateStream(
            sourceStream, System.IO.Compression.CompressionMode.Decompress);
        using var output = new MemoryStream();
        decompressionStream.CopyTo(output);
        return output.ToArray();
    }

    private static byte[] DecompressZstd(ReadOnlySpan<byte> data)
    {
        // Zstd frame header contains the decompressed size
        int size = checked((int)Decompressor.GetDecompressedLength(CompressionCodec.Zstd, data));
        var output = new byte[size];
        Decompressor.Decompress(CompressionCodec.Zstd, data, output);
        return output;
    }

    /// <summary>
    /// Avro LZ4 = 4-byte LE uncompressed size + LZ4 block data.
    /// Compatible with Python lz4.block.compress(store_size=True).
    /// </summary>
    private static byte[] CompressLz4(ReadOnlySpan<byte> data)
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Lz4, data.Length);
        var output = new byte[4 + maxLen]; // 4-byte size prefix + compressed data
        BinaryPrimitives.WriteInt32LittleEndian(output, data.Length);
        int written = Compressor.Compress(CompressionCodec.Lz4, data, output.AsSpan(4));
        return output.AsSpan(0, 4 + written).ToArray();
    }

    private static byte[] DecompressLz4(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
            throw new InvalidDataException("LZ4 data too short for size header.");

        int uncompressedSize = BinaryPrimitives.ReadInt32LittleEndian(data);
        var output = new byte[uncompressedSize];
        Decompressor.Decompress(CompressionCodec.Lz4, data[4..], output);
        return output;
    }

    /// <summary>
    /// Avro Snappy = Snappy block + 4-byte big-endian CRC32C of uncompressed data.
    /// </summary>
    private static byte[] CompressSnappy(ReadOnlySpan<byte> data)
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, data.Length);
        var output = new byte[maxLen + 4]; // +4 for CRC
        int written = Compressor.Compress(CompressionCodec.Snappy, data, output);

        // Append CRC32C of uncompressed data (big-endian)
        uint crc = Crc32C(data);
        BinaryPrimitives.WriteUInt32BigEndian(output.AsSpan(written), crc);

        return output.AsSpan(0, written + 4).ToArray();
    }

    private static byte[] DecompressSnappy(ReadOnlySpan<byte> data)
    {
        // Last 4 bytes are CRC32C (big-endian) of the uncompressed data
        if (data.Length < 4)
            throw new InvalidDataException("Snappy data too short for CRC32C checksum.");

        var compressedData = data[..^4];
        uint expectedCrc = BinaryPrimitives.ReadUInt32BigEndian(data[^4..]);

        // Determine uncompressed size from Snappy header
        int uncompressedSize = checked((int)Decompressor.GetDecompressedLength(
            CompressionCodec.Snappy, compressedData));
        var output = new byte[uncompressedSize];
        Decompressor.Decompress(CompressionCodec.Snappy, compressedData, output);

        // Verify CRC
        uint actualCrc = Crc32C(output);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException(
                $"Snappy CRC32C mismatch: expected 0x{expectedCrc:X8}, got 0x{actualCrc:X8}.");

        return output;
    }

    private static uint Crc32C(ReadOnlySpan<byte> data)
    {
        var crc = new Crc32();
        crc.Append(data);
        Span<byte> hash = stackalloc byte[4];
        crc.GetHashAndReset(hash);
        return BinaryPrimitives.ReadUInt32LittleEndian(hash);
    }

    private static CompressionCodec ToCoreCodec(AvroCodec codec) => codec switch
    {
        AvroCodec.Deflate => CompressionCodec.Deflate,
        AvroCodec.Zstandard => CompressionCodec.Zstd,
        _ => throw new NotSupportedException($"Avro codec {codec} is not supported."),
    };

    public static string CodecName(AvroCodec codec) => codec switch
    {
        AvroCodec.Null => "null",
        AvroCodec.Deflate => "deflate",
        AvroCodec.Snappy => "snappy",
        AvroCodec.Zstandard => "zstandard",
        AvroCodec.Lz4 => "lz4",
        _ => throw new ArgumentOutOfRangeException(nameof(codec)),
    };

    public static AvroCodec ParseCodecName(string name) => name switch
    {
        "null" => AvroCodec.Null,
        "deflate" => AvroCodec.Deflate,
        "snappy" => AvroCodec.Snappy,
        "zstandard" => AvroCodec.Zstandard,
        "lz4" => AvroCodec.Lz4,
        _ => throw new NotSupportedException($"Unknown Avro codec: '{name}'"),
    };
}
