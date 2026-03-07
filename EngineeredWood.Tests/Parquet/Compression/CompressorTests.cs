using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Compression;

namespace EngineeredWood.Tests.Parquet.Compression;

public class CompressorTests
{
    private static readonly byte[] TestData = GenerateTestData();

    private static byte[] GenerateTestData()
    {
        var rng = new Random(42);
        // Mix of compressible and random data
        var data = new byte[10_000];
        for (int i = 0; i < data.Length; i++)
        {
            // Repeat patterns for compressibility
            data[i] = (byte)(i % 256 == 0 ? rng.Next(256) : data[Math.Max(0, i - 1)]);
        }
        return data;
    }

    [Fact]
    public void Uncompressed_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Uncompressed);
    }

    [Fact]
    public void Snappy_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Snappy);
    }

    [Fact]
    public void Gzip_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Gzip);
    }

    [Fact]
    public void Brotli_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Brotli);
    }

    [Fact]
    public void Lz4Raw_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Lz4Raw);
    }

    [Fact]
    public void Zstd_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Zstd);
    }

    [Fact]
    public void Snappy_CompressesSmallerThanOriginal()
    {
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, TestData.Length)];
        int compressedLength = Compressor.Compress(CompressionCodec.Snappy, TestData, compressed);
        Assert.True(compressedLength < TestData.Length,
            $"Snappy compressed {TestData.Length} → {compressedLength} (expected smaller)");
    }

    [Fact]
    public void Zstd_CompressesSmallerThanOriginal()
    {
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Zstd, TestData.Length)];
        int compressedLength = Compressor.Compress(CompressionCodec.Zstd, TestData, compressed);
        Assert.True(compressedLength < TestData.Length,
            $"Zstd compressed {TestData.Length} → {compressedLength} (expected smaller)");
    }

    [Fact]
    public void Uncompressed_SameSize()
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, TestData.Length);
        Assert.Equal(TestData.Length, maxLen);

        var output = new byte[maxLen];
        int written = Compressor.Compress(CompressionCodec.Uncompressed, TestData, output);
        Assert.Equal(TestData.Length, written);
    }

    [Fact]
    public void Uncompressed_EmptyInput()
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, 0);
        Assert.Equal(0, maxLen);

        var output = new byte[1];
        int written = Compressor.Compress(CompressionCodec.Uncompressed, ReadOnlySpan<byte>.Empty, output);
        Assert.Equal(0, written);
    }

    [Fact]
    public void Lzo_NotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            Compressor.GetMaxCompressedLength(CompressionCodec.Lzo, 100));
        Assert.Throws<NotSupportedException>(() =>
            Compressor.Compress(CompressionCodec.Lzo, TestData, new byte[TestData.Length]));
    }

    private static void AssertRoundTrip(CompressionCodec codec)
    {
        int maxLen = Compressor.GetMaxCompressedLength(codec, TestData.Length);
        var compressed = new byte[maxLen];
        int compressedLength = Compressor.Compress(codec, TestData, compressed);

        var decompressed = new byte[TestData.Length];
        int decompressedLength = Decompressor.Decompress(codec, compressed.AsSpan(0, compressedLength), decompressed);

        Assert.Equal(TestData.Length, decompressedLength);
        Assert.Equal(TestData, decompressed);
    }
}
