using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Compression;

namespace EngineeredWood.Tests.Parquet.Compression;

public class CompressorTests
{
    private static readonly byte[] TestData = CreateTestData();

    private static byte[] CreateTestData()
    {
        // Repetitive data that compresses well
        var data = new byte[10_000];
        var rng = new Random(42);
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)(rng.Next(10)); // low cardinality → compressible
        return data;
    }

    [Fact]
    public void Uncompressed_RoundTrips()
    {
        var source = TestData;
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, source.Length)];
        int compLen = Compressor.Compress(CompressionCodec.Uncompressed, source, compressed);

        Assert.Equal(source.Length, compLen);

        var decompressed = new byte[source.Length];
        int decLen = Decompressor.Decompress(CompressionCodec.Uncompressed, compressed.AsSpan(0, compLen), decompressed);

        Assert.Equal(source.Length, decLen);
        Assert.Equal(source, decompressed);
    }

    [Fact]
    public void Snappy_RoundTrips()
    {
        var source = TestData;
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, source.Length)];
        int compLen = Compressor.Compress(CompressionCodec.Snappy, source, compressed);

        // Snappy should compress this repetitive data
        Assert.True(compLen < source.Length, "Snappy should compress repetitive data");

        var decompressed = new byte[source.Length];
        int decLen = Decompressor.Decompress(CompressionCodec.Snappy, compressed.AsSpan(0, compLen), decompressed);

        Assert.Equal(source.Length, decLen);
        Assert.Equal(source, decompressed);
    }

    [Fact]
    public void Zstd_RoundTrips()
    {
        var source = TestData;
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Zstd, source.Length)];
        int compLen = Compressor.Compress(CompressionCodec.Zstd, source, compressed);

        Assert.True(compLen < source.Length, "Zstd should compress repetitive data");

        var decompressed = new byte[source.Length];
        int decLen = Decompressor.Decompress(CompressionCodec.Zstd, compressed.AsSpan(0, compLen), decompressed);

        Assert.Equal(source.Length, decLen);
        Assert.Equal(source, decompressed);
    }

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Zstd)]
    public void EmptyData_RoundTrips(CompressionCodec codec)
    {
        var source = Array.Empty<byte>();
        var compressed = new byte[Math.Max(Compressor.GetMaxCompressedLength(codec, 0), 64)];
        int compLen = Compressor.Compress(codec, source, compressed);

        var decompressed = new byte[64];
        int decLen = Decompressor.Decompress(codec, compressed.AsSpan(0, compLen), decompressed.AsSpan(0, 0));
        Assert.Equal(0, decLen);
    }

    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Zstd)]
    public void SmallData_RoundTrips(CompressionCodec codec)
    {
        var source = new byte[] { 1, 2, 3 };
        var compressed = new byte[Compressor.GetMaxCompressedLength(codec, source.Length)];
        int compLen = Compressor.Compress(codec, source, compressed);

        var decompressed = new byte[source.Length];
        int decLen = Decompressor.Decompress(codec, compressed.AsSpan(0, compLen), decompressed);

        Assert.Equal(source.Length, decLen);
        Assert.Equal(source, decompressed);
    }

    [Fact]
    public void GetMaxCompressedLength_Uncompressed_ReturnsSameLength()
    {
        Assert.Equal(100, Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, 100));
    }

    [Fact]
    public void GetMaxCompressedLength_Snappy_ReturnsLarger()
    {
        int max = Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, 100);
        Assert.True(max >= 100);
    }

    [Fact]
    public void UnsupportedCodec_Throws()
    {
        Assert.Throws<NotSupportedException>(() =>
            Compressor.Compress(CompressionCodec.Gzip, ReadOnlySpan<byte>.Empty, Span<byte>.Empty));
    }
}
