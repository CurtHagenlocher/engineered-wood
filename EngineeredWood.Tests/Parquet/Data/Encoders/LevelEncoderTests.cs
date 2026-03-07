using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class LevelEncoderTests
{
    [Fact]
    public void V1_RoundTrips_SimpleLevels()
    {
        byte[] levels = [1, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1];
        int maxLevel = 1;

        var encoded = LevelEncoder.EncodeV1(levels, maxLevel);

        // V1 has a 4-byte length prefix
        Assert.True(encoded.Length > 4);

        int rleLength = BitConverter.ToInt32(encoded, 0);
        Assert.Equal(encoded.Length - 4, rleLength);

        // Decode with existing LevelDecoder
        var decoded = new byte[levels.Length];
        int bytesConsumed = LevelDecoder.DecodeV1(encoded, maxLevel, levels.Length, decoded, out int matchCount);

        Assert.Equal(encoded.Length, bytesConsumed);
        Assert.Equal(levels, decoded);
        Assert.Equal(13, matchCount); // 13 ones (maxLevel matches)
    }

    [Fact]
    public void V2_RoundTrips_SimpleLevels()
    {
        byte[] levels = [1, 1, 0, 1, 0, 1, 1, 1, 0, 1];
        int maxLevel = 1;

        var encoded = LevelEncoder.EncodeV2(levels, maxLevel);

        var decoded = new byte[levels.Length];
        LevelDecoder.DecodeV2(encoded, maxLevel, levels.Length, decoded, out int matchCount);

        Assert.Equal(levels, decoded);
        Assert.Equal(7, matchCount);
    }

    [Fact]
    public void V1_MaxLevel0_ReturnsEmpty()
    {
        byte[] levels = [0, 0, 0];
        var encoded = LevelEncoder.EncodeV1(levels, 0);
        Assert.Empty(encoded);
    }

    [Fact]
    public void V2_MaxLevel0_ReturnsEmpty()
    {
        byte[] levels = [0, 0, 0];
        var encoded = LevelEncoder.EncodeV2(levels, 0);
        Assert.Empty(encoded);
    }

    [Fact]
    public void V1_AllNonNull_RoundTrips()
    {
        var levels = Enumerable.Repeat((byte)1, 1000).ToArray();
        var encoded = LevelEncoder.EncodeV1(levels, 1);

        var decoded = new byte[levels.Length];
        LevelDecoder.DecodeV1(encoded, 1, levels.Length, decoded, out int matchCount);

        Assert.Equal(levels, decoded);
        Assert.Equal(1000, matchCount);
    }

    [Fact]
    public void V1_AllNull_RoundTrips()
    {
        var levels = new byte[500];
        var encoded = LevelEncoder.EncodeV1(levels, 1);

        var decoded = new byte[levels.Length];
        LevelDecoder.DecodeV1(encoded, 1, levels.Length, decoded, out int matchCount);

        Assert.Equal(levels, decoded);
        Assert.Equal(0, matchCount);
    }

    [Fact]
    public void V2_HigherMaxLevel_RoundTrips()
    {
        // Simulate nested column with maxDefLevel=3
        byte[] levels = [3, 3, 2, 1, 0, 3, 3, 3, 0, 0, 1, 2, 3, 3, 3, 3];
        int maxLevel = 3;

        var encoded = LevelEncoder.EncodeV2(levels, maxLevel);

        var decoded = new byte[levels.Length];
        LevelDecoder.DecodeV2(encoded, maxLevel, levels.Length, decoded, out int matchCount);

        Assert.Equal(levels, decoded);
        Assert.Equal(9, matchCount); // 9 values at maxLevel=3
    }

    [Fact]
    public void V1_LargeDataset_RoundTrips()
    {
        var rng = new Random(42);
        var levels = new byte[10_000];
        for (int i = 0; i < levels.Length; i++)
            levels[i] = (byte)(rng.NextDouble() < 0.85 ? 1 : 0);

        var encoded = LevelEncoder.EncodeV1(levels, 1);

        var decoded = new byte[levels.Length];
        LevelDecoder.DecodeV1(encoded, 1, levels.Length, decoded, out _);

        Assert.Equal(levels, decoded);
    }
}
