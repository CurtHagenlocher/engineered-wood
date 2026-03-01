using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class LevelDecoderTests
{
    [Fact]
    public void DecodeV1_MaxLevelZero_ReturnsZeros()
    {
        byte[] data = [0xFF]; // irrelevant
        var levels = new int[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 0, valueCount: 4, levels);
        Assert.Equal(0, consumed);
        Assert.Equal([0, 0, 0, 0], levels);
    }

    [Fact]
    public void DecodeV1_RleEncoded()
    {
        // 4-byte length prefix + RLE data
        // RLE: header = (4 << 1) | 0 = 8, value = 1 (bit width 1, 1 byte)
        byte[] rleData = [8, 1];
        byte[] data = new byte[4 + rleData.Length];
        BitConverter.TryWriteBytes(data.AsSpan(0), rleData.Length);
        rleData.CopyTo(data.AsSpan(4));

        var levels = new int[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 1, valueCount: 4, levels);

        Assert.Equal(4 + rleData.Length, consumed);
        Assert.Equal([1, 1, 1, 1], levels);
    }

    [Fact]
    public void DecodeV2_MaxLevelZero_ReturnsZeros()
    {
        byte[] data = [];
        var levels = new int[3];
        LevelDecoder.DecodeV2(data, maxLevel: 0, valueCount: 3, levels);
        Assert.Equal([0, 0, 0], levels);
    }

    [Fact]
    public void DecodeV2_RleEncoded()
    {
        // Raw RLE: 3 copies of value 1, bitWidth=1
        // Header = (3 << 1) | 0 = 6, value = 1
        byte[] data = [6, 1];
        var levels = new int[3];
        LevelDecoder.DecodeV2(data, maxLevel: 1, valueCount: 3, levels);
        Assert.Equal([1, 1, 1], levels);
    }

    [Fact]
    public void GetBitWidth_ReturnsCorrectValues()
    {
        Assert.Equal(0, LevelDecoder.GetBitWidth(0));
        Assert.Equal(1, LevelDecoder.GetBitWidth(1));
        Assert.Equal(2, LevelDecoder.GetBitWidth(2));
        Assert.Equal(2, LevelDecoder.GetBitWidth(3));
        Assert.Equal(3, LevelDecoder.GetBitWidth(4));
        Assert.Equal(3, LevelDecoder.GetBitWidth(7));
        Assert.Equal(4, LevelDecoder.GetBitWidth(8));
    }
}
