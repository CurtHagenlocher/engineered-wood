using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class RleBitPackedEncoderTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(8)]
    public void RoundTrips_RleRun(int bitWidth)
    {
        int value = (1 << bitWidth) - 1; // max value for this bit width
        var encoder = new RleBitPackedEncoder(bitWidth);
        for (int i = 0; i < 100; i++)
            encoder.WriteValue(value);

        var encoded = encoder.Finish();
        var decoder = new RleBitPackedDecoder(encoded, bitWidth);
        var decoded = new int[100];
        decoder.ReadBatch(decoded);

        Assert.All(decoded, v => Assert.Equal(value, v));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void RoundTrips_BitPacked(int bitWidth)
    {
        int mask = (1 << bitWidth) - 1;
        var rng = new Random(42);
        var values = new int[64];
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(mask + 1);

        var encoder = new RleBitPackedEncoder(bitWidth);
        encoder.WriteValues(values);
        var encoded = encoder.Finish();

        var decoder = new RleBitPackedDecoder(encoded, bitWidth);
        var decoded = new int[values.Length];
        decoder.ReadBatch(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void RoundTrips_MixedRunTypes()
    {
        var encoder = new RleBitPackedEncoder(2);

        // RLE run: 20 zeros
        for (int i = 0; i < 20; i++)
            encoder.WriteValue(0);

        // Varied values (will be bit-packed)
        encoder.WriteValue(1);
        encoder.WriteValue(2);
        encoder.WriteValue(3);
        encoder.WriteValue(0);
        encoder.WriteValue(1);

        // Another RLE run: 15 ones
        for (int i = 0; i < 15; i++)
            encoder.WriteValue(1);

        var encoded = encoder.Finish();
        var decoder = new RleBitPackedDecoder(encoded, 2);
        var decoded = new int[40];
        decoder.ReadBatch(decoded);

        for (int i = 0; i < 20; i++)
            Assert.Equal(0, decoded[i]);
        Assert.Equal(1, decoded[20]);
        Assert.Equal(2, decoded[21]);
        Assert.Equal(3, decoded[22]);
        Assert.Equal(0, decoded[23]);
        Assert.Equal(1, decoded[24]);
        for (int i = 25; i < 40; i++)
            Assert.Equal(1, decoded[i]);
    }

    [Fact]
    public void BitWidth1_DefinitionLevels()
    {
        // Simulates typical nullable column def levels (0=null, 1=present)
        var values = new int[] { 1, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1 };

        var encoder = new RleBitPackedEncoder(1);
        encoder.WriteValues(values);
        var encoded = encoder.Finish();

        var decoder = new RleBitPackedDecoder(encoded, 1);
        var decoded = new int[values.Length];
        decoder.ReadBatch(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Empty_ProducesEmptyOutput()
    {
        var encoder = new RleBitPackedEncoder(1);
        var encoded = encoder.Finish();
        Assert.Empty(encoded);
    }

    [Fact]
    public void LargeDataSet_RoundTrips()
    {
        var rng = new Random(99);
        var values = new int[10_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(16); // max 4 bits

        var encoder = new RleBitPackedEncoder(4);
        encoder.WriteValues(values);
        var encoded = encoder.Finish();

        var decoder = new RleBitPackedDecoder(encoded, 4);
        var decoded = new int[values.Length];
        decoder.ReadBatch(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void BitWidth_ComputesCorrectly()
    {
        Assert.Equal(0, RleBitPackedEncoder.BitWidth(0));
        Assert.Equal(1, RleBitPackedEncoder.BitWidth(1));
        Assert.Equal(2, RleBitPackedEncoder.BitWidth(2));
        Assert.Equal(2, RleBitPackedEncoder.BitWidth(3));
        Assert.Equal(3, RleBitPackedEncoder.BitWidth(4));
        Assert.Equal(3, RleBitPackedEncoder.BitWidth(7));
        Assert.Equal(4, RleBitPackedEncoder.BitWidth(8));
        Assert.Equal(8, RleBitPackedEncoder.BitWidth(255));
    }

    [Fact]
    public void SingleValue_RoundTrips()
    {
        var encoder = new RleBitPackedEncoder(3);
        encoder.WriteValue(5);
        var encoded = encoder.Finish();

        var decoder = new RleBitPackedDecoder(encoded, 3);
        Assert.Equal(5, decoder.ReadNext());
    }

    [Fact]
    public void AllZeros_RoundTrips()
    {
        var encoder = new RleBitPackedEncoder(1);
        for (int i = 0; i < 1000; i++)
            encoder.WriteValue(0);

        var encoded = encoder.Finish();

        var decoder = new RleBitPackedDecoder(encoded, 1);
        var decoded = new int[1000];
        decoder.ReadBatch(decoded);

        Assert.All(decoded, v => Assert.Equal(0, v));
        // Should be very compact (single RLE run)
        Assert.True(encoded.Length < 10, $"Expected compact RLE encoding, got {encoded.Length} bytes");
    }
}
