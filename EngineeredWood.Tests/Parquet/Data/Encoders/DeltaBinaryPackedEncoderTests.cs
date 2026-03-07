using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class DeltaBinaryPackedEncoderTests
{
    [Fact]
    public void Int32_Sequential_RoundTrips()
    {
        var values = Enumerable.Range(0, 200).ToArray();
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int32_Random_RoundTrips()
    {
        var rng = new Random(42);
        var values = new int[500];
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-1000, 1000);

        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        var rng = new Random(42);
        var values = new long[300];
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.NextInt64(-100000, 100000);

        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new long[values.Length];
        decoder.DecodeInt64s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void SingleValue_RoundTrips()
    {
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValue(42);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[1];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(42, decoded[0]);
    }

    [Fact]
    public void AllSameValues_RoundTrips()
    {
        var values = Enumerable.Repeat(7, 256).ToArray();
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void NegativeValues_RoundTrips()
    {
        var values = new[] { -100, -50, 0, 50, 100, -200, -300, 0, 42 };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void LargeValues_RoundTrips()
    {
        var values = new[] { int.MaxValue, int.MinValue, 0, int.MaxValue, int.MinValue };
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Empty_ProducesValidOutput()
    {
        var encoder = new DeltaBinaryPackedEncoder();
        var encoded = encoder.Finish();
        Assert.True(encoded.Length > 0);
    }

    [Fact]
    public void ExactBlockSize_RoundTrips()
    {
        var values = Enumerable.Range(0, 128).ToArray();
        var encoder = new DeltaBinaryPackedEncoder(blockSize: 128, miniblockCount: 4);
        encoder.AddValues(values);
        var encoded = encoder.Finish();

        var decoder = new DeltaBinaryPackedDecoder(encoded);
        var decoded = new int[values.Length];
        decoder.DecodeInt32s(decoded);

        Assert.Equal(values, decoded);
    }
}
