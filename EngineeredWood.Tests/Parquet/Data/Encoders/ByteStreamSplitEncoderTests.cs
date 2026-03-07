using System.Buffers.Binary;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class ByteStreamSplitEncoderTests
{
    [Fact]
    public void Float_RoundTrips()
    {
        var values = new float[] { 1.0f, 2.5f, -3.14f, 0.0f, float.MaxValue };
        var encoded = ByteStreamSplitEncoder.EncodeFloat(values);

        var decoded = new float[values.Length];
        ByteStreamSplitDecoder.DecodeFloats(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Double_RoundTrips()
    {
        var values = new double[] { 3.14159, -2.71828, 0.0, double.MaxValue, double.MinValue };
        var encoded = ByteStreamSplitEncoder.EncodeDouble(values);

        var decoded = new double[values.Length];
        ByteStreamSplitDecoder.DecodeDoubles(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int32_RoundTrips()
    {
        var values = new int[] { 0, 1, -1, int.MaxValue, int.MinValue, 42 };
        var encoded = ByteStreamSplitEncoder.EncodeInt32(values);

        var decoded = new int[values.Length];
        ByteStreamSplitDecoder.DecodeInt32s(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        var values = new long[] { 0L, 1L, -1L, long.MaxValue, long.MinValue };
        var encoded = ByteStreamSplitEncoder.EncodeInt64(values);

        var decoded = new long[values.Length];
        ByteStreamSplitDecoder.DecodeInt64s(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void LargeDataSet_Float_RoundTrips()
    {
        var rng = new Random(42);
        var values = new float[10_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = (float)(rng.NextDouble() * 1_000_000 - 500_000);

        var encoded = ByteStreamSplitEncoder.EncodeFloat(values);

        var decoded = new float[values.Length];
        ByteStreamSplitDecoder.DecodeFloats(encoded, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Empty_ReturnsEmpty()
    {
        var encoded = ByteStreamSplitEncoder.EncodeFloat(ReadOnlySpan<float>.Empty);
        Assert.Empty(encoded);
    }

    [Fact]
    public void RawEncode_CorrectFormat()
    {
        // Value 0: LE bytes 01 02 03 04
        // Value 1: LE bytes 05 06 07 08
        // Split: Stream0=[01,05] Stream1=[02,06] Stream2=[03,07] Stream3=[04,08]
        var input = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        var encoded = ByteStreamSplitEncoder.Encode(input, 2, 4);

        Assert.Equal(new byte[] { 0x01, 0x05, 0x02, 0x06, 0x03, 0x07, 0x04, 0x08 }, encoded);
    }
}
