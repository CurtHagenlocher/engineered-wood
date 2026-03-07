using System.Buffers.Binary;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class PlainEncoderTests
{
    [Fact]
    public void Boolean_RoundTrips()
    {
        var values = new[] { true, false, true, true, false, false, true, false, true };
        var encoded = PlainEncoder.EncodeBoolean(values);
        var decoded = new bool[values.Length];
        PlainDecoder.DecodeBooleans(encoded, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Boolean_Empty()
    {
        var encoded = PlainEncoder.EncodeBoolean(ReadOnlySpan<bool>.Empty);
        Assert.Empty(encoded);
    }

    [Fact]
    public void Boolean_AllTrue()
    {
        var values = Enumerable.Repeat(true, 16).ToArray();
        var encoded = PlainEncoder.EncodeBoolean(values);
        var decoded = new bool[values.Length];
        PlainDecoder.DecodeBooleans(encoded, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int32_RoundTrips()
    {
        var values = new[] { 0, 1, -1, int.MaxValue, int.MinValue, 42, -42 };
        var encoded = PlainEncoder.EncodeInt32(values);
        Assert.Equal(values.Length * 4, encoded.Length);

        var decoded = new int[values.Length];
        for (int i = 0; i < values.Length; i++)
            decoded[i] = BinaryPrimitives.ReadInt32LittleEndian(encoded.AsSpan(i * 4));
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        var values = new[] { 0L, 1L, -1L, long.MaxValue, long.MinValue, 42L };
        var encoded = PlainEncoder.EncodeInt64(values);
        Assert.Equal(values.Length * 8, encoded.Length);

        var decoded = new long[values.Length];
        for (int i = 0; i < values.Length; i++)
            decoded[i] = BinaryPrimitives.ReadInt64LittleEndian(encoded.AsSpan(i * 8));
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Float_RoundTrips()
    {
        var values = new[] { 0f, 1.5f, -1.5f, float.MaxValue, float.MinValue, float.NaN };
        var encoded = PlainEncoder.EncodeFloat(values);
        Assert.Equal(values.Length * 4, encoded.Length);

        var decoded = new float[values.Length];
        for (int i = 0; i < values.Length; i++)
            decoded[i] = BinaryPrimitives.ReadSingleLittleEndian(encoded.AsSpan(i * 4));

        for (int i = 0; i < values.Length; i++)
        {
            if (float.IsNaN(values[i]))
                Assert.True(float.IsNaN(decoded[i]));
            else
                Assert.Equal(values[i], decoded[i]);
        }
    }

    [Fact]
    public void Double_RoundTrips()
    {
        var values = new[] { 0.0, 3.14159, -2.71828, double.MaxValue, double.MinValue };
        var encoded = PlainEncoder.EncodeDouble(values);
        Assert.Equal(values.Length * 8, encoded.Length);

        var decoded = new double[values.Length];
        for (int i = 0; i < values.Length; i++)
            decoded[i] = BinaryPrimitives.ReadDoubleLittleEndian(encoded.AsSpan(i * 8));
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void ByteArray_RoundTrips()
    {
        var offsets = new[] { 0, 5, 8, 8, 12 }; // 4 values, one empty
        var flatData = "HelloBye    data"u8.ToArray()[..12];
        flatData = new byte[] { 72, 101, 108, 108, 111, 66, 121, 101, 100, 97, 116, 97 };

        var encoded = PlainEncoder.EncodeByteArray(flatData, offsets);

        // Verify format: [4-byte len][data] repeated
        int pos = 0;
        for (int i = 0; i < offsets.Length - 1; i++)
        {
            int expectedLen = offsets[i + 1] - offsets[i];
            int decodedLen = BinaryPrimitives.ReadInt32LittleEndian(encoded.AsSpan(pos));
            Assert.Equal(expectedLen, decodedLen);
            pos += 4;
            Assert.Equal(flatData.AsSpan(offsets[i], expectedLen).ToArray(), encoded.AsSpan(pos, decodedLen).ToArray());
            pos += decodedLen;
        }
    }

    [Fact]
    public void FixedLenByteArray_RoundTrips()
    {
        int typeLength = 16;
        var values = new byte[3 * typeLength];
        Random.Shared.NextBytes(values);

        var encoded = PlainEncoder.EncodeFixedLenByteArray(values, 3, typeLength);
        Assert.Equal(values, encoded);
    }
}
