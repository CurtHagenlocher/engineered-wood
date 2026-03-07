using System.Text;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class DeltaLengthByteArrayEncoderTests
{
    [Fact]
    public void RoundTrips_VariableLengthStrings()
    {
        var strings = new[] { "Hello", "World", "Foo", "Bar", "Baz" };
        var encoder = new DeltaLengthByteArrayEncoder();
        foreach (var s in strings)
            encoder.AddValue(Encoding.UTF8.GetBytes(s));

        var encoded = encoder.Finish();

        // Decode lengths via DeltaBinaryPacked
        var lengthDecoder = new DeltaBinaryPackedDecoder(encoded);
        var lengths = new int[strings.Length];
        lengthDecoder.DecodeInt32s(lengths);

        // Then read raw data
        int dataStart = lengthDecoder.BytesConsumed;
        int pos = dataStart;
        for (int i = 0; i < strings.Length; i++)
        {
            string decoded = Encoding.UTF8.GetString(encoded.AsSpan(pos, lengths[i]));
            Assert.Equal(strings[i], decoded);
            pos += lengths[i];
        }
    }

    [Fact]
    public void RoundTrips_EmptyStrings()
    {
        var encoder = new DeltaLengthByteArrayEncoder();
        encoder.AddValue(ReadOnlySpan<byte>.Empty);
        encoder.AddValue("abc"u8);
        encoder.AddValue(ReadOnlySpan<byte>.Empty);

        var encoded = encoder.Finish();

        var lengthDecoder = new DeltaBinaryPackedDecoder(encoded);
        var lengths = new int[3];
        lengthDecoder.DecodeInt32s(lengths);

        Assert.Equal(0, lengths[0]);
        Assert.Equal(3, lengths[1]);
        Assert.Equal(0, lengths[2]);
    }

    [Fact]
    public void RoundTrips_FromOffsetsApi()
    {
        var data = "HelloWorld"u8.ToArray();
        var offsets = new[] { 0, 5, 10 };

        var encoder = new DeltaLengthByteArrayEncoder();
        encoder.AddValues(data, offsets);

        var encoded = encoder.Finish();

        var lengthDecoder = new DeltaBinaryPackedDecoder(encoded);
        var lengths = new int[2];
        lengthDecoder.DecodeInt32s(lengths);

        Assert.Equal(5, lengths[0]);
        Assert.Equal(5, lengths[1]);
    }
}
