using System.Text;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Data.Encoders;

namespace EngineeredWood.Tests.Parquet.Data.Encoders;

public class DeltaByteArrayEncoderTests
{
    // DeltaByteArray format: [DELTA_BINARY_PACKED prefix_lengths] [DELTA_LENGTH_BYTE_ARRAY suffixes]
    // We decode manually since the existing decoder uses ColumnBuildState.

    private static string[] DecodeManually(byte[] encoded, int count)
    {
        // 1. Decode prefix lengths
        var prefixDecoder = new DeltaBinaryPackedDecoder(encoded);
        var prefixLengths = new int[count];
        prefixDecoder.DecodeInt32s(prefixLengths);
        int afterPrefixes = prefixDecoder.BytesConsumed;

        // 2. Decode suffix lengths (DELTA_BINARY_PACKED inside DELTA_LENGTH_BYTE_ARRAY)
        var suffixLenDecoder = new DeltaBinaryPackedDecoder(encoded.AsSpan(afterPrefixes));
        var suffixLengths = new int[count];
        suffixLenDecoder.DecodeInt32s(suffixLengths);
        int afterSuffixLens = afterPrefixes + suffixLenDecoder.BytesConsumed;

        // 3. Read raw suffix bytes
        int pos = afterSuffixLens;
        var result = new string[count];
        var prev = Array.Empty<byte>();

        for (int i = 0; i < count; i++)
        {
            int prefixLen = prefixLengths[i];
            int suffixLen = suffixLengths[i];
            var full = new byte[prefixLen + suffixLen];

            if (prefixLen > 0)
                Array.Copy(prev, 0, full, 0, prefixLen);
            if (suffixLen > 0)
                Array.Copy(encoded, pos, full, prefixLen, suffixLen);

            pos += suffixLen;
            result[i] = Encoding.UTF8.GetString(full);
            prev = full;
        }

        return result;
    }

    [Fact]
    public void RoundTrips_PrefixSharing()
    {
        var strings = new[]
        {
            "https://example.com/api/v2/users",
            "https://example.com/api/v2/orders",
            "https://example.com/api/v2/products",
            "https://example.com/settings",
        };

        var encoder = new DeltaByteArrayEncoder();
        foreach (var s in strings)
            encoder.AddValue(Encoding.UTF8.GetBytes(s));

        var encoded = encoder.Finish();
        var decoded = DecodeManually(encoded, strings.Length);

        Assert.Equal(strings, decoded);
    }

    [Fact]
    public void RoundTrips_NoPrefixSharing()
    {
        var strings = new[] { "abc", "xyz", "123", "!@#" };

        var encoder = new DeltaByteArrayEncoder();
        foreach (var s in strings)
            encoder.AddValue(Encoding.UTF8.GetBytes(s));

        var encoded = encoder.Finish();
        var decoded = DecodeManually(encoded, strings.Length);

        Assert.Equal(strings, decoded);
    }

    [Fact]
    public void RoundTrips_EmptyValues()
    {
        var encoder = new DeltaByteArrayEncoder();
        encoder.AddValue(ReadOnlySpan<byte>.Empty);
        encoder.AddValue("abc"u8);
        encoder.AddValue(ReadOnlySpan<byte>.Empty);

        var encoded = encoder.Finish();
        var decoded = DecodeManually(encoded, 3);

        Assert.Equal("", decoded[0]);
        Assert.Equal("abc", decoded[1]);
        Assert.Equal("", decoded[2]);
    }

    [Fact]
    public void RoundTrips_SingleValue()
    {
        var encoder = new DeltaByteArrayEncoder();
        encoder.AddValue("hello"u8);

        var encoded = encoder.Finish();
        var decoded = DecodeManually(encoded, 1);

        Assert.Equal("hello", decoded[0]);
    }
}
