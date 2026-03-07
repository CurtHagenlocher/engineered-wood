using System.Buffers;
using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes definition and repetition levels using the RLE/Bit-Packing Hybrid format.
/// Mirror of <see cref="LevelDecoder"/>.
/// </summary>
internal static class LevelEncoder
{
    /// <summary>
    /// Encodes levels for V1 data pages: 4-byte little-endian length prefix followed by RLE data.
    /// </summary>
    public static byte[] EncodeV1(ReadOnlySpan<byte> levels, int maxLevel)
    {
        if (maxLevel == 0)
            return [];

        byte[] rleData = EncodeRle(levels, maxLevel);

        // V1 format: [4-byte length][RLE data]
        var result = new byte[4 + rleData.Length];
        BinaryPrimitives.WriteInt32LittleEndian(result, rleData.Length);
        rleData.CopyTo(result.AsSpan(4));
        return result;
    }

    /// <summary>
    /// Encodes levels for V2 data pages: raw RLE data (no length prefix).
    /// The byte length is stored in the V2 page header instead.
    /// </summary>
    public static byte[] EncodeV2(ReadOnlySpan<byte> levels, int maxLevel)
    {
        if (maxLevel == 0)
            return [];

        return EncodeRle(levels, maxLevel);
    }

    private static byte[] EncodeRle(ReadOnlySpan<byte> levels, int maxLevel)
    {
        int bitWidth = LevelDecoder.GetBitWidth(maxLevel);
        int[] temp = ArrayPool<int>.Shared.Rent(levels.Length);
        try
        {
            for (int i = 0; i < levels.Length; i++)
                temp[i] = levels[i];
            return RleBitPackedEncoder.Encode(temp.AsSpan(0, levels.Length), bitWidth);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(temp);
        }
    }
}
