using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes definition and repetition levels from Parquet data pages.
/// </summary>
internal static class LevelDecoder
{
    /// <summary>
    /// Decodes levels from a V1 data page. The format is a 4-byte little-endian length
    /// prefix followed by RLE/bit-packed encoded level data.
    /// </summary>
    /// <returns>The number of bytes consumed (including the 4-byte length prefix).</returns>
    public static int DecodeV1(
        ReadOnlySpan<byte> data,
        int maxLevel,
        int valueCount,
        Span<int> levels)
    {
        if (maxLevel == 0)
        {
            levels.Slice(0, valueCount).Clear();
            return 0;
        }

        if (data.Length < 4)
            throw new ParquetFormatException("Not enough data for V1 level length prefix.");

        int encodedLength = BinaryPrimitives.ReadInt32LittleEndian(data);
        if (encodedLength < 0 || 4 + encodedLength > data.Length)
            throw new ParquetFormatException(
                $"Invalid V1 level encoded length: {encodedLength}.");

        int bitWidth = GetBitWidth(maxLevel);
        var rleData = data.Slice(4, encodedLength);
        var decoder = new RleBitPackedDecoder(rleData, bitWidth);
        decoder.ReadBatch(levels.Slice(0, valueCount));

        return 4 + encodedLength;
    }

    /// <summary>
    /// Decodes levels from a V2 data page. The format is raw RLE/bit-packed data
    /// with no length prefix (the byte length is provided in the page header).
    /// </summary>
    public static void DecodeV2(
        ReadOnlySpan<byte> data,
        int maxLevel,
        int valueCount,
        Span<int> levels)
    {
        if (maxLevel == 0)
        {
            levels.Slice(0, valueCount).Clear();
            return;
        }

        int bitWidth = GetBitWidth(maxLevel);
        var decoder = new RleBitPackedDecoder(data, bitWidth);
        decoder.ReadBatch(levels.Slice(0, valueCount));
    }

    /// <summary>
    /// Returns the minimum number of bits needed to represent values 0..maxLevel.
    /// </summary>
    internal static int GetBitWidth(int maxLevel)
    {
        if (maxLevel == 0) return 0;
        return 32 - int.LeadingZeroCount(maxLevel);
    }
}
