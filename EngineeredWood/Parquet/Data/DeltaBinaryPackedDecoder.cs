namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes DELTA_BINARY_PACKED encoded values for INT32 and INT64 columns.
/// </summary>
/// <remarks>
/// Format (all varints are unsigned unless noted):
/// <list type="number">
/// <item>Block size in values (varint)</item>
/// <item>Number of miniblocks per block (varint)</item>
/// <item>Total value count (varint)</item>
/// <item>First value (zigzag varint)</item>
/// </list>
/// Then for each block:
/// <list type="number">
/// <item>Min delta (zigzag varint)</item>
/// <item>Bit widths: one byte per miniblock</item>
/// <item>Bit-packed deltas for each miniblock (values per miniblock = blockSize / miniblockCount)</item>
/// </list>
/// </remarks>
internal ref struct DeltaBinaryPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;

    private readonly int _blockSize;
    private readonly int _miniblockCount;
    private readonly int _valuesPerMiniblock;
    private readonly int _totalValueCount;
    private long _lastValue;

    public DeltaBinaryPackedDecoder(ReadOnlySpan<byte> data)
    {
        _data = data;
        _pos = 0;

        _blockSize = checked((int)ReadUnsignedVarInt());
        _miniblockCount = checked((int)ReadUnsignedVarInt());
        _totalValueCount = checked((int)ReadUnsignedVarInt());
        _lastValue = ReadZigZagVarInt();
        _valuesPerMiniblock = _blockSize / _miniblockCount;
    }

    /// <summary>
    /// Decodes all values as INT32 into <paramref name="destination"/>.
    /// Uses unchecked 32-bit arithmetic (deltas may wrap around).
    /// </summary>
    public void DecodeInt32s(Span<int> destination)
    {
        if (_totalValueCount == 0)
            return;

        int lastInt = (int)_lastValue;
        destination[0] = lastInt;
        int valuesDecoded = 1;

        var bitWidths = new byte[_miniblockCount];

        while (valuesDecoded < _totalValueCount)
        {
            int minDelta = (int)ReadZigZagVarInt();

            for (int i = 0; i < _miniblockCount; i++)
                bitWidths[i] = _data[_pos++];

            for (int mb = 0; mb < _miniblockCount && valuesDecoded < _totalValueCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                int valuesToDecode = Math.Min(_valuesPerMiniblock, _totalValueCount - valuesDecoded);

                if (bitWidth == 0)
                {
                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        lastInt += minDelta;
                        destination[valuesDecoded++] = lastInt;
                    }
                }
                else
                {
                    int bitOffset = _pos * 8;

                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        int delta = (int)ReadBitPacked(bitOffset, bitWidth);
                        bitOffset += bitWidth;
                        lastInt += minDelta + delta;
                        destination[valuesDecoded++] = lastInt;
                    }

                    _pos += (_valuesPerMiniblock * bitWidth + 7) / 8;
                }
            }
        }
    }

    /// <summary>
    /// Decodes all values as INT64 into <paramref name="destination"/>.
    /// </summary>
    public void DecodeInt64s(Span<long> destination)
    {
        if (_totalValueCount == 0)
            return;

        destination[0] = _lastValue;
        int valuesDecoded = 1;

        var bitWidths = new byte[_miniblockCount];

        while (valuesDecoded < _totalValueCount)
        {
            long minDelta = ReadZigZagVarInt();

            for (int i = 0; i < _miniblockCount; i++)
                bitWidths[i] = _data[_pos++];

            for (int mb = 0; mb < _miniblockCount && valuesDecoded < _totalValueCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                int valuesToDecode = Math.Min(_valuesPerMiniblock, _totalValueCount - valuesDecoded);

                if (bitWidth == 0)
                {
                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        _lastValue += minDelta;
                        destination[valuesDecoded++] = _lastValue;
                    }
                }
                else
                {
                    int bitOffset = _pos * 8;

                    for (int i = 0; i < valuesToDecode; i++)
                    {
                        long delta = ReadBitPackedLong(bitOffset, bitWidth);
                        bitOffset += bitWidth;
                        _lastValue += minDelta + delta;
                        destination[valuesDecoded++] = _lastValue;
                    }

                    _pos += (_valuesPerMiniblock * bitWidth + 7) / 8;
                }
            }
        }
    }

    private long ReadBitPacked(int bitOffset, int bitWidth)
    {
        int byteIndex = bitOffset / 8;
        int bitIndex = bitOffset % 8;

        long value = 0;
        int bitsRead = 0;
        while (bitsRead < bitWidth)
        {
            int bitsAvailable = 8 - bitIndex;
            int bitsToRead = Math.Min(bitsAvailable, bitWidth - bitsRead);
            int mask = (1 << bitsToRead) - 1;
            value |= (long)((_data[byteIndex] >> bitIndex) & mask) << bitsRead;

            bitsRead += bitsToRead;
            bitIndex = 0;
            byteIndex++;
        }

        return value;
    }

    private long ReadBitPackedLong(int bitOffset, int bitWidth)
    {
        int byteIndex = bitOffset / 8;
        int bitIndex = bitOffset % 8;

        long value = 0;
        int bitsRead = 0;
        while (bitsRead < bitWidth)
        {
            int bitsAvailable = 8 - bitIndex;
            int bitsToRead = Math.Min(bitsAvailable, bitWidth - bitsRead);
            long mask = (1L << bitsToRead) - 1;
            value |= ((long)(_data[byteIndex] >> bitIndex) & mask) << bitsRead;

            bitsRead += bitsToRead;
            bitIndex = 0;
            byteIndex++;
        }

        return value;
    }

    private long ReadUnsignedVarInt()
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            byte b = _data[_pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }
    }

    private long ReadZigZagVarInt()
    {
        long encoded = ReadUnsignedVarInt();
        return (long)((ulong)encoded >> 1) ^ -(encoded & 1);
    }
}
