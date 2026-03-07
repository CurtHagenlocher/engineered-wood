namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes Int32 or Int64 values using DELTA_BINARY_PACKED encoding.
/// Format: [blockSize varint] [miniblockCount varint] [totalValues varint]
///         [firstValue zigzag] [block0] [block1] ...
/// Each block: [minDelta zigzag] [bitWidth0] ... [bitWidthN] [miniblock0] ...
/// </summary>
internal sealed class DeltaBinaryPackedEncoder
{
    private const int DefaultBlockSize = 128;
    private const int DefaultMiniblockCount = 4;

    private readonly int _blockSize;
    private readonly int _miniblockCount;
    private readonly int _valuesPerMiniblock;
    private readonly List<long> _values = new();

    public DeltaBinaryPackedEncoder(int blockSize = DefaultBlockSize, int miniblockCount = DefaultMiniblockCount)
    {
        _blockSize = blockSize;
        _miniblockCount = miniblockCount;
        _valuesPerMiniblock = _blockSize / _miniblockCount;
    }

    public void AddValue(int value) => _values.Add(value);
    public void AddValue(long value) => _values.Add(value);

    public void AddValues(ReadOnlySpan<int> values)
    {
        foreach (int v in values)
            _values.Add(v);
    }

    public void AddValues(ReadOnlySpan<long> values)
    {
        foreach (long v in values)
            _values.Add(v);
    }

    public byte[] Finish()
    {
        var ms = new MemoryStream();

        // Header
        WriteUnsignedVarInt(ms, (ulong)_blockSize);
        WriteUnsignedVarInt(ms, (ulong)_miniblockCount);
        WriteUnsignedVarInt(ms, (ulong)_values.Count);

        if (_values.Count == 0)
        {
            WriteZigZagVarLong(ms, 0);
            return ms.ToArray();
        }

        WriteZigZagVarLong(ms, _values[0]);

        if (_values.Count == 1)
            return ms.ToArray();

        // Compute deltas
        var deltas = new long[_values.Count - 1];
        for (int i = 0; i < deltas.Length; i++)
            deltas[i] = _values[i + 1] - _values[i];

        // Process in blocks
        int deltaIdx = 0;
        while (deltaIdx < deltas.Length)
        {
            int blockEnd = Math.Min(deltaIdx + _blockSize, deltas.Length);

            // Find minimum delta in this block
            long minDelta = long.MaxValue;
            for (int i = deltaIdx; i < blockEnd; i++)
                minDelta = Math.Min(minDelta, deltas[i]);

            WriteZigZagVarLong(ms, minDelta);

            // Compute bit widths for each miniblock
            var bitWidths = new byte[_miniblockCount];
            for (int mb = 0; mb < _miniblockCount; mb++)
            {
                int mbStart = deltaIdx + mb * _valuesPerMiniblock;
                int mbEnd = Math.Min(mbStart + _valuesPerMiniblock, blockEnd);

                if (mbStart >= blockEnd)
                {
                    bitWidths[mb] = 0;
                    continue;
                }

                long maxVal = 0;
                for (int i = mbStart; i < mbEnd; i++)
                    maxVal = Math.Max(maxVal, deltas[i] - minDelta);

                bitWidths[mb] = maxVal == 0 ? (byte)0 : (byte)(64 - long.LeadingZeroCount(maxVal));
            }

            // Write bit widths
            ms.Write(bitWidths, 0, _miniblockCount);

            // Write miniblock data
            for (int mb = 0; mb < _miniblockCount; mb++)
            {
                int mbStart = deltaIdx + mb * _valuesPerMiniblock;
                int mbEnd = Math.Min(mbStart + _valuesPerMiniblock, blockEnd);

                if (bitWidths[mb] == 0)
                    continue;

                int bw = bitWidths[mb];
                // Pad to _valuesPerMiniblock values
                int totalBits = _valuesPerMiniblock * bw;
                int totalBytes = (totalBits + 7) / 8;
                var packed = new byte[totalBytes];

                int bitOffset = 0;
                for (int i = 0; i < _valuesPerMiniblock; i++)
                {
                    long val = (mbStart + i < mbEnd) ? (deltas[mbStart + i] - minDelta) : 0;
                    WriteBits(packed, bitOffset, (ulong)val, bw);
                    bitOffset += bw;
                }

                ms.Write(packed, 0, totalBytes);
            }

            deltaIdx += _blockSize;
        }

        return ms.ToArray();
    }

    private static void WriteBits(byte[] buffer, int bitOffset, ulong value, int bitWidth)
    {
        int byteIdx = bitOffset >> 3;
        int bitIdx = bitOffset & 7;

        // Write bits across bytes, LSB first
        int bitsRemaining = bitWidth;
        while (bitsRemaining > 0)
        {
            int bitsInThisByte = Math.Min(8 - bitIdx, bitsRemaining);
            byte mask = (byte)((1 << bitsInThisByte) - 1);
            buffer[byteIdx] |= (byte)((value & mask) << bitIdx);
            value >>= bitsInThisByte;
            bitsRemaining -= bitsInThisByte;
            byteIdx++;
            bitIdx = 0;
        }
    }

    private static void WriteUnsignedVarInt(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }

    private static void WriteZigZagVarLong(Stream stream, long value)
    {
        ulong zigzag = (ulong)((value << 1) ^ (value >> 63));
        WriteUnsignedVarInt(stream, zigzag);
    }
}
