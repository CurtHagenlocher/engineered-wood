using System.Numerics;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes integer values using the RLE/Bit-Packing Hybrid format
/// as defined in the Parquet specification. Used for definition levels,
/// repetition levels, dictionary indices, and boolean values.
/// </summary>
internal sealed class RleBitPackedEncoder
{
    private readonly int _bitWidth;
    private readonly MemoryStream _output;
    private readonly List<int> _buffered = new();

    public RleBitPackedEncoder(int bitWidth, int initialCapacity = 256)
    {
        _bitWidth = bitWidth;
        _output = new MemoryStream(initialCapacity);
    }

    /// <summary>
    /// Computes the minimum bit width needed to represent values up to <paramref name="maxValue"/>.
    /// </summary>
    public static int BitWidth(int maxValue)
    {
        if (maxValue <= 0) return 0;
        return 32 - BitOperations.LeadingZeroCount((uint)maxValue);
    }

    /// <summary>
    /// Adds a single value to the encoder buffer.
    /// </summary>
    public void WriteValue(int value) => _buffered.Add(value);

    /// <summary>
    /// Adds multiple values to the encoder buffer.
    /// </summary>
    public void WriteValues(ReadOnlySpan<int> values)
    {
        foreach (int v in values)
            _buffered.Add(v);
    }

    /// <summary>
    /// Flushes all buffered values and returns the encoded bytes.
    /// </summary>
    public byte[] Finish()
    {
        _output.SetLength(0);
        if (_buffered.Count == 0 || _bitWidth == 0)
        {
            // bitWidth 0 means all values are 0 — still need to emit an RLE run
            if (_buffered.Count > 0 && _bitWidth == 0)
            {
                WriteRleRun(0, _buffered.Count);
            }
            _buffered.Clear();
            return _output.ToArray();
        }

        EncodeValues();
        _buffered.Clear();
        return _output.ToArray();
    }

    private void EncodeValues()
    {
        int i = 0;
        int count = _buffered.Count;

        while (i < count)
        {
            // Look ahead to decide between RLE and bit-packing.
            // Use RLE if there are >= 8 consecutive equal values.
            int runLength = 1;
            while (i + runLength < count && _buffered[i + runLength] == _buffered[i])
                runLength++;

            if (runLength >= 8)
            {
                WriteRleRun(_buffered[i], runLength);
                i += runLength;
            }
            else
            {
                // Bit-pack in groups of 8. Collect values until we hit an RLE-worthy run.
                int start = i;
                while (i < count)
                {
                    int ahead = 1;
                    while (i + ahead < count && _buffered[i + ahead] == _buffered[i])
                        ahead++;

                    if (ahead >= 8)
                        break; // let the next iteration handle this as RLE

                    i++;
                }

                // When not at the end, absorb extra values to fill the last group of 8.
                // Zero-padding in the middle of the stream would corrupt subsequent values.
                int bitPackedCount = i - start;
                if (i < count && bitPackedCount % 8 != 0)
                {
                    int needed = 8 - (bitPackedCount % 8);
                    i = Math.Min(i + needed, count);
                }

                WriteBitPackedRun(start, i - start);
            }
        }
    }

    private void WriteRleRun(int value, int count)
    {
        // Header: (count << 1) | 0  (LSB = 0 for RLE)
        WriteVarInt(count << 1);

        // Value: ceil(bitWidth / 8) bytes, little-endian
        int byteWidth = (_bitWidth + 7) / 8;
        for (int i = 0; i < byteWidth; i++)
            _output.WriteByte((byte)((value >> (i * 8)) & 0xFF));
    }

    private void WriteBitPackedRun(int start, int count)
    {
        // Pad to a multiple of 8
        int groupCount = (count + 7) / 8;
        int paddedCount = groupCount * 8;

        // Header: (groupCount << 1) | 1  (LSB = 1 for bit-packed)
        WriteVarInt((groupCount << 1) | 1);

        // Bit-pack values, LSB first
        int totalBits = paddedCount * _bitWidth;
        int totalBytes = (totalBits + 7) / 8;
        var packed = new byte[totalBytes];

        int bitOffset = 0;
        for (int i = 0; i < paddedCount; i++)
        {
            int value = (start + i < start + count) ? _buffered[start + i] : 0;
            int byteIdx = bitOffset >> 3;
            int bitIdx = bitOffset & 7;

            // Write value bits starting at bitIdx
            if (_bitWidth > 0)
            {
                long bits = (long)(uint)value << bitIdx;
                int bytesNeeded = (bitIdx + _bitWidth + 7) / 8;
                for (int b = 0; b < bytesNeeded && byteIdx + b < packed.Length; b++)
                    packed[byteIdx + b] |= (byte)(bits >> (b * 8));
            }

            bitOffset += _bitWidth;
        }

        _output.Write(packed, 0, totalBytes);
    }

    private void WriteVarInt(int value)
    {
        uint v = (uint)value;
        while (v >= 0x80)
        {
            _output.WriteByte((byte)(v | 0x80));
            v >>= 7;
        }
        _output.WriteByte((byte)v);
    }
}
