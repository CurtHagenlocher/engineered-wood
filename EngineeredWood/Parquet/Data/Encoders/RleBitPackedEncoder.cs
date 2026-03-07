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
            if (_buffered.Count > 0 && _bitWidth == 0)
            {
                WriteRleRun(0, _buffered.Count);
            }
            _buffered.Clear();
            return _output.ToArray();
        }

        EncodeValues(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_buffered));
        _buffered.Clear();
        return _output.ToArray();
    }

    /// <summary>
    /// Encodes a span of values directly without buffering. More efficient when values are already in contiguous memory.
    /// </summary>
    public static byte[] Encode(ReadOnlySpan<int> values, int bitWidth)
    {
        var encoder = new RleBitPackedEncoder(bitWidth, Math.Max(256, values.Length * bitWidth / 8));
        if (values.Length == 0 || bitWidth == 0)
        {
            if (values.Length > 0 && bitWidth == 0)
                encoder.WriteRleRun(0, values.Length);
            return encoder._output.ToArray();
        }
        encoder.EncodeValues(values);
        return encoder._output.ToArray();
    }

    private void EncodeValues(ReadOnlySpan<int> values)
    {
        int i = 0;
        int count = values.Length;

        while (i < count)
        {
            int runLength = 1;
            while (i + runLength < count && values[i + runLength] == values[i])
                runLength++;

            if (runLength >= 8)
            {
                WriteRleRun(values[i], runLength);
                i += runLength;
            }
            else
            {
                int start = i;
                while (i < count)
                {
                    int ahead = 1;
                    while (i + ahead < count && values[i + ahead] == values[i])
                        ahead++;

                    if (ahead >= 8)
                        break;

                    i++;
                }

                int bitPackedCount = i - start;
                if (i < count && bitPackedCount % 8 != 0)
                {
                    int needed = 8 - (bitPackedCount % 8);
                    i = Math.Min(i + needed, count);
                }

                WriteBitPackedRun(values, start, i - start);
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

    private void WriteBitPackedRun(ReadOnlySpan<int> values, int start, int count)
    {
        // Pad to a multiple of 8
        int groupCount = (count + 7) / 8;
        int paddedCount = groupCount * 8;

        // Header: (groupCount << 1) | 1  (LSB = 1 for bit-packed)
        WriteVarInt((groupCount << 1) | 1);

        // Bit-pack values, LSB first
        int totalBits = paddedCount * _bitWidth;
        int totalBytes = (totalBits + 7) / 8;
        Span<byte> packed = totalBytes <= 256 ? stackalloc byte[totalBytes] : new byte[totalBytes];

        int bitOffset = 0;
        for (int i = 0; i < paddedCount; i++)
        {
            int value = (i < count) ? values[start + i] : 0;
            int byteIdx = bitOffset >> 3;
            int bitIdx = bitOffset & 7;

            if (_bitWidth > 0)
            {
                long bits = (long)(uint)value << bitIdx;
                int bytesNeeded = (bitIdx + _bitWidth + 7) / 8;
                for (int b = 0; b < bytesNeeded && byteIdx + b < packed.Length; b++)
                    packed[byteIdx + b] |= (byte)(bits >> (b * 8));
            }

            bitOffset += _bitWidth;
        }

        _output.Write(packed);
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
