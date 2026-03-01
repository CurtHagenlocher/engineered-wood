namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes RLE/Bit-Packing Hybrid encoded data as defined in the Parquet specification.
/// </summary>
/// <remarks>
/// The encoding uses a header byte (varint) whose LSB determines the mode:
/// - LSB = 0: RLE run. Header >> 1 = repeat count. Followed by a value of <c>bitWidth</c> bits (ceil-byte-aligned).
/// - LSB = 1: Bit-packed group. Header >> 1 = group count (each group is 8 values). Followed by bitWidth * 8 bits per group.
/// </remarks>
internal ref struct RleBitPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;
    private readonly int _bitWidth;

    // Current run state
    private bool _isRle;
    private int _remaining; // values remaining in current run/group
    private int _rleValue;

    // Bit-packed state
    private int _bitPackedPos;  // byte position in data where current bit-packed group started
    private int _bitOffset;     // bit offset within bit-packed bytes

    public RleBitPackedDecoder(ReadOnlySpan<byte> data, int bitWidth)
    {
        _data = data;
        _position = 0;
        _bitWidth = bitWidth;
        _isRle = false;
        _remaining = 0;
        _rleValue = 0;
        _bitPackedPos = 0;
        _bitOffset = 0;
    }

    /// <summary>Current byte position in the data.</summary>
    public int Position => _position;

    /// <summary>
    /// Reads the next value from the RLE/Bit-Packing Hybrid stream.
    /// </summary>
    public int ReadNext()
    {
        if (_remaining == 0)
            ReadNextGroup();

        _remaining--;

        if (_isRle)
            return _rleValue;

        return ReadBitPackedValue();
    }

    /// <summary>
    /// Reads <paramref name="count"/> values into the destination span.
    /// </summary>
    public void ReadBatch(Span<int> destination)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            if (_remaining == 0)
                ReadNextGroup();

            int toCopy = Math.Min(_remaining, destination.Length - offset);

            if (_isRle)
            {
                destination.Slice(offset, toCopy).Fill(_rleValue);
                _remaining -= toCopy;
                offset += toCopy;
            }
            else
            {
                for (int i = 0; i < toCopy; i++)
                {
                    destination[offset++] = ReadBitPackedValue();
                    _remaining--;
                }
            }
        }
    }

    private void ReadNextGroup()
    {
        int header = ReadVarInt();
        if ((header & 1) == 0)
        {
            // RLE run
            _isRle = true;
            _remaining = header >> 1;
            _rleValue = ReadRleValue();
        }
        else
        {
            // Bit-packed run
            _isRle = false;
            int groupCount = header >> 1;
            _remaining = groupCount * 8;
            _bitPackedPos = _position;
            _bitOffset = 0;
            // Advance _position past the bit-packed bytes
            int totalBits = groupCount * 8 * _bitWidth;
            _position += (totalBits + 7) / 8;
        }
    }

    private int ReadVarInt()
    {
        int result = 0;
        int shift = 0;
        while (true)
        {
            if (_position >= _data.Length)
                throw new ParquetFormatException("Unexpected end of RLE data reading varint.");
            byte b = _data[_position++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }
    }

    private int ReadRleValue()
    {
        int byteWidth = (_bitWidth + 7) / 8;
        if (_position + byteWidth > _data.Length)
            throw new ParquetFormatException("Unexpected end of RLE data reading value.");

        int value = 0;
        for (int i = 0; i < byteWidth; i++)
            value |= _data[_position++] << (i * 8);

        return value;
    }

    private int ReadBitPackedValue()
    {
        if (_bitWidth == 0)
            return 0;

        int byteIndex = _bitPackedPos + (_bitOffset / 8);
        int bitIndex = _bitOffset % 8;
        _bitOffset += _bitWidth;

        int value = 0;
        int bitsRead = 0;
        while (bitsRead < _bitWidth)
        {
            if (byteIndex >= _data.Length)
                throw new ParquetFormatException("Unexpected end of bit-packed data.");

            int bitsAvailable = 8 - bitIndex;
            int bitsToRead = Math.Min(bitsAvailable, _bitWidth - bitsRead);
            int mask = (1 << bitsToRead) - 1;
            value |= ((_data[byteIndex] >> bitIndex) & mask) << bitsRead;

            bitsRead += bitsToRead;
            bitIndex = 0;
            byteIndex++;
        }

        return value;
    }
}
