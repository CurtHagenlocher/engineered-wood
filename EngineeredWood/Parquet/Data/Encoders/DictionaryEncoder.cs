using System.Buffers.Binary;
using System.Numerics;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes values using dictionary encoding (PLAIN_DICTIONARY / RLE_DICTIONARY).
/// Produces a PLAIN-encoded dictionary page and RLE-encoded index pages.
/// </summary>
internal sealed class DictionaryEncoder
{
    private readonly PhysicalType _physicalType;
    private readonly int _typeLength; // for FIXED_LEN_BYTE_ARRAY
    private readonly Dictionary<long, int> _fixedWidthDict = new();
    private readonly Dictionary<ByteArrayKey, int> _byteArrayDict = new(ByteArrayKeyComparer.Instance);
    private readonly List<byte[]> _dictEntries = new();
    private readonly List<int> _indices = new();
    private readonly int _entryByteWidth;

    /// <summary>Number of distinct values in the dictionary.</summary>
    public int DictionarySize => _dictEntries.Count;

    /// <summary>Total byte size of the dictionary entries.</summary>
    public int DictionaryByteSize { get; private set; }

    /// <summary>Number of values encoded so far.</summary>
    public int ValueCount => _indices.Count;

    public DictionaryEncoder(PhysicalType physicalType, int typeLength = 0)
    {
        _physicalType = physicalType;
        _typeLength = typeLength;
        _entryByteWidth = physicalType switch
        {
            PhysicalType.Boolean => 1,
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength,
            PhysicalType.ByteArray => 0, // variable
            _ => throw new ArgumentOutOfRangeException(nameof(physicalType)),
        };
    }

    /// <summary>Adds an Int32 value and returns its dictionary index.</summary>
    public int AddInt32(int value)
    {
        long key = value;
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _dictEntries.Count;
            _fixedWidthDict[key] = index;
            var entry = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(entry, value);
            _dictEntries.Add(entry);
            DictionaryByteSize += 4;
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>Adds an Int64 value and returns its dictionary index.</summary>
    public int AddInt64(long value)
    {
        if (!_fixedWidthDict.TryGetValue(value, out int index))
        {
            index = _dictEntries.Count;
            _fixedWidthDict[value] = index;
            var entry = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(entry, value);
            _dictEntries.Add(entry);
            DictionaryByteSize += 8;
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>Adds a Float value and returns its dictionary index.</summary>
    public int AddFloat(float value)
    {
        long key = BitConverter.SingleToInt32Bits(value);
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _dictEntries.Count;
            _fixedWidthDict[key] = index;
            var entry = new byte[4];
            BinaryPrimitives.WriteSingleLittleEndian(entry, value);
            _dictEntries.Add(entry);
            DictionaryByteSize += 4;
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>Adds a Double value and returns its dictionary index.</summary>
    public int AddDouble(double value)
    {
        long key = BitConverter.DoubleToInt64Bits(value);
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _dictEntries.Count;
            _fixedWidthDict[key] = index;
            var entry = new byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(entry, value);
            _dictEntries.Add(entry);
            DictionaryByteSize += 8;
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>Adds a ByteArray value and returns its dictionary index.</summary>
    public int AddByteArray(ReadOnlySpan<byte> value)
    {
        var key = new ByteArrayKey(value.ToArray());
        if (!_byteArrayDict.TryGetValue(key, out int index))
        {
            index = _dictEntries.Count;
            _byteArrayDict[key] = index;
            _dictEntries.Add(value.ToArray());
            DictionaryByteSize += 4 + value.Length; // 4-byte length prefix + data
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>Adds a FixedLenByteArray value and returns its dictionary index.</summary>
    public int AddFixedLenByteArray(ReadOnlySpan<byte> value)
    {
        var key = new ByteArrayKey(value.ToArray());
        if (!_byteArrayDict.TryGetValue(key, out int index))
        {
            index = _dictEntries.Count;
            _byteArrayDict[key] = index;
            _dictEntries.Add(value.ToArray());
            DictionaryByteSize += value.Length;
        }
        _indices.Add(index);
        return index;
    }

    /// <summary>
    /// Produces the PLAIN-encoded dictionary page bytes.
    /// </summary>
    public byte[] EncodeDictionaryPage()
    {
        if (_physicalType == PhysicalType.ByteArray)
        {
            // Length-prefixed format
            var ms = new MemoryStream(DictionaryByteSize);
            Span<byte> lenBuf = stackalloc byte[4];
            foreach (var entry in _dictEntries)
            {
                BinaryPrimitives.WriteInt32LittleEndian(lenBuf, entry.Length);
                ms.Write(lenBuf);
                ms.Write(entry);
            }
            return ms.ToArray();
        }

        // Fixed-width: concatenate entries
        var result = new byte[_dictEntries.Count * (_physicalType == PhysicalType.FixedLenByteArray ? _typeLength : _entryByteWidth)];
        int pos = 0;
        foreach (var entry in _dictEntries)
        {
            entry.CopyTo(result, pos);
            pos += entry.Length;
        }
        return result;
    }

    /// <summary>
    /// Produces the RLE-encoded index page bytes.
    /// The format is: [1-byte bitWidth] [RLE/bit-packed indices].
    /// </summary>
    public byte[] EncodeIndices()
    {
        int bitWidth = DictionarySize <= 1 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)(DictionarySize - 1));
        // Minimum bitWidth is 1 even if dictionary has only one entry (to match spec requirement)
        if (bitWidth == 0 && DictionarySize > 0)
            bitWidth = 1;

        var rle = new RleBitPackedEncoder(bitWidth);
        foreach (int idx in _indices)
            rle.WriteValue(idx);

        var rleBytes = rle.Finish();
        return rleBytes;
    }

    /// <summary>
    /// Returns the bit width needed for dictionary indices.
    /// </summary>
    public int GetIndexBitWidth()
    {
        if (DictionarySize <= 1) return DictionarySize == 0 ? 0 : 1;
        return 32 - BitOperations.LeadingZeroCount((uint)(DictionarySize - 1));
    }

    // --- ByteArray key for dictionary lookups ---

    private readonly struct ByteArrayKey
    {
        public readonly byte[] Data;
        public ByteArrayKey(byte[] data) => Data = data;
    }

    private sealed class ByteArrayKeyComparer : IEqualityComparer<ByteArrayKey>
    {
        public static readonly ByteArrayKeyComparer Instance = new();

        public bool Equals(ByteArrayKey x, ByteArrayKey y) =>
            x.Data.AsSpan().SequenceEqual(y.Data);

        public int GetHashCode(ByteArrayKey obj)
        {
            var data = obj.Data;
            uint hash = 2166136261;
            for (int i = 0; i < data.Length; i++)
                hash = (hash ^ data[i]) * 16777619;
            return (int)hash;
        }
    }
}
