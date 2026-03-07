using System.Buffers.Binary;
using System.Numerics;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes values using dictionary encoding (PLAIN_DICTIONARY / RLE_DICTIONARY).
/// Produces a PLAIN-encoded dictionary page and RLE-encoded index pages.
/// Uses flat buffers to minimize per-entry heap allocations.
/// </summary>
internal sealed class DictionaryEncoder
{
    private readonly PhysicalType _physicalType;
    private readonly int _typeLength;
    private readonly int _entryByteWidth;

    // Fixed-width: value bits → dict index, entries stored in flat buffer
    private readonly Dictionary<long, int> _fixedWidthDict = new();
    private byte[] _fixedBuf = new byte[256];
    private int _fixedCount;

    // Variable-width: entries stored in flat buffer + offsets, lookup via hash
    private byte[] _varBuf = new byte[256];
    private int _varBufLen;
    private readonly List<int> _varOffsets = new();
    private readonly Dictionary<VarKey, int> _varDict;

    // Indices (one per value added)
    private int[] _indices = new int[256];
    private int _indexCount;

    /// <summary>Number of distinct values in the dictionary.</summary>
    public int DictionarySize => _physicalType is PhysicalType.ByteArray or PhysicalType.FixedLenByteArray
        ? _varOffsets.Count : _fixedCount;

    /// <summary>Total byte size of the dictionary entries.</summary>
    public int DictionaryByteSize { get; private set; }

    /// <summary>Number of values encoded so far.</summary>
    public int ValueCount => _indexCount;

    public DictionaryEncoder(PhysicalType physicalType, int typeLength = 0, int estimatedValues = 0)
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
            PhysicalType.ByteArray => 0,
            _ => throw new ArgumentOutOfRangeException(nameof(physicalType)),
        };

        if (estimatedValues > 0)
        {
            _indices = new int[estimatedValues];
            int estDistinct = Math.Min(estimatedValues, 16384);
            _fixedWidthDict = new(estDistinct);
            if (_entryByteWidth > 0)
                _fixedBuf = new byte[estDistinct * _entryByteWidth];
        }
        _varDict = new Dictionary<VarKey, int>(new VarKeyComparer(this));
    }

    public int AddInt32(int value)
    {
        long key = value;
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _fixedCount++;
            _fixedWidthDict[key] = index;
            EnsureFixedCapacity(index);
            BinaryPrimitives.WriteInt32LittleEndian(
                _fixedBuf.AsSpan(index * 4, 4), value);
            DictionaryByteSize += 4;
        }
        AppendIndex(index);
        return index;
    }

    public int AddInt64(long value)
    {
        if (!_fixedWidthDict.TryGetValue(value, out int index))
        {
            index = _fixedCount++;
            _fixedWidthDict[value] = index;
            EnsureFixedCapacity(index);
            BinaryPrimitives.WriteInt64LittleEndian(
                _fixedBuf.AsSpan(index * 8, 8), value);
            DictionaryByteSize += 8;
        }
        AppendIndex(index);
        return index;
    }

    public int AddFloat(float value)
    {
        long key = BitConverter.SingleToInt32Bits(value);
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _fixedCount++;
            _fixedWidthDict[key] = index;
            EnsureFixedCapacity(index);
            BinaryPrimitives.WriteSingleLittleEndian(
                _fixedBuf.AsSpan(index * 4, 4), value);
            DictionaryByteSize += 4;
        }
        AppendIndex(index);
        return index;
    }

    public int AddDouble(double value)
    {
        long key = BitConverter.DoubleToInt64Bits(value);
        if (!_fixedWidthDict.TryGetValue(key, out int index))
        {
            index = _fixedCount++;
            _fixedWidthDict[key] = index;
            EnsureFixedCapacity(index);
            BinaryPrimitives.WriteDoubleLittleEndian(
                _fixedBuf.AsSpan(index * 8, 8), value);
            DictionaryByteSize += 8;
        }
        AppendIndex(index);
        return index;
    }

    public int AddByteArray(ReadOnlySpan<byte> value)
    {
        // Write to buffer temporarily for hash/compare, keep if new
        int tempOffset = _varBufLen;
        EnsureVarCapacity(value.Length);
        value.CopyTo(_varBuf.AsSpan(tempOffset));

        var lookupKey = new VarKey(tempOffset, value.Length);
        if (_varDict.TryGetValue(lookupKey, out int index))
        {
            // Duplicate — don't advance buffer
            AppendIndex(index);
            return index;
        }

        // New entry — keep the data in the buffer
        _varBufLen += value.Length;
        index = _varOffsets.Count;
        _varOffsets.Add(tempOffset);
        _varDict[lookupKey] = index;
        DictionaryByteSize += 4 + value.Length;
        AppendIndex(index);
        return index;
    }

    public int AddFixedLenByteArray(ReadOnlySpan<byte> value)
    {
        int tempOffset = _varBufLen;
        EnsureVarCapacity(value.Length);
        value.CopyTo(_varBuf.AsSpan(tempOffset));

        var lookupKey = new VarKey(tempOffset, value.Length);
        if (_varDict.TryGetValue(lookupKey, out int index))
        {
            AppendIndex(index);
            return index;
        }

        _varBufLen += value.Length;
        index = _varOffsets.Count;
        _varOffsets.Add(tempOffset);
        _varDict[lookupKey] = index;
        DictionaryByteSize += value.Length;
        AppendIndex(index);
        return index;
    }

    /// <summary>Produces the PLAIN-encoded dictionary page bytes.</summary>
    public byte[] EncodeDictionaryPage()
    {
        if (_physicalType == PhysicalType.ByteArray)
        {
            // Length-prefixed format
            var result = new byte[DictionaryByteSize];
            int pos = 0;
            for (int i = 0; i < _varOffsets.Count; i++)
            {
                int start = _varOffsets[i];
                int len = (i + 1 < _varOffsets.Count ? _varOffsets[i + 1] : _varBufLen) - start;
                BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), len);
                pos += 4;
                _varBuf.AsSpan(start, len).CopyTo(result.AsSpan(pos));
                pos += len;
            }
            return result;
        }

        if (_physicalType == PhysicalType.FixedLenByteArray)
        {
            return _varBuf.AsSpan(0, _varBufLen).ToArray();
        }

        // Fixed-width: slice the flat buffer
        return _fixedBuf.AsSpan(0, _fixedCount * _entryByteWidth).ToArray();
    }

    /// <summary>Produces the RLE-encoded index page bytes.</summary>
    public byte[] EncodeIndices()
    {
        int bitWidth = DictionarySize <= 1 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)(DictionarySize - 1));
        if (bitWidth == 0 && DictionarySize > 0)
            bitWidth = 1;

        return RleBitPackedEncoder.Encode(_indices.AsSpan(0, _indexCount), bitWidth);
    }

    /// <summary>Returns the bit width needed for dictionary indices.</summary>
    public int GetIndexBitWidth()
    {
        if (DictionarySize <= 1) return DictionarySize == 0 ? 0 : 1;
        return 32 - BitOperations.LeadingZeroCount((uint)(DictionarySize - 1));
    }

    private void AppendIndex(int index)
    {
        if (_indexCount == _indices.Length)
            Array.Resize(ref _indices, _indices.Length * 2);
        _indices[_indexCount++] = index;
    }

    private void EnsureFixedCapacity(int entryIndex)
    {
        int needed = (entryIndex + 1) * _entryByteWidth;
        if (needed > _fixedBuf.Length)
        {
            int newLen = _fixedBuf.Length;
            while (newLen < needed) newLen *= 2;
            Array.Resize(ref _fixedBuf, newLen);
        }
    }

    private void EnsureVarCapacity(int additionalBytes)
    {
        int needed = _varBufLen + additionalBytes;
        if (needed > _varBuf.Length)
        {
            int newLen = _varBuf.Length;
            while (newLen < needed) newLen *= 2;
            Array.Resize(ref _varBuf, newLen);
        }
    }

    // Key that references a slice of the shared _varBuf (no per-entry byte[] allocation)
    private readonly struct VarKey : IEquatable<VarKey>
    {
        public readonly int Offset;
        public readonly int Length;

        public VarKey(int offset, int length)
        {
            Offset = offset;
            Length = length;
        }

        public bool Equals(VarKey other) => Offset == other.Offset && Length == other.Length;
        public override bool Equals(object? obj) => obj is VarKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Offset, Length);
    }

    // Compares VarKeys by their actual byte content in the shared buffer
    private sealed class VarKeyComparer : IEqualityComparer<VarKey>
    {
        private readonly DictionaryEncoder _encoder;
        public VarKeyComparer(DictionaryEncoder encoder) => _encoder = encoder;

        public bool Equals(VarKey x, VarKey y) =>
            _encoder._varBuf.AsSpan(x.Offset, x.Length)
                .SequenceEqual(_encoder._varBuf.AsSpan(y.Offset, y.Length));

        public int GetHashCode(VarKey key)
        {
            var data = _encoder._varBuf.AsSpan(key.Offset, key.Length);
            uint hash = 2166136261;
            for (int i = 0; i < data.Length; i++)
                hash = (hash ^ data[i]) * 16777619;
            return (int)hash;
        }
    }
}
