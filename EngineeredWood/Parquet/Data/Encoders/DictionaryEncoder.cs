using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data.Encoders;

/// <summary>
/// Encodes values using dictionary encoding (PLAIN_DICTIONARY / RLE_DICTIONARY).
/// Produces a PLAIN-encoded dictionary page and RLE-encoded index pages.
/// Uses flat buffers and a custom open-addressing hash map for variable-width types
/// to enable zero-copy lookups against source data (e.g. Arrow buffers).
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

    // Variable-width: open-addressing hash map for zero-copy span lookups.
    // Unique values stored in _varBuf; hash table entries reference _varBuf slices.
    private byte[] _varBuf = new byte[256];
    private int _varBufLen;
    private int _varCount;
    private VarEntry[] _varTable = new VarEntry[16];
    private int _varTableMask = 15; // _varTable.Length - 1

    // Indices (one per value added)
    private int[] _indices = new int[256];
    private int _indexCount;

    /// <summary>Number of distinct values in the dictionary.</summary>
    public int DictionarySize => _physicalType is PhysicalType.ByteArray or PhysicalType.FixedLenByteArray
        ? _varCount : _fixedCount;

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

        bool isVarWidth = physicalType is PhysicalType.ByteArray or PhysicalType.FixedLenByteArray;

        if (estimatedValues > 0)
        {
            _indices = new int[estimatedValues];
            int estDistinct = Math.Min(estimatedValues, 16384);

            if (isVarWidth)
            {
                // Pre-size hash table for variable-width types only
                int tableSize = 16;
                while (tableSize < estDistinct * 2) tableSize <<= 1;
                _varTable = new VarEntry[tableSize];
                _varTableMask = tableSize - 1;
            }
            else
            {
                // Pre-size Dictionary for fixed-width types only
                _fixedWidthDict = new(estDistinct);
                if (_entryByteWidth > 0)
                    _fixedBuf = new byte[estDistinct * _entryByteWidth];
            }
        }

        // Initialize hash table sentinel values
        _varTable.AsSpan().Fill(VarEntry.Empty);
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

    /// <summary>
    /// Adds a variable-length byte array value. Hashes and compares directly from the source
    /// span — only copies to the internal buffer for genuinely new (unique) values.
    /// </summary>
    public int AddByteArray(ReadOnlySpan<byte> value)
    {
        uint hash = FnvHash(value);
        int slot = (int)(hash & (uint)_varTableMask);

        while (true)
        {
            ref VarEntry entry = ref _varTable[slot];

            if (entry.DictIndex < 0)
            {
                // Empty slot — new unique value. Copy to _varBuf and insert.
                int bufOffset = _varBufLen;
                EnsureVarCapacity(value.Length);
                value.CopyTo(_varBuf.AsSpan(bufOffset));
                _varBufLen += value.Length;

                entry.BufOffset = bufOffset;
                entry.Length = value.Length;
                entry.Hash = hash;
                entry.DictIndex = _varCount++;
                DictionaryByteSize += 4 + value.Length;
                GrowVarTableIfNeeded();

                AppendIndex(entry.DictIndex);
                return entry.DictIndex;
            }

            // Occupied slot — check if it matches
            if (entry.Hash == hash && entry.Length == value.Length &&
                value.SequenceEqual(_varBuf.AsSpan(entry.BufOffset, entry.Length)))
            {
                // Duplicate — no copy needed
                AppendIndex(entry.DictIndex);
                return entry.DictIndex;
            }

            // Collision — linear probe
            slot = (slot + 1) & _varTableMask;
        }
    }

    /// <summary>
    /// Adds a fixed-length byte array value. Same zero-copy lookup as AddByteArray.
    /// </summary>
    public int AddFixedLenByteArray(ReadOnlySpan<byte> value)
    {
        uint hash = FnvHash(value);
        int slot = (int)(hash & (uint)_varTableMask);

        while (true)
        {
            ref VarEntry entry = ref _varTable[slot];

            if (entry.DictIndex < 0)
            {
                int bufOffset = _varBufLen;
                EnsureVarCapacity(value.Length);
                value.CopyTo(_varBuf.AsSpan(bufOffset));
                _varBufLen += value.Length;

                entry.BufOffset = bufOffset;
                entry.Length = value.Length;
                entry.Hash = hash;
                entry.DictIndex = _varCount++;
                DictionaryByteSize += value.Length;
                GrowVarTableIfNeeded();

                AppendIndex(entry.DictIndex);
                return entry.DictIndex;
            }

            if (entry.Hash == hash && entry.Length == value.Length &&
                value.SequenceEqual(_varBuf.AsSpan(entry.BufOffset, entry.Length)))
            {
                AppendIndex(entry.DictIndex);
                return entry.DictIndex;
            }

            slot = (slot + 1) & _varTableMask;
        }
    }

    /// <summary>Produces the PLAIN-encoded dictionary page bytes.</summary>
    public byte[] EncodeDictionaryPage()
    {
        if (_physicalType == PhysicalType.ByteArray)
        {
            // Length-prefixed format: collect entries in dict-index order
            var result = new byte[DictionaryByteSize];
            int pos = 0;
            var entries = GetSortedVarEntries();
            for (int i = 0; i < entries.Length; i++)
            {
                var e = entries[i];
                BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), e.Length);
                pos += 4;
                _varBuf.AsSpan(e.BufOffset, e.Length).CopyTo(result.AsSpan(pos));
                pos += e.Length;
            }
            return result;
        }

        if (_physicalType == PhysicalType.FixedLenByteArray)
        {
            var entries = GetSortedVarEntries();
            var result = new byte[_varBufLen];
            int pos = 0;
            for (int i = 0; i < entries.Length; i++)
            {
                var e = entries[i];
                _varBuf.AsSpan(e.BufOffset, e.Length).CopyTo(result.AsSpan(pos));
                pos += e.Length;
            }
            return result;
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

    /// <summary>
    /// Returns the internal buffer and sorted entry descriptors for variable-width entries.
    /// Used by StatisticsCollector to compute min/max from dictionary entries without
    /// requiring a full decomposition of the source data.
    /// </summary>
    public (byte[] buffer, int count, (int offset, int length)[] entries) GetVarEntries()
    {
        var sorted = GetSortedVarEntries();
        var descriptors = new (int offset, int length)[sorted.Length];
        for (int i = 0; i < sorted.Length; i++)
            descriptors[i] = (sorted[i].BufOffset, sorted[i].Length);
        return (_varBuf, _varCount, descriptors);
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

    /// <summary>Grows the hash table when load factor exceeds 0.7.</summary>
    private void GrowVarTableIfNeeded()
    {
        // Load factor check: _varCount / _varTable.Length > 0.7
        // Equivalent to: _varCount * 10 > _varTable.Length * 7
        if (_varCount * 10 <= _varTable.Length * 7)
            return;

        int newSize = _varTable.Length << 1;
        int newMask = newSize - 1;
        var newTable = new VarEntry[newSize];
        newTable.AsSpan().Fill(VarEntry.Empty);

        // Re-insert all entries
        for (int i = 0; i < _varTable.Length; i++)
        {
            ref var old = ref _varTable[i];
            if (old.DictIndex < 0) continue;

            int slot = (int)(old.Hash & (uint)newMask);
            while (newTable[slot].DictIndex >= 0)
                slot = (slot + 1) & newMask;
            newTable[slot] = old;
        }

        _varTable = newTable;
        _varTableMask = newMask;
    }

    /// <summary>Returns var entries sorted by DictIndex for correct dictionary page order.</summary>
    private VarEntry[] GetSortedVarEntries()
    {
        var entries = new VarEntry[_varCount];
        for (int i = 0; i < _varTable.Length; i++)
        {
            ref var e = ref _varTable[i];
            if (e.DictIndex >= 0)
                entries[e.DictIndex] = e;
        }
        return entries;
    }

    private static uint FnvHash(ReadOnlySpan<byte> data)
    {
        uint hash = 2166136261;
        for (int i = 0; i < data.Length; i++)
            hash = (hash ^ data[i]) * 16777619;
        return hash;
    }

    // Open-addressing hash table entry for variable-width values
    private struct VarEntry
    {
        public int BufOffset;   // offset in _varBuf
        public int Length;      // byte length of value
        public uint Hash;       // cached FNV hash
        public int DictIndex;   // dictionary index, or -1 if empty

        public static readonly VarEntry Empty = new() { DictIndex = -1 };
    }
}
