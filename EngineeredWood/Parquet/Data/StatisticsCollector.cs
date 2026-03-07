using System.Buffers.Binary;
using System.Runtime.InteropServices;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Computes column statistics (min, max, null_count) from decomposed Arrow array data.
/// Values are encoded as raw bytes in the physical type's native encoding (little-endian).
/// For byte arrays exceeding the truncation limit, min/max are truncated and marked non-exact.
/// </summary>
internal static class StatisticsCollector
{
    private const int DefaultMaxBinaryStatLength = 64;

    public static Statistics? Collect(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        PhysicalType physicalType,
        int totalValues,
        int maxBinaryStatLength = DefaultMaxBinaryStatLength)
    {
        long nullCount = totalValues - decomposed.NonNullCount;

        if (decomposed.NonNullCount == 0)
        {
            return new Statistics
            {
                NullCount = nullCount,
                IsMinValueExact = true,
                IsMaxValueExact = true,
            };
        }

        return physicalType switch
        {
            PhysicalType.Boolean => CollectBoolean(decomposed.BoolValues!, decomposed.NonNullCount, nullCount),
            PhysicalType.Int32 => CollectFixedWidth<int>(decomposed.ValueBytes!, decomposed.NonNullCount, nullCount, CompareInt32),
            PhysicalType.Int64 => CollectFixedWidth<long>(decomposed.ValueBytes!, decomposed.NonNullCount, nullCount, CompareInt64),
            PhysicalType.Int96 => CollectFixedWidthBytes(decomposed.ValueBytes!, 12, decomposed.NonNullCount, nullCount),
            PhysicalType.Float => CollectFixedWidth<float>(decomposed.ValueBytes!, decomposed.NonNullCount, nullCount, CompareFloat),
            PhysicalType.Double => CollectFixedWidth<double>(decomposed.ValueBytes!, decomposed.NonNullCount, nullCount, CompareDouble),
            PhysicalType.ByteArray => CollectByteArray(decomposed.ByteArrayData!, decomposed.ByteArrayOffsets!, decomposed.NonNullCount, nullCount, maxBinaryStatLength),
            PhysicalType.FixedLenByteArray => CollectFixedWidthBytes(decomposed.ValueBytes!, decomposed.ValueBytes!.Length / decomposed.NonNullCount, decomposed.NonNullCount, nullCount),
            _ => null,
        };
    }

    private static Statistics CollectBoolean(bool[] values, int count, long nullCount)
    {
        bool min = true, max = false;
        for (int i = 0; i < count; i++)
        {
            if (values[i]) max = true;
            else min = false;
        }

        return new Statistics
        {
            MinValue = [(byte)(min ? 1 : 0)],
            MaxValue = [(byte)(max ? 1 : 0)],
            NullCount = nullCount,
            IsMinValueExact = true,
            IsMaxValueExact = true,
        };
    }

    private delegate int Comparer<T>(T a, T b);

    private static Statistics CollectFixedWidth<T>(
        byte[] valueBytes, int count, long nullCount, Comparer<T> compare)
        where T : struct
    {
        var values = MemoryMarshal.Cast<byte, T>(valueBytes.AsSpan(0, count * Marshal.SizeOf<T>()));
        T min = values[0], max = values[0];

        for (int i = 1; i < values.Length; i++)
        {
            if (compare(values[i], min) < 0) min = values[i];
            if (compare(values[i], max) > 0) max = values[i];
        }

        int size = Marshal.SizeOf<T>();
        var minBytes = new byte[size];
        var maxBytes = new byte[size];
        MemoryMarshal.Write(minBytes, in min);
        MemoryMarshal.Write(maxBytes, in max);

        return new Statistics
        {
            MinValue = minBytes,
            MaxValue = maxBytes,
            NullCount = nullCount,
            IsMinValueExact = true,
            IsMaxValueExact = true,
        };
    }

    private static Statistics CollectFixedWidthBytes(
        byte[] valueBytes, int typeLength, int count, long nullCount)
    {
        var minBytes = valueBytes.AsSpan(0, typeLength).ToArray();
        var maxBytes = valueBytes.AsSpan(0, typeLength).ToArray();

        for (int i = 1; i < count; i++)
        {
            var current = valueBytes.AsSpan(i * typeLength, typeLength);
            if (current.SequenceCompareTo(minBytes) < 0)
                current.CopyTo(minBytes);
            if (current.SequenceCompareTo(maxBytes) > 0)
                current.CopyTo(maxBytes);
        }

        return new Statistics
        {
            MinValue = minBytes,
            MaxValue = maxBytes,
            NullCount = nullCount,
            IsMinValueExact = true,
            IsMaxValueExact = true,
        };
    }

    private static Statistics CollectByteArray(
        byte[] data, int[] offsets, int count, long nullCount, int truncateLength)
    {
        int GetLength(int i) => (i + 1 < offsets.Length ? offsets[i + 1] : data.Length) - offsets[i];

        var minStart = offsets[0];
        var minLen = GetLength(0);
        var maxStart = offsets[0];
        var maxLen = GetLength(0);

        for (int i = 1; i < count; i++)
        {
            var start = offsets[i];
            var len = GetLength(i);
            var current = data.AsSpan(start, len);

            if (current.SequenceCompareTo(data.AsSpan(minStart, minLen)) < 0)
            {
                minStart = start;
                minLen = len;
            }
            if (current.SequenceCompareTo(data.AsSpan(maxStart, maxLen)) > 0)
            {
                maxStart = start;
                maxLen = len;
            }
        }

        bool minExact = true, maxExact = true;
        byte[] minBytes, maxBytes;

        if (minLen <= truncateLength)
        {
            minBytes = data.AsSpan(minStart, minLen).ToArray();
        }
        else
        {
            minBytes = TruncateMin(data.AsSpan(minStart, minLen), truncateLength);
            minExact = false;
        }

        if (maxLen <= truncateLength)
        {
            maxBytes = data.AsSpan(maxStart, maxLen).ToArray();
        }
        else
        {
            maxBytes = TruncateMax(data.AsSpan(maxStart, maxLen), truncateLength);
            maxExact = maxBytes.Length == maxLen; // true if increment failed
        }

        return new Statistics
        {
            MinValue = minBytes,
            MaxValue = maxBytes,
            NullCount = nullCount,
            IsMinValueExact = minExact,
            IsMaxValueExact = maxExact,
        };
    }

    /// <summary>Truncate min value: just take the prefix (it's guaranteed &lt;= original).</summary>
    private static byte[] TruncateMin(ReadOnlySpan<byte> value, int maxLen)
    {
        return value[..Math.Min(value.Length, maxLen)].ToArray();
    }

    /// <summary>Truncate max value: take prefix and try to increment last byte.</summary>
    private static byte[] TruncateMax(ReadOnlySpan<byte> value, int maxLen)
    {
        int len = Math.Min(value.Length, maxLen);
        var result = value[..len].ToArray();

        // Try to increment the last byte to maintain the upper-bound guarantee
        for (int i = result.Length - 1; i >= 0; i--)
        {
            if (result[i] < 0xFF)
            {
                result[i]++;
                return result;
            }
        }

        // All 0xFF — can't increment, return full value
        return value.ToArray();
    }

    private static int CompareInt32(int a, int b) => a.CompareTo(b);
    private static int CompareInt64(long a, long b) => a.CompareTo(b);

    private static int CompareFloat(float a, float b)
    {
        // Handle NaN: NaN should not affect min/max
        if (float.IsNaN(a)) return float.IsNaN(b) ? 0 : 1;
        if (float.IsNaN(b)) return -1;
        return a.CompareTo(b);
    }

    private static int CompareDouble(double a, double b)
    {
        if (double.IsNaN(a)) return double.IsNaN(b) ? 0 : 1;
        if (double.IsNaN(b)) return -1;
        return a.CompareTo(b);
    }

    /// <summary>
    /// Collects ByteArray statistics from dictionary entries rather than full decomposed data.
    /// Since min/max of all values equals min/max of unique dictionary values, this avoids
    /// requiring the full byte array data to be materialized.
    /// </summary>
    internal static Statistics? CollectByteArrayFromEntries(
        byte[] buffer, (int offset, int length)[] entries, int entryCount,
        long nullCount, int maxBinaryStatLength = DefaultMaxBinaryStatLength)
    {
        if (entryCount == 0)
        {
            return new Statistics
            {
                NullCount = nullCount,
                IsMinValueExact = true,
                IsMaxValueExact = true,
            };
        }

        var (minOff, minLen) = entries[0];
        var (maxOff, maxLen) = entries[0];

        for (int i = 1; i < entryCount; i++)
        {
            var (off, len) = entries[i];
            var current = buffer.AsSpan(off, len);

            if (current.SequenceCompareTo(buffer.AsSpan(minOff, minLen)) < 0)
                (minOff, minLen) = (off, len);
            if (current.SequenceCompareTo(buffer.AsSpan(maxOff, maxLen)) > 0)
                (maxOff, maxLen) = (off, len);
        }

        bool minExact = true, maxExact = true;
        byte[] minBytes, maxBytes;

        if (minLen <= maxBinaryStatLength)
            minBytes = buffer.AsSpan(minOff, minLen).ToArray();
        else
        {
            minBytes = TruncateMin(buffer.AsSpan(minOff, minLen), maxBinaryStatLength);
            minExact = false;
        }

        if (maxLen <= maxBinaryStatLength)
            maxBytes = buffer.AsSpan(maxOff, maxLen).ToArray();
        else
        {
            maxBytes = TruncateMax(buffer.AsSpan(maxOff, maxLen), maxBinaryStatLength);
            maxExact = maxBytes.Length == maxLen;
        }

        return new Statistics
        {
            MinValue = minBytes,
            MaxValue = maxBytes,
            NullCount = nullCount,
            IsMinValueExact = minExact,
            IsMaxValueExact = maxExact,
        };
    }
}
