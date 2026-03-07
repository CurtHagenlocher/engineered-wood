using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Computes column-level statistics (min, max, null_count) from Arrow arrays.
/// </summary>
internal static class StatisticsCollector
{
    /// <summary>
    /// Computes statistics for a column chunk.
    /// </summary>
    public static Statistics Compute(
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength,
        int[]? defLevels,
        int nonNullCount,
        int rowCount)
    {
        long nullCount = rowCount - nonNullCount;

        if (nonNullCount == 0)
            return new Statistics { NullCount = nullCount };

        var (minBytes, maxBytes) = physicalType switch
        {
            PhysicalType.Boolean => ComputeBooleanMinMax(array, defLevels),
            PhysicalType.Int32 => ComputeMinMax<int>(array, defLevels, CompareInt32),
            PhysicalType.Int64 => ComputeMinMax<long>(array, defLevels, CompareInt64),
            PhysicalType.Float => ComputeFloatMinMax(array, defLevels),
            PhysicalType.Double => ComputeDoubleMinMax(array, defLevels),
            PhysicalType.ByteArray => ComputeByteArrayMinMax(array, defLevels),
            PhysicalType.FixedLenByteArray => ComputeFlbaMinMax(array, defLevels, typeLength),
            _ => (null, null),
        };

        return new Statistics
        {
            NullCount = nullCount,
            Min = minBytes,
            Max = maxBytes,
            MinValue = minBytes,
            MaxValue = maxBytes,
            IsMinValueExact = minBytes != null,
            IsMaxValueExact = maxBytes != null,
        };
    }

    private static (byte[]?, byte[]?) ComputeBooleanMinMax(IArrowArray array, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        bool hasTrue = false, hasFalse = false;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            if (boolArray.GetValue(i) == true) hasTrue = true;
            else hasFalse = true;
            if (hasTrue && hasFalse) break;
        }

        byte minByte = hasFalse ? (byte)0 : (byte)1;
        byte maxByte = hasTrue ? (byte)1 : (byte)0;
        return ([minByte], [maxByte]);
    }

    private static (byte[]?, byte[]?) ComputeMinMax<T>(
        IArrowArray array, int[]? defLevels, Comparison<T> compare)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        bool first = true;
        T min = default, max = default;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            T val = valueBuffer[i];
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
                if (compare(val, min) < 0) min = val;
                if (compare(val, max) > 0) max = val;
            }
        }

        if (first) return (null, null);

        int size = Marshal.SizeOf<T>();
        var minBytes = new byte[size];
        var maxBytes = new byte[size];
        MemoryMarshal.Write(minBytes, in min);
        MemoryMarshal.Write(maxBytes, in max);
        return (minBytes, maxBytes);
    }

    private static (byte[]?, byte[]?) ComputeFloatMinMax(IArrowArray array, int[]? defLevels)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
        bool first = true;
        float min = 0, max = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            float val = valueBuffer[i];
            if (float.IsNaN(val)) continue; // NaN excluded from stats
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
                if (val < min || (val == 0f && min == 0f && float.IsNegative(val)))
                    min = val;
                if (val > max || (val == 0f && max == 0f && !float.IsNegative(val)))
                    max = val;
            }
        }

        if (first) return (null, null); // all NaN

        var minBytes = new byte[4];
        var maxBytes = new byte[4];
        BinaryPrimitives.WriteSingleLittleEndian(minBytes, min);
        BinaryPrimitives.WriteSingleLittleEndian(maxBytes, max);
        return (minBytes, maxBytes);
    }

    private static (byte[]?, byte[]?) ComputeDoubleMinMax(IArrowArray array, int[]? defLevels)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
        bool first = true;
        double min = 0, max = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            double val = valueBuffer[i];
            if (double.IsNaN(val)) continue;
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
                if (val < min || (val == 0.0 && min == 0.0 && double.IsNegative(val)))
                    min = val;
                if (val > max || (val == 0.0 && max == 0.0 && !double.IsNegative(val)))
                    max = val;
            }
        }

        if (first) return (null, null);

        var minBytes = new byte[8];
        var maxBytes = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(minBytes, min);
        BinaryPrimitives.WriteDoubleLittleEndian(maxBytes, max);
        return (minBytes, maxBytes);
    }

    private static (byte[]?, byte[]?) ComputeByteArrayMinMax(IArrowArray array, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> rawData = data.Buffers[2].Span;

        int minIdx = -1, maxIdx = -1;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
            }
            else
            {
                var val = rawData.Slice(offsets[i], offsets[i + 1] - offsets[i]);
                if (val.SequenceCompareTo(rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx])) < 0)
                    minIdx = i;
                if (val.SequenceCompareTo(rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx])) > 0)
                    maxIdx = i;
            }
        }

        if (minIdx < 0) return (null, null);

        return (
            rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx]).ToArray(),
            rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx]).ToArray());
    }

    private static (byte[]?, byte[]?) ComputeFlbaMinMax(
        IArrowArray array, int[]? defLevels, int typeLength)
    {
        var valueBuffer = array.Data.Buffers[1].Span;
        int minIdx = -1, maxIdx = -1;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
            }
            else
            {
                var val = valueBuffer.Slice(i * typeLength, typeLength);
                if (val.SequenceCompareTo(valueBuffer.Slice(minIdx * typeLength, typeLength)) < 0)
                    minIdx = i;
                if (val.SequenceCompareTo(valueBuffer.Slice(maxIdx * typeLength, typeLength)) > 0)
                    maxIdx = i;
            }
        }

        if (minIdx < 0) return (null, null);

        return (
            valueBuffer.Slice(minIdx * typeLength, typeLength).ToArray(),
            valueBuffer.Slice(maxIdx * typeLength, typeLength).ToArray());
    }

    private static int CompareInt32(int a, int b) => a.CompareTo(b);
    private static int CompareInt64(long a, long b) => a.CompareTo(b);
}
