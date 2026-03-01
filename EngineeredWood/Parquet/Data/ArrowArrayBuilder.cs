using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Builds Apache Arrow arrays from decoded Parquet column data, inserting nulls
/// based on definition levels.
/// </summary>
internal static class ArrowArrayBuilder
{
    /// <summary>
    /// Creates an Arrow <see cref="IArrowArray"/> from accumulated column data.
    /// </summary>
    public static IArrowArray Build(ColumnBuildState state, Field field, int rowCount)
    {
        var arrowType = field.DataType;

        return arrowType switch
        {
            BooleanType => BuildBooleanArray(state, field, rowCount),
            Int8Type => BuildInt8Array(state, field, rowCount),
            UInt8Type => BuildUInt8Array(state, field, rowCount),
            Int16Type => BuildInt16Array(state, field, rowCount),
            UInt16Type => BuildUInt16Array(state, field, rowCount),
            Int32Type or Date32Type or Time32Type => BuildInt32Array(state, field, rowCount),
            UInt32Type => BuildUInt32Array(state, field, rowCount),
            Int64Type or TimestampType or Time64Type => BuildInt64Array(state, field, rowCount),
            UInt64Type => BuildUInt64Array(state, field, rowCount),
            FloatType => BuildFloatArray(state, field, rowCount),
            DoubleType => BuildDoubleArray(state, field, rowCount),
            StringType => BuildStringArray(state, field, rowCount),
            BinaryType => BuildBinaryArray(state, field, rowCount),
            FixedSizeBinaryType fsb => BuildFixedSizeBinaryArray(state, field, rowCount, fsb.ByteWidth),
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for array building."),
        };
    }

    private static IArrowArray BuildBooleanArray(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new BooleanArray.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(state.BoolValues![valueIdx++]);
        }
        return builder.Build();
    }

    private static IArrowArray BuildInt8Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new Int8Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((sbyte)state.Int32Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildUInt8Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new UInt8Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((byte)state.Int32Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildInt16Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new Int16Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((short)state.Int32Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildUInt16Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new UInt16Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((ushort)state.Int32Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildInt32Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new Int32Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(state.Int32Values![valueIdx++]);
        }
        return builder.Build();
    }

    private static IArrowArray BuildUInt32Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new UInt32Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((uint)state.Int32Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildInt64Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new Int64Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(state.Int64Values![valueIdx++]);
        }
        return builder.Build();
    }

    private static IArrowArray BuildUInt64Array(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new UInt64Array.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(checked((ulong)state.Int64Values![valueIdx++]));
        }
        return builder.Build();
    }

    private static IArrowArray BuildFloatArray(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new FloatArray.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(state.FloatValues![valueIdx++]);
        }
        return builder.Build();
    }

    private static IArrowArray BuildDoubleArray(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new DoubleArray.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(state.DoubleValues![valueIdx++]);
        }
        return builder.Build();
    }

    private static IArrowArray BuildStringArray(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new StringArray.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
            {
                var bytes = state.GetByteArrayValue(valueIdx++);
                builder.Append(System.Text.Encoding.UTF8.GetString(bytes));
            }
        }
        return builder.Build();
    }

    private static IArrowArray BuildBinaryArray(ColumnBuildState state, Field field, int rowCount)
    {
        var builder = new BinaryArray.Builder();
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
                builder.AppendNull();
            else
            {
                var bytes = state.GetByteArrayValue(valueIdx++);
                builder.Append(bytes);
            }
        }
        return builder.Build();
    }

    private static IArrowArray BuildFixedSizeBinaryArray(
        ColumnBuildState state, Field field, int rowCount, int byteWidth)
    {
        // FixedSizeBinaryArray has no Builder in Apache.Arrow 18.x, so we build ArrayData directly.
        var fixedType = new FixedSizeBinaryType(byteWidth);
        var valueData = new byte[rowCount * byteWidth];
        var nullBitmap = new byte[(rowCount + 7) / 8];
        int nullCount = 0;
        int valueIdx = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (state.IsNull(i))
            {
                nullCount++;
                // Leave the value bytes as zero, clear the null bit (already 0)
            }
            else
            {
                // Set the validity bit
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
                state.FixedByteValues.AsSpan(valueIdx * byteWidth, byteWidth)
                    .CopyTo(valueData.AsSpan(i * byteWidth, byteWidth));
                valueIdx++;
            }
        }

        var arrayData = new ArrayData(
            fixedType,
            rowCount,
            nullCount,
            offset: 0,
            new[] { new ArrowBuffer(nullBitmap), new ArrowBuffer(valueData) });

        return new FixedSizeBinaryArray(arrayData);
    }
}

/// <summary>
/// Accumulates decoded values and null flags across multiple data pages for one column.
/// </summary>
internal sealed class ColumnBuildState
{
    private readonly int _maxDefLevel;
    private List<int>? _defLevels;

    // Typed value lists — only one set is populated based on physical type
    public List<bool>? BoolValues { get; private set; }
    public List<int>? Int32Values { get; private set; }
    public List<long>? Int64Values { get; private set; }
    public List<float>? FloatValues { get; private set; }
    public List<double>? DoubleValues { get; private set; }
    public byte[]? FixedByteValues => _fixedByteStream?.ToArray();
    private MemoryStream? _fixedByteStream;

    // BYTE_ARRAY values stored as list of byte[]
    private List<byte[]>? _byteArrayValues;

    public ColumnBuildState(PhysicalType physicalType, int maxDefLevel, int estimatedRows)
    {
        _maxDefLevel = maxDefLevel;
        if (maxDefLevel > 0)
            _defLevels = new List<int>(estimatedRows);

        switch (physicalType)
        {
            case PhysicalType.Boolean:
                BoolValues = new List<bool>(estimatedRows);
                break;
            case PhysicalType.Int32:
                Int32Values = new List<int>(estimatedRows);
                break;
            case PhysicalType.Int64:
                Int64Values = new List<long>(estimatedRows);
                break;
            case PhysicalType.Float:
                FloatValues = new List<float>(estimatedRows);
                break;
            case PhysicalType.Double:
                DoubleValues = new List<double>(estimatedRows);
                break;
            case PhysicalType.ByteArray:
                _byteArrayValues = new List<byte[]>(estimatedRows);
                break;
            case PhysicalType.Int96:
            case PhysicalType.FixedLenByteArray:
                _fixedByteStream = new MemoryStream();
                break;
        }
    }

    /// <summary>Returns true if the value at position <paramref name="rowIndex"/> is null.</summary>
    public bool IsNull(int rowIndex) =>
        _maxDefLevel > 0 && _defLevels![rowIndex] < _maxDefLevel;

    /// <summary>Adds definition levels from a page.</summary>
    public void AddDefLevels(ReadOnlySpan<int> levels)
    {
        if (_defLevels == null) return;
        for (int i = 0; i < levels.Length; i++)
            _defLevels.Add(levels[i]);
    }

    /// <summary>Adds boolean values from a page.</summary>
    public void AddBoolValues(ReadOnlySpan<bool> values)
    {
        for (int i = 0; i < values.Length; i++)
            BoolValues!.Add(values[i]);
    }

    /// <summary>Adds Int32 values from a page.</summary>
    public void AddInt32Values(ReadOnlySpan<int> values)
    {
        for (int i = 0; i < values.Length; i++)
            Int32Values!.Add(values[i]);
    }

    /// <summary>Adds Int64 values from a page.</summary>
    public void AddInt64Values(ReadOnlySpan<long> values)
    {
        for (int i = 0; i < values.Length; i++)
            Int64Values!.Add(values[i]);
    }

    /// <summary>Adds Float values from a page.</summary>
    public void AddFloatValues(ReadOnlySpan<float> values)
    {
        for (int i = 0; i < values.Length; i++)
            FloatValues!.Add(values[i]);
    }

    /// <summary>Adds Double values from a page.</summary>
    public void AddDoubleValues(ReadOnlySpan<double> values)
    {
        for (int i = 0; i < values.Length; i++)
            DoubleValues!.Add(values[i]);
    }

    /// <summary>Adds fixed-length byte values from a page.</summary>
    public void AddFixedBytes(ReadOnlySpan<byte> values)
    {
        _fixedByteStream!.Write(values);
    }

    /// <summary>Adds BYTE_ARRAY values from a page (offsets + data).</summary>
    public void AddByteArrayValues(int[] offsets, byte[] data, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int start = offsets[i];
            int len = offsets[i + 1] - start;
            var value = new byte[len];
            data.AsSpan(start, len).CopyTo(value);
            _byteArrayValues!.Add(value);
        }
    }

    /// <summary>Gets a BYTE_ARRAY value by non-null value index.</summary>
    public ReadOnlySpan<byte> GetByteArrayValue(int index) =>
        _byteArrayValues![index];
}
