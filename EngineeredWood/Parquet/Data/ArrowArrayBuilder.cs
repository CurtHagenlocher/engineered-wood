using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Builds Apache Arrow arrays from decoded Parquet column data, inserting nulls
/// based on definition levels. Uses native memory buffers to avoid managed heap copies.
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
            BooleanType => BuildBooleanArray(state, rowCount),
            Int8Type => BuildNarrowIntArray<sbyte>(state, arrowType, rowCount),
            UInt8Type => BuildNarrowIntArray<byte>(state, arrowType, rowCount),
            Int16Type => BuildNarrowIntArray<short>(state, arrowType, rowCount),
            UInt16Type => BuildNarrowIntArray<ushort>(state, arrowType, rowCount),
            Int32Type or Date32Type or Time32Type => BuildFixedArray<int>(state, arrowType, rowCount),
            UInt32Type => BuildFixedArray<uint>(state, arrowType, rowCount),
            Int64Type or TimestampType or Time64Type => BuildFixedArray<long>(state, arrowType, rowCount),
            UInt64Type => BuildFixedArray<ulong>(state, arrowType, rowCount),
            FloatType => BuildFixedArray<float>(state, arrowType, rowCount),
            DoubleType => BuildFixedArray<double>(state, arrowType, rowCount),
            StringType => BuildStringArray(state, rowCount),
            BinaryType => BuildBinaryArray(state, rowCount),
            FixedSizeBinaryType fsb => BuildFixedSizeBinaryArray(state, rowCount, fsb),
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for array building."),
        };
    }

    private static IArrowArray BuildFixedArray<T>(ColumnBuildState state, IArrowType arrowType, int rowCount)
        where T : unmanaged
    {
        if (!state.IsNullable)
        {
            // Non-nullable: the value buffer is already dense and complete
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        // Nullable: scatter dense values into row-position slots, build validity bitmap
        var denseValues = state.GetValueSpan<T>();
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;

        using var scatteredBuf = new NativeBuffer<T>(rowCount);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                scattered[i] = denseValues[valueIdx++];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>
    /// Builds arrays for types narrower than their Parquet physical type (Int8/UInt8/Int16/UInt16
    /// are stored as Int32 in Parquet).
    /// </summary>
    private static IArrowArray BuildNarrowIntArray<TNarrow>(ColumnBuildState state, IArrowType arrowType, int rowCount)
        where TNarrow : unmanaged
    {
        var sourceValues = state.GetValueSpan<int>();
        int nonNullCount = state.ValueCount;

        if (!state.IsNullable)
        {
            using var narrowBuf = new NativeBuffer<TNarrow>(rowCount);
            var dest = narrowBuf.Span;
            for (int i = 0; i < nonNullCount; i++)
                dest[i] = CastNarrow<TNarrow>(sourceValues[i]);

            var valueBuffer = narrowBuf.Build();
            state.DisposeValueBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        // Nullable narrow
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;

        using var scatteredBuf = new NativeBuffer<TNarrow>(rowCount);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                scattered[i] = CastNarrow<TNarrow>(sourceValues[valueIdx++]);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TNarrow CastNarrow<TNarrow>(int value) where TNarrow : unmanaged
    {
        if (typeof(TNarrow) == typeof(sbyte)) return (TNarrow)(object)checked((sbyte)value);
        if (typeof(TNarrow) == typeof(byte)) return (TNarrow)(object)checked((byte)value);
        if (typeof(TNarrow) == typeof(short)) return (TNarrow)(object)checked((short)value);
        if (typeof(TNarrow) == typeof(ushort)) return (TNarrow)(object)checked((ushort)value);
        throw new NotSupportedException($"CastNarrow does not support {typeof(TNarrow).Name}");
    }

    private static IArrowArray BuildBooleanArray(ColumnBuildState state, int rowCount)
    {
        // Boolean values are already bit-packed in the value buffer
        if (!state.IsNullable)
        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(BooleanType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new BooleanArray(arrayData);
        }

        // Nullable: scatter bits and build bitmap
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseBits = state.ValueByteSpan;

        using var scatteredBuf = new NativeBuffer<byte>((rowCount + 7) / 8);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                // Read bit from dense packed buffer
                bool val = ((denseBits[valueIdx >> 3] >> (valueIdx & 7)) & 1) == 1;
                if (val)
                    scattered[i >> 3] |= (byte)(1 << (i & 7));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(BooleanType.Default, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new BooleanArray(data);
    }

    private static IArrowArray BuildStringArray(ColumnBuildState state, int rowCount)
    {
        if (!state.IsNullable)
        {
            var offsetsBuffer = state.BuildOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(Apache.Arrow.Types.StringType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return new StringArray(arrayData);
        }

        // Nullable: scatter offsets with null gaps, build bitmap
        return BuildNullableVarBinaryArray(state, Apache.Arrow.Types.StringType.Default, rowCount);
    }

    private static IArrowArray BuildBinaryArray(ColumnBuildState state, int rowCount)
    {
        if (!state.IsNullable)
        {
            var offsetsBuffer = state.BuildOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(BinaryType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return new BinaryArray(arrayData);
        }

        return BuildNullableVarBinaryArray(state, BinaryType.Default, rowCount);
    }

    private static IArrowArray BuildNullableVarBinaryArray(ColumnBuildState state, IArrowType arrowType, int rowCount)
    {
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseOffsets = state.GetOffsetsSpan();
        var denseData = state.GetDataSpan();

        using var scatteredOffsets = new NativeBuffer<int>(rowCount + 1);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var offsets = scatteredOffsets.Span;
        var bitmap = bitmapBuf.ByteSpan;

        // Data buffer can be shared directly — data bytes stay the same,
        // we just need to build new offsets that repeat the same position for null rows.
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                offsets[i] = denseOffsets[valueIdx];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
            else
            {
                offsets[i] = valueIdx < denseOffsets.Length ? denseOffsets[valueIdx] : (nonNullCount > 0 ? denseOffsets[nonNullCount] : 0);
            }
        }
        offsets[rowCount] = nonNullCount > 0 ? denseOffsets[nonNullCount] : 0;

        var offsetsArrow = scatteredOffsets.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrow = state.BuildDataBuffer();
        state.DisposeOffsetsBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, offsetsArrow, dataArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildFixedSizeBinaryArray(
        ColumnBuildState state, int rowCount, FixedSizeBinaryType fixedType)
    {
        int byteWidth = fixedType.ByteWidth;

        if (!state.IsNullable)
        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(fixedType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new FixedSizeBinaryArray(arrayData);
        }

        // Nullable: scatter fixed-size values
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseBytes = state.ValueByteSpan;

        using var scatteredBuf = new NativeBuffer<byte>(rowCount * byteWidth);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                denseBytes.Slice(valueIdx * byteWidth, byteWidth)
                    .CopyTo(scattered.Slice(i * byteWidth, byteWidth));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(fixedType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new FixedSizeBinaryArray(data);
    }
}

/// <summary>
/// Accumulates decoded values and definition levels across multiple data pages for one column,
/// using native memory buffers to avoid managed heap allocations.
/// </summary>
internal sealed class ColumnBuildState : IDisposable
{
    private readonly PhysicalType _physicalType;
    internal readonly int MaxDefLevel;

    // Definition levels (nullable columns only)
    private NativeBuffer<int>? _defLevels;
    private int _defLevelCount;

    // Value buffer: stores dense non-null values for fixed-width types and fixed-len byte arrays
    private NativeBuffer<byte>? _valueBuffer;
    private int _valueByteOffset;
    private int _valueCount;

    // For Boolean: track bit offset since values are bit-packed
    private int _boolBitOffset;

    // For ByteArray/String: separate offsets and data buffers
    private NativeBuffer<int>? _offsetsBuffer;
    private int _offsetsCount; // number of offset entries written (= _valueCount + 1 after first page)
    private NativeBuffer<byte>? _dataBuffer;
    private int _dataByteOffset;

    private readonly int _rowCount;
    private readonly int _elementSize; // bytes per element for fixed-width types

    /// <summary>Whether this column is nullable (has def levels).</summary>
    public bool IsNullable => MaxDefLevel > 0;

    /// <summary>Number of non-null values accumulated.</summary>
    public int ValueCount => _valueCount;

    public ColumnBuildState(PhysicalType physicalType, int maxDefLevel, int rowCount)
    {
        _physicalType = physicalType;
        MaxDefLevel = maxDefLevel;
        _rowCount = rowCount;

        if (maxDefLevel > 0)
        {
            _defLevels = new NativeBuffer<int>(rowCount);
            _defLevelCount = 0;
        }

        switch (physicalType)
        {
            case PhysicalType.Boolean:
                _elementSize = 0; // bit-packed, special handling
                _valueBuffer = new NativeBuffer<byte>((rowCount + 7) / 8);
                break;
            case PhysicalType.Int32:
                _elementSize = sizeof(int);
                _valueBuffer = new NativeBuffer<byte>(rowCount * _elementSize);
                break;
            case PhysicalType.Int64:
                _elementSize = sizeof(long);
                _valueBuffer = new NativeBuffer<byte>(rowCount * _elementSize);
                break;
            case PhysicalType.Float:
                _elementSize = sizeof(float);
                _valueBuffer = new NativeBuffer<byte>(rowCount * _elementSize);
                break;
            case PhysicalType.Double:
                _elementSize = sizeof(double);
                _valueBuffer = new NativeBuffer<byte>(rowCount * _elementSize);
                break;
            case PhysicalType.Int96:
                _elementSize = 12;
                _valueBuffer = new NativeBuffer<byte>(rowCount * 12);
                break;
            case PhysicalType.FixedLenByteArray:
                // elementSize will be set on first decode (needs TypeLength from column)
                _elementSize = 0;
                break;
            case PhysicalType.ByteArray:
                _elementSize = 0;
                _offsetsBuffer = new NativeBuffer<int>(rowCount + 1);
                _offsetsBuffer.Span[0] = 0;
                _offsetsCount = 1;
                // Data buffer starts with estimated size, can grow
                _dataBuffer = new NativeBuffer<byte>(rowCount * 32);
                break;
        }
    }

    /// <summary>Returns true if the value at position <paramref name="rowIndex"/> is null.</summary>
    public bool IsNull(int rowIndex) =>
        MaxDefLevel > 0 && _defLevels!.Span[rowIndex] < MaxDefLevel;

    /// <summary>Gets the definition levels span (for the build phase).</summary>
    public ReadOnlySpan<int> DefLevelSpan => _defLevels!.Span.Slice(0, _defLevelCount);

    /// <summary>
    /// Reserves space for <paramref name="count"/> definition levels and returns a writable span.
    /// </summary>
    public Span<int> ReserveDefLevels(int count)
    {
        if (_defLevels == null) return Span<int>.Empty;
        var span = _defLevels.Span.Slice(_defLevelCount, count);
        _defLevelCount += count;
        return span;
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> values of type <typeparamref name="T"/>
    /// and returns a writable span into the native value buffer.
    /// </summary>
    public Span<T> ReserveValues<T>(int count) where T : unmanaged
    {
        int byteSize = count * Unsafe.SizeOf<T>();
        var byteSpan = _valueBuffer!.ByteSpan.Slice(_valueByteOffset, byteSize);
        _valueByteOffset += byteSize;
        _valueCount += count;
        return MemoryMarshal.Cast<byte, T>(byteSpan);
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> boolean values (bit-packed).
    /// Writes the decoded booleans into the native bit buffer.
    /// </summary>
    public void AddBoolValues(ReadOnlySpan<bool> values)
    {
        var buf = _valueBuffer!.ByteSpan;
        for (int i = 0; i < values.Length; i++)
        {
            int bitPos = _boolBitOffset + i;
            if (values[i])
                buf[bitPos >> 3] |= (byte)(1 << (bitPos & 7));
        }
        _boolBitOffset += values.Length;
        _valueCount += values.Length;
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> fixed-length byte values and returns a writable span.
    /// </summary>
    public Span<byte> ReserveFixedBytes(int count, int typeLength)
    {
        // Lazy init for FixedLenByteArray (needs typeLength from column descriptor)
        if (_valueBuffer == null)
            _valueBuffer = new NativeBuffer<byte>(_rowCount * typeLength);

        int byteSize = count * typeLength;
        var span = _valueBuffer.ByteSpan.Slice(_valueByteOffset, byteSize);
        _valueByteOffset += byteSize;
        _valueCount += count;
        return span;
    }

    /// <summary>
    /// Adds BYTE_ARRAY values: writes offsets and copies data into native buffers.
    /// </summary>
    public void AddByteArrayValues(ReadOnlySpan<int> sourceOffsets, ReadOnlySpan<byte> sourceData, int count)
    {
        // Ensure data buffer has enough space
        int dataNeeded = _dataByteOffset + sourceData.Length;
        if (dataNeeded > _dataBuffer!.ByteSpan.Length)
            _dataBuffer.Grow(dataNeeded);

        // Copy data
        sourceData.CopyTo(_dataBuffer.ByteSpan.Slice(_dataByteOffset));

        // Write offsets (shifted by current data offset)
        var offsets = _offsetsBuffer!.Span;
        for (int i = 0; i < count; i++)
        {
            offsets[_offsetsCount + i] = _dataByteOffset + sourceOffsets[i + 1];
        }
        _offsetsCount += count;
        _dataByteOffset += sourceData.Length;
        _valueCount += count;
    }

    // --- Build methods: transfer ownership to ArrowBuffer ---

    /// <summary>Gets a typed span over the dense value data (for the build phase).</summary>
    public ReadOnlySpan<T> GetValueSpan<T>() where T : unmanaged
    {
        var bytes = _valueBuffer!.ByteSpan.Slice(0, _valueByteOffset);
        return MemoryMarshal.Cast<byte, T>(bytes);
    }

    /// <summary>Gets a span over the raw value bytes (for boolean and fixed-size binary build).</summary>
    public ReadOnlySpan<byte> ValueByteSpan =>
        _valueBuffer!.ByteSpan.Slice(0, _physicalType == PhysicalType.Boolean
            ? (_boolBitOffset + 7) / 8
            : _valueByteOffset);

    /// <summary>Transfers the value buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildValueBuffer()
    {
        return _valueBuffer!.Build();
    }

    /// <summary>Disposes the value buffer (after data has been copied elsewhere).</summary>
    public void DisposeValueBuffer()
    {
        _valueBuffer?.Dispose();
        _valueBuffer = null;
    }

    /// <summary>Gets the offsets span (for byte array build).</summary>
    public ReadOnlySpan<int> GetOffsetsSpan() =>
        _offsetsBuffer!.Span.Slice(0, _offsetsCount);

    /// <summary>Gets the data span (for byte array build).</summary>
    public ReadOnlySpan<byte> GetDataSpan() =>
        _dataBuffer!.ByteSpan.Slice(0, _dataByteOffset);

    /// <summary>Transfers the offsets buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildOffsetsBuffer()
    {
        return _offsetsBuffer!.Build();
    }

    /// <summary>Disposes the offsets buffer.</summary>
    public void DisposeOffsetsBuffer()
    {
        _offsetsBuffer?.Dispose();
        _offsetsBuffer = null;
    }

    /// <summary>Transfers the data buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildDataBuffer()
    {
        return _dataBuffer!.Build();
    }

    public void Dispose()
    {
        _defLevels?.Dispose();
        _valueBuffer?.Dispose();
        _offsetsBuffer?.Dispose();
        _dataBuffer?.Dispose();
    }
}

/// <summary>
/// Factory to construct the correct Arrow array type from <see cref="ArrayData"/>.
/// </summary>
file static class ArrowArrayFactory
{
    public static IArrowArray BuildArray(ArrayData data) =>
        data.DataType switch
        {
            BooleanType => new BooleanArray(data),
            Int8Type => new Int8Array(data),
            UInt8Type => new UInt8Array(data),
            Int16Type => new Int16Array(data),
            UInt16Type => new UInt16Array(data),
            Int32Type => new Int32Array(data),
            UInt32Type => new UInt32Array(data),
            Int64Type => new Int64Array(data),
            UInt64Type => new UInt64Array(data),
            FloatType => new FloatArray(data),
            DoubleType => new DoubleArray(data),
            Date32Type => new Date32Array(data),
            Time32Type => new Time32Array(data),
            Time64Type => new Time64Array(data),
            TimestampType => new TimestampArray(data),
            Apache.Arrow.Types.StringType => new StringArray(data),
            BinaryType => new BinaryArray(data),
            FixedSizeBinaryType => new FixedSizeBinaryArray(data),
            _ => throw new NotSupportedException($"Cannot construct Arrow array for type '{data.DataType.Name}'."),
        };
}
