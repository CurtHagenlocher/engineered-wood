using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decomposes Arrow arrays into raw typed values and definition levels suitable for Parquet encoding.
/// This is the reverse of <see cref="ArrowArrayBuilder"/>: Arrow arrays → (values, defLevels).
/// Values are extracted densely (no null gaps). Definition levels indicate null positions.
/// </summary>
internal static class ArrowArrayDecomposer
{
    /// <summary>
    /// Result of decomposing an Arrow array.
    /// </summary>
    public readonly struct DecomposedColumn
    {
        /// <summary>Definition levels (maxDefLevel for non-null, 0 for null). Null if column is required.</summary>
        public readonly byte[]? DefLevels;

        /// <summary>Number of non-null values.</summary>
        public readonly int NonNullCount;

        /// <summary>Dense boolean values (only for BooleanArray).</summary>
        public readonly bool[]? BoolValues;

        /// <summary>Dense fixed-width values (Int32, Int64, Float, Double, FLBA, Int96).</summary>
        public readonly byte[]? ValueBytes;

        /// <summary>Dense byte array data (for ByteArray: concatenated non-null values).</summary>
        public readonly byte[]? ByteArrayData;

        /// <summary>Offsets into ByteArrayData for each non-null value.</summary>
        public readonly int[]? ByteArrayOffsets;

        public DecomposedColumn(
            byte[]? defLevels,
            int nonNullCount,
            bool[]? boolValues = null,
            byte[]? valueBytes = null,
            byte[]? byteArrayData = null,
            int[]? byteArrayOffsets = null)
        {
            DefLevels = defLevels;
            NonNullCount = nonNullCount;
            BoolValues = boolValues;
            ValueBytes = valueBytes;
            ByteArrayData = byteArrayData;
            ByteArrayOffsets = byteArrayOffsets;
        }
    }

    /// <summary>
    /// Decomposes an Arrow array into values and definition levels for a flat (non-nested) column.
    /// </summary>
    /// <param name="array">The Arrow array to decompose.</param>
    /// <param name="physicalType">The target Parquet physical type.</param>
    /// <param name="maxDefLevel">Maximum definition level (0 for required, 1 for optional).</param>
    /// <param name="typeLength">Type length for FixedLenByteArray columns.</param>
    public static DecomposedColumn Decompose(
        IArrowArray array,
        PhysicalType physicalType,
        int maxDefLevel,
        int typeLength = 0)
    {
        byte[]? defLevels = null;
        int nonNullCount = array.Length - array.NullCount;

        if (maxDefLevel > 0)
        {
            defLevels = BuildDefLevels(array, (byte)maxDefLevel);
        }

        return physicalType switch
        {
            PhysicalType.Boolean => DecomposeBoolean((BooleanArray)array, defLevels, nonNullCount),
            PhysicalType.Int32 => DecomposeFixedWidth(array, defLevels, nonNullCount, 4),
            PhysicalType.Int64 => DecomposeFixedWidth(array, defLevels, nonNullCount, 8),
            PhysicalType.Float => DecomposeFixedWidth(array, defLevels, nonNullCount, 4),
            PhysicalType.Double => DecomposeFixedWidth(array, defLevels, nonNullCount, 8),
            PhysicalType.Int96 => DecomposeFixedWidth(array, defLevels, nonNullCount, 12),
            PhysicalType.FixedLenByteArray => DecomposeFixedWidth(array, defLevels, nonNullCount, typeLength),
            PhysicalType.ByteArray => DecomposeByteArray(array, defLevels, nonNullCount),
            _ => throw new NotSupportedException($"Physical type {physicalType} not supported for decomposition.")
        };
    }

    private static byte[] BuildDefLevels(IArrowArray array, byte maxDefLevel)
    {
        var defLevels = new byte[array.Length];
        var data = array.Data;

        if (data.NullCount == 0)
        {
            defLevels.AsSpan().Fill(maxDefLevel);
            return defLevels;
        }

        var validityBuffer = data.Buffers[0];
        if (validityBuffer.IsEmpty)
        {
            // No validity bitmap means all valid
            defLevels.AsSpan().Fill(maxDefLevel);
            return defLevels;
        }

        var bitmap = validityBuffer.Span;
        int offset = data.Offset;

        for (int i = 0; i < array.Length; i++)
        {
            int idx = offset + i;
            bool isValid = (bitmap[idx >> 3] & (1 << (idx & 7))) != 0;
            defLevels[i] = isValid ? maxDefLevel : (byte)0;
        }

        return defLevels;
    }

    private static DecomposedColumn DecomposeBoolean(BooleanArray array, byte[]? defLevels, int nonNullCount)
    {
        var values = new bool[nonNullCount];
        int writeIdx = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsValid(i))
                values[writeIdx++] = array.GetValue(i).GetValueOrDefault();
        }

        return new DecomposedColumn(defLevels, nonNullCount, boolValues: values);
    }

    private static DecomposedColumn DecomposeFixedWidth(
        IArrowArray array, byte[]? defLevels, int nonNullCount, int elementSize)
    {
        var data = array.Data;
        var sourceBuffer = data.Buffers[1].Span;
        int sourceOffset = data.Offset;

        if (data.NullCount == 0)
        {
            // No nulls: copy entire value buffer
            var valueBytes = sourceBuffer.Slice(sourceOffset * elementSize, array.Length * elementSize).ToArray();
            return new DecomposedColumn(defLevels, nonNullCount, valueBytes: valueBytes);
        }

        // Has nulls: extract only non-null values densely
        var denseBytes = new byte[nonNullCount * elementSize];
        int writePos = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsValid(i))
            {
                sourceBuffer.Slice((sourceOffset + i) * elementSize, elementSize)
                    .CopyTo(denseBytes.AsSpan(writePos));
                writePos += elementSize;
            }
        }

        return new DecomposedColumn(defLevels, nonNullCount, valueBytes: denseBytes);
    }

    private static DecomposedColumn DecomposeByteArray(
        IArrowArray array, byte[]? defLevels, int nonNullCount)
    {
        return array switch
        {
            StringArray sa => DecomposeStringOrBinary(sa.Data, defLevels, nonNullCount),
            BinaryArray ba => DecomposeStringOrBinary(ba.Data, defLevels, nonNullCount),
            LargeStringArray lsa => DecomposeLargeStringOrBinary(lsa.Data, defLevels, nonNullCount),
            LargeBinaryArray lba => DecomposeLargeStringOrBinary(lba.Data, defLevels, nonNullCount),
            _ => throw new NotSupportedException($"Arrow array type {array.GetType().Name} not supported for ByteArray decomposition.")
        };
    }

    private static DecomposedColumn DecomposeStringOrBinary(
        ArrayData data, byte[]? defLevels, int nonNullCount)
    {
        var offsetsSpan = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        var dataSpan = data.Buffers[2].Span;
        int baseOffset = data.Offset;

        if (data.NullCount == 0)
        {
            // No nulls: copy offsets and data directly
            var offsets = new int[nonNullCount + 1];
            int startOffset = offsetsSpan[baseOffset];
            int endOffset = offsetsSpan[baseOffset + nonNullCount];
            int totalDataLen = endOffset - startOffset;

            // Re-base offsets to start at 0
            for (int i = 0; i <= nonNullCount; i++)
                offsets[i] = offsetsSpan[baseOffset + i] - startOffset;

            var byteData = dataSpan.Slice(startOffset, totalDataLen).ToArray();
            return new DecomposedColumn(defLevels, nonNullCount, byteArrayData: byteData, byteArrayOffsets: offsets);
        }

        // Has nulls: extract only non-null values
        var denseOffsets = new int[nonNullCount + 1];
        int totalBytes = 0;

        // First pass: compute total data size
        int length = defLevels?.Length ?? data.Length;
        for (int i = 0; i < length; i++)
        {
            bool isValid = defLevels == null || defLevels[i] > 0;
            if (isValid)
            {
                int idx = baseOffset + i;
                totalBytes += offsetsSpan[idx + 1] - offsetsSpan[idx];
            }
        }

        var denseData = new byte[totalBytes];
        int writeIdx = 0;
        int dataPos = 0;

        for (int i = 0; i < length; i++)
        {
            bool isValid = defLevels == null || defLevels[i] > 0;
            if (isValid)
            {
                int idx = baseOffset + i;
                int start = offsetsSpan[idx];
                int len = offsetsSpan[idx + 1] - start;
                denseOffsets[writeIdx] = dataPos;
                dataSpan.Slice(start, len).CopyTo(denseData.AsSpan(dataPos));
                dataPos += len;
                writeIdx++;
            }
        }
        denseOffsets[writeIdx] = dataPos;

        return new DecomposedColumn(defLevels, nonNullCount, byteArrayData: denseData, byteArrayOffsets: denseOffsets);
    }

    private static DecomposedColumn DecomposeLargeStringOrBinary(
        ArrayData data, byte[]? defLevels, int nonNullCount)
    {
        var offsetsSpan = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span);
        var dataSpan = data.Buffers[2].Span;
        int baseOffset = data.Offset;

        if (data.NullCount == 0)
        {
            var offsets = new int[nonNullCount + 1];
            long startOffset = offsetsSpan[baseOffset];
            long endOffset = offsetsSpan[baseOffset + nonNullCount];
            int totalDataLen = checked((int)(endOffset - startOffset));

            for (int i = 0; i <= nonNullCount; i++)
                offsets[i] = checked((int)(offsetsSpan[baseOffset + i] - startOffset));

            var byteData = dataSpan.Slice(checked((int)startOffset), totalDataLen).ToArray();
            return new DecomposedColumn(defLevels, nonNullCount, byteArrayData: byteData, byteArrayOffsets: offsets);
        }

        var denseOffsets = new int[nonNullCount + 1];
        int totalBytes = 0;
        int length = defLevels?.Length ?? data.Length;

        for (int i = 0; i < length; i++)
        {
            bool isValid = defLevels == null || defLevels[i] > 0;
            if (isValid)
            {
                int idx = baseOffset + i;
                totalBytes += checked((int)(offsetsSpan[idx + 1] - offsetsSpan[idx]));
            }
        }

        var denseData = new byte[totalBytes];
        int writeIdx = 0;
        int dataPos = 0;

        for (int i = 0; i < length; i++)
        {
            bool isValid = defLevels == null || defLevels[i] > 0;
            if (isValid)
            {
                int idx = baseOffset + i;
                long start = offsetsSpan[idx];
                int len = checked((int)(offsetsSpan[idx + 1] - start));
                denseOffsets[writeIdx] = dataPos;
                dataSpan.Slice(checked((int)start), len).CopyTo(denseData.AsSpan(dataPos));
                dataPos += len;
                writeIdx++;
            }
        }
        denseOffsets[writeIdx] = dataPos;

        return new DecomposedColumn(defLevels, nonNullCount, byteArrayData: denseData, byteArrayOffsets: denseOffsets);
    }
}
