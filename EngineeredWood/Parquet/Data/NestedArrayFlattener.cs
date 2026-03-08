using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Flattens nested Arrow arrays (StructArray, ListArray, MapArray) into flat leaf
/// columns with pre-computed definition and repetition levels suitable for Parquet encoding.
/// This is the reverse of <see cref="NestedAssembler"/>.
/// </summary>
internal static class NestedArrayFlattener
{
    /// <summary>
    /// Result of flattening: one entry per Parquet leaf column, in pre-order traversal order.
    /// </summary>
    internal readonly struct FlattenedColumn
    {
        /// <summary>The decomposed leaf data with pre-computed def/rep levels and dense values.</summary>
        public readonly ArrowArrayDecomposer.DecomposedColumn Decomposed;

        /// <summary>Total entry count (= DefLevels.Length if present, else = value count).</summary>
        public readonly int NumValues;

        public FlattenedColumn(ArrowArrayDecomposer.DecomposedColumn decomposed, int numValues)
        {
            Decomposed = decomposed;
            NumValues = numValues;
        }
    }

    /// <summary>
    /// Flattens a RecordBatch's columns into leaf-level decomposed columns aligned with
    /// the given column descriptors. For flat columns, delegates to ArrowArrayDecomposer.
    /// For nested columns, computes multi-level definition (and repetition) levels.
    /// </summary>
    public static FlattenedColumn[] Flatten(
        RecordBatch batch,
        SchemaNode schemaRoot,
        IReadOnlyList<ColumnDescriptor> leafDescriptors)
    {
        var result = new List<FlattenedColumn>();
        int arrowColIndex = 0;

        for (int i = 0; i < schemaRoot.Children.Count; i++)
        {
            var node = schemaRoot.Children[i];
            var arrowArray = batch.Column(arrowColIndex++);
            FlattenNode(arrowArray, node, leafDescriptors, result);
        }

        return result.ToArray();
    }

    private static void FlattenNode(
        IArrowArray array,
        SchemaNode node,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result,
        byte[]? ancestorDefLevels = null,
        int ancestorMaxDef = 0)
    {
        if (node.IsLeaf)
        {
            // Flat leaf — use standard decomposition (or ancestor-aware if nested)
            var descriptor = leafDescriptors[result.Count];
            int typeLength = descriptor.TypeLength ?? 0;

            if (ancestorDefLevels != null)
            {
                // Leaf inside a nested structure — need ancestor-aware decomposition
                var decomposed = DecomposeWithAncestorNullability(
                    array, ancestorDefLevels, ancestorMaxDef, descriptor, typeLength);
                result.Add(new FlattenedColumn(decomposed, array.Length));
            }
            else
            {
                var decomposed = ArrowArrayDecomposer.Decompose(
                    array, descriptor.PhysicalType, descriptor.MaxDefinitionLevel, typeLength);
                result.Add(new FlattenedColumn(decomposed, array.Length));
            }
            return;
        }

        // Currently only structs are supported; lists/maps will be added later
        if (array is StructArray structArray)
        {
            FlattenStruct(structArray, node, leafDescriptors, result, ancestorDefLevels, ancestorMaxDef);
            return;
        }

        throw new NotSupportedException(
            $"Nested array type '{array.GetType().Name}' is not yet supported for writing. " +
            "Struct, List, and Map support is being added incrementally.");
    }

    /// <summary>
    /// Flattens a StructArray into its leaf columns with correct multi-level definition levels.
    /// Propagates ancestor nullability context so that nested structs produce correct def levels.
    /// </summary>
    private static void FlattenStruct(
        StructArray structArray,
        SchemaNode structNode,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result,
        byte[]? ancestorDefLevels,
        int ancestorMaxDef)
    {
        bool structIsNullable = structNode.Element.RepetitionType == FieldRepetitionType.Optional;
        int rowCount = structArray.Length;

        // Compute the "current max def" after accounting for this struct
        int currentMaxDef = ancestorMaxDef + (structIsNullable ? 1 : 0);

        // Build effective def levels that include this struct's contribution
        byte[]? effectiveDefLevels = null;

        if (structIsNullable)
        {
            effectiveDefLevels = new byte[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
                {
                    // An ancestor is null — propagate its def level
                    effectiveDefLevels[i] = ancestorDefLevels[i];
                }
                else if (!structArray.IsValid(i))
                {
                    // This struct is null (ancestors are valid)
                    effectiveDefLevels[i] = (byte)ancestorMaxDef;
                }
                else
                {
                    // This struct is valid
                    effectiveDefLevels[i] = (byte)currentMaxDef;
                }
            }
        }
        else if (ancestorDefLevels != null)
        {
            // Required struct but we have ancestor nullability to propagate
            effectiveDefLevels = ancestorDefLevels;
        }

        for (int childIdx = 0; childIdx < structNode.Children.Count; childIdx++)
        {
            var childNode = structNode.Children[childIdx];
            var childArray = structArray.Fields[childIdx];

            if (!childNode.IsLeaf)
            {
                // Nested struct or other complex type — recurse with current context
                FlattenNode(childArray, childNode, leafDescriptors, result,
                    effectiveDefLevels, currentMaxDef);
                continue;
            }

            // Leaf child of struct
            var descriptor = leafDescriptors[result.Count];
            int typeLength = descriptor.TypeLength ?? 0;

            if (effectiveDefLevels == null)
            {
                // Required struct with no ancestor nullability → same as top-level column
                var decomposed = ArrowArrayDecomposer.Decompose(
                    childArray, descriptor.PhysicalType, descriptor.MaxDefinitionLevel, typeLength);
                result.Add(new FlattenedColumn(decomposed, structArray.Length));
                continue;
            }

            // Need to build def levels combining ancestor/struct nullability with child nullability
            var leafDecomposed = DecomposeLeafWithContext(
                childArray, childNode, effectiveDefLevels, currentMaxDef,
                descriptor, typeLength);
            result.Add(new FlattenedColumn(leafDecomposed, structArray.Length));
        }
    }

    /// <summary>
    /// Decomposes a leaf array that has ancestor/struct nullability context.
    /// Builds multi-level definition levels encoding ancestor nullability, struct nullability,
    /// and leaf-level nullability.
    /// </summary>
    private static ArrowArrayDecomposer.DecomposedColumn DecomposeLeafWithContext(
        IArrowArray childArray,
        SchemaNode childNode,
        byte[] parentDefLevels,
        int parentMaxDef,
        ColumnDescriptor descriptor,
        int typeLength)
    {
        int rowCount = parentDefLevels.Length;
        bool childIsNullable = childNode.Element.RepetitionType == FieldRepetitionType.Optional;
        int maxDef = descriptor.MaxDefinitionLevel;

        var defLevels = new byte[rowCount];
        int nonNullCount = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (parentDefLevels[i] < parentMaxDef)
            {
                // Some ancestor or the parent struct is null
                defLevels[i] = parentDefLevels[i];
            }
            else if (childIsNullable && !childArray.IsValid(i))
            {
                // Parent struct is valid but child is null
                defLevels[i] = (byte)parentMaxDef;
            }
            else
            {
                // Value present at maximum def level
                defLevels[i] = (byte)maxDef;
                nonNullCount++;
            }
        }

        return ExtractDenseValues(childArray, defLevels, maxDef, nonNullCount,
            descriptor.PhysicalType, typeLength);
    }

    /// <summary>
    /// Decomposes a leaf that has ancestor nullability context.
    /// Used for leaf nodes nested inside structs (not direct children of a struct).
    /// </summary>
    private static ArrowArrayDecomposer.DecomposedColumn DecomposeWithAncestorNullability(
        IArrowArray array,
        byte[] ancestorDefLevels,
        int ancestorMaxDef,
        ColumnDescriptor descriptor,
        int typeLength)
    {
        int rowCount = ancestorDefLevels.Length;
        bool childIsNullable = descriptor.MaxDefinitionLevel > ancestorMaxDef;
        int maxDef = descriptor.MaxDefinitionLevel;

        var defLevels = new byte[rowCount];
        int nonNullCount = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (ancestorDefLevels[i] < ancestorMaxDef)
            {
                defLevels[i] = ancestorDefLevels[i];
            }
            else if (childIsNullable && !array.IsValid(i))
            {
                defLevels[i] = (byte)ancestorMaxDef;
            }
            else
            {
                defLevels[i] = (byte)maxDef;
                nonNullCount++;
            }
        }

        return ExtractDenseValues(array, defLevels, maxDef, nonNullCount,
            descriptor.PhysicalType, typeLength);
    }

    /// <summary>
    /// Extracts dense (non-null) values from a leaf array using pre-computed definition levels.
    /// Only values where defLevels[i] == maxDef are included.
    /// </summary>
    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseValues(
        IArrowArray array,
        byte[] defLevels,
        int maxDef,
        int nonNullCount,
        PhysicalType physicalType,
        int typeLength)
    {
        if (physicalType == PhysicalType.Boolean)
            return ExtractDenseBooleans(array, defLevels, maxDef, nonNullCount);

        if (physicalType == PhysicalType.ByteArray)
            return ExtractDenseByteArray(array, defLevels, maxDef, nonNullCount);

        int elementSize = physicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength,
            _ => throw new NotSupportedException($"Physical type {physicalType} not supported"),
        };

        return ExtractDenseFixedWidth(array, defLevels, maxDef, nonNullCount, elementSize);
    }

    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseBooleans(
        IArrowArray array, byte[] defLevels, int maxDef, int nonNullCount)
    {
        var boolArray = (BooleanArray)array;
        var values = new bool[nonNullCount];
        int writeIdx = 0;

        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
                values[writeIdx++] = boolArray.GetValue(i).GetValueOrDefault();
        }

        return new ArrowArrayDecomposer.DecomposedColumn(defLevels, nonNullCount, boolValues: values);
    }

    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseFixedWidth(
        IArrowArray array, byte[] defLevels, int maxDef, int nonNullCount, int elementSize)
    {
        var data = array.Data;
        var sourceBuffer = data.Buffers[1].Span;
        int sourceOffset = data.Offset;

        var denseBytes = new byte[nonNullCount * elementSize];
        int writePos = 0;

        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
            {
                sourceBuffer.Slice((sourceOffset + i) * elementSize, elementSize)
                    .CopyTo(denseBytes.AsSpan(writePos));
                writePos += elementSize;
            }
        }

        return new ArrowArrayDecomposer.DecomposedColumn(defLevels, nonNullCount, valueBytes: denseBytes);
    }

    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseByteArray(
        IArrowArray array, byte[] defLevels, int maxDef, int nonNullCount)
    {
        return array switch
        {
            StringArray sa => ExtractDenseStringOrBinary(sa.Data, defLevels, maxDef, nonNullCount),
            BinaryArray ba => ExtractDenseStringOrBinary(ba.Data, defLevels, maxDef, nonNullCount),
            _ => throw new NotSupportedException($"Array type '{array.GetType().Name}' not supported for ByteArray extraction."),
        };
    }

    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseStringOrBinary(
        ArrayData data, byte[] defLevels, int maxDef, int nonNullCount)
    {
        var offsetsSpan = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        var dataSpan = data.Buffers[2].Span;
        int baseOffset = data.Offset;

        // First pass: compute total data size
        int totalBytes = 0;
        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
            {
                int idx = baseOffset + i;
                totalBytes += offsetsSpan[idx + 1] - offsetsSpan[idx];
            }
        }

        var denseData = new byte[totalBytes];
        var denseOffsets = new int[nonNullCount + 1];
        int writeIdx = 0;
        int dataPos = 0;

        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
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

        return new ArrowArrayDecomposer.DecomposedColumn(
            defLevels, nonNullCount, byteArrayData: denseData, byteArrayOffsets: denseOffsets);
    }
}
