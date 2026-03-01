using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Groups flat leaf arrays into nested <see cref="StructArray"/>s
/// based on the schema tree, deriving struct validity bitmaps from raw definition levels.
/// </summary>
internal static class StructAssembler
{
    /// <summary>
    /// Assembles top-level arrays from flat leaf arrays, grouping struct columns.
    /// </summary>
    /// <param name="root">Schema tree root.</param>
    /// <param name="leafArrays">Flat leaf arrays in pre-order traversal order.</param>
    /// <param name="leafDefLevels">Raw definition levels per leaf (null for required leaves).</param>
    /// <param name="rowCount">Number of rows in the row group.</param>
    /// <returns>Top-level arrays matching the root's children.</returns>
    public static IArrowArray[] Assemble(
        SchemaNode root,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int rowCount)
    {
        var result = new IArrowArray[root.Children.Count];
        int leafIndex = 0;

        for (int i = 0; i < root.Children.Count; i++)
            result[i] = AssembleNode(root.Children[i], leafArrays, leafDefLevels, rowCount, ref leafIndex);

        return result;
    }

    private static IArrowArray AssembleNode(
        SchemaNode node,
        IArrowArray[] leafArrays,
        int[]?[] leafDefLevels,
        int rowCount,
        ref int leafIndex)
    {
        if (node.IsLeaf)
            return leafArrays[leafIndex++];

        // Record the leaf index at the start of this group's subtree
        int firstLeafIndex = leafIndex;

        // Recurse into children to build child arrays
        var childArrays = new IArrowArray[node.Children.Count];
        for (int i = 0; i < node.Children.Count; i++)
            childArrays[i] = AssembleNode(node.Children[i], leafArrays, leafDefLevels, rowCount, ref leafIndex);

        int lastLeafIndex = leafIndex; // exclusive

        var structType = new StructType(BuildChildFields(node));

        if (node.Element.RepetitionType != FieldRepetitionType.Optional)
        {
            // Required struct: no validity bitmap needed
            return new StructArray(structType, rowCount, childArrays, ArrowBuffer.Empty, nullCount: 0);
        }

        // Optional struct: derive validity bitmap from descendant def levels.
        int structDefLevel = ComputeAccumulatedDefLevel(node);

        // Find def levels from any descendant leaf
        int[]? defLevels = null;
        for (int i = firstLeafIndex; i < lastLeafIndex; i++)
        {
            if (leafDefLevels[i] != null)
            {
                defLevels = leafDefLevels[i];
                break;
            }
        }

        if (defLevels == null)
        {
            // All descendant leaves are required — struct is always non-null
            return new StructArray(structType, rowCount, childArrays, ArrowBuffer.Empty, nullCount: 0);
        }

        // Build validity bitmap: struct is non-null at row i if defLevel[i] >= structDefLevel
        int nullCount = 0;
        var bitmapBytes = new byte[(rowCount + 7) / 8];

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] >= structDefLevel)
                bitmapBytes[i >> 3] |= (byte)(1 << (i & 7));
            else
                nullCount++;
        }

        var bitmapBuffer = new ArrowBuffer(bitmapBytes);
        return new StructArray(structType, rowCount, childArrays, bitmapBuffer, nullCount);
    }

    private static Field[] BuildChildFields(SchemaNode groupNode)
    {
        var fields = new Field[groupNode.Children.Count];
        for (int i = 0; i < groupNode.Children.Count; i++)
        {
            var child = groupNode.Children[i];
            bool nullable = child.Element.RepetitionType == FieldRepetitionType.Optional;
            IArrowType type;

            if (child.IsLeaf)
            {
                type = ArrowSchemaConverter.ToArrowType(new ColumnDescriptor
                {
                    Path = [child.Name],
                    PhysicalType = child.Element.Type!.Value,
                    TypeLength = child.Element.TypeLength,
                    MaxDefinitionLevel = 0,
                    MaxRepetitionLevel = 0,
                    SchemaElement = child.Element,
                    SchemaNode = child,
                });
            }
            else
            {
                var childFields = BuildChildFields(child);
                type = new StructType(childFields);
            }

            fields[i] = new Field(child.Name, type, nullable);
        }
        return fields;
    }

    /// <summary>
    /// Computes the accumulated definition level for a node by counting optional ancestors
    /// (including itself) up to but not including the root.
    /// </summary>
    private static int ComputeAccumulatedDefLevel(SchemaNode node)
    {
        int level = 0;
        var current = node;
        while (current.Parent != null)
        {
            if (current.Element.RepetitionType == FieldRepetitionType.Optional)
                level++;
            current = current.Parent;
        }
        return level;
    }
}
