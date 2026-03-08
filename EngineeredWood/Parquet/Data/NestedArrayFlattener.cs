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

        // Map node: MAP group → repeated key_value → key + value (two leaf columns)
        if (array is MapArray mapArray && ArrowSchemaConverter.IsMapNode(node))
        {
            FlattenMap(mapArray, node, leafDescriptors, result, ancestorDefLevels, ancestorMaxDef);
            return;
        }

        // List node: schema has LIST group → repeated group → element child(ren)
        // Check after MapArray since MapArray extends ListArray
        if (array is ListArray listArray && ArrowSchemaConverter.IsListNode(node))
        {
            FlattenList(listArray, node, leafDescriptors, result, ancestorDefLevels, ancestorMaxDef);
            return;
        }

        throw new NotSupportedException(
            $"Nested array type '{array.GetType().Name}' is not yet supported for writing.");
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
    /// Flattens a ListArray into leaf columns with correct def/rep levels (Dremel encoding).
    /// For a standard 3-level list (optional group LIST → repeated group → element):
    ///   null list   → def=ancestorMaxDef, rep=0
    ///   empty list  → def=listExistsDef,  rep=0
    ///   first elem  → def=maxDef or elementNullDef, rep=0
    ///   next elems  → def=maxDef or elementNullDef, rep=maxRep
    /// </summary>
    private static void FlattenList(
        ListArray listArray,
        SchemaNode listNode,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result,
        byte[]? ancestorDefLevels,
        int ancestorMaxDef)
    {
        int rowCount = listArray.Length;
        bool listIsNullable = listNode.Element.RepetitionType == FieldRepetitionType.Optional;

        // Def level contributions:
        // listNode (optional/required group): +1 if optional
        // repeatedChild (repeated group): +1 always
        // element: depends on element type
        int listExistsDef = ancestorMaxDef + (listIsNullable ? 1 : 0);
        int repeatedEntryDef = listExistsDef + 1;

        // The repeated child is the first child of the LIST node
        var repeatedChild = listNode.Children[0];
        // The element is the child of the repeated group (for 3-level encoding)
        var elementNode = repeatedChild.Children.Count == 1 ? repeatedChild.Children[0] : null;

        // Get the first leaf descriptor to determine maxRep
        var firstLeafDescriptor = leafDescriptors[result.Count];
        int maxRep = firstLeafDescriptor.MaxRepetitionLevel;

        // Access the ListArray offsets
        var offsets = listArray.ValueOffsets;
        var valuesArray = listArray.Values;

        // Count total entries (one per element + one per null/empty list)
        int totalEntries = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
                totalEntries++; // ancestor null
            else if (listIsNullable && !listArray.IsValid(i))
                totalEntries++; // null list
            else
            {
                int len = offsets[i + 1] - offsets[i];
                totalEntries += len > 0 ? len : 1; // empty list still emits one entry
            }
        }

        // For simple leaf elements (the common case), flatten directly
        if (elementNode != null && elementNode.IsLeaf)
        {
            FlattenListLeaf(listArray, elementNode, ancestorDefLevels, ancestorMaxDef,
                listIsNullable, listExistsDef, repeatedEntryDef, maxRep,
                totalEntries, leafDescriptors, result);
            return;
        }

        // Complex element (struct, nested list, etc.):
        // For each leaf column in the element, build full def/rep levels that combine
        // list-level entries (null/empty markers) with element-level nullability.
        // The values array only contains actual elements, not phantom entries.
        FlattenListComplexElement(
            listArray, elementNode ?? repeatedChild, listIsNullable,
            ancestorDefLevels, ancestorMaxDef, listExistsDef, repeatedEntryDef, maxRep,
            totalEntries, leafDescriptors, result);
    }

    /// <summary>
    /// Flattens a list with a complex (non-leaf) element by iterating leaf descendants
    /// and merging list-level def/rep levels with element-level nullability.
    /// </summary>
    private static void FlattenListComplexElement(
        ListArray listArray,
        SchemaNode elementNode,
        bool listIsNullable,
        byte[]? ancestorDefLevels,
        int ancestorMaxDef,
        int listExistsDef,
        int repeatedEntryDef,
        int maxRep,
        int totalEntries,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result)
    {
        int rowCount = listArray.Length;
        var offsets = listArray.ValueOffsets;
        var valuesArray = listArray.Values;

        // Count leaf columns in the element subtree to know how many we need to produce
        int leafCount = CountSchemaLeaves(elementNode);

        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++)
        {
            var descriptor = leafDescriptors[result.Count];
            int maxDef = descriptor.MaxDefinitionLevel;

            // Get the leaf's path info to determine element-level nullability
            var defLevels = new byte[totalEntries];
            var repLevels = new byte[totalEntries];
            int nonNullCount = 0;
            int writeIdx = 0;

            // We need to access the leaf values from the element array.
            // Use GetLeafArray to extract the leaf array and its def level contributions.
            var (leafArray, leafDefContributions) = GetLeafArrayAndDefInfo(
                valuesArray, elementNode, leafIdx);

            int valIdx = 0; // index into the values array (actual elements only)

            for (int i = 0; i < rowCount; i++)
            {
                if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
                {
                    defLevels[writeIdx] = ancestorDefLevels[i];
                    repLevels[writeIdx] = 0;
                    writeIdx++;
                    continue;
                }

                if (listIsNullable && !listArray.IsValid(i))
                {
                    defLevels[writeIdx] = (byte)ancestorMaxDef;
                    repLevels[writeIdx] = 0;
                    writeIdx++;
                    continue;
                }

                int start = offsets[i];
                int end = offsets[i + 1];

                if (start == end)
                {
                    defLevels[writeIdx] = (byte)listExistsDef;
                    repLevels[writeIdx] = 0;
                    writeIdx++;
                    continue;
                }

                for (int j = start; j < end; j++)
                {
                    repLevels[writeIdx] = j == start ? (byte)0 : (byte)maxRep;

                    // Compute element-level def for this entry
                    int elementDef = ComputeElementDef(leafDefContributions, leafArray, valIdx, repeatedEntryDef, maxDef);
                    defLevels[writeIdx] = (byte)elementDef;
                    if (elementDef == maxDef)
                        nonNullCount++;

                    valIdx++;
                    writeIdx++;
                }
            }

            // Extract dense values from the leaf array
            var decomposed = ExtractDenseValuesFromFlatIndexed(
                leafArray, defLevels, repLevels, maxDef, nonNullCount,
                descriptor.PhysicalType, descriptor.TypeLength ?? 0);
            result.Add(new FlattenedColumn(decomposed, totalEntries));
        }
    }

    /// <summary>
    /// Counts the number of leaf columns in a schema node subtree.
    /// </summary>
    private static int CountSchemaLeaves(SchemaNode node)
    {
        if (node.IsLeaf) return 1;
        int count = 0;
        foreach (var child in node.Children)
            count += CountSchemaLeaves(child);
        return count;
    }

    /// <summary>
    /// Navigates to the nth leaf array within a complex element array and returns
    /// the leaf array plus intermediate nullability information.
    /// </summary>
    private static (IArrowArray leafArray, LeafDefInfo defInfo) GetLeafArrayAndDefInfo(
        IArrowArray elementArray,
        SchemaNode elementNode,
        int targetLeafIndex)
    {
        int currentLeafIndex = 0;
        return GetLeafArrayRecursive(elementArray, elementNode, targetLeafIndex, ref currentLeafIndex);
    }

    private static (IArrowArray, LeafDefInfo) GetLeafArrayRecursive(
        IArrowArray array,
        SchemaNode node,
        int targetLeafIndex,
        ref int currentLeafIndex)
    {
        if (node.IsLeaf)
        {
            if (currentLeafIndex == targetLeafIndex)
            {
                bool isNullable = node.Element.RepetitionType == FieldRepetitionType.Optional;
                return (array, new LeafDefInfo([], isNullable));
            }
            currentLeafIndex++;
            return (null!, default);
        }

        if (array is StructArray structArray)
        {
            bool structIsNullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

            for (int i = 0; i < node.Children.Count; i++)
            {
                var childNode = node.Children[i];
                var childArray = structArray.Fields[i];

                var (leafArr, info) = GetLeafArrayRecursive(childArray, childNode, targetLeafIndex, ref currentLeafIndex);
                if (leafArr != null)
                {
                    // Prepend struct nullability
                    var ancestors = new List<(IArrowArray array, bool nullable)>();
                    if (structIsNullable)
                        ancestors.Add((structArray, true));
                    ancestors.AddRange(info.Ancestors);
                    return (leafArr, new LeafDefInfo(ancestors, info.LeafIsNullable));
                }
            }
        }

        return (null!, default);
    }

    private readonly struct LeafDefInfo
    {
        /// <summary>Ancestor nullable arrays on the path from the element to the leaf.</summary>
        public readonly IReadOnlyList<(IArrowArray array, bool nullable)> Ancestors;
        public readonly bool LeafIsNullable;

        public LeafDefInfo(IReadOnlyList<(IArrowArray, bool)> ancestors, bool leafIsNullable)
        {
            Ancestors = ancestors;
            LeafIsNullable = leafIsNullable;
        }
    }

    /// <summary>
    /// Computes the element-level definition level for a specific value index
    /// within a complex list element, accounting for struct and child nullability.
    /// </summary>
    private static int ComputeElementDef(
        LeafDefInfo defInfo,
        IArrowArray leafArray,
        int elementIndex,
        int baseDef,
        int maxDef)
    {
        int def = baseDef;

        // Check each ancestor nullable node
        foreach (var (ancestorArray, isNullable) in defInfo.Ancestors)
        {
            if (isNullable)
            {
                if (!ancestorArray.IsValid(elementIndex))
                    return def; // ancestor null, stop here
                def++;
            }
        }

        // Check leaf nullability
        if (defInfo.LeafIsNullable)
        {
            if (!leafArray.IsValid(elementIndex))
                return def; // leaf null
            def++;
        }

        return maxDef;
    }

    /// <summary>
    /// Extracts dense values from a leaf array, using element indices derived from
    /// the def level array. Values are read sequentially from the leaf array for
    /// entries where def == maxDef.
    /// </summary>
    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseValuesFromFlatIndexed(
        IArrowArray valuesArray,
        byte[] defLevels,
        byte[] repLevels,
        int maxDef,
        int nonNullCount,
        PhysicalType physicalType,
        int typeLength)
    {
        // This delegates to ExtractDenseValuesFromFlat which already handles
        // sequential value extraction from a flat array
        return ExtractDenseValuesFromFlat(valuesArray, defLevels, repLevels,
            maxDef, nonNullCount, physicalType, typeLength);
    }

    /// <summary>
    /// Optimized path for flattening a list with a primitive leaf element directly into
    /// def/rep levels and dense values without recursion.
    /// </summary>
    private static void FlattenListLeaf(
        ListArray listArray,
        SchemaNode elementNode,
        byte[]? ancestorDefLevels,
        int ancestorMaxDef,
        bool listIsNullable,
        int listExistsDef,
        int repeatedEntryDef,
        int maxRep,
        int totalEntries,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result)
    {
        var descriptor = leafDescriptors[result.Count];
        int maxDef = descriptor.MaxDefinitionLevel;
        bool elementIsNullable = elementNode.Element.RepetitionType == FieldRepetitionType.Optional;
        int elementNullDef = repeatedEntryDef; // element exists but is null

        int rowCount = listArray.Length;
        var offsets = listArray.ValueOffsets;
        var valuesArray = listArray.Values;

        var defLevels = new byte[totalEntries];
        var repLevels = new byte[totalEntries];
        int writeIdx = 0;
        int nonNullCount = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
            {
                defLevels[writeIdx] = ancestorDefLevels[i];
                repLevels[writeIdx] = 0;
                writeIdx++;
                continue;
            }

            if (listIsNullable && !listArray.IsValid(i))
            {
                defLevels[writeIdx] = (byte)ancestorMaxDef;
                repLevels[writeIdx] = 0;
                writeIdx++;
                continue;
            }

            int start = offsets[i];
            int end = offsets[i + 1];

            if (start == end)
            {
                // Empty list
                defLevels[writeIdx] = (byte)listExistsDef;
                repLevels[writeIdx] = 0;
                writeIdx++;
                continue;
            }

            for (int j = start; j < end; j++)
            {
                repLevels[writeIdx] = j == start ? (byte)0 : (byte)maxRep;

                if (elementIsNullable && !valuesArray.IsValid(j))
                {
                    defLevels[writeIdx] = (byte)elementNullDef;
                }
                else
                {
                    defLevels[writeIdx] = (byte)maxDef;
                    nonNullCount++;
                }
                writeIdx++;
            }
        }

        // Extract dense values from the values array
        var decomposed = ExtractDenseValuesFromFlat(
            valuesArray, defLevels, repLevels, maxDef, nonNullCount,
            descriptor.PhysicalType, descriptor.TypeLength ?? 0);
        result.Add(new FlattenedColumn(decomposed, totalEntries));
    }

    /// <summary>
    /// Flattens a MapArray into two leaf columns (key and value) with correct def/rep levels.
    /// Maps are structurally similar to lists: MAP group → repeated key_value → key + value.
    /// Both leaf columns share the same rep levels (from the map's repeated structure).
    /// </summary>
    private static void FlattenMap(
        MapArray mapArray,
        SchemaNode mapNode,
        IReadOnlyList<ColumnDescriptor> leafDescriptors,
        List<FlattenedColumn> result,
        byte[]? ancestorDefLevels,
        int ancestorMaxDef)
    {
        int rowCount = mapArray.Length;
        bool mapIsNullable = mapNode.Element.RepetitionType == FieldRepetitionType.Optional;

        int mapExistsDef = ancestorMaxDef + (mapIsNullable ? 1 : 0);
        int repeatedEntryDef = mapExistsDef + 1;

        var keyValueGroup = mapNode.Children[0]; // repeated key_value
        var keyNode = keyValueGroup.Children[0];
        var valueNode = keyValueGroup.Children.Count > 1 ? keyValueGroup.Children[1] : null;

        var offsets = mapArray.ValueOffsets;
        var keysArray = mapArray.Keys;
        var valuesArray = valueNode != null ? mapArray.Values : null;

        // Get descriptors for key and value columns
        int keyLeafIndex = result.Count;
        var keyDescriptor = leafDescriptors[keyLeafIndex];
        int keyMaxDef = keyDescriptor.MaxDefinitionLevel;
        int maxRep = keyDescriptor.MaxRepetitionLevel;

        // Count total entries
        int totalEntries = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
                totalEntries++;
            else if (mapIsNullable && !mapArray.IsValid(i))
                totalEntries++;
            else
            {
                int len = offsets[i + 1] - offsets[i];
                totalEntries += len > 0 ? len : 1;
            }
        }

        // Build shared rep levels and base def levels for the key_value repeated group
        var repLevels = new byte[totalEntries];
        var baseDefLevels = new byte[totalEntries];
        int writeIdx = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (ancestorDefLevels != null && ancestorDefLevels[i] < ancestorMaxDef)
            {
                baseDefLevels[writeIdx] = ancestorDefLevels[i];
                repLevels[writeIdx] = 0;
                writeIdx++;
            }
            else if (mapIsNullable && !mapArray.IsValid(i))
            {
                baseDefLevels[writeIdx] = (byte)ancestorMaxDef;
                repLevels[writeIdx] = 0;
                writeIdx++;
            }
            else
            {
                int start = offsets[i];
                int end = offsets[i + 1];

                if (start == end)
                {
                    baseDefLevels[writeIdx] = (byte)mapExistsDef;
                    repLevels[writeIdx] = 0;
                    writeIdx++;
                }
                else
                {
                    for (int j = start; j < end; j++)
                    {
                        baseDefLevels[writeIdx] = (byte)repeatedEntryDef;
                        repLevels[writeIdx] = j == start ? (byte)0 : (byte)maxRep;
                        writeIdx++;
                    }
                }
            }
        }

        // Flatten key column
        FlattenMapLeafColumn(keysArray, keyNode, baseDefLevels, repLevels,
            repeatedEntryDef, keyDescriptor, totalEntries, result);

        // Flatten value column
        if (valueNode != null && valuesArray != null)
        {
            var valueDescriptor = leafDescriptors[keyLeafIndex + 1];
            FlattenMapLeafColumn(valuesArray, valueNode, baseDefLevels, repLevels,
                repeatedEntryDef, valueDescriptor, totalEntries, result);
        }
    }

    /// <summary>
    /// Flattens a single leaf column of a map (key or value) using the shared base def/rep levels.
    /// </summary>
    private static void FlattenMapLeafColumn(
        IArrowArray leafArray,
        SchemaNode leafNode,
        byte[] baseDefLevels,
        byte[] repLevels,
        int repeatedEntryDef,
        ColumnDescriptor descriptor,
        int totalEntries,
        List<FlattenedColumn> result)
    {
        int maxDef = descriptor.MaxDefinitionLevel;
        bool isNullable = leafNode.Element.RepetitionType == FieldRepetitionType.Optional;

        var defLevels = new byte[totalEntries];
        int nonNullCount = 0;

        for (int i = 0; i < totalEntries; i++)
        {
            if (baseDefLevels[i] < repeatedEntryDef)
            {
                // Map null, empty, or ancestor null — pass through base def level
                defLevels[i] = baseDefLevels[i];
            }
            else
            {
                // We're inside a map entry — check leaf nullability
                // We need the value-array index: count entries with baseDefLevels >= repeatedEntryDef
                // up to position i
                // (This is just a running count that we'll use to index into the values array)
                defLevels[i] = (byte)maxDef;
                nonNullCount++;
            }
        }

        // For nullable columns, we need to check actual values for nulls
        if (isNullable)
        {
            nonNullCount = 0;
            int valIdx = 0;
            for (int i = 0; i < totalEntries; i++)
            {
                if (baseDefLevels[i] >= repeatedEntryDef)
                {
                    if (!leafArray.IsValid(valIdx))
                    {
                        defLevels[i] = (byte)(maxDef - 1); // entry exists but value null
                    }
                    else
                    {
                        defLevels[i] = (byte)maxDef;
                        nonNullCount++;
                    }
                    valIdx++;
                }
            }
        }

        var repLevelsCopy = new byte[totalEntries];
        System.Array.Copy(repLevels, repLevelsCopy, totalEntries);

        var decomposed = ExtractDenseValuesFromFlat(
            leafArray, defLevels, repLevelsCopy, maxDef, nonNullCount,
            descriptor.PhysicalType, descriptor.TypeLength ?? 0);
        result.Add(new FlattenedColumn(decomposed, totalEntries));
    }

    /// <summary>
    /// Extracts dense values from a flat values array using pre-computed def levels.
    /// Unlike ExtractDenseValues which filters by row, this reads directly from the
    /// values array positions that correspond to maxDef entries.
    /// </summary>
    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseValuesFromFlat(
        IArrowArray valuesArray,
        byte[] defLevels,
        byte[] repLevels,
        int maxDef,
        int nonNullCount,
        PhysicalType physicalType,
        int typeLength)
    {
        // The values array is the concatenation of all list elements.
        // We need to extract only the non-null values (where def == maxDef).
        // The values array index corresponds to the "element position" which we track
        // by counting entries with def >= repeatedEntryDef (i.e., actual list elements, not null/empty markers).

        if (physicalType == PhysicalType.Boolean)
        {
            var boolArray = (BooleanArray)valuesArray;
            var values = new bool[nonNullCount];
            int writePos = 0;
            int valueIdx = 0;

            for (int i = 0; i < defLevels.Length; i++)
            {
                if (defLevels[i] == maxDef)
                {
                    values[writePos++] = boolArray.GetValue(valueIdx).GetValueOrDefault();
                    valueIdx++;
                }
                else if (defLevels[i] >= maxDef - 1 && defLevels[i] > 0 &&
                         IsRepeatedEntry(defLevels[i], maxDef))
                {
                    // Null element within a list — still advances valueIdx
                    valueIdx++;
                }
            }

            return new ArrowArrayDecomposer.DecomposedColumn(defLevels, nonNullCount,
                boolValues: values, repLevels: repLevels);
        }

        if (physicalType == PhysicalType.ByteArray)
        {
            return ExtractDenseByteArrayFromFlat(valuesArray, defLevels, repLevels,
                maxDef, nonNullCount);
        }

        // Fixed-width types
        int elementSize = physicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength,
            _ => throw new NotSupportedException($"Physical type {physicalType} not supported"),
        };

        var data = valuesArray.Data;
        var sourceBuffer = data.Buffers[1].Span;
        int sourceOffset = data.Offset;

        var denseBytes = new byte[nonNullCount * elementSize];
        int writeBytePos = 0;
        int valIdx = 0;

        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
            {
                sourceBuffer.Slice((sourceOffset + valIdx) * elementSize, elementSize)
                    .CopyTo(denseBytes.AsSpan(writeBytePos));
                writeBytePos += elementSize;
                valIdx++;
            }
            else if (IsElementEntry(defLevels, i, maxDef))
            {
                // Null element — skip value but advance position in values array
                valIdx++;
            }
        }

        return new ArrowArrayDecomposer.DecomposedColumn(defLevels, nonNullCount,
            valueBytes: denseBytes, repLevels: repLevels);
    }

    /// <summary>
    /// Returns true if the entry at position i represents an actual element in the values array
    /// (i.e., the repeated group entry exists but the element may be null).
    /// An element entry has def >= maxDef - 1 when element is nullable, or def == maxDef when required.
    /// More precisely, it's any entry where we advanced into the repeated group.
    /// </summary>
    private static bool IsElementEntry(byte[] defLevels, int i, int maxDef)
    {
        // An element entry is one where def == maxDef (non-null) or def == maxDef - 1 (null element).
        // Entries with lower def are null lists or empty lists.
        return defLevels[i] == maxDef - 1;
    }

    private static bool IsRepeatedEntry(int defLevel, int maxDef)
    {
        return defLevel >= maxDef - 1;
    }

    private static ArrowArrayDecomposer.DecomposedColumn ExtractDenseByteArrayFromFlat(
        IArrowArray valuesArray,
        byte[] defLevels,
        byte[] repLevels,
        int maxDef,
        int nonNullCount)
    {
        ArrayData data = valuesArray switch
        {
            StringArray sa => sa.Data,
            BinaryArray ba => ba.Data,
            _ => throw new NotSupportedException(
                $"Array type '{valuesArray.GetType().Name}' not supported for ByteArray list extraction."),
        };

        var offsetsSpan = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        var dataSpan = data.Buffers[2].Span;
        int baseOffset = data.Offset;

        // First pass: compute total data size and count values
        int totalBytes = 0;
        int valIdx = 0;
        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
            {
                int idx = baseOffset + valIdx;
                totalBytes += offsetsSpan[idx + 1] - offsetsSpan[idx];
                valIdx++;
            }
            else if (IsElementEntry(defLevels, i, maxDef))
            {
                valIdx++;
            }
        }

        var denseData = new byte[totalBytes];
        var denseOffsets = new int[nonNullCount + 1];
        int writeIdx = 0;
        int dataPos = 0;
        valIdx = 0;

        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDef)
            {
                int idx = baseOffset + valIdx;
                int start = offsetsSpan[idx];
                int len = offsetsSpan[idx + 1] - start;
                denseOffsets[writeIdx] = dataPos;
                dataSpan.Slice(start, len).CopyTo(denseData.AsSpan(dataPos));
                dataPos += len;
                writeIdx++;
                valIdx++;
            }
            else if (IsElementEntry(defLevels, i, maxDef))
            {
                valIdx++;
            }
        }
        denseOffsets[writeIdx] = dataPos;

        return new ArrowArrayDecomposer.DecomposedColumn(
            defLevels, nonNullCount, byteArrayData: denseData, byteArrayOffsets: denseOffsets,
            repLevels: repLevels);
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
