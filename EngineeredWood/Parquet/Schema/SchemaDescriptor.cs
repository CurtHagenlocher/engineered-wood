using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// Reconstructs the schema tree from the flat list of <see cref="SchemaElement"/>s
/// and computes definition/repetition levels for each leaf column.
/// </summary>
public sealed class SchemaDescriptor
{
    /// <summary>The root node of the schema tree.</summary>
    public SchemaNode Root { get; }

    /// <summary>All leaf (primitive) columns in pre-order traversal order.</summary>
    public IReadOnlyList<ColumnDescriptor> Columns { get; }

    public SchemaDescriptor(IReadOnlyList<SchemaElement> schemaElements)
    {
        if (schemaElements.Count == 0)
            throw new ParquetFormatException("Schema must contain at least a root element.");

        int index = 0;
        Root = BuildTree(schemaElements, ref index, parent: null);

        if (index != schemaElements.Count)
            throw new ParquetFormatException(
                $"Schema element count mismatch: consumed {index} of {schemaElements.Count} elements.");

        var columns = new List<ColumnDescriptor>();
        CollectLeaves(Root, [], defLevel: 0, repLevel: 0, columns);
        Columns = columns;
    }

    private static SchemaNode BuildTree(
        IReadOnlyList<SchemaElement> elements,
        ref int index,
        SchemaNode? parent)
    {
        if (index >= elements.Count)
            throw new ParquetFormatException("Unexpected end of schema elements.");

        var element = elements[index++];
        int numChildren = element.NumChildren ?? 0;

        var children = new List<SchemaNode>(numChildren);
        // Create the node first (children list will be populated next).
        var node = new SchemaNode
        {
            Element = element,
            Parent = parent,
            Children = children,
        };

        for (int i = 0; i < numChildren; i++)
            children.Add(BuildTree(elements, ref index, node));

        return node;
    }

    private static void CollectLeaves(
        SchemaNode node,
        List<string> path,
        int defLevel,
        int repLevel,
        List<ColumnDescriptor> columns)
    {
        // The root has no repetition type and doesn't contribute to levels.
        if (node.Parent != null)
        {
            path.Add(node.Name);

            var rep = node.Element.RepetitionType;
            if (rep == FieldRepetitionType.Optional)
                defLevel++;
            else if (rep == FieldRepetitionType.Repeated)
            {
                defLevel++;
                repLevel++;
            }
        }

        if (node.IsLeaf)
        {
            columns.Add(new ColumnDescriptor
            {
                Path = path.ToArray(),
                PhysicalType = node.Element.Type!.Value,
                TypeLength = node.Element.TypeLength,
                MaxDefinitionLevel = defLevel,
                MaxRepetitionLevel = repLevel,
                SchemaElement = node.Element,
                SchemaNode = node,
            });
        }
        else
        {
            foreach (var child in node.Children)
                CollectLeaves(child, path, defLevel, repLevel, columns);
        }

        if (node.Parent != null)
            path.RemoveAt(path.Count - 1);
    }
}
