using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Converts Parquet column descriptors to Apache Arrow fields and types.
/// </summary>
internal static class ArrowSchemaConverter
{
    /// <summary>
    /// Converts a Parquet <see cref="ColumnDescriptor"/> to an Arrow <see cref="Apache.Arrow.Field"/>.
    /// </summary>
    public static Apache.Arrow.Field ToArrowField(ColumnDescriptor column)
    {
        bool nullable = column.MaxDefinitionLevel > 0;
        var arrowType = ToArrowType(column);
        return new Apache.Arrow.Field(column.DottedPath, arrowType, nullable);
    }

    /// <summary>
    /// Converts the top-level children of a schema root into Arrow fields,
    /// recursing into group nodes to produce nested <see cref="StructType"/> fields.
    /// </summary>
    public static Apache.Arrow.Field[] ToArrowFields(SchemaNode root)
    {
        var fields = new Apache.Arrow.Field[root.Children.Count];
        for (int i = 0; i < root.Children.Count; i++)
            fields[i] = NodeToArrowField(root.Children[i]);
        return fields;
    }

    private static Apache.Arrow.Field NodeToArrowField(SchemaNode node)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        if (node.IsLeaf)
        {
            // Bare repeated primitive: repeated leaf at top level → list of that type
            if (node.Element.RepetitionType == FieldRepetitionType.Repeated)
            {
                var elementType = LeafToArrowType(node);
                var elementField = new Apache.Arrow.Field("element", elementType, nullable: false);
                return new Apache.Arrow.Field(node.Name, new ListType(elementField), nullable: false);
            }

            var arrowType = LeafToArrowType(node);
            return new Apache.Arrow.Field(node.Name, arrowType, nullable);
        }

        if (IsListNode(node))
            return BuildListField(node);
        if (IsMapNode(node))
            return BuildMapField(node);

        // Group node → StructType
        var childFields = new Apache.Arrow.Field[node.Children.Count];
        for (int i = 0; i < node.Children.Count; i++)
            childFields[i] = NodeToArrowField(node.Children[i]);

        var structType = new StructType(childFields);
        return new Apache.Arrow.Field(node.Name, structType, nullable);
    }

    internal static bool IsListNode(SchemaNode node) =>
        !node.IsLeaf &&
        (node.Element.LogicalType is LogicalType.ListType ||
         node.Element.ConvertedType == ConvertedType.List);

    internal static bool IsMapNode(SchemaNode node) =>
        !node.IsLeaf &&
        (node.Element.LogicalType is LogicalType.MapType ||
         node.Element.ConvertedType == ConvertedType.Map ||
         node.Element.ConvertedType == ConvertedType.MapKeyValue);

    private static Apache.Arrow.Field BuildListField(SchemaNode node)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        // Determine element schema:
        // 3-level standard: node → repeated group (list/bag/array) → element child
        // 2-level legacy: node → repeated leaf (the leaf is the element)
        var repeatedChild = node.Children[0]; // always has exactly one repeated child

        Apache.Arrow.Field elementField;
        if (repeatedChild.IsLeaf)
        {
            // 2-level: repeated leaf is the element
            var elementType = LeafToArrowType(repeatedChild);
            elementField = new Apache.Arrow.Field(repeatedChild.Name, elementType, nullable: false);
        }
        else if (repeatedChild.Children.Count == 1)
        {
            // 3-level standard: recurse into single element child
            elementField = NodeToArrowField(repeatedChild.Children[0]);
        }
        else
        {
            // 3-level with multiple children in repeated group → treat repeated group as struct element
            var childFields = new Apache.Arrow.Field[repeatedChild.Children.Count];
            for (int i = 0; i < repeatedChild.Children.Count; i++)
                childFields[i] = NodeToArrowField(repeatedChild.Children[i]);
            var structType = new StructType(childFields);
            elementField = new Apache.Arrow.Field(repeatedChild.Name, structType, nullable: false);
        }

        return new Apache.Arrow.Field(node.Name, new ListType(elementField), nullable);
    }

    private static Apache.Arrow.Field BuildMapField(SchemaNode node)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        // Standard map: node → repeated key_value → key + value
        var keyValueGroup = node.Children[0];
        var keyField = NodeToArrowField(keyValueGroup.Children[0]);
        // Key field must be non-nullable per Arrow spec
        keyField = new Apache.Arrow.Field(keyField.Name, keyField.DataType, nullable: false);

        Apache.Arrow.Field valueField;
        if (keyValueGroup.Children.Count > 1)
        {
            valueField = NodeToArrowField(keyValueGroup.Children[1]);
        }
        else
        {
            // Map with no value column — use null type (shouldn't happen often)
            valueField = new Apache.Arrow.Field("value", Apache.Arrow.Types.StringType.Default, nullable: true);
        }

        var mapType = new MapType(keyField, valueField);
        return new Apache.Arrow.Field(node.Name, mapType, nullable);
    }

    private static IArrowType LeafToArrowType(SchemaNode node)
    {
        var element = node.Element;

        // LogicalType → ConvertedType → PhysicalType fallthrough
        if (element.LogicalType != null)
        {
            // Build a temporary descriptor to reuse existing logic
            var desc = BuildTempDescriptor(node);
            var result = FromLogicalType(element.LogicalType, desc);
            if (result != null) return result;
        }

        if (element.ConvertedType.HasValue)
        {
            var desc = BuildTempDescriptor(node);
            var result = FromConvertedType(element.ConvertedType.Value, desc);
            if (result != null) return result;
        }

        return FromPhysicalType(BuildTempDescriptor(node));
    }

    private static ColumnDescriptor BuildTempDescriptor(SchemaNode node) => new()
    {
        Path = [node.Name],
        PhysicalType = node.Element.Type!.Value,
        TypeLength = node.Element.TypeLength,
        MaxDefinitionLevel = 0,
        MaxRepetitionLevel = 0,
        SchemaElement = node.Element,
        SchemaNode = node,
    };

    /// <summary>
    /// Converts a Parquet column's type information to an Arrow <see cref="IArrowType"/>.
    /// Falls through: LogicalType → ConvertedType → PhysicalType.
    /// </summary>
    public static IArrowType ToArrowType(ColumnDescriptor column)
    {
        var element = column.SchemaElement;

        // First: check LogicalType
        if (element.LogicalType != null)
        {
            var arrowType = FromLogicalType(element.LogicalType, column);
            if (arrowType != null)
                return arrowType;
        }

        // Second: check ConvertedType
        if (element.ConvertedType.HasValue)
        {
            var arrowType = FromConvertedType(element.ConvertedType.Value, column);
            if (arrowType != null)
                return arrowType;
        }

        // Third: fall back to PhysicalType
        return FromPhysicalType(column);
    }

    private static IArrowType? FromLogicalType(LogicalType logicalType, ColumnDescriptor column)
    {
        return logicalType switch
        {
            LogicalType.StringType => Apache.Arrow.Types.StringType.Default,
            LogicalType.DateType => Date32Type.Default,
            LogicalType.IntType intType => intType switch
            {
                { BitWidth: 8, IsSigned: true } => Int8Type.Default,
                { BitWidth: 8, IsSigned: false } => UInt8Type.Default,
                { BitWidth: 16, IsSigned: true } => Int16Type.Default,
                { BitWidth: 16, IsSigned: false } => UInt16Type.Default,
                { BitWidth: 32, IsSigned: true } => Int32Type.Default,
                { BitWidth: 32, IsSigned: false } => UInt32Type.Default,
                { BitWidth: 64, IsSigned: true } => Int64Type.Default,
                { BitWidth: 64, IsSigned: false } => UInt64Type.Default,
                _ => null,
            },
            LogicalType.TimestampType ts => new TimestampType(
                ts.Unit switch
                {
                    Metadata.TimeUnit.Millis => Apache.Arrow.Types.TimeUnit.Millisecond,
                    Metadata.TimeUnit.Micros => Apache.Arrow.Types.TimeUnit.Microsecond,
                    Metadata.TimeUnit.Nanos => Apache.Arrow.Types.TimeUnit.Nanosecond,
                    _ => Apache.Arrow.Types.TimeUnit.Microsecond,
                },
                ts.IsAdjustedToUtc ? TimeZoneInfo.Utc : null),
            LogicalType.TimeType time => new Time32Type(
                time.Unit switch
                {
                    Metadata.TimeUnit.Millis => Apache.Arrow.Types.TimeUnit.Millisecond,
                    _ => Apache.Arrow.Types.TimeUnit.Microsecond,
                }),
            LogicalType.EnumType => Apache.Arrow.Types.StringType.Default,
            LogicalType.JsonType => Apache.Arrow.Types.StringType.Default,
            LogicalType.UuidType => new FixedSizeBinaryType(16),
            LogicalType.Float16Type => HalfFloatType.Default,
            _ => null, // fall through to ConvertedType or PhysicalType
        };
    }

    private static IArrowType? FromConvertedType(ConvertedType convertedType, ColumnDescriptor column)
    {
        return convertedType switch
        {
            ConvertedType.Utf8 => Apache.Arrow.Types.StringType.Default,
            ConvertedType.Date => Date32Type.Default,
            ConvertedType.TimestampMillis => new TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, TimeZoneInfo.Utc),
            ConvertedType.TimestampMicros => new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, TimeZoneInfo.Utc),
            ConvertedType.TimeMillis => new Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond),
            ConvertedType.TimeMicros => new Time64Type(Apache.Arrow.Types.TimeUnit.Microsecond),
            ConvertedType.Int8 => Int8Type.Default,
            ConvertedType.Int16 => Int16Type.Default,
            ConvertedType.Int32 => Int32Type.Default,
            ConvertedType.Int64 => Int64Type.Default,
            ConvertedType.Uint8 => UInt8Type.Default,
            ConvertedType.Uint16 => UInt16Type.Default,
            ConvertedType.Uint32 => UInt32Type.Default,
            ConvertedType.Uint64 => UInt64Type.Default,
            ConvertedType.Enum => Apache.Arrow.Types.StringType.Default,
            ConvertedType.Json => Apache.Arrow.Types.StringType.Default,
            _ => null, // fall through to PhysicalType
        };
    }

    private static IArrowType FromPhysicalType(ColumnDescriptor column)
    {
        return column.PhysicalType switch
        {
            PhysicalType.Boolean => BooleanType.Default,
            PhysicalType.Int32 => Int32Type.Default,
            PhysicalType.Int64 => Int64Type.Default,
            PhysicalType.Float => FloatType.Default,
            PhysicalType.Double => DoubleType.Default,
            PhysicalType.ByteArray => BinaryType.Default,
            PhysicalType.FixedLenByteArray => new FixedSizeBinaryType(column.TypeLength ?? 0),
            PhysicalType.Int96 => new FixedSizeBinaryType(12),
            _ => throw new NotSupportedException(
                $"Unsupported physical type '{column.PhysicalType}' for Arrow conversion."),
        };
    }
}
