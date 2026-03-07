using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Converts an Arrow <see cref="Schema"/> to a flat list of Parquet <see cref="SchemaElement"/>s.
/// Reverse of <see cref="ArrowSchemaConverter"/>.
/// </summary>
internal static class ArrowToSchemaConverter
{
    /// <summary>
    /// Converts an Arrow schema to a flat list of Parquet schema elements
    /// (pre-order traversal with root "schema" element).
    /// </summary>
    public static IReadOnlyList<SchemaElement> Convert(Apache.Arrow.Schema arrowSchema)
    {
        var elements = new List<SchemaElement>();

        // Root schema element
        elements.Add(new SchemaElement
        {
            Name = "schema",
            NumChildren = arrowSchema.FieldsList.Count,
        });

        foreach (var field in arrowSchema.FieldsList)
            AddField(elements, field);

        return elements;
    }

    private static void AddField(List<SchemaElement> elements, Field field)
    {
        var repetition = field.IsNullable
            ? FieldRepetitionType.Optional
            : FieldRepetitionType.Required;

        switch (field.DataType)
        {
            case StructType structType:
                AddStructField(elements, field.Name, repetition, structType);
                return;

            case ListType listType:
                AddListField(elements, field.Name, repetition, listType);
                return;

            case MapType mapType:
                AddMapField(elements, field.Name, repetition, mapType);
                return;
        }

        var (physicalType, typeLength, logicalType, convertedType, scale, precision) =
            MapArrowType(field.DataType);

        elements.Add(new SchemaElement
        {
            Name = field.Name,
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = repetition,
            LogicalType = logicalType,
            ConvertedType = convertedType,
            Scale = scale,
            Precision = precision,
        });
    }

    private static void AddStructField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, StructType structType)
    {
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = structType.Fields.Count,
        });

        foreach (var child in structType.Fields)
            AddField(elements, child);
    }

    private static void AddListField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, ListType listType)
    {
        // 3-level encoding: optional/required group (LIST) → repeated group "list" → element
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = 1,
            LogicalType = new LogicalType.ListType(),
            ConvertedType = ConvertedType.List,
        });

        // Repeated group "list" with one child
        elements.Add(new SchemaElement
        {
            Name = "list",
            RepetitionType = FieldRepetitionType.Repeated,
            NumChildren = 1,
        });

        // Element field
        var elementField = listType.ValueField;
        AddField(elements, elementField);
    }

    private static void AddMapField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, MapType mapType)
    {
        // optional/required group (MAP) → repeated group "key_value" → key + value
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = 1,
            LogicalType = new LogicalType.MapType(),
            ConvertedType = ConvertedType.Map,
        });

        // Repeated group "key_value" with 2 children
        elements.Add(new SchemaElement
        {
            Name = "key_value",
            RepetitionType = FieldRepetitionType.Repeated,
            NumChildren = 2,
        });

        // Key field (always required)
        var keyField = new Field(mapType.KeyField.Name, mapType.KeyField.DataType, nullable: false);
        AddField(elements, keyField);

        // Value field
        AddField(elements, mapType.ValueField);
    }

    /// <summary>
    /// Maps an Arrow type to Parquet physical type and annotations.
    /// </summary>
    internal static (
        PhysicalType PhysicalType,
        int? TypeLength,
        LogicalType? LogicalType,
        ConvertedType? ConvertedType,
        int? Scale,
        int? Precision)
        MapArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            BooleanType => (PhysicalType.Boolean, null, null, null, null, null),

            Int8Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(8, true), ConvertedType.Int8, null, null),
            Int16Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(16, true), ConvertedType.Int16, null, null),
            Int32Type => (PhysicalType.Int32, null, null, null, null, null),
            Int64Type => (PhysicalType.Int64, null, null, null, null, null),

            UInt8Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(8, false), ConvertedType.Uint8, null, null),
            UInt16Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(16, false), ConvertedType.Uint16, null, null),
            UInt32Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(32, false), ConvertedType.Uint32, null, null),
            UInt64Type => (PhysicalType.Int64, null,
                new LogicalType.IntType(64, false), ConvertedType.Uint64, null, null),

            FloatType => (PhysicalType.Float, null, null, null, null, null),
            DoubleType => (PhysicalType.Double, null, null, null, null, null),

            Apache.Arrow.Types.StringType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            LargeStringType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            StringViewType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            BinaryType => (PhysicalType.ByteArray, null, null, null, null, null),
            LargeBinaryType => (PhysicalType.ByteArray, null, null, null, null, null),
            BinaryViewType => (PhysicalType.ByteArray, null, null, null, null, null),

            Date32Type => (PhysicalType.Int32, null,
                new LogicalType.DateType(), ConvertedType.Date, null, null),

            TimestampType ts => (PhysicalType.Int64, null,
                new LogicalType.TimestampType(
                    ts.Timezone != null,
                    MapTimeUnit(ts.Unit)),
                ts.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Millisecond => ConvertedType.TimestampMillis,
                    _ => ConvertedType.TimestampMicros,
                },
                null, null),

            Time32Type t32 => (PhysicalType.Int32, null,
                new LogicalType.TimeType(false, MapTimeUnit(t32.Unit)),
                ConvertedType.TimeMillis,
                null, null),

            Time64Type t64 => (PhysicalType.Int64, null,
                new LogicalType.TimeType(false, MapTimeUnit(t64.Unit)),
                ConvertedType.TimeMicros,
                null, null),

            HalfFloatType => (PhysicalType.FixedLenByteArray, 2,
                new LogicalType.Float16Type(), null, null, null),

            // Decimal types: order matters — more specific before FixedSizeBinaryType
            Decimal32Type d32 => (PhysicalType.Int32, null,
                new LogicalType.DecimalType(d32.Scale, d32.Precision),
                ConvertedType.Decimal, d32.Scale, d32.Precision),
            Decimal64Type d64 => (PhysicalType.Int64, null,
                new LogicalType.DecimalType(d64.Scale, d64.Precision),
                ConvertedType.Decimal, d64.Scale, d64.Precision),
            Decimal128Type d128 => (PhysicalType.FixedLenByteArray, 16,
                new LogicalType.DecimalType(d128.Scale, d128.Precision),
                ConvertedType.Decimal, d128.Scale, d128.Precision),
            Decimal256Type d256 => (PhysicalType.FixedLenByteArray, 32,
                new LogicalType.DecimalType(d256.Scale, d256.Precision),
                ConvertedType.Decimal, d256.Scale, d256.Precision),

            FixedSizeBinaryType fsb => (PhysicalType.FixedLenByteArray, fsb.ByteWidth,
                null, null, null, null),

            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for Parquet writing."),
        };
    }

    private static Metadata.TimeUnit MapTimeUnit(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Millisecond => Metadata.TimeUnit.Millis,
        Apache.Arrow.Types.TimeUnit.Microsecond => Metadata.TimeUnit.Micros,
        Apache.Arrow.Types.TimeUnit.Nanosecond => Metadata.TimeUnit.Nanos,
        _ => Metadata.TimeUnit.Micros,
    };
}
