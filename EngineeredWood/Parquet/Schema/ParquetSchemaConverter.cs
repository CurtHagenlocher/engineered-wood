using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// Converts an Arrow Schema to Parquet SchemaElements (flattened, pre-order traversal).
/// This is the reverse of <see cref="Data.ArrowSchemaConverter"/> which converts Parquet → Arrow.
/// </summary>
internal static class ParquetSchemaConverter
{
    /// <summary>
    /// Converts an Arrow <see cref="Apache.Arrow.Schema"/> to a flat list of <see cref="SchemaElement"/>
    /// suitable for the Parquet file footer.
    /// </summary>
    public static IReadOnlyList<SchemaElement> ToParquetSchema(Apache.Arrow.Schema arrowSchema)
    {
        var elements = new List<SchemaElement>();

        // Root schema element (message)
        elements.Add(new SchemaElement
        {
            Name = "schema",
            NumChildren = arrowSchema.FieldsList.Count,
            RepetitionType = null,
        });

        foreach (var field in arrowSchema.FieldsList)
        {
            elements.Add(ToSchemaElement(field));
        }

        return elements;
    }

    /// <summary>
    /// Converts a single Arrow field to a Parquet SchemaElement.
    /// </summary>
    public static SchemaElement ToSchemaElement(Field field)
    {
        var (physicalType, logicalType, convertedType, typeLength, scale, precision) = MapArrowType(field.DataType);

        return new SchemaElement
        {
            Name = field.Name,
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = field.IsNullable ? FieldRepetitionType.Optional : FieldRepetitionType.Required,
            LogicalType = logicalType,
            ConvertedType = convertedType,
            Scale = scale,
            Precision = precision,
        };
    }

    /// <summary>
    /// Builds a list of <see cref="ColumnDescriptor"/> from the schema elements.
    /// </summary>
    public static IReadOnlyList<ColumnDescriptor> BuildColumnDescriptors(
        IReadOnlyList<SchemaElement> schema)
    {
        var descriptors = new List<ColumnDescriptor>();

        // Skip the root element (index 0)
        for (int i = 1; i < schema.Count; i++)
        {
            var element = schema[i];
            if (!element.Type.HasValue) continue; // skip group nodes

            var node = new SchemaNode
            {
                Element = element,
                Children = [],
            };

            descriptors.Add(new ColumnDescriptor
            {
                Path = [element.Name],
                PhysicalType = element.Type.Value,
                TypeLength = element.TypeLength,
                MaxDefinitionLevel = element.RepetitionType == FieldRepetitionType.Optional ? 1 : 0,
                MaxRepetitionLevel = 0,
                SchemaElement = element,
                SchemaNode = node,
            });
        }

        return descriptors;
    }

    private static (PhysicalType physical, LogicalType? logical, ConvertedType? converted,
        int? typeLength, int? scale, int? precision)
        MapArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            BooleanType => (PhysicalType.Boolean, null, null, null, null, null),

            Int8Type => (PhysicalType.Int32, new LogicalType.IntType(8, true),
                ConvertedType.Int8, null, null, null),
            Int16Type => (PhysicalType.Int32, new LogicalType.IntType(16, true),
                ConvertedType.Int16, null, null, null),
            Int32Type => (PhysicalType.Int32, null, null, null, null, null),
            Int64Type => (PhysicalType.Int64, null, null, null, null, null),

            UInt8Type => (PhysicalType.Int32, new LogicalType.IntType(8, false),
                ConvertedType.Uint8, null, null, null),
            UInt16Type => (PhysicalType.Int32, new LogicalType.IntType(16, false),
                ConvertedType.Uint16, null, null, null),
            UInt32Type => (PhysicalType.Int32, new LogicalType.IntType(32, false),
                ConvertedType.Uint32, null, null, null),
            UInt64Type => (PhysicalType.Int64, new LogicalType.IntType(64, false),
                ConvertedType.Uint64, null, null, null),

            FloatType => (PhysicalType.Float, null, null, null, null, null),
            DoubleType => (PhysicalType.Double, null, null, null, null, null),
            HalfFloatType => (PhysicalType.FixedLenByteArray, new LogicalType.Float16Type(),
                null, 2, null, null),

            StringType => (PhysicalType.ByteArray, new LogicalType.StringType(),
                ConvertedType.Utf8, null, null, null),
            BinaryType => (PhysicalType.ByteArray, null, null, null, null, null),

            Date32Type => (PhysicalType.Int32, new LogicalType.DateType(),
                ConvertedType.Date, null, null, null),

            Time32Type t32 => (PhysicalType.Int32,
                new LogicalType.TimeType(true, MapTimeUnit(t32.Unit)),
                t32.Unit == Apache.Arrow.Types.TimeUnit.Millisecond ? ConvertedType.TimeMillis : null,
                null, null, null),

            Time64Type t64 => (PhysicalType.Int64,
                new LogicalType.TimeType(true, MapTimeUnit(t64.Unit)),
                t64.Unit == Apache.Arrow.Types.TimeUnit.Microsecond ? ConvertedType.TimeMicros : null,
                null, null, null),

            TimestampType ts => (PhysicalType.Int64,
                new LogicalType.TimestampType(ts.Timezone != null, MapTimeUnit(ts.Unit)),
                ts.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Millisecond => ConvertedType.TimestampMillis,
                    Apache.Arrow.Types.TimeUnit.Microsecond => ConvertedType.TimestampMicros,
                    _ => null,
                },
                null, null, null),

            Decimal128Type d128 => (
                d128.Precision <= 9 ? PhysicalType.Int32 :
                d128.Precision <= 18 ? PhysicalType.Int64 :
                PhysicalType.FixedLenByteArray,
                new LogicalType.DecimalType(d128.Scale, d128.Precision),
                ConvertedType.Decimal,
                d128.Precision <= 18 ? null : 16,
                d128.Scale, d128.Precision),

            Decimal256Type d256 => (PhysicalType.FixedLenByteArray,
                new LogicalType.DecimalType(d256.Scale, d256.Precision),
                ConvertedType.Decimal,
                32, d256.Scale, d256.Precision),

            FixedSizeBinaryType fsb => (PhysicalType.FixedLenByteArray, null, null,
                fsb.ByteWidth, null, null),

            NullType => (PhysicalType.Int32,
                new LogicalType.UnknownLogicalType(11),
                null, null, null, null),

            _ => throw new NotSupportedException($"Arrow type '{arrowType.Name}' is not supported for Parquet writing."),
        };
    }

    private static Metadata.TimeUnit MapTimeUnit(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Millisecond => Metadata.TimeUnit.Millis,
        Apache.Arrow.Types.TimeUnit.Microsecond => Metadata.TimeUnit.Micros,
        Apache.Arrow.Types.TimeUnit.Nanosecond => Metadata.TimeUnit.Nanos,
        _ => Metadata.TimeUnit.Millis,
    };
}
