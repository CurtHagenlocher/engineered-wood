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
