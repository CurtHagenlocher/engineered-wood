using System.Text.Json;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Appends Avro default values (from JSON) into column builders.
/// </summary>
internal static class DefaultValueApplicator
{
    /// <summary>
    /// Appends a default value to a builder based on the schema type and JSON element.
    /// </summary>
    public static void AppendDefault(IColumnBuilder builder, JsonElement defaultValue, AvroSchemaNode schema)
    {
        // Avro spec: for unions, the default must match the first branch type.
        // A null default for a nullable union means AppendNull.
        if (schema is AvroUnionSchema union)
        {
            if (defaultValue.ValueKind == JsonValueKind.Null)
            {
                builder.AppendNull();
                return;
            }
            // For nullable unions, the default type corresponds to the first branch
            // (which is the non-null branch if null isn't first).
            // The NullableBuilder handles the dispatch, but for defaults we apply directly.
            AppendPrimitiveDefault(builder, defaultValue, union.Branches[0]);
            return;
        }

        AppendPrimitiveDefault(builder, defaultValue, schema);
    }

    private static void AppendPrimitiveDefault(
        IColumnBuilder builder, JsonElement defaultValue, AvroSchemaNode schema)
    {
        switch (schema.Type)
        {
            case AvroType.Null:
                builder.AppendNull();
                break;

            case AvroType.Boolean:
                if (builder is BooleanBuilder bb)
                    bb.AppendDefault(defaultValue.GetBoolean());
                else
                    builder.AppendNull(); // fallback
                break;

            case AvroType.Int:
                if (builder is Int32Builder ib)
                    ib.AppendDefault(defaultValue.GetInt32());
                else if (builder is Date32Builder db)
                    db.AppendDefault(defaultValue.GetInt32());
                else
                    builder.AppendNull();
                break;

            case AvroType.Long:
                if (builder is Int64Builder lb)
                    lb.AppendDefault(defaultValue.GetInt64());
                else
                    builder.AppendNull();
                break;

            case AvroType.Float:
                if (builder is FloatBuilder fb)
                    fb.AppendDefault(defaultValue.GetSingle());
                else
                    builder.AppendNull();
                break;

            case AvroType.Double:
                if (builder is DoubleBuilder dob)
                    dob.AppendDefault(defaultValue.GetDouble());
                else
                    builder.AppendNull();
                break;

            case AvroType.String:
                if (builder is Data.StringBuilder sb)
                    sb.AppendDefault(defaultValue.GetString()!);
                else
                    builder.AppendNull();
                break;

            case AvroType.Bytes:
                if (builder is BinaryBuilder binb)
                    binb.AppendDefault(defaultValue.GetBytesFromBase64());
                else
                    builder.AppendNull();
                break;

            case AvroType.Enum:
                if (builder is EnumBuilder eb && schema is AvroEnumSchema enumSchema)
                {
                    var symbolName = defaultValue.GetString()!;
                    int idx = ((IList<string>)enumSchema.Symbols).IndexOf(symbolName);
                    if (idx < 0)
                        throw new InvalidOperationException(
                            $"Default enum value '{symbolName}' not found in symbols.");
                    eb.AppendDefault(idx);
                }
                else
                    builder.AppendNull();
                break;

            default:
                // For complex types (arrays, maps, records), null is the safest fallback
                builder.AppendNull();
                break;
        }
    }
}
