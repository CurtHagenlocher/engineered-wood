using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Represents a parsed Avro schema (JSON). This is the library's fundamental schema type.
/// </summary>
public sealed class AvroSchema
{
    private readonly string _json;
    private AvroSchemaNode? _parsed;

    /// <summary>The raw JSON text of the Avro schema.</summary>
    public string Json => _json;

    public AvroSchema(string json)
    {
        _json = json ?? throw new ArgumentNullException(nameof(json));
    }

    internal AvroSchemaNode Parsed => _parsed ??= AvroSchemaParser.Parse(_json);

    /// <summary>Convert this Avro schema to an equivalent Arrow schema.</summary>
    public Apache.Arrow.Schema ToArrowSchema()
    {
        if (Parsed is AvroRecordSchema record)
            return ArrowSchemaConverter.ToArrow(record);
        throw new InvalidOperationException("Only record schemas can be converted to Arrow schemas.");
    }

    /// <summary>Create an Avro schema from an Arrow schema.</summary>
    public static AvroSchema FromArrowSchema(
        Apache.Arrow.Schema schema,
        string recordName = "Record",
        string? recordNamespace = null)
    {
        var avroRecord = ArrowSchemaConverter.FromArrow(schema, recordName, recordNamespace);
        var json = AvroSchemaWriter.ToJson(avroRecord);
        return new AvroSchema(json);
    }
}
