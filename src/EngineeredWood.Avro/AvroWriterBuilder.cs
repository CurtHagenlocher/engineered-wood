using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Fluent builder that configures and constructs Avro writers.
/// </summary>
public sealed class AvroWriterBuilder
{
    private readonly Apache.Arrow.Schema _arrowSchema;
    private AvroCodec _codec = AvroCodec.Null;
    private string _recordName = "Record";
    private string? _recordNamespace;
    private AvroSchema? _explicitAvroSchema;

    public AvroWriterBuilder(Apache.Arrow.Schema arrowSchema)
    {
        _arrowSchema = arrowSchema ?? throw new ArgumentNullException(nameof(arrowSchema));
    }

    /// <summary>Use an explicit Avro schema instead of auto-converting from Arrow.</summary>
    public AvroWriterBuilder WithAvroSchema(AvroSchema schema)
    {
        _explicitAvroSchema = schema;
        return this;
    }

    /// <summary>Set the Avro record name and optional namespace.</summary>
    public AvroWriterBuilder WithRecordName(string name, string? ns = null)
    {
        _recordName = name;
        _recordNamespace = ns;
        return this;
    }

    /// <summary>OCF block compression codec (default: Null / no compression).</summary>
    public AvroWriterBuilder WithCompression(AvroCodec codec)
    {
        _codec = codec;
        return this;
    }

    /// <summary>Build a synchronous OCF writer targeting the given stream.</summary>
    public AvroWriter Build(Stream output)
    {
        AvroRecordSchema avroRecord;
        if (_explicitAvroSchema != null)
        {
            if (_explicitAvroSchema.Parsed is not AvroRecordSchema r)
                throw new InvalidOperationException("Explicit Avro schema must be a record.");
            avroRecord = r;
        }
        else
        {
            avroRecord = ArrowSchemaConverter.FromArrow(_arrowSchema, _recordName, _recordNamespace);
        }

        var ocf = new OcfWriter(output, _codec);
        return new AvroWriter(ocf, _arrowSchema, avroRecord);
    }
}
