using EngineeredWood.Avro.Container;

namespace EngineeredWood.Avro;

/// <summary>
/// Fluent builder that configures and constructs Avro readers.
/// </summary>
public sealed class AvroReaderBuilder
{
    private int _batchSize = 1024;

    /// <summary>Maximum rows per RecordBatch (default: 1024).</summary>
    public AvroReaderBuilder WithBatchSize(int batchSize)
    {
        _batchSize = batchSize;
        return this;
    }

    /// <summary>Build a synchronous OCF reader.</summary>
    public AvroReader Build(Stream input)
    {
        var ocf = OcfReader.Open(input);
        return new AvroReader(ocf, _batchSize);
    }
}
