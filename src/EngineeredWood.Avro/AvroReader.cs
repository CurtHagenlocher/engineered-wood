using System.Collections;
using Apache.Arrow;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Reads Avro Object Container Files into Arrow RecordBatches.
/// </summary>
public sealed class AvroReader : IEnumerable<RecordBatch>, IDisposable
{
    private readonly OcfReader _ocf;
    private readonly RecordBatchAssembler _assembler;
    private readonly int _batchSize;

    /// <summary>The Arrow schema for all batches produced by this reader.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The Avro writer schema from the OCF header.</summary>
    public AvroSchema WriterSchema { get; }

    /// <summary>The compression codec declared in the OCF header.</summary>
    public AvroCodec Codec => _ocf.Codec;

    /// <summary>Arbitrary metadata from the OCF header.</summary>
    public IReadOnlyDictionary<string, byte[]> Metadata => _ocf.Metadata;

    internal AvroReader(OcfReader ocf, int batchSize)
    {
        _ocf = ocf;
        _batchSize = batchSize;

        var writerRecord = ocf.WriterSchema;
        Schema = ArrowSchemaConverter.ToArrow(writerRecord);
        WriterSchema = new AvroSchema(AvroSchemaWriter.ToJson(writerRecord));
        _assembler = new RecordBatchAssembler(writerRecord, Schema);
    }

    /// <summary>Read the next batch, or null on EOF.</summary>
    public RecordBatch? ReadNextBatch()
    {
        var block = _ocf.ReadBlock();
        if (block == null) return null;

        var (data, objectCount) = block.Value;
        return _assembler.DecodeBlock(data, checked((int)objectCount));
    }

    public IEnumerator<RecordBatch> GetEnumerator()
    {
        RecordBatch? batch;
        while ((batch = ReadNextBatch()) != null)
            yield return batch;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose() => _ocf.Dispose();
}
