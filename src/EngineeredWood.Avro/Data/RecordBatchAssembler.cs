using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Decodes Avro binary-encoded records into Arrow RecordBatches.
/// </summary>
internal sealed class RecordBatchAssembler
{
    private readonly AvroRecordSchema _schema;
    private readonly Apache.Arrow.Schema _arrowSchema;

    public RecordBatchAssembler(AvroRecordSchema schema, Apache.Arrow.Schema arrowSchema)
    {
        _schema = schema;
        _arrowSchema = arrowSchema;
    }

    /// <summary>
    /// Decodes up to <paramref name="maxRows"/> records from the data span.
    /// Returns the RecordBatch and the number of bytes consumed.
    /// </summary>
    public (RecordBatch batch, int bytesConsumed) Decode(ReadOnlySpan<byte> data, int maxRows)
    {
        var reader = new AvroBinaryReader(data);
        var builders = CreateBuilders(_arrowSchema);

        int rowCount = 0;
        for (int i = 0; i < maxRows && !reader.IsEmpty; i++)
        {
            DecodeRecord(ref reader, _schema, builders);
            rowCount++;
        }

        var arrays = BuildArrays(builders, _arrowSchema);
        var batch = new RecordBatch(_arrowSchema, arrays, rowCount);
        return (batch, reader.Position);
    }

    /// <summary>
    /// Decodes all records from a block (known object count).
    /// </summary>
    public RecordBatch DecodeBlock(ReadOnlySpan<byte> data, int objectCount)
    {
        var reader = new AvroBinaryReader(data);
        var builders = CreateBuilders(_arrowSchema);

        for (int i = 0; i < objectCount; i++)
            DecodeRecord(ref reader, _schema, builders);

        var arrays = BuildArrays(builders, _arrowSchema);
        return new RecordBatch(_arrowSchema, arrays, objectCount);
    }

    private static void DecodeRecord(ref AvroBinaryReader reader, AvroRecordSchema schema, IList<IColumnBuilder> builders)
    {
        for (int i = 0; i < schema.Fields.Count; i++)
            builders[i].Append(ref reader);
    }

    private static IList<IColumnBuilder> CreateBuilders(Apache.Arrow.Schema arrowSchema)
    {
        var builders = new List<IColumnBuilder>();
        foreach (var field in arrowSchema.FieldsList)
            builders.Add(CreateBuilder(field));
        return builders;
    }

    private static IColumnBuilder CreateBuilder(Field field)
    {
        return CreateBuilderForType(field.DataType, field.IsNullable);
    }

    private static IColumnBuilder CreateBuilderForType(IArrowType type, bool nullable)
    {
        if (nullable)
            return new NullableBuilder(CreateBuilderForType(type, false));

        return type switch
        {
            NullType => new NullBuilder(),
            BooleanType => new BooleanBuilder(),
            Int32Type => new Int32Builder(),
            Int64Type => new Int64Builder(),
            FloatType => new FloatBuilder(),
            DoubleType => new DoubleBuilder(),
            BinaryType => new BinaryBuilder(),
            StringType => new StringBuilder(),
            _ => throw new NotSupportedException($"Arrow type {type} not yet supported in Avro decoder."),
        };
    }

    private static IArrowArray[] BuildArrays(IList<IColumnBuilder> builders, Apache.Arrow.Schema schema)
    {
        var arrays = new IArrowArray[builders.Count];
        for (int i = 0; i < builders.Count; i++)
            arrays[i] = builders[i].Build(schema.FieldsList[i]);
        return arrays;
    }
}

/// <summary>Column builder interface for accumulating decoded Avro values.</summary>
internal interface IColumnBuilder
{
    void Append(ref AvroBinaryReader reader);
    void AppendNull();
    IArrowArray Build(Field field);
}

/// <summary>Wraps a non-nullable builder to handle ["null", T] unions.</summary>
internal sealed class NullableBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _inner;
    private readonly List<bool> _validity = new();

    public NullableBuilder(IColumnBuilder inner) => _inner = inner;

    public void Append(ref AvroBinaryReader reader)
    {
        int branchIndex = reader.ReadUnionIndex();
        if (branchIndex == 0) // null branch (convention: ["null", T])
        {
            _inner.AppendNull();
            _validity.Add(false);
        }
        else
        {
            _inner.Append(ref reader);
            _validity.Add(true);
        }
    }

    public void AppendNull()
    {
        _inner.AppendNull();
        _validity.Add(false);
    }

    public IArrowArray Build(Field field) => _inner.Build(field);
}

internal sealed class NullBuilder : IColumnBuilder
{
    private int _count;
    public void Append(ref AvroBinaryReader reader) => _count++;
    public void AppendNull() => _count++;
    public IArrowArray Build(Field field) => new NullArray(_count);
}

internal sealed class BooleanBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.BooleanArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadBoolean());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Int32Builder : IColumnBuilder
{
    private readonly Apache.Arrow.Int32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Int64Builder : IColumnBuilder
{
    private readonly Apache.Arrow.Int64Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class FloatBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadFloat());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class DoubleBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadDouble());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class BinaryBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.BinaryArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadBytes());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class StringBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.StringArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadString());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}
