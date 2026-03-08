using System.Runtime.InteropServices;
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
    private readonly AvroRecordSchema _writerSchema;
    private readonly Apache.Arrow.Schema _arrowSchema;
    private readonly SchemaResolution? _resolution;

    public RecordBatchAssembler(AvroRecordSchema schema, Apache.Arrow.Schema arrowSchema)
    {
        _writerSchema = schema;
        _arrowSchema = arrowSchema;
    }

    public RecordBatchAssembler(AvroRecordSchema writerSchema, SchemaResolution resolution)
    {
        _writerSchema = writerSchema;
        _arrowSchema = resolution.ArrowSchema;
        _resolution = resolution;
    }

    /// <summary>
    /// Decodes up to <paramref name="maxRows"/> records from the data span.
    /// Returns the RecordBatch and the number of bytes consumed.
    /// </summary>
    public (RecordBatch batch, int bytesConsumed) Decode(ReadOnlySpan<byte> data, int maxRows)
    {
        var reader = new AvroBinaryReader(data);
        var effectiveSchema = _resolution?.ArrowSchema ?? _arrowSchema;
        var builders = CreateBuildersForDecode();

        int rowCount = 0;
        for (int i = 0; i < maxRows && !reader.IsEmpty; i++)
        {
            DecodeRecordDispatch(ref reader, builders);
            rowCount++;
        }

        var arrays = BuildArrays(builders, effectiveSchema);
        var batch = new RecordBatch(effectiveSchema, arrays, rowCount);
        return (batch, reader.Position);
    }

    /// <summary>
    /// Decodes all records from a block (known object count).
    /// </summary>
    public RecordBatch DecodeBlock(ReadOnlySpan<byte> data, int objectCount)
    {
        var reader = new AvroBinaryReader(data);
        var effectiveSchema = _resolution?.ArrowSchema ?? _arrowSchema;
        var builders = CreateBuildersForDecode();

        for (int i = 0; i < objectCount; i++)
            DecodeRecordDispatch(ref reader, builders);

        var arrays = BuildArrays(builders, effectiveSchema);
        return new RecordBatch(effectiveSchema, arrays, objectCount);
    }

    private IList<IColumnBuilder> CreateBuildersForDecode()
    {
        if (_resolution != null)
            return CreateResolvedBuilders(_writerSchema, _resolution);
        return CreateBuilders(_writerSchema, _arrowSchema);
    }

    private void DecodeRecordDispatch(ref AvroBinaryReader reader, IList<IColumnBuilder> builders)
    {
        if (_resolution != null)
            DecodeRecordWithResolution(ref reader, _writerSchema, _resolution, builders);
        else
            DecodeRecord(ref reader, _writerSchema, builders);
    }

    private static void DecodeRecord(ref AvroBinaryReader reader, AvroRecordSchema schema, IList<IColumnBuilder> builders)
    {
        for (int i = 0; i < schema.Fields.Count; i++)
            builders[i].Append(ref reader);
    }

    /// <summary>
    /// Decodes a record using schema resolution: iterates writer fields, skips unmatched,
    /// dispatches to reader builders, and fills defaults for missing reader fields.
    /// </summary>
    private static void DecodeRecordWithResolution(
        ref AvroBinaryReader reader,
        AvroRecordSchema writerSchema,
        SchemaResolution resolution,
        IList<IColumnBuilder> builders)
    {
        // builders are indexed by reader field index.
        for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
        {
            var action = resolution.WriterActions[wi];
            if (action.Skip)
            {
                reader.Skip(writerSchema.Fields[wi].Schema);
            }
            else
            {
                builders[action.ReaderFieldIndex].Append(ref reader);
            }
        }

        // Fill defaults for reader fields not in writer
        foreach (var df in resolution.DefaultFields)
        {
            DefaultValueApplicator.AppendDefault(builders[df.ReaderIndex], df.DefaultValue, df.Schema);
        }
    }

    /// <summary>
    /// Creates builders for resolved schema: one builder per reader field,
    /// using promoting builders where type promotion is needed.
    /// </summary>
    private static IList<IColumnBuilder> CreateResolvedBuilders(
        AvroRecordSchema writerSchema, SchemaResolution resolution)
    {
        var readerSchema = resolution.ReaderSchema;
        var arrowSchema = resolution.ArrowSchema;
        var builders = new IColumnBuilder[readerSchema.Fields.Count];

        for (int ri = 0; ri < readerSchema.Fields.Count; ri++)
        {
            var field = arrowSchema.FieldsList[ri];
            var readerFieldSchema = readerSchema.Fields[ri].Schema;
            var promotion = resolution.Promotions[ri];

            if (promotion != null)
            {
                builders[ri] = CreatePromotingBuilder(field, readerFieldSchema, promotion);
            }
            else
            {
                // Find the writer field that maps to this reader field, if any
                AvroSchemaNode avroSchemaForBuilder = readerFieldSchema;
                for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
                {
                    if (!resolution.WriterActions[wi].Skip &&
                        resolution.WriterActions[wi].ReaderFieldIndex == ri)
                    {
                        avroSchemaForBuilder = writerSchema.Fields[wi].Schema;
                        break;
                    }
                }
                builders[ri] = CreateBuilder(field, avroSchemaForBuilder);
            }
        }

        return builders;
    }

    /// <summary>
    /// Creates a builder that reads using writer encoding but stores in reader type.
    /// </summary>
    private static IColumnBuilder CreatePromotingBuilder(
        Field field, AvroSchemaNode readerSchema, TypePromotion promotion)
    {
        // Handle nullable wrapper
        bool isNullable = readerSchema is AvroUnionSchema union && union.IsNullable(out _, out _);
        int nullIndex = 0;
        if (readerSchema is AvroUnionSchema u && u.IsNullable(out _, out int ni))
            nullIndex = ni;

        IColumnBuilder inner = promotion.Kind switch
        {
            PromotionKind.IntToLong => new PromotingIntToLongBuilder(),
            PromotionKind.IntToFloat => new PromotingIntToFloatBuilder(),
            PromotionKind.IntToDouble => new PromotingIntToDoubleBuilder(),
            PromotionKind.LongToFloat => new PromotingLongToFloatBuilder(),
            PromotionKind.LongToDouble => new PromotingLongToDoubleBuilder(),
            PromotionKind.FloatToDouble => new PromotingFloatToDoubleBuilder(),
            PromotionKind.StringToBytes => new PromotingStringToBytesBuilder(),
            PromotionKind.BytesToString => new PromotingBytesToStringBuilder(),
            _ => CreateBuilder(field, readerSchema), // Nested record etc. — use standard builder
        };

        if (isNullable)
            return new NullableBuilder(inner, nullIndex);
        return inner;
    }

    private static IList<IColumnBuilder> CreateBuilders(AvroRecordSchema avroSchema, Apache.Arrow.Schema arrowSchema)
    {
        var builders = new List<IColumnBuilder>();
        for (int i = 0; i < arrowSchema.FieldsList.Count; i++)
        {
            var field = arrowSchema.FieldsList[i];
            var avroFieldSchema = avroSchema.Fields[i].Schema;
            builders.Add(CreateBuilder(field, avroFieldSchema));
        }
        return builders;
    }

    private static IColumnBuilder CreateBuilder(Field field, AvroSchemaNode avroSchema)
    {
        // Handle nullable unions
        if (avroSchema is AvroUnionSchema union && union.IsNullable(out var inner, out int nullIndex))
            return new NullableBuilder(CreateBuilderForType(field.DataType, inner), nullIndex);

        // Handle general unions → DenseUnion
        if (avroSchema is AvroUnionSchema generalUnion)
        {
            var unionType = (UnionType)field.DataType;
            var branchBuilders = new IColumnBuilder[generalUnion.Branches.Count];
            for (int i = 0; i < generalUnion.Branches.Count; i++)
            {
                var branchField = unionType.Fields[i];
                branchBuilders[i] = CreateBuilderForType(branchField.DataType, generalUnion.Branches[i]);
            }
            return new DenseUnionBuilder(generalUnion, branchBuilders);
        }

        return CreateBuilderForType(field.DataType, avroSchema);
    }

    private static IColumnBuilder CreateBuilderForType(IArrowType type, AvroSchemaNode avroSchema)
    {
        // Check for logical types on primitives
        if (avroSchema is AvroPrimitiveSchema { LogicalType: not null } p)
        {
            return p.LogicalType switch
            {
                "date" => new Date32Builder(),
                "time-millis" => new Time32MillisBuilder(),
                "time-micros" => new Time64MicrosBuilder(),
                "timestamp-millis" or "local-timestamp-millis" => new TimestampBuilder(),
                "timestamp-micros" or "local-timestamp-micros" => new TimestampBuilder(),
                "timestamp-nanos" or "local-timestamp-nanos" => new TimestampBuilder(),
                "decimal" => new DecimalBytesBuilder(p.Precision ?? 38, p.Scale ?? 0),
                "uuid" => new StringBuilder(), // UUID is a string logical type
                _ => CreateBuilderForBaseType(type, avroSchema),
            };
        }

        return CreateBuilderForBaseType(type, avroSchema);
    }

    private static IColumnBuilder CreateBuilderForBaseType(IArrowType type, AvroSchemaNode avroSchema)
    {
        switch (avroSchema)
        {
            case AvroPrimitiveSchema { Type: AvroType.Null }:
                return new NullBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Boolean }:
                return new BooleanBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Int }:
                return new Int32Builder();
            case AvroPrimitiveSchema { Type: AvroType.Long }:
                return new Int64Builder();
            case AvroPrimitiveSchema { Type: AvroType.Float }:
                return new FloatBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Double }:
                return new DoubleBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Bytes }:
                return new BinaryBuilder();
            case AvroPrimitiveSchema { Type: AvroType.String }:
                return new StringBuilder();

            case AvroEnumSchema e:
                return new EnumBuilder(e.Symbols);

            case AvroFixedSchema f when f.LogicalType == "decimal":
                return new DecimalFixedBuilder(f.Size, f.Precision ?? 38, f.Scale ?? 0);
            case AvroFixedSchema f:
                return new FixedBuilder(f.Size);

            case AvroArraySchema a:
            {
                var (itemArrowType, itemNullable) = ArrowSchemaConverter.ToArrowType(a.Items);
                var itemBuilder = CreateBuilder(
                    new Field("item", itemArrowType, itemNullable), a.Items);
                return new ArrayBuilder(itemBuilder);
            }

            case AvroMapSchema m:
            {
                var (valArrowType, valNullable) = ArrowSchemaConverter.ToArrowType(m.Values);
                var valBuilder = CreateBuilder(
                    new Field("value", valArrowType, valNullable), m.Values);
                return new MapBuilder(valBuilder);
            }

            case AvroRecordSchema r:
            {
                var childBuilders = new List<(string name, IColumnBuilder builder)>();
                foreach (var f in r.Fields)
                {
                    var (ft, fn) = ArrowSchemaConverter.ToArrowType(f.Schema);
                    childBuilders.Add((f.Name, CreateBuilder(new Field(f.Name, ft, fn), f.Schema)));
                }
                return new StructBuilder(childBuilders);
            }

            default:
                throw new NotSupportedException(
                    $"Avro schema type {avroSchema.Type} not yet supported in decoder.");
        }
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
    private readonly int _nullIndex;

    public NullableBuilder(IColumnBuilder inner, int nullIndex = 0)
    {
        _inner = inner;
        _nullIndex = nullIndex;
    }

    public void Append(ref AvroBinaryReader reader)
    {
        int branchIndex = reader.ReadUnionIndex();
        if (branchIndex == _nullIndex)
        {
            _inner.AppendNull();
        }
        else
        {
            _inner.Append(ref reader);
        }
    }

    public void AppendNull() => _inner.AppendNull();
    public IArrowArray Build(Field field) => _inner.Build(field);
}

// ─── Primitive builders ───

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
    public void AppendDefault(bool value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Int32Builder : IColumnBuilder
{
    private readonly Apache.Arrow.Int32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Int64Builder : IColumnBuilder
{
    private readonly Apache.Arrow.Int64Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(long value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class FloatBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadFloat());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(float value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class DoubleBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadDouble());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(double value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class BinaryBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.BinaryArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadBytes());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(byte[] value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class StringBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.StringArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadString());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(string value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
}

// ─── Logical type builders ───

internal sealed class Date32Builder : IColumnBuilder
{
    private static readonly DateOnly Epoch = new(1970, 1, 1);
    private readonly Apache.Arrow.Date32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader)
        => _builder.Append(Epoch.AddDays(reader.ReadInt()));
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int daysSinceEpoch) => _builder.Append(Epoch.AddDays(daysSinceEpoch));
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Time32MillisBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.Time32Array.Builder _builder = new(new Time32Type(TimeUnit.Millisecond));
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class Time64MicrosBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.Time64Array.Builder _builder = new(new Time64Type(TimeUnit.Microsecond));
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

internal sealed class TimestampBuilder : IColumnBuilder
{
    private readonly List<long> _values = new();
    private readonly List<bool> _validity = new();
    public void Append(ref AvroBinaryReader reader) { _values.Add(reader.ReadLong()); _validity.Add(true); }
    public void AppendNull() { _values.Add(0); _validity.Add(false); }
    public IArrowArray Build(Field field)
    {
        int count = _values.Count;
        var valueBuffer = new byte[count * 8];
        for (int i = 0; i < count; i++)
            BitConverter.TryWriteBytes(valueBuffer.AsSpan(i * 8), _values[i]);

        int nullBitmapLength = (count + 7) / 8;
        var nullBitmap = new byte[nullBitmapLength];
        int nullCount = 0;
        for (int i = 0; i < count; i++)
        {
            if (_validity[i])
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            else
                nullCount++;
        }

        var data = new ArrayData(field.DataType, count, nullCount,
            0, [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBuffer)]);
        return ArrowArrayFactory.BuildArray(data);
    }
}

// ─── Complex type builders ───

internal sealed class EnumBuilder : IColumnBuilder
{
    private readonly IReadOnlyList<string> _symbols;
    private readonly List<int> _indices = new();
    private readonly List<bool> _validity = new();

    public EnumBuilder(IReadOnlyList<string> symbols) => _symbols = symbols;

    public void Append(ref AvroBinaryReader reader)
    {
        _indices.Add(reader.ReadInt());
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _indices.Add(0);
        _validity.Add(false);
    }

    public void AppendDefault(int index)
    {
        _indices.Add(index);
        _validity.Add(true);
    }

    public IArrowArray Build(Field field)
    {
        // Build the dictionary (all symbols)
        var dictBuilder = new StringArray.Builder();
        foreach (var sym in _symbols)
            dictBuilder.Append(sym);
        var dictionary = dictBuilder.Build();

        // Build the indices
        var indexBuilder = new Int32Array.Builder();
        for (int i = 0; i < _indices.Count; i++)
        {
            if (_validity[i]) indexBuilder.Append(_indices[i]);
            else indexBuilder.AppendNull();
        }
        var indices = indexBuilder.Build();

        return new DictionaryArray((DictionaryType)field.DataType, indices, dictionary);
    }
}

internal sealed class FixedBuilder : IColumnBuilder
{
    private readonly int _size;
    private readonly List<byte[]> _values = new();
    private readonly List<bool> _validity = new();

    public FixedBuilder(int size) => _size = size;

    public void Append(ref AvroBinaryReader reader)
    {
        _values.Add(reader.ReadFixed(_size).ToArray());
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _values.Add(new byte[_size]);
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        int count = _values.Count;
        var valueBuffer = new byte[count * _size];
        for (int i = 0; i < count; i++)
            _values[i].CopyTo(valueBuffer, i * _size);

        int nullBitmapLength = (count + 7) / 8;
        var nullBitmap = new byte[nullBitmapLength];
        int nullCount = 0;
        for (int i = 0; i < count; i++)
        {
            if (_validity[i])
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            else
                nullCount++;
        }

        var dataType = field.DataType as FixedSizeBinaryType ?? new FixedSizeBinaryType(_size);
        var data = new ArrayData(dataType, count, nullCount,
            0, [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBuffer)]);
        return ArrowArrayFactory.BuildArray(data);
    }
}

internal sealed class ArrayBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _itemBuilder;
    private readonly List<int> _offsets = new() { 0 };
    private readonly List<bool> _validity = new();

    public ArrayBuilder(IColumnBuilder itemBuilder) => _itemBuilder = itemBuilder;

    public void Append(ref AvroBinaryReader reader)
    {
        int count = 0;
        while (true)
        {
            long blockCount = reader.ReadLong();
            if (blockCount == 0) break;
            if (blockCount < 0)
            {
                blockCount = -blockCount;
                reader.ReadLong(); // skip block byte size
            }
            for (long i = 0; i < blockCount; i++)
            {
                _itemBuilder.Append(ref reader);
                count++;
            }
        }
        _offsets.Add(_offsets[^1] + count);
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _offsets.Add(_offsets[^1]);
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        var listType = (ListType)field.DataType;
        var values = _itemBuilder.Build(listType.ValueField);
        return BuildListArray(listType, values, _offsets, _validity);
    }

    internal static ListArray BuildListArray(
        ListType listType, IArrowArray values, List<int> offsets, List<bool> validity)
    {
        var offsetBuffer = new ArrowBuffer.Builder<int>();
        foreach (var o in offsets) offsetBuffer.Append(o);

        var validityBuffer = new ArrowBuffer.BitmapBuilder();
        foreach (var v in validity) validityBuffer.Append(v);

        int nullCount = validity.Count(v => !v);
        return new ListArray(listType, validity.Count,
            offsetBuffer.Build(), values, validityBuffer.Build(), nullCount);
    }
}

internal sealed class MapBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _valueBuilder;
    private readonly StringArray.Builder _keyBuilder = new();
    private readonly List<int> _offsets = new() { 0 };
    private readonly List<bool> _validity = new();

    public MapBuilder(IColumnBuilder valueBuilder) => _valueBuilder = valueBuilder;

    public void Append(ref AvroBinaryReader reader)
    {
        int count = 0;
        while (true)
        {
            long blockCount = reader.ReadLong();
            if (blockCount == 0) break;
            if (blockCount < 0)
            {
                blockCount = -blockCount;
                reader.ReadLong(); // skip block byte size
            }
            for (long i = 0; i < blockCount; i++)
            {
                _keyBuilder.Append(reader.ReadString());
                _valueBuilder.Append(ref reader);
                count++;
            }
        }
        _offsets.Add(_offsets[^1] + count);
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _offsets.Add(_offsets[^1]);
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        var mapType = (MapType)field.DataType;
        var keys = _keyBuilder.Build();
        var values = _valueBuilder.Build(mapType.ValueField);

        // Build the struct array for entries (key, value)
        var structFields = new List<Field> { mapType.KeyField, mapType.ValueField };
        var structType = new StructType(structFields);
        var entries = new StructArray(structType, keys.Length,
            new IArrowArray[] { keys, values }, ArrowBuffer.Empty);

        var offsetBuffer = new ArrowBuffer.Builder<int>();
        foreach (var o in _offsets) offsetBuffer.Append(o);

        var validityBuffer = new ArrowBuffer.BitmapBuilder();
        foreach (var v in _validity) validityBuffer.Append(v);

        int nullCount = _validity.Count(v => !v);
        return new MapArray(mapType, _validity.Count,
            offsetBuffer.Build(), entries, validityBuffer.Build(), nullCount);
    }
}

internal sealed class StructBuilder : IColumnBuilder
{
    private readonly List<(string name, IColumnBuilder builder)> _children;
    private readonly List<bool> _validity = new();

    public StructBuilder(List<(string name, IColumnBuilder builder)> children)
        => _children = children;

    public void Append(ref AvroBinaryReader reader)
    {
        foreach (var (_, builder) in _children)
            builder.Append(ref reader);
        _validity.Add(true);
    }

    public void AppendNull()
    {
        foreach (var (_, builder) in _children)
            builder.AppendNull();
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        var structType = (StructType)field.DataType;
        var childArrays = new IArrowArray[_children.Count];
        for (int i = 0; i < _children.Count; i++)
            childArrays[i] = _children[i].builder.Build(structType.Fields[i]);

        var validityBuffer = new ArrowBuffer.BitmapBuilder();
        foreach (var v in _validity) validityBuffer.Append(v);

        int nullCount = _validity.Count(v => !v);
        return new StructArray(structType, _validity.Count,
            childArrays, validityBuffer.Build(), nullCount);
    }
}

// ─── Decimal builders ───

/// <summary>Reads Avro fixed-size bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalFixedBuilder : IColumnBuilder
{
    private readonly int _size;
    private readonly int _precision;
    private readonly int _scale;
    private readonly List<byte[]> _values = new();
    private readonly List<bool> _validity = new();

    public DecimalFixedBuilder(int size, int precision, int scale)
    {
        _size = size;
        _precision = precision;
        _scale = scale;
    }

    public void Append(ref AvroBinaryReader reader)
    {
        var bytes = reader.ReadFixed(_size);
        _values.Add(DecimalArrayHelper.BigEndianToDecimal128Bytes(bytes));
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _values.Add(new byte[16]);
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        return DecimalArrayHelper.BuildDecimal128Array(field, _values, _validity, _precision, _scale);
    }
}

/// <summary>Reads Avro variable-length bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalBytesBuilder : IColumnBuilder
{
    private readonly int _precision;
    private readonly int _scale;
    private readonly List<byte[]> _values = new();
    private readonly List<bool> _validity = new();

    public DecimalBytesBuilder(int precision, int scale)
    {
        _precision = precision;
        _scale = scale;
    }

    public void Append(ref AvroBinaryReader reader)
    {
        var bytes = reader.ReadBytes();
        _values.Add(DecimalArrayHelper.BigEndianToDecimal128Bytes(bytes));
        _validity.Add(true);
    }

    public void AppendNull()
    {
        _values.Add(new byte[16]);
        _validity.Add(false);
    }

    public IArrowArray Build(Field field)
    {
        return DecimalArrayHelper.BuildDecimal128Array(field, _values, _validity, _precision, _scale);
    }
}

internal static class DecimalArrayHelper
{
    /// <summary>Converts Avro big-endian two's complement bytes to 16-byte little-endian for Arrow Decimal128.</summary>
    public static byte[] BigEndianToDecimal128Bytes(ReadOnlySpan<byte> bigEndian)
    {
        var result = new byte[16];
        byte signExtend = (bigEndian.Length > 0 && (bigEndian[0] & 0x80) != 0) ? (byte)0xFF : (byte)0x00;
        result.AsSpan().Fill(signExtend);
        for (int i = 0; i < bigEndian.Length && i < 16; i++)
            result[i] = bigEndian[bigEndian.Length - 1 - i];
        return result;
    }

    public static IArrowArray BuildDecimal128Array(
        Field field, List<byte[]> values, List<bool> validity, int precision, int scale)
    {
        int count = values.Count;
        var valueBuffer = new byte[count * 16];
        for (int i = 0; i < count; i++)
            values[i].CopyTo(valueBuffer, i * 16);

        int nullBitmapLength = (count + 7) / 8;
        var nullBitmap = new byte[nullBitmapLength];
        int nullCount = 0;
        for (int i = 0; i < count; i++)
        {
            if (validity[i])
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            else
                nullCount++;
        }

        var dataType = new Decimal128Type(precision, scale);
        var data = new ArrayData(dataType, count, nullCount,
            0, [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBuffer)]);
        return ArrowArrayFactory.BuildArray(data);
    }
}

// ─── Dense Union builder ───

internal sealed class DenseUnionBuilder : IColumnBuilder
{
    private readonly IColumnBuilder[] _branchBuilders;
    private readonly AvroUnionSchema _unionSchema;
    private readonly List<byte> _typeIds = new();
    private readonly List<int> _offsets = new();
    private readonly int[] _branchCounts;

    public DenseUnionBuilder(AvroUnionSchema unionSchema, IColumnBuilder[] branchBuilders)
    {
        _unionSchema = unionSchema;
        _branchBuilders = branchBuilders;
        _branchCounts = new int[branchBuilders.Length];
    }

    public void Append(ref AvroBinaryReader reader)
    {
        int branchIndex = reader.ReadUnionIndex();
        _typeIds.Add(checked((byte)branchIndex));
        _offsets.Add(_branchCounts[branchIndex]);
        _branchCounts[branchIndex]++;
        _branchBuilders[branchIndex].Append(ref reader);
    }

    public void AppendNull()
    {
        throw new NotSupportedException("Cannot append null to a non-nullable union.");
    }

    public IArrowArray Build(Field field)
    {
        var unionType = (UnionType)field.DataType;

        // Build child arrays
        var children = new IArrowArray[_branchBuilders.Length];
        for (int i = 0; i < _branchBuilders.Length; i++)
            children[i] = _branchBuilders[i].Build(unionType.Fields[i]);

        var typeIdBuffer = new ArrowBuffer(_typeIds.ToArray());
        var offsetBytes = new int[_offsets.Count];
        for (int i = 0; i < _offsets.Count; i++)
            offsetBytes[i] = _offsets[i];
        var offsetBuffer = new ArrowBuffer(MemoryMarshal.AsBytes(offsetBytes.AsSpan()).ToArray());

        return new DenseUnionArray(unionType, _typeIds.Count, children, typeIdBuffer, offsetBuffer);
    }
}
