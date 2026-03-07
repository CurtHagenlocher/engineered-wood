using Apache.Arrow;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

public class ColumnWriterTests
{
    private static ColumnDescriptor MakeDescriptor(
        PhysicalType physicalType,
        string name = "col",
        int maxDefLevel = 0,
        int? typeLength = null,
        SchemaElement? schemaElement = null)
    {
        var element = schemaElement ?? new SchemaElement
        {
            Name = name,
            Type = physicalType,
            RepetitionType = maxDefLevel > 0
                ? FieldRepetitionType.Optional
                : FieldRepetitionType.Required,
        };

        var node = new SchemaNode
        {
            Element = element,
            Children = [],
        };

        return new ColumnDescriptor
        {
            Path = [name],
            PhysicalType = physicalType,
            TypeLength = typeLength,
            MaxDefinitionLevel = maxDefLevel,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = node,
        };
    }

    private static IArrowArray ReadBackColumn(
        byte[] data,
        ColumnDescriptor column,
        ColumnMetaData meta,
        int rowCount)
    {
        var arrowField = ArrowSchemaConverter.ToArrowField(column);
        var result = ColumnChunkReader.ReadColumn(data, column, meta, rowCount, arrowField);
        return result.Array;
    }

    [Fact]
    public void Int32_NonNullable_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32);
        var writer = new ColumnWriter(column);

        var builder = new Int32Array.Builder();
        builder.Append(1);
        builder.Append(2);
        builder.Append(3);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Equal(PhysicalType.Int32, result.Metadata.Type);
        Assert.Equal(3, result.Metadata.NumValues);
        Assert.Contains(Encoding.Plain, result.Metadata.Encodings);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 3);
        Assert.Equal(3, readBack.Length);
        Assert.Equal(1, readBack.GetValue(0));
        Assert.Equal(2, readBack.GetValue(1));
        Assert.Equal(3, readBack.GetValue(2));
    }

    [Fact]
    public void Int32_Nullable_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32, maxDefLevel: 1);
        var writer = new ColumnWriter(column);

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Equal(5, result.Metadata.NumValues);
        Assert.Contains(Encoding.Rle, result.Metadata.Encodings);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 5);
        Assert.Equal(5, readBack.Length);
        Assert.Equal(10, readBack.GetValue(0));
        Assert.False(readBack.IsValid(1));
        Assert.Equal(30, readBack.GetValue(2));
        Assert.False(readBack.IsValid(3));
        Assert.Equal(50, readBack.GetValue(4));
    }

    [Fact]
    public void Int64_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int64);
        var writer = new ColumnWriter(column);

        var builder = new Int64Array.Builder();
        builder.Append(100L);
        builder.Append(200L);
        var array = builder.Build();

        var result = writer.Write(array);

        var readBack = (Int64Array)ReadBackColumn(result.Data, column, result.Metadata, 2);
        Assert.Equal(100L, readBack.GetValue(0));
        Assert.Equal(200L, readBack.GetValue(1));
    }

    [Fact]
    public void Float_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Float);
        var writer = new ColumnWriter(column);

        var builder = new FloatArray.Builder();
        builder.Append(1.5f);
        builder.Append(2.5f);
        var array = builder.Build();

        var result = writer.Write(array);

        var readBack = (FloatArray)ReadBackColumn(result.Data, column, result.Metadata, 2);
        Assert.Equal(1.5f, readBack.GetValue(0));
        Assert.Equal(2.5f, readBack.GetValue(1));
    }

    [Fact]
    public void Double_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Double);
        var writer = new ColumnWriter(column);

        var builder = new DoubleArray.Builder();
        builder.Append(3.14);
        builder.Append(2.71);
        var array = builder.Build();

        var result = writer.Write(array);

        var readBack = (DoubleArray)ReadBackColumn(result.Data, column, result.Metadata, 2);
        Assert.Equal(3.14, readBack.GetValue(0));
        Assert.Equal(2.71, readBack.GetValue(1));
    }

    [Fact]
    public void Boolean_Nullable_Plain_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Boolean, maxDefLevel: 1);
        var writer = new ColumnWriter(column);

        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.AppendNull();
        builder.Append(false);
        builder.Append(true);
        var array = builder.Build();

        var result = writer.Write(array);

        var readBack = (BooleanArray)ReadBackColumn(result.Data, column, result.Metadata, 4);
        Assert.True(readBack.GetValue(0));
        Assert.False(readBack.IsValid(1));
        Assert.False(readBack.GetValue(2));
        Assert.True(readBack.GetValue(3));
    }

    [Fact]
    public void String_Nullable_Plain_RoundTrips()
    {
        var element = new SchemaElement
        {
            Name = "name",
            Type = PhysicalType.ByteArray,
            RepetitionType = FieldRepetitionType.Optional,
            LogicalType = new LogicalType.StringType(),
        };
        var column = MakeDescriptor(PhysicalType.ByteArray, "name", maxDefLevel: 1, schemaElement: element);
        var writer = new ColumnWriter(column);

        var builder = new StringArray.Builder();
        builder.Append("hello");
        builder.AppendNull();
        builder.Append("world");
        var array = builder.Build();

        var result = writer.Write(array);

        var readBack = (StringArray)ReadBackColumn(result.Data, column, result.Metadata, 3);
        Assert.Equal(3, readBack.Length);
        Assert.Equal("hello", readBack.GetString(0));
        Assert.False(readBack.IsValid(1));
        Assert.Equal("world", readBack.GetString(2));
    }

    [Fact]
    public void Int32_Dictionary_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32, maxDefLevel: 1);
        var writer = new ColumnWriter(column, encoding: Encoding.RleDictionary);

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.Append(20);
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.Append(20);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.NotNull(result.Metadata.DictionaryPageOffset);
        Assert.Contains(Encoding.RleDictionary, result.Metadata.Encodings);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 6);
        Assert.Equal(6, readBack.Length);
        Assert.Equal(10, readBack.GetValue(0));
        Assert.Equal(20, readBack.GetValue(1));
        Assert.Equal(10, readBack.GetValue(2));
        Assert.False(readBack.IsValid(3));
        Assert.Equal(30, readBack.GetValue(4));
        Assert.Equal(20, readBack.GetValue(5));
    }

    [Fact]
    public void Int32_DeltaBinaryPacked_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32);
        var writer = new ColumnWriter(column, encoding: Encoding.DeltaBinaryPacked);

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 200; i++)
            builder.Append(i * 3);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Contains(Encoding.DeltaBinaryPacked, result.Metadata.Encodings);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 200);
        for (int i = 0; i < 200; i++)
            Assert.Equal(i * 3, readBack.GetValue(i));
    }

    [Fact]
    public void Float_ByteStreamSplit_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Float);
        var writer = new ColumnWriter(column, encoding: Encoding.ByteStreamSplit);

        var builder = new FloatArray.Builder();
        builder.Append(1.0f);
        builder.Append(2.5f);
        builder.Append(-3.14f);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Contains(Encoding.ByteStreamSplit, result.Metadata.Encodings);

        var readBack = (FloatArray)ReadBackColumn(result.Data, column, result.Metadata, 3);
        Assert.Equal(1.0f, readBack.GetValue(0));
        Assert.Equal(2.5f, readBack.GetValue(1));
        Assert.Equal(-3.14f, readBack.GetValue(2));
    }

    [Fact]
    public void Int32_Snappy_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32);
        var writer = new ColumnWriter(column, codec: CompressionCodec.Snappy);

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 1000; i++)
            builder.Append(i % 10); // highly compressible
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Equal(CompressionCodec.Snappy, result.Metadata.Codec);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 1000);
        for (int i = 0; i < 1000; i++)
            Assert.Equal(i % 10, readBack.GetValue(i));
    }

    [Fact]
    public void Int32_Zstd_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32);
        var writer = new ColumnWriter(column, codec: CompressionCodec.Zstd);

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 500; i++)
            builder.Append(i % 10);
        var array = builder.Build();

        var result = writer.Write(array);

        Assert.Equal(CompressionCodec.Zstd, result.Metadata.Codec);

        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 500);
        for (int i = 0; i < 500; i++)
            Assert.Equal(i % 10, readBack.GetValue(i));
    }

    [Fact]
    public void LargeDataset_RoundTrips()
    {
        var column = MakeDescriptor(PhysicalType.Int32, maxDefLevel: 1);
        var writer = new ColumnWriter(column, codec: CompressionCodec.Snappy);

        var rng = new Random(42);
        var builder = new Int32Array.Builder();
        var expected = new int?[10_000];
        for (int i = 0; i < 10_000; i++)
        {
            if (rng.NextDouble() < 0.1)
            {
                builder.AppendNull();
                expected[i] = null;
            }
            else
            {
                int v = rng.Next(-1000, 1000);
                builder.Append(v);
                expected[i] = v;
            }
        }
        var array = builder.Build();

        var result = writer.Write(array);
        var readBack = (Int32Array)ReadBackColumn(result.Data, column, result.Metadata, 10_000);

        Assert.Equal(10_000, readBack.Length);
        for (int i = 0; i < 10_000; i++)
        {
            if (expected[i] == null)
                Assert.False(readBack.IsValid(i));
            else
                Assert.Equal(expected[i], readBack.GetValue(i));
        }
    }
}
