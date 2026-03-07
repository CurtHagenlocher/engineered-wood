using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

public class StatisticsCollectorTests
{
    private static ColumnDescriptor MakeDescriptor(
        PhysicalType physicalType,
        int maxDefLevel = 0,
        int? typeLength = null)
    {
        var element = new SchemaElement
        {
            Name = "col",
            Type = physicalType,
            RepetitionType = maxDefLevel > 0
                ? FieldRepetitionType.Optional
                : FieldRepetitionType.Required,
        };
        var node = new SchemaNode { Element = element, Children = [] };
        return new ColumnDescriptor
        {
            Path = ["col"],
            PhysicalType = physicalType,
            TypeLength = typeLength,
            MaxDefinitionLevel = maxDefLevel,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = node,
        };
    }

    [Fact]
    public void Int32_ComputesMinMax()
    {
        var column = MakeDescriptor(PhysicalType.Int32);
        var writer = new ColumnWriter(column);

        var builder = new Int32Array.Builder();
        builder.Append(50);
        builder.Append(-10);
        builder.Append(100);
        builder.Append(25);
        var result = writer.Write(builder.Build());

        Assert.NotNull(result.Metadata.Statistics);
        var stats = result.Metadata.Statistics!;

        Assert.NotNull(stats.MinValue);
        Assert.NotNull(stats.MaxValue);
        Assert.Equal(0L, stats.NullCount);

        int min = BitConverter.ToInt32(stats.MinValue!);
        int max = BitConverter.ToInt32(stats.MaxValue!);
        Assert.Equal(-10, min);
        Assert.Equal(100, max);
        Assert.True(stats.IsMinValueExact);
        Assert.True(stats.IsMaxValueExact);
    }

    [Fact]
    public void Int32_Nullable_ComputesNullCount()
    {
        var column = MakeDescriptor(PhysicalType.Int32, maxDefLevel: 1);
        var writer = new ColumnWriter(column);

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.AppendNull();
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(3L, stats.NullCount);

        int min = BitConverter.ToInt32(stats.MinValue!);
        int max = BitConverter.ToInt32(stats.MaxValue!);
        Assert.Equal(10, min);
        Assert.Equal(30, max);
    }

    [Fact]
    public void Int64_ComputesMinMax()
    {
        var column = MakeDescriptor(PhysicalType.Int64);
        var writer = new ColumnWriter(column);

        var builder = new Int64Array.Builder();
        builder.Append(long.MaxValue);
        builder.Append(long.MinValue);
        builder.Append(0L);
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(long.MinValue, BitConverter.ToInt64(stats.MinValue!));
        Assert.Equal(long.MaxValue, BitConverter.ToInt64(stats.MaxValue!));
        Assert.Equal(0L, stats.NullCount);
    }

    [Fact]
    public void Float_ComputesMinMax()
    {
        var column = MakeDescriptor(PhysicalType.Float);
        var writer = new ColumnWriter(column);

        var builder = new FloatArray.Builder();
        builder.Append(1.5f);
        builder.Append(-2.5f);
        builder.Append(3.0f);
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(-2.5f, BitConverter.ToSingle(stats.MinValue!));
        Assert.Equal(3.0f, BitConverter.ToSingle(stats.MaxValue!));
    }

    [Fact]
    public void Double_ComputesMinMax()
    {
        var column = MakeDescriptor(PhysicalType.Double);
        var writer = new ColumnWriter(column);

        var builder = new DoubleArray.Builder();
        builder.Append(3.14);
        builder.Append(-1.0);
        builder.Append(2.71);
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(-1.0, BitConverter.ToDouble(stats.MinValue!));
        Assert.Equal(3.14, BitConverter.ToDouble(stats.MaxValue!));
    }

    [Fact]
    public void Boolean_ComputesMinMax()
    {
        var column = MakeDescriptor(PhysicalType.Boolean, maxDefLevel: 1);
        var writer = new ColumnWriter(column);

        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.AppendNull();
        builder.Append(false);
        builder.Append(true);
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(1L, stats.NullCount);
        Assert.Equal((byte)0, stats.MinValue![0]);
        Assert.Equal((byte)1, stats.MaxValue![0]);
    }

    [Fact]
    public void String_ComputesMinMax()
    {
        var element = new SchemaElement
        {
            Name = "s",
            Type = PhysicalType.ByteArray,
            RepetitionType = FieldRepetitionType.Required,
            LogicalType = new LogicalType.StringType(),
        };
        var column = MakeDescriptor(PhysicalType.ByteArray);

        var writer = new ColumnWriter(column);
        var builder = new StringArray.Builder();
        builder.Append("banana");
        builder.Append("apple");
        builder.Append("cherry");
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal("apple", System.Text.Encoding.UTF8.GetString(stats.MinValue!));
        Assert.Equal("cherry", System.Text.Encoding.UTF8.GetString(stats.MaxValue!));
    }

    [Fact]
    public void AllNulls_HasNullCountOnly()
    {
        var column = MakeDescriptor(PhysicalType.Int32, maxDefLevel: 1);
        var writer = new ColumnWriter(column);

        var builder = new Int32Array.Builder();
        builder.AppendNull();
        builder.AppendNull();
        builder.AppendNull();
        var result = writer.Write(builder.Build());

        var stats = result.Metadata.Statistics!;
        Assert.Equal(3L, stats.NullCount);
        Assert.Null(stats.MinValue);
        Assert.Null(stats.MaxValue);
    }

    [Fact]
    public void Statistics_SurviveRoundTrip()
    {
        // Write a file and verify statistics are preserved in metadata
        var path = Path.Combine(Path.GetTempPath(), $"ew-stats-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("value", Int32Type.Default, nullable: true))
                .Build();

            var builder = new Int32Array.Builder();
            builder.Append(42);
            builder.AppendNull();
            builder.Append(-7);
            builder.Append(100);
            var batch = new RecordBatch(schema, [builder.Build()], 4);

            // Write
            using (var output = new LocalOutputFile(path))
            {
                var writer = new ParquetFileWriter(output);
                writer.WriteAsync(batch).GetAwaiter().GetResult();
                writer.DisposeAsync().GetAwaiter().GetResult();
            }

            // Read metadata and check statistics
            using var input = new LocalRandomAccessFile(path);
            var reader = new ParquetFileReader(input);
            var meta = reader.ReadMetadataAsync().GetAwaiter().GetResult();

            var colMeta = meta.RowGroups[0].Columns[0].MetaData!;
            Assert.NotNull(colMeta.Statistics);

            var stats = colMeta.Statistics!;
            Assert.Equal(1L, stats.NullCount);
            Assert.Equal(-7, BitConverter.ToInt32(stats.MinValue!));
            Assert.Equal(100, BitConverter.ToInt32(stats.MaxValue!));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}
