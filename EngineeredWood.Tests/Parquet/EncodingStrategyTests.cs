using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet;

public class EncodingStrategyTests
{
    [Theory]
    [InlineData(PhysicalType.Int32, Encoding.DeltaBinaryPacked)]
    [InlineData(PhysicalType.Int64, Encoding.DeltaBinaryPacked)]
    [InlineData(PhysicalType.Float, Encoding.ByteStreamSplit)]
    [InlineData(PhysicalType.Double, Encoding.ByteStreamSplit)]
    [InlineData(PhysicalType.ByteArray, Encoding.DeltaLengthByteArray)]
    [InlineData(PhysicalType.FixedLenByteArray, Encoding.DeltaByteArray)]
    [InlineData(PhysicalType.Boolean, Encoding.Plain)]
    public void Adaptive_GetsFallbackEncoding(PhysicalType type, Encoding expected)
    {
        var fallback = EncodingStrategyResolver.GetFallbackEncoding(EncodingStrategy.Adaptive, type);
        Assert.Equal(expected, fallback);
    }

    [Theory]
    [InlineData(PhysicalType.Int32)]
    [InlineData(PhysicalType.Float)]
    [InlineData(PhysicalType.ByteArray)]
    public void Adaptive_ResolvesDictionary(PhysicalType type)
    {
        var encoding = EncodingStrategyResolver.Resolve(EncodingStrategy.Adaptive, Encoding.Plain, type);
        Assert.Equal(Encoding.PlainDictionary, encoding);
    }

    [Fact]
    public void None_UsesExplicitEncoding()
    {
        var encoding = EncodingStrategyResolver.Resolve(
            EncodingStrategy.None, Encoding.DeltaBinaryPacked, PhysicalType.Int32);
        Assert.Equal(Encoding.DeltaBinaryPacked, encoding);
    }

    [Fact]
    public void None_FallbackIsPlain()
    {
        var fallback = EncodingStrategyResolver.GetFallbackEncoding(EncodingStrategy.None, PhysicalType.Int32);
        Assert.Equal(Encoding.Plain, fallback);
    }

    [Fact]
    public void Aggressive_HasTighterDictionaryLimit()
    {
        int defaultSize = 1024 * 1024;
        int aggressive = EncodingStrategyResolver.GetMaxDictionarySize(EncodingStrategy.Aggressive, defaultSize);
        int adaptive = EncodingStrategyResolver.GetMaxDictionarySize(EncodingStrategy.Adaptive, defaultSize);
        Assert.True(aggressive < adaptive, "Aggressive should have tighter dictionary limit");
    }

    [Fact]
    public void Adaptive_RoundTrip_Int32()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-strategy-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("value", Int32Type.Default, nullable: false))
                .Build();

            var builder = new Int32Array.Builder();
            for (int i = 0; i < 1000; i++)
                builder.Append(i * 3 + 7);
            var batch = new RecordBatch(schema, [builder.Build()], 1000);

            var options = new ParquetWriteOptions
            {
                EncodingStrategy = EncodingStrategy.Adaptive,
            };

            using (var output = new LocalOutputFile(path))
            {
                var writer = new ParquetFileWriter(output, options);
                writer.WriteAsync(batch).GetAwaiter().GetResult();
                writer.DisposeAsync().GetAwaiter().GetResult();
            }

            // Read back and verify data
            using var input = new LocalRandomAccessFile(path);
            var reader = new ParquetFileReader(input);
            var meta = reader.ReadMetadataAsync().GetAwaiter().GetResult();
            var readBatch = reader.ReadRowGroupAsync(0).GetAwaiter().GetResult();

            Assert.Equal(1000, readBatch.Length);
            var col = (Int32Array)readBatch.Column(0);
            for (int i = 0; i < 1000; i++)
                Assert.Equal(i * 3 + 7, col.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void Adaptive_RoundTrip_HighCardinalityStrings()
    {
        // High-cardinality strings should trigger dictionary fallback to DeltaLengthByteArray
        var path = Path.Combine(Path.GetTempPath(), $"ew-strategy-str-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("name", StringType.Default, nullable: false))
                .Build();

            var builder = new StringArray.Builder();
            // Create values that should fill dictionary quickly with maxDictionarySize limited
            for (int i = 0; i < 500; i++)
                builder.Append($"unique-value-{i:D6}-padding-to-make-longer");
            var batch = new RecordBatch(schema, [builder.Build()], 500);

            var options = new ParquetWriteOptions
            {
                EncodingStrategy = EncodingStrategy.Adaptive,
                MaxDictionarySize = 1024, // Very small to trigger fallback
            };

            using (var output = new LocalOutputFile(path))
            {
                var writer = new ParquetFileWriter(output, options);
                writer.WriteAsync(batch).GetAwaiter().GetResult();
                writer.DisposeAsync().GetAwaiter().GetResult();
            }

            // Read back and verify
            using var input = new LocalRandomAccessFile(path);
            var reader = new ParquetFileReader(input);
            var meta = reader.ReadMetadataAsync().GetAwaiter().GetResult();
            var readBatch = reader.ReadRowGroupAsync(0).GetAwaiter().GetResult();

            Assert.Equal(500, readBatch.Length);
            var col = (StringArray)readBatch.Column(0);
            for (int i = 0; i < 500; i++)
                Assert.Equal($"unique-value-{i:D6}-padding-to-make-longer", col.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void Aggressive_RoundTrip_Float()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-strategy-flt-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("temp", FloatType.Default, nullable: false))
                .Build();

            var builder = new FloatArray.Builder();
            for (int i = 0; i < 500; i++)
                builder.Append(i * 0.1f + 20.0f);
            var batch = new RecordBatch(schema, [builder.Build()], 500);

            var options = new ParquetWriteOptions
            {
                EncodingStrategy = EncodingStrategy.Aggressive,
                MaxDictionarySize = 256, // Force fallback to ByteStreamSplit
            };

            using (var output = new LocalOutputFile(path))
            {
                var writer = new ParquetFileWriter(output, options);
                writer.WriteAsync(batch).GetAwaiter().GetResult();
                writer.DisposeAsync().GetAwaiter().GetResult();
            }

            using var input = new LocalRandomAccessFile(path);
            var reader = new ParquetFileReader(input);
            var meta = reader.ReadMetadataAsync().GetAwaiter().GetResult();
            var readBatch = reader.ReadRowGroupAsync(0).GetAwaiter().GetResult();

            Assert.Equal(500, readBatch.Length);
            var col = (FloatArray)readBatch.Column(0);
            for (int i = 0; i < 500; i++)
                Assert.Equal(i * 0.1f + 20.0f, col.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}
