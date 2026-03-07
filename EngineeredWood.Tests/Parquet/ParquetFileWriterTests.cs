using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

public class ParquetFileWriterTests : IDisposable
{
    private readonly string _tempDir;

    public ParquetFileWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private string TempFile(string name = "test.parquet") => Path.Combine(_tempDir, name);

    private async Task<RecordBatch> WriteAndRead(RecordBatch batch, ParquetWriteOptions? options = null)
    {
        var path = TempFile();

        // Write
        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output, options))
        {
            await writer.WriteAsync(batch);
        }

        // Read
        using var input = new LocalRandomAccessFile(path);
        var reader = new ParquetFileReader(input);
        return await reader.ReadRowGroupAsync(0);
    }

    [Fact]
    public async Task Int32_NonNullable_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i);

        var batch = new RecordBatch(schema, [builder.Build()], 100);
        var result = await WriteAndRead(batch);

        Assert.Equal(100, result.Length);
        var col = (Int32Array)result.Column(0);
        for (int i = 0; i < 100; i++)
            Assert.Equal(i, col.GetValue(i));
    }

    [Fact]
    public async Task Int32_Nullable_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);

        var batch = new RecordBatch(schema, [builder.Build()], 5);
        var result = await WriteAndRead(batch);

        var col = (Int32Array)result.Column(0);
        Assert.Equal(5, col.Length);
        Assert.Equal(10, col.GetValue(0));
        Assert.False(col.IsValid(1));
        Assert.Equal(30, col.GetValue(2));
        Assert.False(col.IsValid(3));
        Assert.Equal(50, col.GetValue(4));
    }

    [Fact]
    public async Task Int64_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", Int64Type.Default, nullable: false))
            .Build();

        var builder = new Int64Array.Builder();
        builder.Append(100L);
        builder.Append(200L);

        var batch = new RecordBatch(schema, [builder.Build()], 2);
        var result = await WriteAndRead(batch);

        var col = (Int64Array)result.Column(0);
        Assert.Equal(100L, col.GetValue(0));
        Assert.Equal(200L, col.GetValue(1));
    }

    [Fact]
    public async Task Float_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("temperature", FloatType.Default, nullable: false))
            .Build();

        var builder = new FloatArray.Builder();
        builder.Append(1.5f);
        builder.Append(2.5f);

        var batch = new RecordBatch(schema, [builder.Build()], 2);
        var result = await WriteAndRead(batch);

        var col = (FloatArray)result.Column(0);
        Assert.Equal(1.5f, col.GetValue(0));
        Assert.Equal(2.5f, col.GetValue(1));
    }

    [Fact]
    public async Task Double_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("price", DoubleType.Default, nullable: false))
            .Build();

        var builder = new DoubleArray.Builder();
        builder.Append(3.14);
        builder.Append(2.71);

        var batch = new RecordBatch(schema, [builder.Build()], 2);
        var result = await WriteAndRead(batch);

        var col = (DoubleArray)result.Column(0);
        Assert.Equal(3.14, col.GetValue(0));
        Assert.Equal(2.71, col.GetValue(1));
    }

    [Fact]
    public async Task Boolean_Nullable_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("flag", BooleanType.Default, nullable: true))
            .Build();

        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.AppendNull();
        builder.Append(false);

        var batch = new RecordBatch(schema, [builder.Build()], 3);
        var result = await WriteAndRead(batch);

        var col = (BooleanArray)result.Column(0);
        Assert.True(col.GetValue(0));
        Assert.False(col.IsValid(1));
        Assert.False(col.GetValue(2));
    }

    [Fact]
    public async Task String_Nullable_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        var builder = new StringArray.Builder();
        builder.Append("hello");
        builder.AppendNull();
        builder.Append("world");
        builder.Append("");

        var batch = new RecordBatch(schema, [builder.Build()], 4);
        var result = await WriteAndRead(batch);

        var col = (StringArray)result.Column(0);
        Assert.Equal(4, col.Length);
        Assert.Equal("hello", col.GetString(0));
        Assert.False(col.IsValid(1));
        Assert.Equal("world", col.GetString(2));
        Assert.Equal("", col.GetString(3));
    }

    [Fact]
    public async Task MultipleColumns_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Field(new Field("score", DoubleType.Default, nullable: true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var scoreBuilder = new DoubleArray.Builder();

        for (int i = 0; i < 50; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append(i % 5 == 0 ? null : $"name_{i}");
            scoreBuilder.Append(i % 3 == 0 ? null : (double?)(i * 1.5));
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), nameBuilder.Build(), scoreBuilder.Build()], 50);
        var result = await WriteAndRead(batch);

        Assert.Equal(50, result.Length);
        Assert.Equal(3, result.ColumnCount);

        var ids = (Int32Array)result.Column(0);
        var names = (StringArray)result.Column(1);
        var scores = (DoubleArray)result.Column(2);

        for (int i = 0; i < 50; i++)
        {
            Assert.Equal(i, ids.GetValue(i));

            if (i % 5 == 0)
                Assert.False(names.IsValid(i));
            else
                Assert.Equal($"name_{i}", names.GetString(i));

            if (i % 3 == 0)
                Assert.False(scores.IsValid(i));
            else
                Assert.Equal(i * 1.5, scores.GetValue(i));
        }
    }

    [Fact]
    public async Task WithSnappyCompression_RoundTrips()
    {
        var options = new ParquetWriteOptions { Codec = CompressionCodec.Snappy };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 1000; i++) builder.Append(i);

        var batch = new RecordBatch(schema, [builder.Build()], 1000);
        var result = await WriteAndRead(batch, options);

        var col = (Int32Array)result.Column(0);
        for (int i = 0; i < 1000; i++)
            Assert.Equal(i, col.GetValue(i));
    }

    [Fact]
    public async Task WithZstdCompression_RoundTrips()
    {
        var options = new ParquetWriteOptions { Codec = CompressionCodec.Zstd };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int64Type.Default, nullable: false))
            .Build();

        var builder = new Int64Array.Builder();
        for (int i = 0; i < 500; i++) builder.Append(i);

        var batch = new RecordBatch(schema, [builder.Build()], 500);
        var result = await WriteAndRead(batch, options);

        var col = (Int64Array)result.Column(0);
        for (int i = 0; i < 500; i++)
            Assert.Equal((long)i, col.GetValue(i));
    }

    [Fact]
    public async Task MultipleRowGroups_RoundTrips()
    {
        var path = TempFile("multi-rg.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: false))
            .Build();

        // Write two row groups
        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output))
        {
            var b1 = new Int32Array.Builder();
            for (int i = 0; i < 100; i++) b1.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b1.Build()], 100));

            var b2 = new Int32Array.Builder();
            for (int i = 100; i < 250; i++) b2.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b2.Build()], 150));
        }

        // Read back
        using var input = new LocalRandomAccessFile(path);
        var reader = new ParquetFileReader(input);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal(250, meta.NumRows);
        Assert.Equal(2, meta.RowGroups.Count);
        Assert.Equal(100, meta.RowGroups[0].NumRows);
        Assert.Equal(150, meta.RowGroups[1].NumRows);

        var rg0 = await reader.ReadRowGroupAsync(0);
        var rg1 = await reader.ReadRowGroupAsync(1);

        var col0 = (Int32Array)rg0.Column(0);
        for (int i = 0; i < 100; i++)
            Assert.Equal(i, col0.GetValue(i));

        var col1 = (Int32Array)rg1.Column(0);
        for (int i = 0; i < 150; i++)
            Assert.Equal(i + 100, col1.GetValue(i));
    }

    [Fact]
    public async Task LargeDataset_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var rng = new Random(42);

        int rowCount = 50_000;
        var expectedValues = new double?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            if (rng.NextDouble() < 0.1)
            {
                valBuilder.AppendNull();
                expectedValues[i] = null;
            }
            else
            {
                double v = rng.NextDouble() * 1000;
                valBuilder.Append(v);
                expectedValues[i] = v;
            }
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build()], rowCount);
        var result = await WriteAndRead(batch);

        Assert.Equal(rowCount, result.Length);

        var ids = (Int32Array)result.Column(0);
        var vals = (DoubleArray)result.Column(1);

        for (int i = 0; i < rowCount; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            if (expectedValues[i] == null)
                Assert.False(vals.IsValid(i));
            else
                Assert.Equal(expectedValues[i], vals.GetValue(i));
        }
    }

    [Fact]
    public async Task FileStructure_IsValid()
    {
        var path = TempFile("structure.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(42);

        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output))
        {
            await writer.WriteAsync(new RecordBatch(schema, [builder.Build()], 1));
        }

        // Verify file structure
        var bytes = await File.ReadAllBytesAsync(path);

        // Check header magic
        Assert.Equal((byte)'P', bytes[0]);
        Assert.Equal((byte)'A', bytes[1]);
        Assert.Equal((byte)'R', bytes[2]);
        Assert.Equal((byte)'1', bytes[3]);

        // Check trailer magic
        Assert.Equal((byte)'P', bytes[^4]);
        Assert.Equal((byte)'A', bytes[^3]);
        Assert.Equal((byte)'R', bytes[^2]);
        Assert.Equal((byte)'1', bytes[^1]);

        // Footer length
        int footerLen = BitConverter.ToInt32(bytes.AsSpan(bytes.Length - 8, 4));
        Assert.True(footerLen > 0);
        Assert.True(footerLen < bytes.Length - 8);
    }

    [Fact]
    public async Task CreatedBy_WrittenToFooter()
    {
        var options = new ParquetWriteOptions { CreatedBy = "TestWriter 1.0" };
        var path = TempFile("created-by.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(1);

        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output, options))
        {
            await writer.WriteAsync(new RecordBatch(schema, [builder.Build()], 1));
        }

        using var input = new LocalRandomAccessFile(path);
        var reader = new ParquetFileReader(input);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal("TestWriter 1.0", meta.CreatedBy);
    }

    // --- V2 Data Page Tests (file-level) ---

    [Fact]
    public async Task V2_DefaultPageVersion_Is_V2()
    {
        // The default options should write V2 pages
        var options = new ParquetWriteOptions();
        Assert.Equal(DataPageVersion.V2, options.DataPageVersion);
    }

    [Fact]
    public async Task V2_Int32_Nullable_RoundTrips()
    {
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V2 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);

        var batch = new RecordBatch(schema, [builder.Build()], 5);
        var result = await WriteAndRead(batch, options);

        var col = (Int32Array)result.Column(0);
        Assert.Equal(5, col.Length);
        Assert.Equal(10, col.GetValue(0));
        Assert.False(col.IsValid(1));
        Assert.Equal(30, col.GetValue(2));
        Assert.False(col.IsValid(3));
        Assert.Equal(50, col.GetValue(4));
    }

    [Fact]
    public async Task V2_MultipleColumns_WithCompression_RoundTrips()
    {
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Field(new Field("score", DoubleType.Default, nullable: true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var scoreBuilder = new DoubleArray.Builder();

        for (int i = 0; i < 200; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append(i % 7 == 0 ? null : $"item_{i}");
            scoreBuilder.Append(i % 4 == 0 ? null : (double?)(i * 2.5));
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), nameBuilder.Build(), scoreBuilder.Build()], 200);
        var result = await WriteAndRead(batch, options);

        Assert.Equal(200, result.Length);
        var ids = (Int32Array)result.Column(0);
        var names = (StringArray)result.Column(1);
        var scores = (DoubleArray)result.Column(2);

        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            if (i % 7 == 0)
                Assert.False(names.IsValid(i));
            else
                Assert.Equal($"item_{i}", names.GetString(i));
            if (i % 4 == 0)
                Assert.False(scores.IsValid(i));
            else
                Assert.Equal(i * 2.5, scores.GetValue(i));
        }
    }

    [Fact]
    public async Task V2_Zstd_RoundTrips()
    {
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Zstd,
        };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int64Type.Default, nullable: true))
            .Build();

        var builder = new Int64Array.Builder();
        for (int i = 0; i < 500; i++)
            builder.Append(i % 3 == 0 ? null : (long?)(i * 100L));

        var batch = new RecordBatch(schema, [builder.Build()], 500);
        var result = await WriteAndRead(batch, options);

        var col = (Int64Array)result.Column(0);
        Assert.Equal(500, col.Length);
        for (int i = 0; i < 500; i++)
        {
            if (i % 3 == 0)
                Assert.False(col.IsValid(i));
            else
                Assert.Equal(i * 100L, col.GetValue(i));
        }
    }

    [Fact]
    public async Task V2_LargeDataset_RoundTrips()
    {
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var rng = new Random(42);

        int rowCount = 50_000;
        var expectedValues = new double?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            if (rng.NextDouble() < 0.1)
            {
                valBuilder.AppendNull();
                expectedValues[i] = null;
            }
            else
            {
                double v = rng.NextDouble() * 1000;
                valBuilder.Append(v);
                expectedValues[i] = v;
            }
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build()], rowCount);
        var result = await WriteAndRead(batch, options);

        Assert.Equal(rowCount, result.Length);
        var ids = (Int32Array)result.Column(0);
        var vals = (DoubleArray)result.Column(1);

        for (int i = 0; i < rowCount; i++)
        {
            Assert.Equal(i, ids.GetValue(i));
            if (expectedValues[i] == null)
                Assert.False(vals.IsValid(i));
            else
                Assert.Equal(expectedValues[i], vals.GetValue(i));
        }
    }

    [Fact]
    public async Task V2_MultipleRowGroups_RoundTrips()
    {
        var path = TempFile("v2-multi-rg.parquet");
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V2 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output, options))
        {
            var b1 = new Int32Array.Builder();
            for (int i = 0; i < 100; i++)
                b1.Append(i % 5 == 0 ? null : (int?)i);
            await writer.WriteAsync(new RecordBatch(schema, [b1.Build()], 100));

            var b2 = new Int32Array.Builder();
            for (int i = 100; i < 250; i++)
                b2.Append(i % 7 == 0 ? null : (int?)i);
            await writer.WriteAsync(new RecordBatch(schema, [b2.Build()], 150));
        }

        using var input = new LocalRandomAccessFile(path);
        var reader = new ParquetFileReader(input);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal(250, meta.NumRows);
        Assert.Equal(2, meta.RowGroups.Count);

        var rg0 = await reader.ReadRowGroupAsync(0);
        var col0 = (Int32Array)rg0.Column(0);
        for (int i = 0; i < 100; i++)
        {
            if (i % 5 == 0)
                Assert.False(col0.IsValid(i));
            else
                Assert.Equal(i, col0.GetValue(i));
        }

        var rg1 = await reader.ReadRowGroupAsync(1);
        var col1 = (Int32Array)rg1.Column(0);
        for (int i = 0; i < 150; i++)
        {
            int val = i + 100;
            if (val % 7 == 0)
                Assert.False(col1.IsValid(i));
            else
                Assert.Equal(val, col1.GetValue(i));
        }
    }

    // --- Explicit V1 Tests (ensure V1 still works when selected) ---

    [Fact]
    public async Task V1_Explicit_Int32_Nullable_RoundTrips()
    {
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V1 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(1);
        builder.AppendNull();
        builder.Append(3);

        var batch = new RecordBatch(schema, [builder.Build()], 3);
        var result = await WriteAndRead(batch, options);

        var col = (Int32Array)result.Column(0);
        Assert.Equal(3, col.Length);
        Assert.Equal(1, col.GetValue(0));
        Assert.False(col.IsValid(1));
        Assert.Equal(3, col.GetValue(2));
    }

    [Fact]
    public async Task V1_Explicit_WithSnappy_RoundTrips()
    {
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
            Codec = CompressionCodec.Snappy,
        };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 1000; i++) builder.Append(i);

        var batch = new RecordBatch(schema, [builder.Build()], 1000);
        var result = await WriteAndRead(batch, options);

        var col = (Int32Array)result.Column(0);
        for (int i = 0; i < 1000; i++)
            Assert.Equal(i, col.GetValue(i));
    }
}
