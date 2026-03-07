using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Cross-validation tests: write files with EngineeredWood and verify they can be
/// read correctly by ParquetSharp, and write files with ParquetSharp and verify
/// EngineeredWood can read them back correctly.
/// </summary>
public class CrossValidationTests : IDisposable
{
    private readonly string _tempDir;

    public CrossValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-xval-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // ────────────────────────────────────────────────────────────────────
    //  EW writes → ParquetSharp reads
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EWWrite_PSRead_Int32_Required()
    {
        var path = TempPath("ew-int32-req.parquet");
        var values = new int[] { 1, -42, 0, int.MaxValue, int.MinValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        Assert.Equal(values.Length, rg.MetaData.NumRows);

        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Int32_Nullable()
    {
        var path = TempPath("ew-int32-null.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);
        var batch = new RecordBatch(schema, [builder.Build()], 5);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int?>();
        var buffer = new int?[5];
        col.ReadBatch(buffer);
        Assert.Equal(10, buffer[0]);
        Assert.Null(buffer[1]);
        Assert.Equal(30, buffer[2]);
        Assert.Null(buffer[3]);
        Assert.Equal(50, buffer[4]);
    }

    [Fact]
    public async Task EWWrite_PSRead_Int64()
    {
        var path = TempPath("ew-int64.parquet");
        var values = new long[] { 0, long.MaxValue, long.MinValue, 12345678901234L };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int64Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<long>();
        var buffer = new long[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Float()
    {
        var path = TempPath("ew-float.parquet");
        var values = new float[] { 0f, 1.5f, -3.14f, float.MaxValue, float.MinValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", FloatType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new FloatArray.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<float>();
        var buffer = new float[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Double()
    {
        var path = TempPath("ew-double.parquet");
        var values = new double[] { 0.0, -1.23e45, double.Epsilon, double.MaxValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new DoubleArray.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<double>();
        var buffer = new double[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Boolean()
    {
        var path = TempPath("ew-bool.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", BooleanType.Default, nullable: true))
            .Build();

        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.Append(false);
        builder.AppendNull();
        builder.Append(true);
        var batch = new RecordBatch(schema, [builder.Build()], 4);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<bool?>();
        var buffer = new bool?[4];
        col.ReadBatch(buffer);
        Assert.True(buffer[0]);
        Assert.False(buffer[1]);
        Assert.Null(buffer[2]);
        Assert.True(buffer[3]);
    }

    [Fact]
    public async Task EWWrite_PSRead_String()
    {
        var path = TempPath("ew-string.parquet");
        var values = new string?[] { "hello", "", null, "world", "café" };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: true))
            .Build();

        var builder = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v == null) builder.AppendNull();
            else builder.Append(v);
        }
        var batch = new RecordBatch(schema, [builder.Build()], values.Length);

        await WriteEW(path, batch);

        // EW writes UTF8 logical type, so ParquetSharp reads as string
        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var buffer = new string[values.Length];
        col.ReadBatch(buffer);

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] == null)
                Assert.Null(buffer[i]);
            else
                Assert.Equal(values[i], buffer[i]);
        }
    }

    [Fact]
    public async Task EWWrite_PSRead_MultipleColumns()
    {
        var path = TempPath("ew-multi.parquet");
        int rowCount = 1000;
        var random = new Random(42);

        var intBuilder = new Int32Array.Builder();
        var dblBuilder = new DoubleArray.Builder();
        var strBuilder = new StringArray.Builder();
        int[] expectedInts = new int[rowCount];
        double?[] expectedDoubles = new double?[rowCount];
        string?[] expectedStrings = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            expectedInts[i] = random.Next();
            intBuilder.Append(expectedInts[i]);

            if (random.NextDouble() < 0.1)
            {
                expectedDoubles[i] = null;
                dblBuilder.AppendNull();
            }
            else
            {
                expectedDoubles[i] = random.NextDouble() * 1000;
                dblBuilder.Append(expectedDoubles[i].Value);
            }

            if (random.NextDouble() < 0.1)
            {
                expectedStrings[i] = null;
                strBuilder.AppendNull();
            }
            else
            {
                expectedStrings[i] = $"val-{random.Next(5000)}";
                strBuilder.Append(expectedStrings[i]);
            }
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("val", DoubleType.Default, nullable: true))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Build();
        var batch = new RecordBatch(schema,
            [intBuilder.Build(), dblBuilder.Build(), strBuilder.Build()], rowCount);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);

        using var intCol = rg.Column(0).LogicalReader<int>();
        var intBuffer = new int[rowCount];
        intCol.ReadBatch(intBuffer);
        Assert.Equal(expectedInts, intBuffer);

        using var dblCol = rg.Column(1).LogicalReader<double?>();
        var dblBuffer = new double?[rowCount];
        dblCol.ReadBatch(dblBuffer);
        for (int i = 0; i < rowCount; i++)
        {
            if (expectedDoubles[i] == null)
                Assert.Null(dblBuffer[i]);
            else
                Assert.Equal(expectedDoubles[i]!.Value, dblBuffer[i]!.Value, precision: 10);
        }

        // EW writes UTF8 logical type → ParquetSharp reads as string
        using var strCol = rg.Column(2).LogicalReader<string>();
        var strBuffer = new string[rowCount];
        strCol.ReadBatch(strBuffer);
        for (int i = 0; i < rowCount; i++)
        {
            if (expectedStrings[i] == null)
                Assert.Null(strBuffer[i]);
            else
                Assert.Equal(expectedStrings[i], strBuffer[i]);
        }
    }

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4Raw)]
    public async Task EWWrite_PSRead_AllCompressions(CompressionCodec codec)
    {
        var path = TempPath($"ew-comp-{codec}.parquet");
        var values = Enumerable.Range(0, 500).ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch, new ParquetWriteOptions { Compression = codec });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task EWWrite_PSRead_DataPageVersions(DataPageVersion version)
    {
        var path = TempPath($"ew-dpv{(int)version}.parquet");
        var values = Enumerable.Range(0, 500).ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch, new ParquetWriteOptions
        {
            DataPageVersion = version,
            Compression = CompressionCodec.Snappy,
        });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_DictionaryEncoded()
    {
        var path = TempPath("ew-dict.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("category", StringType.Default, nullable: false))
            .Build();

        var builder = new StringArray.Builder();
        var categories = new[] { "alpha", "beta", "gamma" };
        for (int i = 0; i < 1000; i++)
            builder.Append(categories[i % 3]);

        var batch = new RecordBatch(schema, [builder.Build()], 1000);
        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var buffer = new string[1000];
        col.ReadBatch(buffer);

        for (int i = 0; i < 1000; i++)
            Assert.Equal(categories[i % 3], buffer[i]);
    }

    [Fact]
    public async Task EWWrite_PSRead_Statistics()
    {
        var path = TempPath("ew-stats.parquet");
        var values = new int[] { 5, 2, 8, 1, 9 };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        var stats = rg.MetaData.GetColumnChunkMetaData(0).Statistics as ParquetSharp.Statistics<int>;
        Assert.NotNull(stats);
        Assert.Equal(1, stats.Min);
        Assert.Equal(9, stats.Max);
        Assert.Equal(0, stats.NullCount);
    }

    [Fact]
    public async Task EWWrite_PSRead_MultipleRowGroups()
    {
        var path = TempPath("ew-multi-rg.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        // Write and close file before ParquetSharp reads (Windows file locking)
        {
            await using var file = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(file, ownsFile: false);

            var batch1 = new RecordBatch(schema,
                [new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build()], 100);
            var batch2 = new RecordBatch(schema,
                [new Int32Array.Builder().AppendRange(Enumerable.Range(100, 200)).Build()], 200);

            await writer.WriteRowGroupAsync(batch1);
            await writer.WriteRowGroupAsync(batch2);
            await writer.CloseAsync();
        }

        using var reader = new ParquetSharp.ParquetFileReader(path);
        Assert.Equal(2, reader.FileMetaData.NumRowGroups);
        Assert.Equal(300, reader.FileMetaData.NumRows);

        using var rg0 = reader.RowGroup(0);
        using var col0 = rg0.Column(0).LogicalReader<int>();
        var buf0 = new int[100];
        col0.ReadBatch(buf0);
        Assert.Equal(Enumerable.Range(0, 100).ToArray(), buf0);

        using var rg1 = reader.RowGroup(1);
        using var col1 = rg1.Column(0).LogicalReader<int>();
        var buf1 = new int[200];
        col1.ReadBatch(buf1);
        Assert.Equal(Enumerable.Range(100, 200).ToArray(), buf1);
    }

    // ────────────────────────────────────────────────────────────────────
    //  ParquetSharp writes → EW reads
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PSWrite_EWRead_Int32_Required()
    {
        var path = TempPath("ps-int32-req.parquet");
        var values = new int[] { 1, -42, 0, int.MaxValue, int.MinValue };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<int>("x")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<int>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)batch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetValue(i));
    }

    [Fact]
    public async Task PSWrite_EWRead_Int32_Nullable()
    {
        var path = TempPath("ps-int32-null.parquet");
        var values = new int?[] { 10, null, 30, null, 50 };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<int?>("x")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<int?>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)batch.Column(0);
        Assert.Equal(10, arr.GetValue(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal(30, arr.GetValue(2));
        Assert.True(arr.IsNull(3));
        Assert.Equal(50, arr.GetValue(4));
    }

    [Fact]
    public async Task PSWrite_EWRead_String()
    {
        var path = TempPath("ps-string.parquet");
        // Use string type so ParquetSharp writes UTF8 logical type → EW returns StringArray
        var values = new string[] { "hello", "", "world", "café" };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<string>("s")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<string>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)batch.Column(0);
        Assert.Equal("hello", arr.GetString(0));
        Assert.Equal("", arr.GetString(1));
        Assert.Equal("world", arr.GetString(2));
        Assert.Equal("café", arr.GetString(3));
    }

    [Fact]
    public async Task PSWrite_EWRead_MultipleColumns()
    {
        var path = TempPath("ps-multi.parquet");
        int rowCount = 1000;
        var random = new Random(42);

        var intValues = new int[rowCount];
        var dblValues = new double?[rowCount];
        var strValues = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            intValues[i] = random.Next();
            dblValues[i] = random.NextDouble() < 0.1 ? null : random.NextDouble() * 1000;
            strValues[i] = random.NextDouble() < 0.1 ? null : $"val-{random.Next(5000)}";
        }

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [
                new ParquetSharp.Column<int>("id"),
                new ParquetSharp.Column<double?>("val"),
                new ParquetSharp.Column<string>("name"),
            ]))
        {
            using var rg = writer.AppendRowGroup();
            using var intCol = rg.NextColumn().LogicalWriter<int>();
            intCol.WriteBatch(intValues);
            using var dblCol = rg.NextColumn().LogicalWriter<double?>();
            dblCol.WriteBatch(dblValues);
            using var strCol = rg.NextColumn().LogicalWriter<string>();
            strCol.WriteBatch(strValues!);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var intArr = (Int32Array)batch.Column(0);
        for (int i = 0; i < rowCount; i++)
            Assert.Equal(intValues[i], intArr.GetValue(i));

        var dblArr = (DoubleArray)batch.Column(1);
        for (int i = 0; i < rowCount; i++)
        {
            if (dblValues[i] == null)
                Assert.True(dblArr.IsNull(i));
            else
                Assert.Equal(dblValues[i]!.Value, dblArr.GetValue(i)!.Value, precision: 10);
        }

        var strArr = (StringArray)batch.Column(2);
        for (int i = 0; i < rowCount; i++)
        {
            if (strValues[i] == null)
                Assert.True(strArr.IsNull(i));
            else
                Assert.Equal(strValues[i], strArr.GetString(i));
        }
    }

    // ────────────────────────────────────────────────────────────────────
    //  Roundtrip: EW writes → EW reads
    // ────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4Raw)]
    public async Task Roundtrip_AllCompressions(CompressionCodec codec)
    {
        var path = TempPath($"rt-{codec}.parquet");
        int rowCount = 500;
        var random = new Random(42);

        var intBuilder = new Int32Array.Builder();
        var strBuilder = new StringArray.Builder();
        int[] expectedInts = new int[rowCount];
        string[] expectedStrings = new string[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            expectedInts[i] = random.Next();
            intBuilder.Append(expectedInts[i]);
            expectedStrings[i] = $"val-{random.Next(100)}";
            strBuilder.Append(expectedStrings[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [intBuilder.Build(), strBuilder.Build()], rowCount);

        await WriteEW(path, batch, new ParquetWriteOptions { Compression = codec });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);

        var intArr = (Int32Array)readBatch.Column(0);
        var strArr = (StringArray)readBatch.Column(1);
        for (int i = 0; i < rowCount; i++)
        {
            Assert.Equal(expectedInts[i], intArr.GetValue(i));
            Assert.Equal(expectedStrings[i], strArr.GetString(i));
        }
    }

    [Fact]
    public async Task Roundtrip_LargeRowCount()
    {
        var path = TempPath("rt-large.parquet");
        int rowCount = 100_000;

        var builder = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++)
            builder.Append(i);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [builder.Build()], rowCount);

        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(rowCount, meta.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)readBatch.Column(0);
        Assert.Equal(0, arr.GetValue(0));
        Assert.Equal(rowCount - 1, arr.GetValue(rowCount - 1));
    }

    [Fact]
    public async Task Roundtrip_AllNulls()
    {
        var path = TempPath("rt-allnull.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.AppendNull();
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < 100; i++)
            Assert.True(arr.IsNull(i));
    }

    [Fact]
    public async Task Roundtrip_EmptyFile()
    {
        var path = TempPath("rt-empty.parquet");

        await WriteEmptyEW(path);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(0, meta.NumRows);
        Assert.Empty(meta.RowGroups);
    }

    // ────────────────────────────────────────────────────────────────────
    //  Helpers
    // ────────────────────────────────────────────────────────────────────

    private static async Task WriteEW(string path, RecordBatch batch, ParquetWriteOptions? options = null)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task WriteEmptyEW(string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.CloseAsync();
    }
}
