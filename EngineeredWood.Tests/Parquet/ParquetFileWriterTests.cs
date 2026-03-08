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

    // --- Struct Write Tests ---

    [Fact]
    public async Task Struct_NullableStruct_NullableChildren_RoundTrips()
    {
        // Schema: struct_col (optional) → a (optional int32), b (optional string)
        var structType = new StructType(new[]
        {
            new Field("a", Int32Type.Default, nullable: true),
            new Field("b", Apache.Arrow.Types.StringType.Default, nullable: true),
        });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("struct_col", structType, nullable: true))
            .Build();

        int count = 10;
        // Build child arrays
        var aBuilder = new Int32Array.Builder();
        var bBuilder = new StringArray.Builder();
        var structNullBitmap = new byte[(count + 7) / 8];

        // Row layout:
        // 0: struct null
        // 1: struct present, a null, b null
        // 2: struct present, a=10, b="hello"
        // 3: struct present, a null, b="world"
        // 4: struct present, a=40, b null
        // 5: struct null
        // 6: struct present, a=60, b="test"
        // 7: struct present, a=70, b="foo"
        // 8: struct null
        // 9: struct present, a=90, b="bar"

        int?[] expectedA = [null, null, 10, null, 40, null, 60, 70, null, 90];
        string?[] expectedB = [null, null, "hello", "world", null, null, "test", "foo", null, "bar"];
        bool[] structValid = [false, true, true, true, true, false, true, true, false, true];

        for (int i = 0; i < count; i++)
        {
            if (structValid[i])
                BitUtility.SetBit(structNullBitmap, i, true);

            if (expectedA[i] != null) aBuilder.Append(expectedA[i]!.Value);
            else aBuilder.AppendNull();

            if (expectedB[i] != null) bBuilder.Append(expectedB[i]);
            else bBuilder.AppendNull();
        }

        var structArray = new StructArray(structType, count,
            new IArrowArray[] { aBuilder.Build(), bBuilder.Build() },
            new ArrowBuffer(structNullBitmap),
            count - structValid.Count(v => v));

        var batch = new RecordBatch(schema, new IArrowArray[] { structArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        Assert.Single(result.Schema.FieldsList);
        Assert.IsType<StructType>(result.Schema.FieldsList[0].DataType);

        var resultStruct = (StructArray)result.Column(0);
        Assert.Equal(2, resultStruct.Fields.Count);

        var resultA = (Int32Array)resultStruct.Fields[0];
        var resultB = (StringArray)resultStruct.Fields[1];

        for (int i = 0; i < count; i++)
        {
            if (!structValid[i])
            {
                Assert.True(resultStruct.IsNull(i), $"Row {i}: struct should be null");
                Assert.True(resultA.IsNull(i), $"Row {i}: a should be null (struct null)");
                Assert.True(resultB.IsNull(i), $"Row {i}: b should be null (struct null)");
            }
            else if (expectedA[i] == null)
            {
                Assert.True(resultA.IsNull(i), $"Row {i}: a should be null");
            }
            else
            {
                Assert.Equal(expectedA[i], resultA.GetValue(i));
            }

            if (structValid[i])
            {
                if (expectedB[i] == null)
                    Assert.True(resultB.IsNull(i), $"Row {i}: b should be null");
                else
                    Assert.Equal(expectedB[i], resultB.GetString(i));
            }
        }
    }

    [Fact]
    public async Task Struct_RequiredStruct_NullableChildren_RoundTrips()
    {
        // Schema: struct_col (required) → x (optional int32), y (optional double)
        var structType = new StructType(new[]
        {
            new Field("x", Int32Type.Default, nullable: true),
            new Field("y", DoubleType.Default, nullable: true),
        });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("struct_col", structType, nullable: false))
            .Build();

        int count = 8;
        var xBuilder = new Int32Array.Builder();
        var yBuilder = new DoubleArray.Builder();

        int?[] expectedX = [1, null, 3, null, 5, 6, null, 8];
        double?[] expectedY = [1.1, 2.2, null, null, 5.5, null, 7.7, 8.8];

        for (int i = 0; i < count; i++)
        {
            if (expectedX[i] != null) xBuilder.Append(expectedX[i]!.Value);
            else xBuilder.AppendNull();

            if (expectedY[i] != null) yBuilder.Append(expectedY[i]!.Value);
            else yBuilder.AppendNull();
        }

        // Required struct: no null bitmap needed (all valid)
        var structArray = new StructArray(structType, count,
            new IArrowArray[] { xBuilder.Build(), yBuilder.Build() },
            ArrowBuffer.Empty, 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { structArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        var resultStruct = (StructArray)result.Column(0);
        var resultX = (Int32Array)resultStruct.Fields[0];
        var resultY = (DoubleArray)resultStruct.Fields[1];

        for (int i = 0; i < count; i++)
        {
            Assert.False(resultStruct.IsNull(i), $"Row {i}: required struct should never be null");
            if (expectedX[i] == null)
                Assert.True(resultX.IsNull(i));
            else
                Assert.Equal(expectedX[i], resultX.GetValue(i));

            if (expectedY[i] == null)
                Assert.True(resultY.IsNull(i));
            else
                Assert.Equal(expectedY[i], resultY.GetValue(i));
        }
    }

    [Fact]
    public async Task Struct_NestedStructInStruct_RoundTrips()
    {
        // Schema: outer (optional) → x (required int32), inner (optional) → y (required int64)
        var innerType = new StructType(new[]
        {
            new Field("y", Int64Type.Default, nullable: false),
        });
        var outerType = new StructType(new[]
        {
            new Field("x", Int32Type.Default, nullable: false),
            new Field("inner", innerType, nullable: true),
        });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("outer", outerType, nullable: true))
            .Build();

        int count = 5;
        // Row layout:
        // 0: outer null
        // 1: outer present, x=10, inner present, y=100
        // 2: outer present, x=20, inner null
        // 3: outer present, x=30, inner present, y=300
        // 4: outer null

        bool[] outerValid = [false, true, true, true, false];
        int[] xValues = [0, 10, 20, 30, 0]; // 0s are placeholders for null outer rows
        bool[] innerValid = [false, true, false, true, false];
        long[] yValues = [0, 100, 0, 300, 0];

        var xBuilder = new Int32Array.Builder();
        var yBuilder = new Int64Array.Builder();

        for (int i = 0; i < count; i++)
        {
            xBuilder.Append(xValues[i]);
            yBuilder.Append(yValues[i]);
        }

        var innerNullBitmap = new byte[(count + 7) / 8];
        var outerNullBitmap = new byte[(count + 7) / 8];
        for (int i = 0; i < count; i++)
        {
            BitUtility.SetBit(innerNullBitmap, i, innerValid[i]);
            BitUtility.SetBit(outerNullBitmap, i, outerValid[i]);
        }

        var innerArray = new StructArray(innerType, count,
            new IArrowArray[] { yBuilder.Build() },
            new ArrowBuffer(innerNullBitmap),
            count - innerValid.Count(v => v));

        var outerArray = new StructArray(outerType, count,
            new IArrowArray[] { xBuilder.Build(), innerArray },
            new ArrowBuffer(outerNullBitmap),
            count - outerValid.Count(v => v));

        var batch = new RecordBatch(schema, new IArrowArray[] { outerArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        var resultOuter = (StructArray)result.Column(0);
        Assert.Equal(2, resultOuter.Fields.Count);

        var resultX = (Int32Array)resultOuter.Fields[0];
        var resultInner = (StructArray)resultOuter.Fields[1];
        var resultY = (Int64Array)resultInner.Fields[0];

        for (int i = 0; i < count; i++)
        {
            if (!outerValid[i])
            {
                Assert.True(resultOuter.IsNull(i), $"Row {i}: outer should be null");
                continue;
            }

            Assert.False(resultOuter.IsNull(i));
            Assert.Equal(xValues[i], resultX.GetValue(i));

            if (!innerValid[i])
            {
                Assert.True(resultInner.IsNull(i), $"Row {i}: inner should be null");
            }
            else
            {
                Assert.False(resultInner.IsNull(i));
                Assert.Equal(yValues[i], resultY.GetValue(i));
            }
        }
    }

    [Fact]
    public async Task Struct_V1_NullableStruct_RoundTrips()
    {
        var structType = new StructType(new[]
        {
            new Field("val", Int32Type.Default, nullable: true),
        });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", structType, nullable: true))
            .Build();

        int count = 6;
        bool[] structValid = [true, false, true, true, false, true];
        int?[] expectedVal = [42, null, null, 99, null, 7];

        var valBuilder = new Int32Array.Builder();
        var nullBitmap = new byte[(count + 7) / 8];
        for (int i = 0; i < count; i++)
        {
            BitUtility.SetBit(nullBitmap, i, structValid[i]);
            if (expectedVal[i] != null) valBuilder.Append(expectedVal[i]!.Value);
            else valBuilder.AppendNull();
        }

        var structArray = new StructArray(structType, count,
            new IArrowArray[] { valBuilder.Build() },
            new ArrowBuffer(nullBitmap),
            count - structValid.Count(v => v));

        var batch = new RecordBatch(schema, new IArrowArray[] { structArray }, count);
        var result = await WriteAndRead(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
        });

        var resultStruct = (StructArray)result.Column(0);
        var resultVal = (Int32Array)resultStruct.Fields[0];

        for (int i = 0; i < count; i++)
        {
            if (!structValid[i])
            {
                Assert.True(resultStruct.IsNull(i));
                continue;
            }
            if (expectedVal[i] == null)
                Assert.True(resultVal.IsNull(i));
            else
                Assert.Equal(expectedVal[i], resultVal.GetValue(i));
        }
    }

    [Fact]
    public async Task Struct_MixedWithFlatColumns_RoundTrips()
    {
        // Schema: id (int32), info (optional struct) → name (string), score (double), active (bool)
        var structType = new StructType(new[]
        {
            new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true),
            new Field("score", DoubleType.Default, nullable: true),
        });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("info", structType, nullable: true))
            .Field(new Field("active", BooleanType.Default, nullable: true))
            .Build();

        int count = 8;
        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var scoreBuilder = new DoubleArray.Builder();
        var activeBuilder = new BooleanArray.Builder();
        var structNullBitmap = new byte[(count + 7) / 8];

        bool[] structValid = [true, true, false, true, false, true, true, false];
        string?[] expectedNames = ["alice", null, null, "dave", null, null, "gina", null];
        double?[] expectedScores = [95.0, 82.5, null, null, null, 70.0, 88.0, null];
        bool?[] expectedActive = [true, false, null, true, true, null, false, true];

        for (int i = 0; i < count; i++)
        {
            idBuilder.Append(i);
            BitUtility.SetBit(structNullBitmap, i, structValid[i]);
            if (expectedNames[i] != null) nameBuilder.Append(expectedNames[i]);
            else nameBuilder.AppendNull();
            if (expectedScores[i] != null) scoreBuilder.Append(expectedScores[i]!.Value);
            else scoreBuilder.AppendNull();
            if (expectedActive[i] != null) activeBuilder.Append(expectedActive[i]!.Value);
            else activeBuilder.AppendNull();
        }

        var structArray = new StructArray(structType, count,
            new IArrowArray[] { nameBuilder.Build(), scoreBuilder.Build() },
            new ArrowBuffer(structNullBitmap),
            count - structValid.Count(v => v));

        var batch = new RecordBatch(schema,
            new IArrowArray[] { idBuilder.Build(), structArray, activeBuilder.Build() }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        Assert.Equal(3, result.Schema.FieldsList.Count);

        var resultId = (Int32Array)result.Column(0);
        var resultStruct = (StructArray)result.Column(1);
        var resultActive = (BooleanArray)result.Column(2);

        var resultName = (StringArray)resultStruct.Fields[0];
        var resultScore = (DoubleArray)resultStruct.Fields[1];

        for (int i = 0; i < count; i++)
        {
            Assert.Equal(i, resultId.GetValue(i));

            if (!structValid[i])
            {
                Assert.True(resultStruct.IsNull(i));
            }
            else
            {
                Assert.False(resultStruct.IsNull(i));
                if (expectedNames[i] == null) Assert.True(resultName.IsNull(i));
                else Assert.Equal(expectedNames[i], resultName.GetString(i));
                if (expectedScores[i] == null) Assert.True(resultScore.IsNull(i));
                else Assert.Equal(expectedScores[i], resultScore.GetValue(i));
            }

            if (expectedActive[i] == null) Assert.True(resultActive.IsNull(i));
            else Assert.Equal(expectedActive[i], resultActive.GetValue(i));
        }
    }

    // --- List Write Tests ---

    [Fact]
    public async Task List_NullableListOfNullableInt32_RoundTrips()
    {
        // Schema: values (optional list<optional int32>)
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("values", listType, nullable: true))
            .Build();

        // Row 0: [1, 2, 3]
        // Row 1: null (list is null)
        // Row 2: [] (empty list)
        // Row 3: [null, 5]
        // Row 4: [6]
        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.Append(1); valuesBuilder.Append(2); valuesBuilder.Append(3); // row 0
        valuesBuilder.AppendNull(); valuesBuilder.Append(5); // row 3
        valuesBuilder.Append(6); // row 4

        var offsetsBuilder = new ArrowBuffer.Builder<int>();
        offsetsBuilder.Append(0); // row 0 start
        offsetsBuilder.Append(3); // row 1 start (null)
        offsetsBuilder.Append(3); // row 2 start (empty)
        offsetsBuilder.Append(3); // row 3 start
        offsetsBuilder.Append(5); // row 4 start
        offsetsBuilder.Append(6); // end

        var nullBitmap = new byte[1];
        BitUtility.SetBit(nullBitmap, 0, true);  // row 0: valid
        BitUtility.SetBit(nullBitmap, 1, false); // row 1: null
        BitUtility.SetBit(nullBitmap, 2, true);  // row 2: valid (empty)
        BitUtility.SetBit(nullBitmap, 3, true);  // row 3: valid
        BitUtility.SetBit(nullBitmap, 4, true);  // row 4: valid

        int count = 5;
        var listArray = new ListArray(listType, count,
            offsetsBuilder.Build(), valuesBuilder.Build(),
            new ArrowBuffer(nullBitmap), nullCount: 1);

        var batch = new RecordBatch(schema, new IArrowArray[] { listArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        Assert.IsType<ListType>(result.Schema.FieldsList[0].DataType);
        var resultList = (ListArray)result.Column(0);

        // Row 0: [1, 2, 3]
        Assert.False(resultList.IsNull(0));
        var r0 = (Int32Array)resultList.GetSlicedValues(0);
        Assert.Equal(3, r0.Length);
        Assert.Equal(1, r0.GetValue(0));
        Assert.Equal(2, r0.GetValue(1));
        Assert.Equal(3, r0.GetValue(2));

        // Row 1: null
        Assert.True(resultList.IsNull(1));

        // Row 2: empty
        Assert.False(resultList.IsNull(2));
        var r2 = (Int32Array)resultList.GetSlicedValues(2);
        Assert.Equal(0, r2.Length);

        // Row 3: [null, 5]
        Assert.False(resultList.IsNull(3));
        var r3 = (Int32Array)resultList.GetSlicedValues(3);
        Assert.Equal(2, r3.Length);
        Assert.True(r3.IsNull(0));
        Assert.Equal(5, r3.GetValue(1));

        // Row 4: [6]
        Assert.False(resultList.IsNull(4));
        var r4 = (Int32Array)resultList.GetSlicedValues(4);
        Assert.Equal(1, r4.Length);
        Assert.Equal(6, r4.GetValue(0));
    }

    [Fact]
    public async Task List_NullableListOfStrings_RoundTrips()
    {
        var listType = new ListType(new Field("element", Apache.Arrow.Types.StringType.Default, nullable: true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("names", listType, nullable: true))
            .Build();

        // Row 0: ["hello", "world"]
        // Row 1: null
        // Row 2: ["test"]
        // Row 3: [null, "foo", "bar"]
        var valuesBuilder = new StringArray.Builder();
        valuesBuilder.Append("hello"); valuesBuilder.Append("world"); // row 0
        valuesBuilder.Append("test"); // row 2
        valuesBuilder.AppendNull(); valuesBuilder.Append("foo"); valuesBuilder.Append("bar"); // row 3

        var offsetsBuilder = new ArrowBuffer.Builder<int>();
        offsetsBuilder.Append(0); offsetsBuilder.Append(2); offsetsBuilder.Append(2);
        offsetsBuilder.Append(3); offsetsBuilder.Append(6);

        var nullBitmap = new byte[1];
        BitUtility.SetBit(nullBitmap, 0, true);
        BitUtility.SetBit(nullBitmap, 1, false);
        BitUtility.SetBit(nullBitmap, 2, true);
        BitUtility.SetBit(nullBitmap, 3, true);

        int count = 4;
        var listArray = new ListArray(listType, count,
            offsetsBuilder.Build(), valuesBuilder.Build(),
            new ArrowBuffer(nullBitmap), nullCount: 1);

        var batch = new RecordBatch(schema, new IArrowArray[] { listArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        var resultList = (ListArray)result.Column(0);

        // Row 0: ["hello", "world"]
        var r0 = (StringArray)resultList.GetSlicedValues(0);
        Assert.Equal(2, r0.Length);
        Assert.Equal("hello", r0.GetString(0));
        Assert.Equal("world", r0.GetString(1));

        // Row 1: null
        Assert.True(resultList.IsNull(1));

        // Row 2: ["test"]
        var r2 = (StringArray)resultList.GetSlicedValues(2);
        Assert.Equal(1, r2.Length);
        Assert.Equal("test", r2.GetString(0));

        // Row 3: [null, "foo", "bar"]
        var r3 = (StringArray)resultList.GetSlicedValues(3);
        Assert.Equal(3, r3.Length);
        Assert.True(r3.IsNull(0));
        Assert.Equal("foo", r3.GetString(1));
        Assert.Equal("bar", r3.GetString(2));
    }

    [Fact]
    public async Task List_RequiredListOfRequiredInt32_RoundTrips()
    {
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("numbers", listType, nullable: false))
            .Build();

        // Row 0: [10, 20, 30]
        // Row 1: [40]
        // Row 2: [50, 60]
        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.Append(10); valuesBuilder.Append(20); valuesBuilder.Append(30);
        valuesBuilder.Append(40);
        valuesBuilder.Append(50); valuesBuilder.Append(60);

        var offsetsBuilder = new ArrowBuffer.Builder<int>();
        offsetsBuilder.Append(0); offsetsBuilder.Append(3); offsetsBuilder.Append(4); offsetsBuilder.Append(6);

        int count = 3;
        var listArray = new ListArray(listType, count,
            offsetsBuilder.Build(), valuesBuilder.Build(),
            ArrowBuffer.Empty, nullCount: 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { listArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        var resultList = (ListArray)result.Column(0);

        var r0 = (Int32Array)resultList.GetSlicedValues(0);
        Assert.Equal(3, r0.Length);
        Assert.Equal(10, r0.GetValue(0));
        Assert.Equal(20, r0.GetValue(1));
        Assert.Equal(30, r0.GetValue(2));

        var r1 = (Int32Array)resultList.GetSlicedValues(1);
        Assert.Equal(1, r1.Length);
        Assert.Equal(40, r1.GetValue(0));

        var r2 = (Int32Array)resultList.GetSlicedValues(2);
        Assert.Equal(2, r2.Length);
        Assert.Equal(50, r2.GetValue(0));
        Assert.Equal(60, r2.GetValue(1));
    }

    [Fact]
    public async Task List_MixedWithFlatColumns_RoundTrips()
    {
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("tags", listType, nullable: true))
            .Build();

        int count = 3;
        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1); idBuilder.Append(2); idBuilder.Append(3);

        // Row 0: [10, 20]
        // Row 1: null
        // Row 2: [30]
        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.Append(10); valuesBuilder.Append(20);
        valuesBuilder.Append(30);

        var offsetsBuilder = new ArrowBuffer.Builder<int>();
        offsetsBuilder.Append(0); offsetsBuilder.Append(2); offsetsBuilder.Append(2); offsetsBuilder.Append(3);

        var nullBitmap = new byte[1];
        BitUtility.SetBit(nullBitmap, 0, true);
        BitUtility.SetBit(nullBitmap, 1, false);
        BitUtility.SetBit(nullBitmap, 2, true);

        var listArray = new ListArray(listType, count,
            offsetsBuilder.Build(), valuesBuilder.Build(),
            new ArrowBuffer(nullBitmap), nullCount: 1);

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), listArray }, count);
        var result = await WriteAndRead(batch);

        Assert.Equal(count, result.Length);
        var resultId = (Int32Array)result.Column(0);
        var resultList = (ListArray)result.Column(1);

        Assert.Equal(1, resultId.GetValue(0));
        Assert.Equal(2, resultId.GetValue(1));
        Assert.Equal(3, resultId.GetValue(2));

        var r0 = (Int32Array)resultList.GetSlicedValues(0);
        Assert.Equal(2, r0.Length);
        Assert.Equal(10, r0.GetValue(0));
        Assert.Equal(20, r0.GetValue(1));

        Assert.True(resultList.IsNull(1));

        var r2 = (Int32Array)resultList.GetSlicedValues(2);
        Assert.Equal(1, r2.Length);
        Assert.Equal(30, r2.GetValue(0));
    }

    [Fact]
    public async Task List_V1_NullableList_RoundTrips()
    {
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("values", listType, nullable: true))
            .Build();

        // Row 0: [1, 2]
        // Row 1: null
        // Row 2: [3]
        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.Append(1); valuesBuilder.Append(2);
        valuesBuilder.Append(3);

        var offsetsBuilder = new ArrowBuffer.Builder<int>();
        offsetsBuilder.Append(0); offsetsBuilder.Append(2); offsetsBuilder.Append(2); offsetsBuilder.Append(3);

        var nullBitmap = new byte[1];
        BitUtility.SetBit(nullBitmap, 0, true);
        BitUtility.SetBit(nullBitmap, 1, false);
        BitUtility.SetBit(nullBitmap, 2, true);

        int count = 3;
        var listArray = new ListArray(listType, count,
            offsetsBuilder.Build(), valuesBuilder.Build(),
            new ArrowBuffer(nullBitmap), nullCount: 1);

        var batch = new RecordBatch(schema, new IArrowArray[] { listArray }, count);
        var result = await WriteAndRead(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
        });

        var resultList = (ListArray)result.Column(0);
        var r0 = (Int32Array)resultList.GetSlicedValues(0);
        Assert.Equal(2, r0.Length);
        Assert.Equal(1, r0.GetValue(0));
        Assert.Equal(2, r0.GetValue(1));

        Assert.True(resultList.IsNull(1));

        var r2 = (Int32Array)resultList.GetSlicedValues(2);
        Assert.Equal(1, r2.Length);
        Assert.Equal(3, r2.GetValue(0));
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
