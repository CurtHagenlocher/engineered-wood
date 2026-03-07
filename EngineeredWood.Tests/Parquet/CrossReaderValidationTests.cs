using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Schema;
using Field = Apache.Arrow.Field;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Validates that Parquet files written by EngineeredWood can be correctly read
/// by ParquetSharp and Parquet.NET — proving cross-implementation compatibility.
/// </summary>
public class CrossReaderValidationTests : IDisposable
{
    private readonly string _tempDir;

    public CrossReaderValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew-xread-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private string TempFile(string name) => Path.Combine(_tempDir, name);

    private async Task<string> WriteWithEW(RecordBatch batch, ParquetWriteOptions? options = null, string name = "test.parquet")
    {
        var path = TempFile(name);
        await using var output = new LocalOutputFile(path);
        await using var writer = new ParquetFileWriter(output, options);
        await writer.WriteAsync(batch);
        return path;
    }

    // ----------------------------------------------------------------
    //  ParquetSharp cross-reader tests
    // ----------------------------------------------------------------

    [Fact]
    public async Task PS_Int32_NonNullable_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 500; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 500);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Uncompressed,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        Assert.Equal(500, rg.MetaData.NumRows);

        using var col = rg.Column(0).LogicalReader<int>();
        var values = col.ReadAll(500);
        for (int i = 0; i < 500; i++)
            Assert.Equal(i, values[i]);
    }

    [Fact]
    public async Task PS_Int32_Nullable_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        var rng = new Random(42);
        var expected = new int?[1000];
        for (int i = 0; i < 1000; i++)
        {
            if (rng.NextDouble() < 0.15)
            {
                builder.AppendNull();
                expected[i] = null;
            }
            else
            {
                builder.Append(i);
                expected[i] = i;
            }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 1000);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int?>();
        var values = col.ReadAll(1000);

        for (int i = 0; i < 1000; i++)
            Assert.Equal(expected[i], values[i]);
    }

    [Fact]
    public async Task PS_Int64_V2_Zstd()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", Int64Type.Default, nullable: false))
            .Build();

        var builder = new Int64Array.Builder();
        for (int i = 0; i < 300; i++) builder.Append((long)i * 1_000_000);
        var batch = new RecordBatch(schema, [builder.Build()], 300);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Zstd,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<long>();
        var values = col.ReadAll(300);

        for (int i = 0; i < 300; i++)
            Assert.Equal((long)i * 1_000_000, values[i]);
    }

    [Fact]
    public async Task PS_Float_Double_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", FloatType.Default, nullable: false))
            .Field(new Field("d", DoubleType.Default, nullable: false))
            .Build();

        var fb = new FloatArray.Builder();
        var db = new DoubleArray.Builder();
        for (int i = 0; i < 200; i++)
        {
            fb.Append(i * 1.5f);
            db.Append(i * 3.14);
        }
        var batch = new RecordBatch(schema, [fb.Build(), db.Build()], 200);

        var path = await WriteWithEW(batch);

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);

        using var fCol = rg.Column(0).LogicalReader<float>();
        var fValues = fCol.ReadAll(200);

        using var dCol = rg.Column(1).LogicalReader<double>();
        var dValues = dCol.ReadAll(200);

        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(i * 1.5f, fValues[i]);
            Assert.Equal(i * 3.14, dValues[i]);
        }
    }

    [Fact]
    public async Task PS_Boolean_Nullable_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("flag", BooleanType.Default, nullable: true))
            .Build();

        var builder = new BooleanArray.Builder();
        var expected = new bool?[100];
        var rng = new Random(7);
        for (int i = 0; i < 100; i++)
        {
            double r = rng.NextDouble();
            if (r < 0.2)
            {
                builder.AppendNull();
                expected[i] = null;
            }
            else
            {
                bool v = r > 0.6;
                builder.Append(v);
                expected[i] = v;
            }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<bool?>();
        var values = col.ReadAll(100);

        for (int i = 0; i < 100; i++)
            Assert.Equal(expected[i], values[i]);
    }

    [Fact]
    public async Task PS_String_Nullable_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        var builder = new StringArray.Builder();
        var expected = new string?[200];
        var rng = new Random(99);
        for (int i = 0; i < 200; i++)
        {
            if (rng.NextDouble() < 0.15)
            {
                builder.AppendNull();
                expected[i] = null;
            }
            else
            {
                var s = $"item_{i}_{rng.Next(10000)}";
                builder.Append(s);
                expected[i] = s;
            }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 200);

        var path = await WriteWithEW(batch);

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string?>();
        var values = col.ReadAll(200);

        for (int i = 0; i < 200; i++)
            Assert.Equal(expected[i], values[i]);
    }

    [Fact]
    public async Task PS_MultipleColumns_Mixed_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Field(new Field("score", DoubleType.Default, nullable: true))
            .Field(new Field("active", BooleanType.Default, nullable: true))
            .Build();

        int count = 500;
        var idB = new Int32Array.Builder();
        var nameB = new StringArray.Builder();
        var scoreB = new DoubleArray.Builder();
        var activeB = new BooleanArray.Builder();

        var expectedNames = new string?[count];
        var expectedScores = new double?[count];
        var expectedActive = new bool?[count];
        var rng = new Random(42);

        for (int i = 0; i < count; i++)
        {
            idB.Append(i);

            if (rng.NextDouble() < 0.1) { nameB.AppendNull(); expectedNames[i] = null; }
            else { var s = $"user_{i}"; nameB.Append(s); expectedNames[i] = s; }

            if (rng.NextDouble() < 0.1) { scoreB.AppendNull(); expectedScores[i] = null; }
            else { var d = rng.NextDouble() * 100; scoreB.Append(d); expectedScores[i] = d; }

            if (rng.NextDouble() < 0.1) { activeB.AppendNull(); expectedActive[i] = null; }
            else { var b = rng.NextDouble() > 0.5; activeB.Append(b); expectedActive[i] = b; }
        }

        var batch = new RecordBatch(schema,
            [idB.Build(), nameB.Build(), scoreB.Build(), activeB.Build()], count);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);

        using var idCol = rg.Column(0).LogicalReader<int>();
        var ids = idCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(i, ids[i]);

        using var nameCol = rg.Column(1).LogicalReader<string?>();
        var names = nameCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedNames[i], names[i]);

        using var scoreCol = rg.Column(2).LogicalReader<double?>();
        var scores = scoreCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedScores[i], scores[i]);

        using var activeCol = rg.Column(3).LogicalReader<bool?>();
        var actives = activeCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedActive[i], actives[i]);
    }

    [Fact]
    public async Task PS_V1_Pages_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        var expected = new int?[300];
        for (int i = 0; i < 300; i++)
        {
            if (i % 10 == 0) { builder.AppendNull(); expected[i] = null; }
            else { builder.Append(i * 3); expected[i] = i * 3; }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 300);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
            Codec = CompressionCodec.Snappy,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int?>();
        var values = col.ReadAll(300);

        for (int i = 0; i < 300; i++)
            Assert.Equal(expected[i], values[i]);
    }

    [Fact]
    public async Task PS_MultipleRowGroups_V2()
    {
        var path = TempFile("multi-rg.parquet");
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V2 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: false))
            .Build();

        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output, options))
        {
            var b1 = new Int32Array.Builder();
            for (int i = 0; i < 200; i++) b1.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b1.Build()], 200));

            var b2 = new Int32Array.Builder();
            for (int i = 200; i < 500; i++) b2.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b2.Build()], 300));
        }

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        Assert.Equal(2, psReader.FileMetaData.NumRowGroups);
        Assert.Equal(500, psReader.FileMetaData.NumRows);

        // Row group 0
        using var rg0 = psReader.RowGroup(0);
        using var col0 = rg0.Column(0).LogicalReader<int>();
        var vals0 = col0.ReadAll(200);
        for (int i = 0; i < 200; i++)
            Assert.Equal(i, vals0[i]);

        // Row group 1
        using var rg1 = psReader.RowGroup(1);
        using var col1 = rg1.Column(0).LogicalReader<int>();
        var vals1 = col1.ReadAll(300);
        for (int i = 0; i < 300; i++)
            Assert.Equal(i + 200, vals1[i]);
    }

    [Fact]
    public async Task PS_LargeDataset_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Field(new Field("label", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        int count = 50_000;
        var idB = new Int32Array.Builder();
        var valB = new DoubleArray.Builder();
        var lblB = new StringArray.Builder();
        var expectedVals = new double?[count];
        var expectedLbls = new string?[count];
        var rng = new Random(42);

        for (int i = 0; i < count; i++)
        {
            idB.Append(i);
            if (rng.NextDouble() < 0.1) { valB.AppendNull(); expectedVals[i] = null; }
            else { var v = rng.NextDouble() * 1000; valB.Append(v); expectedVals[i] = v; }
            if (rng.NextDouble() < 0.05) { lblB.AppendNull(); expectedLbls[i] = null; }
            else { var s = $"cat_{i % 100}"; lblB.Append(s); expectedLbls[i] = s; }
        }

        var batch = new RecordBatch(schema,
            [idB.Build(), valB.Build(), lblB.Build()], count);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        });

        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        Assert.Equal(count, rg.MetaData.NumRows);

        using var idCol = rg.Column(0).LogicalReader<int>();
        var ids = idCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(i, ids[i]);

        using var valCol = rg.Column(1).LogicalReader<double?>();
        var vals = valCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedVals[i], vals[i]);

        using var lblCol = rg.Column(2).LogicalReader<string?>();
        var lbls = lblCol.ReadAll(count);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedLbls[i], lbls[i]);
    }

    // ----------------------------------------------------------------
    //  Parquet.NET cross-reader tests
    // ----------------------------------------------------------------

    [Fact]
    public async Task PNet_Int32_NonNullable_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 500; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 500);

        var path = await WriteWithEW(batch, name: "pnet_int32.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        var fields = reader.Schema.GetDataFields();
        Assert.Single(fields);
        Assert.Equal("id", fields[0].Name);

        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(fields[0]);
        var data = (int[])column.Data;

        Assert.Equal(500, data.Length);
        for (int i = 0; i < 500; i++)
            Assert.Equal(i, data[i]);
    }

    [Fact]
    public async Task PNet_Int32_Nullable_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        var expected = new int?[300];
        for (int i = 0; i < 300; i++)
        {
            if (i % 7 == 0) { builder.AppendNull(); expected[i] = null; }
            else { builder.Append(i * 2); expected[i] = i * 2; }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 300);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        }, name: "pnet_int32_null.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
        var data = (int?[])column.Data;

        Assert.Equal(300, data.Length);
        for (int i = 0; i < 300; i++)
            Assert.Equal(expected[i], data[i]);
    }

    [Fact]
    public async Task PNet_Int64_V2_Zstd()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", Int64Type.Default, nullable: false))
            .Build();

        var builder = new Int64Array.Builder();
        for (int i = 0; i < 200; i++) builder.Append((long)i * 1_000_000);
        var batch = new RecordBatch(schema, [builder.Build()], 200);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Zstd,
        }, name: "pnet_int64.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
        var data = (long[])column.Data;

        Assert.Equal(200, data.Length);
        for (int i = 0; i < 200; i++)
            Assert.Equal((long)i * 1_000_000, data[i]);
    }

    [Fact]
    public async Task PNet_Float_Double_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", FloatType.Default, nullable: false))
            .Field(new Field("d", DoubleType.Default, nullable: false))
            .Build();

        var fb = new FloatArray.Builder();
        var db = new DoubleArray.Builder();
        for (int i = 0; i < 150; i++)
        {
            fb.Append(i * 1.5f);
            db.Append(i * 3.14);
        }
        var batch = new RecordBatch(schema, [fb.Build(), db.Build()], 150);

        var path = await WriteWithEW(batch, name: "pnet_float_double.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        var fields = reader.Schema.GetDataFields();
        using var rgReader = reader.OpenRowGroupReader(0);

        var fData = (float[])((await rgReader.ReadColumnAsync(fields[0])).Data);
        var dData = (double[])((await rgReader.ReadColumnAsync(fields[1])).Data);

        for (int i = 0; i < 150; i++)
        {
            Assert.Equal(i * 1.5f, fData[i]);
            Assert.Equal(i * 3.14, dData[i]);
        }
    }

    [Fact]
    public async Task PNet_String_Nullable_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        var builder = new StringArray.Builder();
        var expected = new string?[200];
        var rng = new Random(77);
        for (int i = 0; i < 200; i++)
        {
            if (rng.NextDouble() < 0.15) { builder.AppendNull(); expected[i] = null; }
            else { var s = $"val_{i}"; builder.Append(s); expected[i] = s; }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 200);

        var path = await WriteWithEW(batch, name: "pnet_string.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
        var data = (string?[])column.Data;

        Assert.Equal(200, data.Length);
        for (int i = 0; i < 200; i++)
            Assert.Equal(expected[i], data[i]);
    }

    [Fact]
    public async Task PNet_Boolean_Nullable_V2()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("flag", BooleanType.Default, nullable: true))
            .Build();

        var builder = new BooleanArray.Builder();
        var expected = new bool?[100];
        var rng = new Random(11);
        for (int i = 0; i < 100; i++)
        {
            double r = rng.NextDouble();
            if (r < 0.2) { builder.AppendNull(); expected[i] = null; }
            else { bool v = r > 0.6; builder.Append(v); expected[i] = v; }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        var path = await WriteWithEW(batch, name: "pnet_bool.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
        var data = (bool?[])column.Data;

        Assert.Equal(100, data.Length);
        for (int i = 0; i < 100; i++)
            Assert.Equal(expected[i], data[i]);
    }

    [Fact]
    public async Task PNet_MultipleColumns_V2_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Field(new Field("score", DoubleType.Default, nullable: true))
            .Build();

        int count = 400;
        var idB = new Int32Array.Builder();
        var nameB = new StringArray.Builder();
        var scoreB = new DoubleArray.Builder();
        var expectedNames = new string?[count];
        var expectedScores = new double?[count];
        var rng = new Random(42);

        for (int i = 0; i < count; i++)
        {
            idB.Append(i);
            if (rng.NextDouble() < 0.1) { nameB.AppendNull(); expectedNames[i] = null; }
            else { var s = $"user_{i}"; nameB.Append(s); expectedNames[i] = s; }
            if (rng.NextDouble() < 0.1) { scoreB.AppendNull(); expectedScores[i] = null; }
            else { var d = rng.NextDouble() * 100; scoreB.Append(d); expectedScores[i] = d; }
        }

        var batch = new RecordBatch(schema,
            [idB.Build(), nameB.Build(), scoreB.Build()], count);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            Codec = CompressionCodec.Snappy,
        }, name: "pnet_multi.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        var fields = reader.Schema.GetDataFields();
        using var rgReader = reader.OpenRowGroupReader(0);

        var ids = (int[])((await rgReader.ReadColumnAsync(fields[0])).Data);
        for (int i = 0; i < count; i++)
            Assert.Equal(i, ids[i]);

        var names = (string?[])((await rgReader.ReadColumnAsync(fields[1])).Data);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedNames[i], names[i]);

        var scores = (double?[])((await rgReader.ReadColumnAsync(fields[2])).Data);
        for (int i = 0; i < count; i++)
            Assert.Equal(expectedScores[i], scores[i]);
    }

    [Fact]
    public async Task PNet_V1_Pages_Snappy()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        var expected = new int?[200];
        for (int i = 0; i < 200; i++)
        {
            if (i % 8 == 0) { builder.AppendNull(); expected[i] = null; }
            else { builder.Append(i); expected[i] = i; }
        }
        var batch = new RecordBatch(schema, [builder.Build()], 200);

        var path = await WriteWithEW(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
            Codec = CompressionCodec.Snappy,
        }, name: "pnet_v1.parquet");

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        using var rgReader = reader.OpenRowGroupReader(0);
        var column = await rgReader.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
        var data = (int?[])column.Data;

        Assert.Equal(200, data.Length);
        for (int i = 0; i < 200; i++)
            Assert.Equal(expected[i], data[i]);
    }

    [Fact]
    public async Task PNet_MultipleRowGroups_V2()
    {
        var path = TempFile("pnet_multi_rg.parquet");
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V2 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: false))
            .Build();

        await using (var output = new LocalOutputFile(path))
        await using (var writer = new ParquetFileWriter(output, options))
        {
            var b1 = new Int32Array.Builder();
            for (int i = 0; i < 150; i++) b1.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b1.Build()], 150));

            var b2 = new Int32Array.Builder();
            for (int i = 150; i < 400; i++) b2.Append(i);
            await writer.WriteAsync(new RecordBatch(schema, [b2.Build()], 250));
        }

        using var stream = File.OpenRead(path);
        using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(2, reader.RowGroupCount);

        // Row group 0
        using var rg0 = reader.OpenRowGroupReader(0);
        var data0 = (int[])((await rg0.ReadColumnAsync(reader.Schema.GetDataFields()[0])).Data);
        Assert.Equal(150, data0.Length);
        for (int i = 0; i < 150; i++)
            Assert.Equal(i, data0[i]);

        // Row group 1
        using var rg1 = reader.OpenRowGroupReader(1);
        var data1 = (int[])((await rg1.ReadColumnAsync(reader.Schema.GetDataFields()[0])).Data);
        Assert.Equal(250, data1.Length);
        for (int i = 0; i < 250; i++)
            Assert.Equal(i + 150, data1[i]);
    }
}
