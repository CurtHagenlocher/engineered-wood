using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Cross-validates EngineeredWood.Avro against Python's fastavro library.
/// Tests both directions:
///   1. fastavro writes → EngineeredWood reads
///   2. EngineeredWood writes → fastavro reads
/// </summary>
public class CrossValidationTests
{
    private static string TestDataDir =>
        Path.Combine(AppContext.BaseDirectory, "TestData");

    private static bool IsFastavroAvailable()
    {
        try
        {
            var psi = new ProcessStartInfo("python", "-c \"import fastavro\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            var p = Process.Start(psi)!;
            p.WaitForExit(5000);
            return p.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    // ─── Direction 1: fastavro writes → EngineeredWood reads ───

    [Fact]
    public void ReadFastavro_Primitives_Null()
    {
        var path = Path.Combine(TestDataDir, "primitives_null.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();

        int totalRows = batches.Sum(b => b.Length);
        Assert.Equal(100, totalRows);

        var batch = batches[0];
        Assert.Equal(7, batch.ColumnCount);

        // Verify first row
        Assert.Equal(true, ((BooleanArray)batch.Column(0)).GetValue(0));
        Assert.Equal(-50, ((Int32Array)batch.Column(1)).GetValue(0));
        Assert.Equal(-500000L, ((Int64Array)batch.Column(2)).GetValue(0));
        Assert.Equal(0f, ((FloatArray)batch.Column(3)).GetValue(0));
        Assert.Equal(0.0, ((DoubleArray)batch.Column(4)).GetValue(0));
        Assert.Equal("row_0", ((StringArray)batch.Column(5)).GetString(0));
    }

    [Fact]
    public void ReadFastavro_Primitives_Deflate()
    {
        var path = Path.Combine(TestDataDir, "primitives_deflate.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        Assert.Equal(AvroCodec.Deflate, reader.Codec);
        var batches = reader.ToList();
        Assert.Equal(100, batches.Sum(b => b.Length));
    }

    [Fact]
    public void ReadFastavro_Primitives_Snappy()
    {
        var path = Path.Combine(TestDataDir, "primitives_snappy.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        Assert.Equal(AvroCodec.Snappy, reader.Codec);
        var batches = reader.ToList();
        Assert.Equal(100, batches.Sum(b => b.Length));
    }

    [Fact]
    public void ReadFastavro_Nullable()
    {
        var path = Path.Combine(TestDataDir, "nullable_null.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(50, batches.Sum(b => b.Length));

        var batch = batches[0];

        // Row 0: id=0, nullable_int=null (0 % 3 == 0), nullable_string=null (0 % 5 == 0)
        Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.False(batch.Column(1).IsValid(0)); // null
        Assert.False(batch.Column(2).IsValid(0)); // null

        // Row 1: id=1, nullable_int=10, nullable_string="value_1"
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(1));
        Assert.True(batch.Column(1).IsValid(1));
        Assert.Equal(10, ((Int32Array)batch.Column(1)).GetValue(1));
        Assert.Equal("value_1", ((StringArray)batch.Column(2)).GetString(1));
    }

    [Fact]
    public void ReadFastavro_EdgeCases()
    {
        var path = Path.Combine(TestDataDir, "edge_cases.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batch = reader.First();

        Assert.Equal(4, batch.Length);

        // Row 0: zeros and empty string
        Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(0L, ((Int64Array)batch.Column(1)).GetValue(0));
        Assert.Equal("", ((StringArray)batch.Column(2)).GetString(0));

        // Row 1: max values
        Assert.Equal(int.MaxValue, ((Int32Array)batch.Column(0)).GetValue(1));
        Assert.Equal(long.MaxValue, ((Int64Array)batch.Column(1)).GetValue(1));

        // Row 2: min values
        Assert.Equal(int.MinValue, ((Int32Array)batch.Column(0)).GetValue(2));
        Assert.Equal(long.MinValue, ((Int64Array)batch.Column(1)).GetValue(2));

        // Row 3: unicode
        Assert.Equal("hello 🌍", ((StringArray)batch.Column(2)).GetString(3));
    }

    [Fact]
    public void ReadFastavro_Empty()
    {
        var path = Path.Combine(TestDataDir, "empty.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Empty(batches);
    }

    // ─── Direction 2: EngineeredWood writes → fastavro reads ───

    [Fact]
    public void WriteThenReadWithFastavro_Primitives()
    {
        if (!IsFastavroAvailable()) return;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("float_col", FloatType.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        var longBuilder = new Int64Array.Builder();
        var floatBuilder = new FloatArray.Builder();
        var doubleBuilder = new DoubleArray.Builder();
        var stringBuilder = new StringArray.Builder();
        var boolBuilder = new BooleanArray.Builder();

        for (int i = 0; i < 20; i++)
        {
            intBuilder.Append(i);
            longBuilder.Append(i * 1000L);
            floatBuilder.Append(i * 0.1f);
            doubleBuilder.Append(i * 0.01);
            stringBuilder.Append($"str_{i}");
            boolBuilder.Append(i % 2 == 0);
        }

        var batch = new RecordBatch(schema,
            [intBuilder.Build(), longBuilder.Build(), floatBuilder.Build(),
             doubleBuilder.Build(), stringBuilder.Build(), boolBuilder.Build()], 20);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(20, result.GetProperty("num_rows").GetInt32());

            var rows = result.GetProperty("rows");
            var row0 = rows[0];
            Assert.Equal(0, row0.GetProperty("int_col").GetInt32());
            Assert.Equal(0L, row0.GetProperty("long_col").GetInt64());
            Assert.Equal("str_0", row0.GetProperty("string_col").GetString());
            Assert.True(row0.GetProperty("bool_col").GetBoolean());

            var row19 = rows[19];
            Assert.Equal(19, row19.GetProperty("int_col").GetInt32());
            Assert.Equal("str_19", row19.GetProperty("string_col").GetString());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [Fact]
    public void WriteThenReadWithFastavro_WithNulls()
    {
        if (!IsFastavroAvailable()) return;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("nullable_int", Int32Type.Default, true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(42);
        builder.AppendNull();
        builder.Append(99);

        var batch = new RecordBatch(schema, [builder.Build()], 3);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(3, result.GetProperty("num_rows").GetInt32());

            var rows = result.GetProperty("rows");
            Assert.Equal(42, rows[0].GetProperty("nullable_int").GetInt32());
            Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("nullable_int").ValueKind);
            Assert.Equal(99, rows[2].GetProperty("nullable_int").GetInt32());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    private static JsonElement ReadWithFastavro(string path)
    {
        var escapedPath = path.Replace("\\", "\\\\").Replace("'", "\\'");
        var script = """
import fastavro
import json

with open(r'__PATH__', 'rb') as f:
    reader = fastavro.reader(f)
    rows = list(reader)

def convert(obj):
    if isinstance(obj, bytes):
        return list(obj)
    if isinstance(obj, dict):
        return {k: convert(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert(v) for v in obj]
    return obj

result = {
    "num_rows": len(rows),
    "rows": [convert(r) for r in rows]
}
print(json.dumps(result))
""".Replace("__PATH__", escapedPath);

        var scriptPath = Path.Combine(Path.GetTempPath(), $"ew_fastavro_{Guid.NewGuid()}.py");
        File.WriteAllText(scriptPath, script);

        try
        {
            var psi = new ProcessStartInfo("python", scriptPath)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(10000);

            if (p.ExitCode != 0)
                throw new Exception($"fastavro script failed: {stderr}");

            return JsonDocument.Parse(stdout).RootElement;
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }
}
