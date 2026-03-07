using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Per-encoding write benchmarks measuring the cost of different encoding strategies.
/// Uses EW writer with explicit encoding overrides to isolate encoding performance.
/// </summary>
[MemoryDiagnoser]
public class EncodingWriteBenchmarks
{
    private const int RowCount = 100_000;

    private string _tempDir = null!;
    private RecordBatch _intBatch = null!;
    private RecordBatch _doubleBatch = null!;
    private RecordBatch _stringBatch = null!;
    private RecordBatch _stringHighCardBatch = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew-encbench-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _intBatch = GenerateIntBatch();
        _doubleBatch = GenerateDoubleBatch();
        _stringBatch = GenerateStringBatch(cardinality: 100);
        _stringHighCardBatch = GenerateStringBatch(cardinality: 50_000);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private string TempPath(string suffix) =>
        Path.Combine(_tempDir, $"enc_{suffix}.parquet");

    // ----------------------------------------------------------------
    //  Int32 encoding benchmarks
    // ----------------------------------------------------------------

    [Benchmark(Description = "Int32_Plain")]
    public async Task Int32_Plain()
    {
        await WriteEW(_intBatch, TempPath("int_plain"), Encoding.Plain);
    }

    [Benchmark(Description = "Int32_DeltaBinaryPacked")]
    public async Task Int32_DeltaBinaryPacked()
    {
        await WriteEW(_intBatch, TempPath("int_dbp"), Encoding.DeltaBinaryPacked);
    }

    [Benchmark(Description = "Int32_Dictionary")]
    public async Task Int32_Dictionary()
    {
        await WriteEW(_intBatch, TempPath("int_dict"), Encoding.PlainDictionary);
    }

    // ----------------------------------------------------------------
    //  Double encoding benchmarks
    // ----------------------------------------------------------------

    [Benchmark(Description = "Double_Plain")]
    public async Task Double_Plain()
    {
        await WriteEW(_doubleBatch, TempPath("dbl_plain"), Encoding.Plain);
    }

    [Benchmark(Description = "Double_ByteStreamSplit")]
    public async Task Double_ByteStreamSplit()
    {
        await WriteEW(_doubleBatch, TempPath("dbl_bss"), Encoding.ByteStreamSplit);
    }

    [Benchmark(Description = "Double_Dictionary")]
    public async Task Double_Dictionary()
    {
        await WriteEW(_doubleBatch, TempPath("dbl_dict"), Encoding.PlainDictionary);
    }

    // ----------------------------------------------------------------
    //  String encoding benchmarks (low cardinality — good for dictionary)
    // ----------------------------------------------------------------

    [Benchmark(Description = "String_Plain")]
    public async Task String_Plain()
    {
        await WriteEW(_stringBatch, TempPath("str_plain"), Encoding.Plain);
    }

    [Benchmark(Description = "String_Dictionary")]
    public async Task String_Dictionary()
    {
        await WriteEW(_stringBatch, TempPath("str_dict"), Encoding.PlainDictionary);
    }

    [Benchmark(Description = "String_DeltaByteArray")]
    public async Task String_DeltaByteArray()
    {
        await WriteEW(_stringBatch, TempPath("str_dba"), Encoding.DeltaByteArray);
    }

    [Benchmark(Description = "String_DeltaLengthByteArray")]
    public async Task String_DeltaLengthByteArray()
    {
        await WriteEW(_stringBatch, TempPath("str_dlba"), Encoding.DeltaLengthByteArray);
    }

    // ----------------------------------------------------------------
    //  String high cardinality (dict fallback expected)
    // ----------------------------------------------------------------

    [Benchmark(Description = "StringHighCard_Plain")]
    public async Task StringHighCard_Plain()
    {
        await WriteEW(_stringHighCardBatch, TempPath("strhc_plain"), Encoding.Plain);
    }

    [Benchmark(Description = "StringHighCard_DeltaByteArray")]
    public async Task StringHighCard_DeltaByteArray()
    {
        await WriteEW(_stringHighCardBatch, TempPath("strhc_dba"), Encoding.DeltaByteArray);
    }

    // ----------------------------------------------------------------
    //  Helpers
    // ----------------------------------------------------------------

    private static async Task WriteEW(RecordBatch batch, string path, Encoding encoding)
    {
        var columnName = batch.Schema.FieldsList[0].Name;
        var options = new ParquetWriteOptions
        {
            Codec = CompressionCodec.Uncompressed,
            DataPageVersion = DataPageVersion.V2,
            Encoding = encoding,
        };

        await using var output = new LocalOutputFile(path);
        await using var writer = new ParquetFileWriter(output, options);
        await writer.WriteAsync(batch);
    }

    private static RecordBatch GenerateIntBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Build();

        var rng = new Random(42);
        var b = new Int32Array.Builder();
        for (int i = 0; i < RowCount; i++)
        {
            if (rng.NextDouble() < 0.05)
                b.AppendNull();
            else
                b.Append(rng.Next(-10_000, 10_000));
        }
        return new RecordBatch(schema, [b.Build()], RowCount);
    }

    private static RecordBatch GenerateDoubleBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Build();

        var rng = new Random(42);
        var b = new DoubleArray.Builder();
        for (int i = 0; i < RowCount; i++)
        {
            if (rng.NextDouble() < 0.05)
                b.AppendNull();
            else
                b.Append(rng.NextDouble() * 1_000_000 - 500_000);
        }
        return new RecordBatch(schema, [b.Build()], RowCount);
    }

    private static RecordBatch GenerateStringBatch(int cardinality)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        var rng = new Random(42);
        var pool = new string[cardinality];
        for (int i = 0; i < cardinality; i++)
        {
            int len = rng.Next(5, 80);
            var chars = new char[len];
            for (int j = 0; j < len; j++)
                chars[j] = (char)rng.Next('a', 'z' + 1);
            pool[i] = new string(chars);
        }

        var b = new StringArray.Builder();
        for (int i = 0; i < RowCount; i++)
        {
            if (rng.NextDouble() < 0.05)
                b.AppendNull();
            else
                b.Append(pool[rng.Next(cardinality)]);
        }
        return new RecordBatch(schema, [b.Build()], RowCount);
    }
}
