using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Perfolizer.Horology;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Compares the three Parquet implementations at their default settings,
/// writing a ~1MB dataset and measuring both wall-clock time and output file size.
///
/// Defaults:
///   - EngineeredWood: Snappy, Plain encoding, V2 data pages
///   - ParquetSharp:   Snappy, dictionary + fallback encodings, V1 data pages
///   - Parquet.NET:    Snappy
/// </summary>
[MemoryDiagnoser]
[Config(typeof(FileSizeConfig))]
public class DefaultSettingsBenchmarks
{
    // ~1MB uncompressed: TallNarrow (4 cols) at 15K rows ≈ 1MB
    private const int RowCount = 15_000;

    private WriteTestDataGenerator.WriteData _data = null!;
    private string _tempDir = null!;

    // File size tracking — persisted to temp file for cross-process access by BDN column
    private static readonly string SizeFile = Path.Combine(Path.GetTempPath(), "ew-bench-sizes.txt");
    private static readonly Dictionary<string, long> FileSizes = new();

    [GlobalSetup]
    public void GlobalSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew-defaults-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _data = WriteTestDataGenerator.GenerateTallNarrow(rowCount: RowCount);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // Print file sizes and persist them for the BDN summary column
        Console.WriteLine();
        Console.WriteLine("=== Output File Sizes ===");
        foreach (var (name, size) in FileSizes.OrderBy(kv => kv.Key))
            Console.WriteLine($"  {name,-25} {size,10:N0} bytes  ({size / 1024.0:F1} KB)");
        Console.WriteLine();

        // Append to shared file so host process can read all sizes
        try
        {
            var lines = FileSizes.Select(kv => $"{kv.Key}={kv.Value}");
            File.AppendAllLines(SizeFile, lines);
        }
        catch { }

        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private string TempPath(string suffix) =>
        Path.Combine(_tempDir, $"defaults_{suffix}.parquet");

    private static void RecordFileSize(string label, string path)
    {
        if (File.Exists(path))
            FileSizes[label] = new FileInfo(path).Length;
    }

    // ----------------------------------------------------------------
    //  EngineeredWood — default settings (Snappy, Plain, V2)
    // ----------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "EW (defaults)")]
    public async Task EW_Defaults()
    {
        var path = TempPath("ew");
        // ParquetWriteOptions.Default: Snappy, Plain, V2
        await using var output = new LocalOutputFile(path);
        await using var writer = new ParquetFileWriter(output);
        await writer.WriteAsync(_data.Batch);
        RecordFileSize("EW (defaults)", path);
    }

    // ----------------------------------------------------------------
    //  ParquetSharp — default settings (Snappy, dictionary+fallback, V1)
    // ----------------------------------------------------------------

    [Benchmark(Description = "ParquetSharp (defaults)")]
    public void ParquetSharp_Defaults()
    {
        var path = TempPath("ps");

        var columns = new ParquetSharp.Column[]
        {
            new ParquetSharp.Column<int>("id"),
            new ParquetSharp.Column<long>("timestamp"),
            new ParquetSharp.Column<double?>("value"),
            new ParquetSharp.Column<byte[]>("tag"),
        };

        // No WriterProperties → uses C++ defaults (Snappy, dictionary enabled)
        using var writer = new ParquetSharp.ParquetFileWriter(path, columns);
        using var rg = writer.AppendRowGroup();
        var raw = _data.Raw;

        using (var col = rg.NextColumn().LogicalWriter<int>())
            col.WriteBatch(raw.Int32Required);
        using (var col = rg.NextColumn().LogicalWriter<long>())
            col.WriteBatch(raw.Int64Required);
        using (var col = rg.NextColumn().LogicalWriter<double?>())
            col.WriteBatch(raw.DoubleNullable);
        using (var col = rg.NextColumn().LogicalWriter<byte[]>())
            col.WriteBatch(raw.BytesNullable!);

        writer.Close();
        RecordFileSize("ParquetSharp (defaults)", path);
    }

    // ----------------------------------------------------------------
    //  Parquet.NET — default settings (Snappy)
    // ----------------------------------------------------------------

    [Benchmark(Description = "Parquet.NET (defaults)")]
    public async Task ParquetNet_Defaults()
    {
        var path = TempPath("pnet");

        var fields = new global::Parquet.Schema.DataField[]
        {
            new global::Parquet.Schema.DataField<int>("id"),
            new global::Parquet.Schema.DataField<long>("timestamp"),
            new global::Parquet.Schema.DataField<double?>("value"),
            new global::Parquet.Schema.DataField<string?>("tag"),
        };
        var schema = new global::Parquet.Schema.ParquetSchema(fields);
        var raw = _data.Raw;

        // Default settings — no explicit compression or encoding overrides
        using var stream = File.Create(path);
        using var writer = await global::Parquet.ParquetWriter.CreateAsync(schema, stream);
        using var rgWriter = writer.CreateRowGroup();

        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[0], raw.Int32Required));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[1], raw.Int64Required));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[2], raw.DoubleNullable));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[3], raw.StringNullable));

        RecordFileSize("Parquet.NET (defaults)", path);
    }

    // ----------------------------------------------------------------
    //  Custom BDN config to add a FileSizeColumn
    // ----------------------------------------------------------------

    private sealed class FileSizeConfig : ManualConfig
    {
        public FileSizeConfig()
        {
            AddColumn(new FileSizeColumn());
        }
    }

    private sealed class FileSizeColumn : IColumn
    {
        private Dictionary<string, long>? _cache;

        public string Id => "FileSize";
        public string ColumnName => "File Size";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Size;
        public string Legend => "Output Parquet file size in bytes";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            return GetValue(summary, benchmarkCase, SummaryStyle.Default);
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            _cache ??= LoadSizes();

            var label = benchmarkCase.Descriptor.WorkloadMethod.Name switch
            {
                "EW_Defaults" => "EW (defaults)",
                "ParquetSharp_Defaults" => "ParquetSharp (defaults)",
                "ParquetNet_Defaults" => "Parquet.NET (defaults)",
                _ => "",
            };

            return _cache.TryGetValue(label, out var size)
                ? $"{size / 1024.0:F1} KB"
                : "N/A";
        }

        private static Dictionary<string, long> LoadSizes()
        {
            var result = new Dictionary<string, long>();
            try
            {
                if (!File.Exists(SizeFile)) return result;
                foreach (var line in File.ReadAllLines(SizeFile))
                {
                    var parts = line.Split('=', 2);
                    if (parts.Length == 2 && long.TryParse(parts[1], out var size))
                        result[parts[0]] = size;
                }
            }
            catch { }
            return result;
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
        public bool IsAvailable(Summary summary) => true;
    }
}
