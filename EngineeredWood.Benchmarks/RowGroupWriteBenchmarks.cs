using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Compares row group write throughput across EW, ParquetSharp, and Parquet.NET.
/// Each iteration writes an entire row group from pre-generated in-memory data
/// to a temporary file, measuring encoding + compression + IO cost.
/// </summary>
[MemoryDiagnoser]
public class RowGroupWriteBenchmarks
{
    private string _tempDir = null!;
    private WriteTestDataGenerator.WriteData _data = null!;

    [Params(WriteTestDataGenerator.WideFlat, WriteTestDataGenerator.TallNarrow)]
    public string Profile { get; set; } = null!;

    [Params("uncompressed", "snappy")]
    public string Compression { get; set; } = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew-wbench-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _data = Profile switch
        {
            WriteTestDataGenerator.WideFlat => WriteTestDataGenerator.GenerateWideFlat(),
            WriteTestDataGenerator.TallNarrow => WriteTestDataGenerator.GenerateTallNarrow(),
            _ => throw new ArgumentException($"Unknown profile: {Profile}"),
        };
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private string TempPath(string suffix) =>
        Path.Combine(_tempDir, $"bench_{suffix}.parquet");

    private CompressionCodec EwCodec => Compression == "snappy"
        ? CompressionCodec.Snappy : CompressionCodec.Uncompressed;
    private ParquetSharp.Compression PsCompression => Compression == "snappy"
        ? ParquetSharp.Compression.Snappy : ParquetSharp.Compression.Uncompressed;

    // ----------------------------------------------------------------
    //  EngineeredWood
    // ----------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "EW_Write")]
    public async Task EW_Write()
    {
        var path = TempPath("ew");
        var options = new ParquetWriteOptions
        {
            Codec = EwCodec,
            DataPageVersion = DataPageVersion.V2,
        };

        await using var output = new LocalOutputFile(path);
        await using var writer = new ParquetFileWriter(output, options);
        await writer.WriteAsync(_data.Batch);
    }

    [Benchmark(Description = "EW_Write_V1")]
    public async Task EW_Write_V1()
    {
        var path = TempPath("ew_v1");
        var options = new ParquetWriteOptions
        {
            Codec = EwCodec,
            DataPageVersion = DataPageVersion.V1,
        };

        await using var output = new LocalOutputFile(path);
        await using var writer = new ParquetFileWriter(output, options);
        await writer.WriteAsync(_data.Batch);
    }

    // ----------------------------------------------------------------
    //  ParquetSharp
    // ----------------------------------------------------------------

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_Write()
    {
        var path = TempPath("ps");
        int halfCols = _data.ColumnCount / 2;

        if (Profile == WriteTestDataGenerator.WideFlat)
            ParquetSharp_WriteWideFlat(path, halfCols);
        else
            ParquetSharp_WriteTallNarrow(path);
    }

    private void ParquetSharp_WriteWideFlat(string path, int halfCols)
    {
        var columns = new ParquetSharp.Column[_data.ColumnCount];
        for (int i = 0; i < _data.ColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            string name = $"c{i}";
            columns[i] = (i % 4, nullable) switch
            {
                (0, false) => new ParquetSharp.Column<int>(name),
                (0, true) => new ParquetSharp.Column<int?>(name),
                (1, false) => new ParquetSharp.Column<long>(name),
                (1, true) => new ParquetSharp.Column<long?>(name),
                (2, false) => new ParquetSharp.Column<double>(name),
                (2, true) => new ParquetSharp.Column<double?>(name),
                _ => new ParquetSharp.Column<byte[]>(name),
            };
        }

        using var props = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(PsCompression)
            .Build();
        using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
        using var rg = writer.AppendRowGroup();
        var raw = _data.Raw;

        for (int i = 0; i < _data.ColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            switch (i % 4)
            {
                case 0:
                    if (nullable)
                    {
                        using var col = rg.NextColumn().LogicalWriter<int?>();
                        col.WriteBatch(raw.Int32Nullable);
                    }
                    else
                    {
                        using var col = rg.NextColumn().LogicalWriter<int>();
                        col.WriteBatch(raw.Int32Required);
                    }
                    break;
                case 1:
                    if (nullable)
                    {
                        using var col = rg.NextColumn().LogicalWriter<long?>();
                        col.WriteBatch(raw.Int64Nullable);
                    }
                    else
                    {
                        using var col = rg.NextColumn().LogicalWriter<long>();
                        col.WriteBatch(raw.Int64Required);
                    }
                    break;
                case 2:
                    if (nullable)
                    {
                        using var col = rg.NextColumn().LogicalWriter<double?>();
                        col.WriteBatch(raw.DoubleNullable);
                    }
                    else
                    {
                        using var col = rg.NextColumn().LogicalWriter<double>();
                        col.WriteBatch(raw.DoubleRequired);
                    }
                    break;
                default:
                    if (nullable)
                    {
                        using var col = rg.NextColumn().LogicalWriter<byte[]>();
                        col.WriteBatch(raw.BytesNullable!);
                    }
                    else
                    {
                        using var col = rg.NextColumn().LogicalWriter<byte[]>();
                        col.WriteBatch(raw.BytesRequired);
                    }
                    break;
            }
        }

        writer.Close();
    }

    private void ParquetSharp_WriteTallNarrow(string path)
    {
        var columns = new ParquetSharp.Column[]
        {
            new ParquetSharp.Column<int>("id"),
            new ParquetSharp.Column<long>("timestamp"),
            new ParquetSharp.Column<double?>("value"),
            new ParquetSharp.Column<byte[]>("tag"),
        };

        using var props = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(PsCompression)
            .Build();
        using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
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
    }

    // ----------------------------------------------------------------
    //  Parquet.NET
    // ----------------------------------------------------------------

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_Write()
    {
        var path = TempPath("pnet");

        if (Profile == WriteTestDataGenerator.WideFlat)
            await ParquetNet_WriteWideFlat(path);
        else
            await ParquetNet_WriteTallNarrow(path);
    }

    private async Task ParquetNet_WriteWideFlat(string path)
    {
        int halfCols = _data.ColumnCount / 2;
        var fields = new global::Parquet.Schema.DataField[_data.ColumnCount];
        for (int i = 0; i < _data.ColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            string name = $"c{i}";
            fields[i] = (i % 4) switch
            {
                0 => nullable ? new global::Parquet.Schema.DataField<int?>(name) : new global::Parquet.Schema.DataField<int>(name),
                1 => nullable ? new global::Parquet.Schema.DataField<long?>(name) : new global::Parquet.Schema.DataField<long>(name),
                2 => nullable ? new global::Parquet.Schema.DataField<double?>(name) : new global::Parquet.Schema.DataField<double>(name),
                _ => nullable ? new global::Parquet.Schema.DataField<string?>(name) : new global::Parquet.Schema.DataField<string>(name),
            };
        }

        var schema = new global::Parquet.Schema.ParquetSchema(fields);
        var raw = _data.Raw;

        using var stream = File.Create(path);
        using var writer = await global::Parquet.ParquetWriter.CreateAsync(schema, stream);
        writer.CompressionMethod = Compression == "snappy"
            ? global::Parquet.CompressionMethod.Snappy
            : global::Parquet.CompressionMethod.None;

        using var rgWriter = writer.CreateRowGroup();
        for (int i = 0; i < _data.ColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            switch (i % 4)
            {
                case 0:
                    await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[i],
                        nullable ? (Array)raw.Int32Nullable : raw.Int32Required));
                    break;
                case 1:
                    await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[i],
                        nullable ? (Array)raw.Int64Nullable : raw.Int64Required));
                    break;
                case 2:
                    await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[i],
                        nullable ? (Array)raw.DoubleNullable : raw.DoubleRequired));
                    break;
                default:
                    await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[i],
                        nullable ? (Array)raw.StringNullable : raw.StringRequired));
                    break;
            }
        }
    }

    private async Task ParquetNet_WriteTallNarrow(string path)
    {
        var fields = new global::Parquet.Schema.DataField[]
        {
            new global::Parquet.Schema.DataField<int>("id"),
            new global::Parquet.Schema.DataField<long>("timestamp"),
            new global::Parquet.Schema.DataField<double?>("value"),
            new global::Parquet.Schema.DataField<string?>("tag"),
        };
        var schema = new global::Parquet.Schema.ParquetSchema(fields);
        var raw = _data.Raw;

        using var stream = File.Create(path);
        using var writer = await global::Parquet.ParquetWriter.CreateAsync(schema, stream);
        writer.CompressionMethod = Compression == "snappy"
            ? global::Parquet.CompressionMethod.Snappy
            : global::Parquet.CompressionMethod.None;

        using var rgWriter = writer.CreateRowGroup();
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[0], raw.Int32Required));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[1], raw.Int64Required));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[2], raw.DoubleNullable));
        await rgWriter.WriteColumnAsync(new global::Parquet.Data.DataColumn(fields[3], raw.StringNullable));
    }
}
