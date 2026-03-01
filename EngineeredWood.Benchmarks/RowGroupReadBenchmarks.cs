using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Schema;

namespace EngineeredWood.Benchmarks;

[MemoryDiagnoser]
public class RowGroupReadBenchmarks
{
    private string _dir = null!;
    private Dictionary<string, string> _files = null!;

    [Params(TestFileGenerator.WideFlat, TestFileGenerator.TallNarrow, TestFileGenerator.Snappy)]
    public string File { get; set; } = null!;

    private string FilePath => _files[File];

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = TestFileGenerator.GenerateAll();
        _files = new Dictionary<string, string>
        {
            [TestFileGenerator.WideFlat] = Path.Combine(_dir, TestFileGenerator.WideFlat + ".parquet"),
            [TestFileGenerator.TallNarrow] = Path.Combine(_dir, TestFileGenerator.TallNarrow + ".parquet"),
            [TestFileGenerator.Snappy] = Path.Combine(_dir, TestFileGenerator.Snappy + ".parquet"),
        };
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        TestFileGenerator.Cleanup(_dir);
    }

    [Benchmark(Baseline = true, Description = "EW_ReadAll_SeqDecode")]
    public async Task<Apache.Arrow.RecordBatch> EW_ReadAll_SeqDecode()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        return await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "EW_Incremental_Seq")]
    public async Task<Apache.Arrow.RecordBatch> EW_Incremental_Seq()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        return await reader.ReadRowGroupIncrementalAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "EW_ReadAll_Parallel")]
    public async Task<Apache.Arrow.RecordBatch> EW_ReadAll_Parallel()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        return await reader.ReadRowGroupParallelAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "EW_Incremental_Parallel")]
    public async Task<Apache.Arrow.RecordBatch> EW_Incremental_Parallel()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        return await reader.ReadRowGroupIncrementalParallelAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_ReadRowGroup()
    {
        using var reader = new ParquetSharp.ParquetFileReader(FilePath);
        using var rowGroup = reader.RowGroup(0);
        int numColumns = rowGroup.MetaData.NumColumns;
        long numRows = rowGroup.MetaData.NumRows;

        // Second half of columns are nullable (optional) in wide files;
        // for tall_narrow (4 cols): columns 2,3 are optional
        int nullableStart = numColumns / 2;

        for (int c = 0; c < numColumns; c++)
        {
            using var col = rowGroup.Column(c);
            bool nullable = c >= nullableStart;

            switch (c % 4)
            {
                case 0: // int
                    if (nullable)
                    {
                        using var logical = col.LogicalReader<int?>();
                        var buffer = new int?[numRows];
                        logical.ReadBatch(buffer);
                    }
                    else
                    {
                        using var logical = col.LogicalReader<int>();
                        var buffer = new int[numRows];
                        logical.ReadBatch(buffer);
                    }
                    break;
                case 1: // long
                    if (nullable)
                    {
                        using var logical = col.LogicalReader<long?>();
                        var buffer = new long?[numRows];
                        logical.ReadBatch(buffer);
                    }
                    else
                    {
                        using var logical = col.LogicalReader<long>();
                        var buffer = new long[numRows];
                        logical.ReadBatch(buffer);
                    }
                    break;
                case 2: // double
                    if (nullable)
                    {
                        using var logical = col.LogicalReader<double?>();
                        var buffer = new double?[numRows];
                        logical.ReadBatch(buffer);
                    }
                    else
                    {
                        using var logical = col.LogicalReader<double>();
                        var buffer = new double[numRows];
                        logical.ReadBatch(buffer);
                    }
                    break;
                default: // byte[] — always optional (reference type)
                {
                    using var logical = col.LogicalReader<byte[]>();
                    var buffer = new byte[numRows][];
                    logical.ReadBatch(buffer);
                    break;
                }
            }
        }
    }

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_ReadRowGroup()
    {
        using var stream = System.IO.File.OpenRead(FilePath);
        using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
        using var rowGroupReader = reader.OpenRowGroupReader(0);

        foreach (DataField field in reader.Schema.GetDataFields())
        {
            await rowGroupReader.ReadColumnAsync(field).ConfigureAwait(false);
        }
    }
}
