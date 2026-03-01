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

    [Benchmark(Description = "EngineeredWood")]
    public async Task<Apache.Arrow.RecordBatch> EngineeredWood_ReadRowGroup()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        return await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_ReadRowGroup()
    {
        using var reader = new ParquetSharp.ParquetFileReader(FilePath);
        using var rowGroup = reader.RowGroup(0);
        int numColumns = rowGroup.MetaData.NumColumns;
        long numRows = rowGroup.MetaData.NumRows;

        for (int c = 0; c < numColumns; c++)
        {
            using var col = rowGroup.Column(c);
            switch (c % 4)
            {
                case 0:
                {
                    using var logical = col.LogicalReader<int>();
                    var buffer = new int[numRows];
                    logical.ReadBatch(buffer);
                    break;
                }
                case 1:
                {
                    using var logical = col.LogicalReader<long>();
                    var buffer = new long[numRows];
                    logical.ReadBatch(buffer);
                    break;
                }
                case 2:
                {
                    using var logical = col.LogicalReader<double>();
                    var buffer = new double[numRows];
                    logical.ReadBatch(buffer);
                    break;
                }
                default:
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
