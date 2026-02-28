using ParquetSharp;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Generates synthetic Parquet files for benchmarking using ParquetSharp.
/// </summary>
public static class TestFileGenerator
{
    public const string WideFlat = "wide_flat";
    public const string TallNarrow = "tall_narrow";
    public const string Snappy = "snappy";

    private const int WideFlatColumnCount = 100;
    private const int WideFlatRowCount = 1_000_000;

    private const int TallNarrowColumnCount = 4;
    private const int TallNarrowRowCount = 10_000_000;
    private const int TallNarrowRowGroupSize = 2_000_000;

    public static string GenerateAll()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ew-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        GenerateWideFlat(Path.Combine(dir, WideFlat + ".parquet"), compressed: false);
        GenerateWideFlat(Path.Combine(dir, Snappy + ".parquet"), compressed: true);
        GenerateTallNarrow(Path.Combine(dir, TallNarrow + ".parquet"));

        return dir;
    }

    public static void Cleanup(string dir)
    {
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
    }

    private static void GenerateWideFlat(string path, bool compressed)
    {
        var columns = new Column[WideFlatColumnCount];
        for (int i = 0; i < WideFlatColumnCount; i++)
        {
            columns[i] = (i % 4) switch
            {
                0 => new Column<int>($"col_int32_{i}"),
                1 => new Column<long>($"col_int64_{i}"),
                2 => new Column<double>($"col_double_{i}"),
                _ => new Column<byte[]>($"col_bytes_{i}"),
            };
        }

        var compression = compressed ? Compression.Snappy : Compression.Uncompressed;

        using var writer = new ParquetFileWriter(path, columns, compression);
        using var rowGroup = writer.AppendRowGroup();

        for (int i = 0; i < WideFlatColumnCount; i++)
        {
            switch (i % 4)
            {
                case 0:
                    WriteInt32Column(rowGroup, WideFlatRowCount, i);
                    break;
                case 1:
                    WriteInt64Column(rowGroup, WideFlatRowCount, i);
                    break;
                case 2:
                    WriteDoubleColumn(rowGroup, WideFlatRowCount, i);
                    break;
                default:
                    WriteBytesColumn(rowGroup, WideFlatRowCount, i);
                    break;
            }
        }

        writer.Close();
    }

    private static void GenerateTallNarrow(string path)
    {
        var columns = new Column[]
        {
            new Column<int>("id"),
            new Column<long>("timestamp"),
            new Column<double>("value"),
            new Column<byte[]>("tag"),
        };

        using var writerProperties = new WriterPropertiesBuilder()
            .Compression(Compression.Uncompressed)
            .Build();

        using var writer = new ParquetFileWriter(path, columns, writerProperties);

        int remaining = TallNarrowRowCount;
        int groupIndex = 0;
        while (remaining > 0)
        {
            int rowsInGroup = Math.Min(TallNarrowRowGroupSize, remaining);
            using var rowGroup = writer.AppendRowGroup();

            WriteInt32Column(rowGroup, rowsInGroup, groupIndex * TallNarrowRowGroupSize);
            WriteInt64Column(rowGroup, rowsInGroup, groupIndex);
            WriteDoubleColumn(rowGroup, rowsInGroup, groupIndex);
            WriteBytesColumn(rowGroup, rowsInGroup, groupIndex);

            remaining -= rowsInGroup;
            groupIndex++;
        }

        writer.Close();
    }

    private static void WriteInt32Column(RowGroupWriter rowGroup, int rowCount, int seed)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<int>();
        var values = new int[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = seed + j;
        col.WriteBatch(values);
    }

    private static void WriteInt64Column(RowGroupWriter rowGroup, int rowCount, int seed)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<long>();
        var values = new long[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = (long)seed * rowCount + j;
        col.WriteBatch(values);
    }

    private static void WriteDoubleColumn(RowGroupWriter rowGroup, int rowCount, int seed)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<double>();
        var values = new double[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = seed + j * 0.1;
        col.WriteBatch(values);
    }

    private static void WriteBytesColumn(RowGroupWriter rowGroup, int rowCount, int seed)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<byte[]>();
        var tag = System.Text.Encoding.UTF8.GetBytes($"tag_{seed}");
        var values = new byte[rowCount][];
        for (int j = 0; j < rowCount; j++)
            values[j] = tag;
        col.WriteBatch(values);
    }
}
