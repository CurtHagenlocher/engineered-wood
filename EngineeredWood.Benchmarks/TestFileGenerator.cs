using ParquetSharp;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Generates synthetic Parquet files for benchmarking using ParquetSharp.
/// Data is designed to be representative of real workloads:
/// - Random numeric values (not sequential)
/// - High-cardinality, variable-length strings
/// - Mix of required (non-nullable) and optional (nullable, ~10% nulls) columns
/// - Some columns forced to PLAIN encoding (no dictionary)
/// </summary>
public static class TestFileGenerator
{
    public const string WideFlat = "wide_flat";
    public const string TallNarrow = "tall_narrow";
    public const string Snappy = "snappy";

    private const int WideFlatColumnCount = 100;
    private const int WideFlatRowCount = 1_000_000;

    private const int TallNarrowRowCount = 10_000_000;
    private const int TallNarrowRowGroupSize = 2_000_000;

    private const double NullRate = 0.10;
    private const int StringPoolSize = 10_000;
    private const int StringMinLen = 5;
    private const int StringMaxLen = 100;

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

    /// <summary>
    /// Generates a wide file with 100 columns (25 each of int, long, double, byte[]).
    /// Columns 0-49 are REQUIRED (non-nullable), columns 50-99 are OPTIONAL (~10% nulls).
    /// Columns 0-3 have dictionary disabled (PLAIN encoding).
    /// </summary>
    private static void GenerateWideFlat(string path, bool compressed)
    {
        var random = new Random(42);
        var stringPool = GenerateStringPool(random, StringPoolSize, StringMinLen, StringMaxLen);

        int halfCols = WideFlatColumnCount / 2;

        // Define columns: first half required, second half optional
        var columns = new Column[WideFlatColumnCount];
        for (int i = 0; i < WideFlatColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            string name = $"c{i}";
            columns[i] = (i % 4, nullable) switch
            {
                (0, false) => new Column<int>(name),
                (0, true) => new Column<int?>(name),
                (1, false) => new Column<long>(name),
                (1, true) => new Column<long?>(name),
                (2, false) => new Column<double>(name),
                (2, true) => new Column<double?>(name),
                // byte[] is always optional in ParquetSharp (reference type)
                _ => new Column<byte[]>(name),
            };
        }

        // Force PLAIN encoding on first 4 columns (one of each type)
        using var props = new WriterPropertiesBuilder()
            .Compression(compressed ? Compression.Snappy : Compression.Uncompressed)
            .DisableDictionary("c0")
            .DisableDictionary("c1")
            .DisableDictionary("c2")
            .DisableDictionary("c3")
            .Build();

        using var writer = new ParquetFileWriter(path, columns, props);
        using var rowGroup = writer.AppendRowGroup();

        for (int i = 0; i < WideFlatColumnCount; i++)
        {
            bool nullable = i >= halfCols;
            switch (i % 4)
            {
                case 0:
                    if (nullable)
                        WriteNullableInt32Column(rowGroup, random, WideFlatRowCount);
                    else
                        WriteInt32Column(rowGroup, random, WideFlatRowCount);
                    break;
                case 1:
                    if (nullable)
                        WriteNullableInt64Column(rowGroup, random, WideFlatRowCount);
                    else
                        WriteInt64Column(rowGroup, random, WideFlatRowCount);
                    break;
                case 2:
                    if (nullable)
                        WriteNullableDoubleColumn(rowGroup, random, WideFlatRowCount);
                    else
                        WriteDoubleColumn(rowGroup, random, WideFlatRowCount);
                    break;
                default:
                    if (nullable)
                        WriteNullableBytesColumn(rowGroup, random, stringPool, WideFlatRowCount);
                    else
                        WriteBytesColumn(rowGroup, random, stringPool, WideFlatRowCount);
                    break;
            }
        }

        writer.Close();
    }

    /// <summary>
    /// Generates a tall file with 4 columns and 5 row groups of 2M rows each.
    /// Columns: int (required), long (required), double? (optional, 10% nulls), byte[] (optional).
    /// </summary>
    private static void GenerateTallNarrow(string path)
    {
        var random = new Random(123);
        var stringPool = GenerateStringPool(random, StringPoolSize, StringMinLen, StringMaxLen);

        var columns = new Column[]
        {
            new Column<int>("id"),
            new Column<long>("timestamp"),
            new Column<double?>("value"),
            new Column<byte[]>("tag"),
        };

        using var props = new WriterPropertiesBuilder()
            .Compression(Compression.Uncompressed)
            .Build();

        using var writer = new ParquetFileWriter(path, columns, props);

        int remaining = TallNarrowRowCount;
        while (remaining > 0)
        {
            int rowsInGroup = Math.Min(TallNarrowRowGroupSize, remaining);
            using var rowGroup = writer.AppendRowGroup();

            WriteInt32Column(rowGroup, random, rowsInGroup);
            WriteInt64Column(rowGroup, random, rowsInGroup);
            WriteNullableDoubleColumn(rowGroup, random, rowsInGroup);
            WriteBytesColumn(rowGroup, random, stringPool, rowsInGroup);

            remaining -= rowsInGroup;
        }

        writer.Close();
    }

    // --- Non-nullable column writers ---

    private static void WriteInt32Column(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<int>();
        var values = new int[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.Next();
        col.WriteBatch(values);
    }

    private static void WriteInt64Column(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<long>();
        var values = new long[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextInt64();
        col.WriteBatch(values);
    }

    private static void WriteDoubleColumn(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<double>();
        var values = new double[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextDouble() * 1_000_000.0 - 500_000.0;
        col.WriteBatch(values);
    }

    private static void WriteBytesColumn(
        RowGroupWriter rowGroup, Random random, byte[][] pool, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<byte[]>();
        var values = new byte[rowCount][];
        for (int j = 0; j < rowCount; j++)
            values[j] = pool[random.Next(pool.Length)];
        col.WriteBatch(values);
    }

    // --- Nullable column writers ---

    private static void WriteNullableInt32Column(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<int?>();
        var values = new int?[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextDouble() < NullRate ? null : random.Next();
        col.WriteBatch(values);
    }

    private static void WriteNullableInt64Column(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<long?>();
        var values = new long?[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextDouble() < NullRate ? null : random.NextInt64();
        col.WriteBatch(values);
    }

    private static void WriteNullableDoubleColumn(RowGroupWriter rowGroup, Random random, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<double?>();
        var values = new double?[rowCount];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextDouble() < NullRate ? null : random.NextDouble() * 1_000_000.0 - 500_000.0;
        col.WriteBatch(values);
    }

    private static void WriteNullableBytesColumn(
        RowGroupWriter rowGroup, Random random, byte[][] pool, int rowCount)
    {
        using var col = rowGroup.NextColumn().LogicalWriter<byte[]>();
        var values = new byte[rowCount][];
        for (int j = 0; j < rowCount; j++)
            values[j] = random.NextDouble() < NullRate ? null! : pool[random.Next(pool.Length)];
        col.WriteBatch(values);
    }

    // --- Helpers ---

    private static byte[][] GenerateStringPool(Random random, int count, int minLen, int maxLen)
    {
        var pool = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            int len = random.Next(minLen, maxLen + 1);
            pool[i] = new byte[len];
            for (int j = 0; j < len; j++)
                pool[i][j] = (byte)random.Next(0x20, 0x7F); // printable ASCII
        }
        return pool;
    }
}
