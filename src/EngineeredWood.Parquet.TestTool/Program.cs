using System.Diagnostics;
using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

return args[0] switch
{
    "create_test_file" => await CreateTestFile(args[1..]),
    "read_test_file" => await ReadTestFile(args[1..]),
    _ => PrintUsage(),
};

static int PrintUsage()
{
    Console.Error.WriteLine("""
        Usage:
          ew-test-tool create_test_file <path> --size <bytes>
            Creates a Parquet file with a single row group of approximately <bytes>
            uncompressed data using random values that resist compression.

          ew-test-tool read_test_file <path> [--batch-rows <n>] [--batch-bytes <n>]
            Reads the file one RecordBatch at a time and reports peak memory.
        """);
    return 1;
}

// ---------------------------------------------------------------------------
// create_test_file
// ---------------------------------------------------------------------------

static async Task<int> CreateTestFile(string[] args)
{
    if (args.Length < 3 || args[1] != "--size")
    {
        Console.Error.WriteLine("Usage: create_test_file <path> --size <bytes>");
        return 1;
    }

    string path = args[0];
    long targetBytes = ParseSize(args[2]);

    // Schema: 4 columns of different types to make a realistic workload.
    //   id        Int64   (8 bytes)
    //   payload   Binary  (variable, ~64 bytes avg)
    //   value     Double  (8 bytes)
    //   flag      Int32   (4 bytes)
    // Approximate row size: 8 + 64 + 4 (offset) + 8 + 4 ≈ 88 bytes
    const int approxRowBytes = 88;
    int totalRows = (int)Math.Min(targetBytes / approxRowBytes, int.MaxValue);

    Console.WriteLine($"Target size:  {FormatBytes(targetBytes)}");
    Console.WriteLine($"Rows:         {totalRows:N0}");
    Console.WriteLine($"Output:       {path}");

    var schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, nullable: false))
        .Field(new Field("payload", BinaryType.Default, nullable: false))
        .Field(new Field("value", DoubleType.Default, nullable: false))
        .Field(new Field("flag", Int32Type.Default, nullable: false))
        .Build();

    // Write in chunks to avoid building a huge in-memory RecordBatch.
    // RowGroupMaxRows is set very high so the writer doesn't auto-split.
    var writeOptions = new ParquetWriteOptions
    {
        Compression = CompressionCodec.Snappy,
        RowGroupMaxRows = totalRows,
        DataPageSize = 1024 * 1024, // 1 MB pages
        DictionaryEnabled = false,  // random data won't benefit
    };

    var sw = Stopwatch.StartNew();
    long rowsWritten = 0;
    const int chunkRows = 100_000;

    await using (var file = new LocalSequentialFile(path))
    await using (var writer = new ParquetFileWriter(file, ownsFile: false, writeOptions))
    {
        while (rowsWritten < totalRows)
        {
            int batchRows = (int)Math.Min(chunkRows, totalRows - rowsWritten);
            var batch = GenerateRandomBatch(schema, batchRows, rowsWritten);
            await writer.WriteRowGroupAsync(batch);
            rowsWritten += batchRows;

            if (rowsWritten % 1_000_000 == 0 || rowsWritten == totalRows)
                Console.Write($"\r  Written {rowsWritten:N0} / {totalRows:N0} rows...");
        }
        await writer.CloseAsync();
    }

    Console.WriteLine();

    var fileInfo = new FileInfo(path);
    Console.WriteLine($"File size:    {FormatBytes(fileInfo.Length)}");
    Console.WriteLine($"Elapsed:      {sw.Elapsed.TotalSeconds:F1}s");
    return 0;
}

static RecordBatch GenerateRandomBatch(Apache.Arrow.Schema schema, int rowCount, long startId)
{
    var idBuilder = new Int64Array.Builder();
    var payloadBuilder = new BinaryArray.Builder();
    var valueBuilder = new DoubleArray.Builder();
    var flagBuilder = new Int32Array.Builder();

    // Pre-allocate a buffer for random bytes
    byte[] randomBuf = new byte[64];

    for (int i = 0; i < rowCount; i++)
    {
        idBuilder.Append(startId + i);

        // Random binary payload (resists compression)
        RandomNumberGenerator.Fill(randomBuf);
        payloadBuilder.Append(randomBuf);

        valueBuilder.Append(BitConverter.Int64BitsToDouble(
            (uint)RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue)
            | ((long)RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue) << 32)));

        flagBuilder.Append(RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue));
    }

    return new RecordBatch(schema,
        [idBuilder.Build(), payloadBuilder.Build(), valueBuilder.Build(), flagBuilder.Build()],
        rowCount);
}

// ---------------------------------------------------------------------------
// read_test_file
// ---------------------------------------------------------------------------

static async Task<int> ReadTestFile(string[] args)
{
    if (args.Length < 1)
    {
        Console.Error.WriteLine("Usage: read_test_file <path> [--batch-rows <n>] [--batch-bytes <n>]");
        return 1;
    }

    string path = args[0];
    int? batchRows = null;
    long? batchBytes = null;

    for (int i = 1; i < args.Length - 1; i++)
    {
        if (args[i] == "--batch-rows")
            batchRows = int.Parse(args[++i]);
        else if (args[i] == "--batch-bytes")
            batchBytes = ParseSize(args[++i]);
    }

    if (batchRows is null && batchBytes is null)
    {
        Console.Error.WriteLine("Specify at least one of --batch-rows or --batch-bytes.");
        return 1;
    }

    var readOptions = new ParquetReadOptions
    {
        BatchSize = batchRows,
        MaxBatchByteSize = batchBytes,
    };

    Console.WriteLine($"File:         {path}");
    Console.WriteLine($"BatchSize:    {(batchRows.HasValue ? $"{batchRows:N0} rows" : "(none)")}");
    Console.WriteLine($"MaxBytes:     {(batchBytes.HasValue ? FormatBytes(batchBytes.Value) : "(none)")}");

    // Force a GC before we start to get a clean baseline.
    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    long baselineMemory = GC.GetTotalMemory(forceFullCollection: true);

    await using var file = new LocalRandomAccessFile(path);
    await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

    var metadata = await reader.ReadMetadataAsync();
    Console.WriteLine($"Row groups:   {metadata.RowGroups.Count}");
    Console.WriteLine($"Total rows:   {metadata.NumRows:N0}");
    Console.WriteLine();

    var sw = Stopwatch.StartNew();
    long totalRowsRead = 0;
    int batchCount = 0;
    long peakMemory = 0;
    long peakDelta = 0;

    await foreach (var batch in reader.ReadAllAsync())
    {
        batchCount++;
        totalRowsRead += batch.Length;

        // Sample memory after each batch is alive.
        long currentMemory = GC.GetTotalMemory(forceFullCollection: false);
        if (currentMemory > peakMemory)
            peakMemory = currentMemory;

        long delta = currentMemory - baselineMemory;
        if (delta > peakDelta)
            peakDelta = delta;

        if (batchCount % 10 == 0 || batchCount <= 3)
        {
            Console.WriteLine(
                $"  Batch {batchCount,5}: {batch.Length,10:N0} rows  |  " +
                $"total {totalRowsRead,12:N0}  |  " +
                $"mem {FormatBytes(currentMemory),10}  |  " +
                $"Δ {FormatBytes(delta),10}");
        }

        // Dispose arrays explicitly to free native buffers promptly.
        for (int c = 0; c < batch.ColumnCount; c++)
            batch.Column(c).Dispose();
    }

    // Final GC to measure steady-state.
    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    long finalMemory = GC.GetTotalMemory(forceFullCollection: true);

    Console.WriteLine();
    Console.WriteLine($"Batches read: {batchCount:N0}");
    Console.WriteLine($"Total rows:   {totalRowsRead:N0}");
    Console.WriteLine($"Elapsed:      {sw.Elapsed.TotalSeconds:F1}s");
    Console.WriteLine($"Baseline mem: {FormatBytes(baselineMemory)}");
    Console.WriteLine($"Peak mem:     {FormatBytes(peakMemory)}");
    Console.WriteLine($"Peak Δ:       {FormatBytes(peakDelta)}");
    Console.WriteLine($"Final mem:    {FormatBytes(finalMemory)}");
    return 0;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static long ParseSize(string s)
{
    s = s.Trim();
    long multiplier = 1;
    if (s.EndsWith("GB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 30;
        s = s[..^2];
    }
    else if (s.EndsWith("MB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 20;
        s = s[..^2];
    }
    else if (s.EndsWith("KB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 10;
        s = s[..^2];
    }
    return (long)(double.Parse(s.Trim()) * multiplier);
}

static string FormatBytes(long bytes)
{
    return bytes switch
    {
        >= 1L << 30 => $"{bytes / (double)(1L << 30):F2} GB",
        >= 1L << 20 => $"{bytes / (double)(1L << 20):F1} MB",
        >= 1L << 10 => $"{bytes / (double)(1L << 10):F1} KB",
        _ => $"{bytes} B",
    };
}
