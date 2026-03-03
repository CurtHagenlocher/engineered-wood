using System.Diagnostics;
using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Interactive cloud benchmark comparing EngineeredWood, ParquetSharp, and Parquet.Net
/// reading from Azure Blob Storage.
/// </summary>
public static class CloudBenchmark
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== Cloud Parquet Read Benchmark ===\n");

        Console.Write("Azure Blob URL (e.g. https://account.blob.core.windows.net/container/file.parquet): ");
        string blobUrl = Console.ReadLine()?.Trim() ?? "";
        if (string.IsNullOrEmpty(blobUrl))
        {
            Console.WriteLine("No URL provided. Exiting.");
            return;
        }

        Console.Write("Account key (leave blank for anonymous access): ");
        string accountKey = Console.ReadLine()?.Trim() ?? "";

        var blobUri = new Uri(blobUrl);
        BlobClient blobClient;

        if (!string.IsNullOrEmpty(accountKey))
        {
            // Extract account name from URL
            string accountName = blobUri.Host.Split('.')[0];
            var credential = new Azure.Storage.StorageSharedKeyCredential(accountName, accountKey);
            blobClient = new BlobClient(blobUri, credential);
        }
        else
        {
            blobClient = new BlobClient(blobUri);
        }

        // Verify connectivity and get file size
        Console.Write("\nConnecting... ");
        long fileSize;
        try
        {
            var props = await blobClient.GetPropertiesAsync();
            fileSize = props.Value.ContentLength;
            Console.WriteLine($"OK ({fileSize / 1024.0 / 1024.0:F1} MB)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FAILED: {ex.Message}");
            return;
        }

        // Discover row groups via EngineeredWood (first read also warms DNS/TLS)
        Console.Write("Reading metadata... ");
        int rowGroupCount;
        int totalColumns;
        long firstRowGroupRows;
        using (var ewFile = new AzureBlobRandomAccessFile(blobClient, fileSize))
        using (var reader = new ParquetFileReader(ewFile))
        {
            var meta = await reader.ReadMetadataAsync();
            rowGroupCount = meta.RowGroups.Count;
            totalColumns = meta.RowGroups[0].Columns.Count;
            firstRowGroupRows = meta.RowGroups[0].NumRows;
        }
        Console.WriteLine($"{rowGroupCount} row group(s), {totalColumns} column(s), " +
                          $"{firstRowGroupRows:N0} rows in RG0");

        Console.Write("\nRow group index to read [0]: ");
        string rgInput = Console.ReadLine()?.Trim() ?? "";
        int rowGroupIndex = string.IsNullOrEmpty(rgInput) ? 0 : int.Parse(rgInput);

        Console.Write("Number of iterations [3]: ");
        string iterInput = Console.ReadLine()?.Trim() ?? "";
        int iterations = string.IsNullOrEmpty(iterInput) ? 3 : int.Parse(iterInput);

        Console.WriteLine($"\nBenchmarking row group {rowGroupIndex} × {iterations} iterations...\n");
        Console.WriteLine($"{"Method",-52} {"Mean",10} {"Min",10} {"Max",10}");
        Console.WriteLine(new string('-', 84));

        // --- EngineeredWood (direct, no coalescing) ---
        await RunEW("EW (direct)", blobClient, fileSize, rowGroupIndex, iterations,
            coalesce: false);

        // --- EngineeredWood (with coalescing) ---
        await RunEW("EW (coalesced)", blobClient, fileSize, rowGroupIndex, iterations,
            coalesce: true);

        // --- ParquetSharp (via Azure BlobClient.OpenRead stream) ---
        await RunParquetSharp(blobClient, rowGroupIndex, iterations);

        // --- Parquet.Net (via Azure BlobClient.OpenRead stream) ---
        await RunParquetNet(blobClient, rowGroupIndex, iterations);

        Console.WriteLine();
    }

    private static async Task RunEW(
        string label, BlobClient blobClient, long fileSize,
        int rowGroupIndex, int iterations, bool coalesce)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
            IRandomAccessFile file = new AzureBlobRandomAccessFile(blobClient, fileSize);
            if (coalesce)
                file = new CoalescingFileReader(file);

            await using (file)
            using (var reader = new ParquetFileReader(file))
            {
                using var batch = await reader.ReadRowGroupAsync(rowGroupIndex);
            }
            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult(label, times);
    }

    private static async Task RunParquetSharp(
        BlobClient blobClient, int rowGroupIndex, int iterations)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));

            using var reader = new ParquetSharp.ParquetFileReader(stream, leaveOpen: false);
            using var rowGroup = reader.RowGroup(rowGroupIndex);
            int numColumns = rowGroup.MetaData.NumColumns;
            long numRows = rowGroup.MetaData.NumRows;

            for (int c = 0; c < numColumns; c++)
            {
                using var col = rowGroup.Column(c);
                ReadParquetSharpColumn(col, numRows);
            }

            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult("ParquetSharp (stream)", times);
    }

    private static void ReadParquetSharpColumn(
        ParquetSharp.ColumnReader col, long numRows)
    {
        // Use the non-generic LogicalReader so ParquetSharp resolves the correct
        // element type (e.g. DateTimeNanos for TIMESTAMP columns) automatically.
        using var logicalReader = col.LogicalReader();
        logicalReader.Apply(new DrainVisitor(numRows));
    }

    /// <summary>
    /// Visitor that reads all values from a ParquetSharp LogicalColumnReader
    /// regardless of the element type. This avoids InvalidCastException when
    /// the column's logical type maps to a ParquetSharp-specific CLR type
    /// (e.g. DateTimeNanos, TimeSpanNanos).
    /// </summary>
    private sealed class DrainVisitor : ParquetSharp.ILogicalColumnReaderVisitor<bool>
    {
        private readonly long _numRows;
        public DrainVisitor(long numRows) => _numRows = numRows;

        public bool OnLogicalColumnReader<TElement>(
            ParquetSharp.LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[_numRows];
            columnReader.ReadBatch(buffer);
            return true;
        }
    }

    private static async Task RunParquetNet(
        BlobClient blobClient, int rowGroupIndex, int iterations)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));

            using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
            using var rg = reader.OpenRowGroupReader(rowGroupIndex);
            foreach (var field in reader.Schema.GetDataFields())
                await rg.ReadColumnAsync(field);

            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult("Parquet.Net (stream)", times);
    }

    private static void PrintResult(string label, double[] times)
    {
        Array.Sort(times);
        double mean = times.Average();
        double min = times[0];
        double max = times[^1];

        Console.WriteLine();
        Console.WriteLine($"{label,-52} {mean,8:F0} ms {min,8:F0} ms {max,8:F0} ms");
    }
}
