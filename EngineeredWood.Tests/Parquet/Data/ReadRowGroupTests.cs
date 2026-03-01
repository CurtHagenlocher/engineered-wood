using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

public class ReadRowGroupTests
{
    [Fact]
    public async Task AllTypesPlain_ReadsRowGroup0()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(8, batch.Length);
        Assert.True(batch.Schema.FieldsList.Count > 0);

        // Verify we can access some known columns
        var schema = batch.Schema;
        Assert.Contains(schema.FieldsList, f => f.Name == "id");
    }

    [Fact]
    public async Task AllTypesPlain_SpecificColumns()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0, ["id", "bool_col"]);

        Assert.Equal(8, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);
        Assert.Equal("id", batch.Schema.FieldsList[0].Name);
        Assert.Equal("bool_col", batch.Schema.FieldsList[1].Name);

        // Check values
        var idArray = (Int32Array)batch.Column(0);
        Assert.Equal(8, idArray.Length);

        var boolArray = (BooleanArray)batch.Column(1);
        Assert.Equal(8, boolArray.Length);
    }

    [Fact]
    public async Task AllTypesPlain_VerifyValues()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0, ["id", "int_col", "float_col", "double_col"]);

        // id column: should have 8 values
        var idArray = (Int32Array)batch.Column(0);
        Assert.Equal(8, idArray.Length);
        // First few IDs in alltypes_plain.parquet
        Assert.NotNull(idArray.GetValue(0));

        // int_col
        var intArray = (Int32Array)batch.Column(1);
        Assert.Equal(8, intArray.Length);
        Assert.NotNull(intArray.GetValue(0));

        // float_col
        var floatArray = (FloatArray)batch.Column(2);
        Assert.Equal(8, floatArray.Length);

        // double_col
        var doubleArray = (DoubleArray)batch.Column(3);
        Assert.Equal(8, doubleArray.Length);
    }

    [Fact]
    public async Task AllTypesDictionary_ReadsDictionaryEncoding()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_dictionary.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(2, batch.Length);
        Assert.True(batch.Schema.FieldsList.Count > 0);
    }

    [Fact]
    public async Task AllTypesPlainSnappy_ReadsCompressedData()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(2, batch.Length);
        Assert.True(batch.Schema.FieldsList.Count > 0);
    }

    [Fact]
    public async Task DataPageV2_Snappy_ReadsAllColumns()
    {
        // Column "d" uses RLE encoding for boolean values.
        await using var file = new LocalRandomAccessFile(TestData.GetPath("datapage_v2.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        // Read all flat columns (column "e" is nested and unsupported)
        var batch = await reader.ReadRowGroupAsync(0, ["a", "b", "c", "d"]);

        Assert.True(batch.Length > 0);
        Assert.Equal(4, batch.Schema.FieldsList.Count);

        // Cross-verify column "d" (RLE boolean) against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("datapage_v2.snappy.parquet"));
        using var rg = psReader.RowGroup(0);
        long numRows = rg.MetaData.NumRows;

        using var col = rg.Column(3).LogicalReader<bool>();
        var expected = col.ReadAll(checked((int)numRows));

        var arr = (BooleanArray)batch.Column("d");
        Assert.Equal(numRows, arr.Length);
        for (int i = 0; i < numRows; i++)
            Assert.Equal(expected[i], arr.GetValue(i));
    }

    [Fact]
    public async Task DeltaBinaryPacked_MatchesExpectedValues()
    {
        // delta_binary_packed.parquet: 66 columns (65 INT64 + 1 INT32), 200 rows
        await using var file = new LocalRandomAccessFile(TestData.GetPath("delta_binary_packed.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(200, batch.Length);
        Assert.Equal(66, batch.Schema.FieldsList.Count);

        // Load expected values from CSV
        var csvPath = Path.Combine(
            Path.GetDirectoryName(TestData.GetPath("delta_binary_packed.parquet"))!,
            "delta_binary_packed_expect.csv");
        var lines = File.ReadAllLines(csvPath);
        var headers = lines[0].Split(',');

        // Verify a selection of columns against expected CSV
        for (int col = 0; col < 66; col++)
        {
            var column = batch.Column(headers[col]);

            for (int row = 0; row < 200; row++)
            {
                var expected = lines[row + 1].Split(',')[col];

                if (col == 65) // int_value: INT32
                {
                    var arr = (Int32Array)column;
                    Assert.Equal(int.Parse(expected), arr.GetValue(row));
                }
                else // INT64
                {
                    var arr = (Int64Array)column;
                    Assert.Equal(long.Parse(expected), arr.GetValue(row));
                }
            }
        }
    }

    [Fact]
    public async Task DeltaByteArray_RequiredColumns_MatchesExpectedValues()
    {
        // delta_encoding_required_column.parquet: 17 columns (9 INT32 + 8 STRING), 100 rows
        // INT32 columns use DELTA_BINARY_PACKED, STRING columns use DELTA_BYTE_ARRAY
        await using var file = new LocalRandomAccessFile(TestData.GetPath("delta_encoding_required_column.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(100, batch.Length);
        Assert.Equal(17, batch.Schema.FieldsList.Count);

        // Load expected values from CSV (column names differ from Parquet schema — use indices)
        var csvPath = Path.Combine(
            Path.GetDirectoryName(TestData.GetPath("delta_encoding_required_column.parquet"))!,
            "delta_encoding_required_column_expect.csv");
        var lines = File.ReadAllLines(csvPath);

        for (int col = 0; col < 17; col++)
        {
            for (int row = 0; row < 100; row++)
            {
                var expected = ParseCsvFields(lines[row + 1])[col];

                if (col < 9) // INT32 columns (DELTA_BINARY_PACKED)
                {
                    var arr = (Int32Array)batch.Column(col);
                    Assert.Equal(int.Parse(expected), arr.GetValue(row));
                }
                else // STRING columns (DELTA_BYTE_ARRAY)
                {
                    var arr = (StringArray)batch.Column(col);
                    Assert.Equal(expected, arr.GetString(row));
                }
            }
        }
    }

    [Fact]
    public async Task DeltaByteArray_OptionalColumns_HandlesNulls()
    {
        // delta_encoding_optional_column.parquet: 17 columns (9 INT64 + 8 STRING), 100 rows, with nulls
        await using var file = new LocalRandomAccessFile(TestData.GetPath("delta_encoding_optional_column.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(100, batch.Length);
        Assert.Equal(17, batch.Schema.FieldsList.Count);

        // Load expected values from CSV — use column indices
        var csvPath = Path.Combine(
            Path.GetDirectoryName(TestData.GetPath("delta_encoding_optional_column.parquet"))!,
            "delta_encoding_optional_column_expect.csv");
        var lines = File.ReadAllLines(csvPath);

        // Verify string columns (cols 9-16) against CSV, handling nulls
        for (int col = 9; col < 17; col++)
        {
            var arr = (StringArray)batch.Column(col);

            for (int row = 0; row < 100; row++)
            {
                var expected = ParseCsvFields(lines[row + 1])[col];
                if (expected == "")
                {
                    Assert.True(arr.IsNull(row),
                        $"Column {col} row {row}: expected null but got '{arr.GetString(row)}'");
                }
                else
                {
                    Assert.Equal(expected, arr.GetString(row));
                }
            }
        }
    }

    [Fact]
    public async Task ByteStreamSplit_Zstd_ReadsFloatAndDouble()
    {
        // byte_stream_split.zstd.parquet: 2 columns (float32, float64), 300 rows, Zstd compressed
        await using var file = new LocalRandomAccessFile(TestData.GetPath("byte_stream_split.zstd.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(300, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);

        // Cross-verify against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("byte_stream_split.zstd.parquet"));
        using var rowGroupReader = psReader.RowGroup(0);

        var floatArray = (FloatArray)batch.Column(0);
        using (var col = rowGroupReader.Column(0).LogicalReader<float?>())
        {
            var expected = col.ReadAll(300);
            for (int i = 0; i < 300; i++)
                Assert.Equal(expected[i], floatArray.GetValue(i));
        }

        var doubleArray = (DoubleArray)batch.Column(1);
        using (var col = rowGroupReader.Column(1).LogicalReader<double?>())
        {
            var expected = col.ReadAll(300);
            for (int i = 0; i < 300; i++)
                Assert.Equal(expected[i], doubleArray.GetValue(i));
        }
    }

    [Fact]
    public async Task ByteStreamSplit_RoundTrip_AllTypes()
    {
        // Write Float, Double, Int32, Int64 columns with BYTE_STREAM_SPLIT via ParquetSharp
        var path = Path.Combine(Path.GetTempPath(), $"ew-bss-{Guid.NewGuid():N}.parquet");
        try
        {
            int rowCount = 500;
            var floats = Enumerable.Range(0, rowCount).Select(i => (float)(i * 1.5 - 100)).ToArray();
            var doubles = Enumerable.Range(0, rowCount).Select(i => i * 3.14159 - 500).ToArray();
            var ints = Enumerable.Range(0, rowCount).Select(i => i * 7 - 1000).ToArray();
            var longs = Enumerable.Range(0, rowCount).Select(i => (long)i * 100_000 - 25_000_000).ToArray();

            {
                var columns = new ParquetSharp.Column[]
                {
                    new ParquetSharp.Column<float>("f"),
                    new ParquetSharp.Column<double>("d"),
                    new ParquetSharp.Column<int>("i"),
                    new ParquetSharp.Column<long>("l"),
                };

                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Encoding(ParquetSharp.Encoding.ByteStreamSplit)
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
                using var rowGroup = writer.AppendRowGroup();

                using (var col = rowGroup.NextColumn().LogicalWriter<float>())
                    col.WriteBatch(floats);
                using (var col = rowGroup.NextColumn().LogicalWriter<double>())
                    col.WriteBatch(doubles);
                using (var col = rowGroup.NextColumn().LogicalWriter<int>())
                    col.WriteBatch(ints);
                using (var col = rowGroup.NextColumn().LogicalWriter<long>())
                    col.WriteBatch(longs);

                writer.Close();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var batch = await reader.ReadRowGroupAsync(0);

            Assert.Equal(rowCount, batch.Length);

            var fArr = (FloatArray)batch.Column("f");
            for (int i = 0; i < rowCount; i++)
                Assert.Equal(floats[i], fArr.GetValue(i));

            var dArr = (DoubleArray)batch.Column("d");
            for (int i = 0; i < rowCount; i++)
                Assert.Equal(doubles[i], dArr.GetValue(i));

            var iArr = (Int32Array)batch.Column("i");
            for (int i = 0; i < rowCount; i++)
                Assert.Equal(ints[i], iArr.GetValue(i));

            var lArr = (Int64Array)batch.Column("l");
            for (int i = 0; i < rowCount; i++)
                Assert.Equal(longs[i], lArr.GetValue(i));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task DeltaLengthByteArray_CrossVerified()
    {
        // Cross-verify delta_length_byte_array.parquet against ParquetSharp
        var parquetPath = TestData.GetPath("delta_length_byte_array.parquet");

        await using var file = new LocalRandomAccessFile(parquetPath);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        using var psReader = new ParquetSharp.ParquetFileReader(parquetPath);
        using var rowGroupReader = psReader.RowGroup(0);

        Assert.True(batch.Length > 0);

        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var col = batch.Column(c);
            if (col is StringArray strArr)
            {
                using var psCol = rowGroupReader.Column(c).LogicalReader<string>();
                var expected = psCol.ReadAll(batch.Length);

                for (int r = 0; r < batch.Length; r++)
                    Assert.Equal(expected[r], strArr.GetString(r));
            }
        }
    }

    [Fact]
    public async Task NullsSnappy_HandlesNulls()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nulls.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.True(batch.Length > 0);

        // Verify nulls exist in at least one column
        bool hasNulls = false;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var col = batch.Column(c);
            if (col.NullCount > 0)
            {
                hasNulls = true;
                break;
            }
        }
        Assert.True(hasNulls, "Expected at least one column with null values.");
    }

    [Fact]
    public async Task DictPageOffsetZero_EdgeCase()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("dict-page-offset-zero.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.True(batch.Length > 0);
    }

    [Fact]
    public async Task InvalidRowGroupIndex_Throws()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => reader.ReadRowGroupAsync(99).AsTask());
    }

    [Fact]
    public async Task NonExistentColumn_Throws()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        await Assert.ThrowsAsync<ArgumentException>(
            () => reader.ReadRowGroupAsync(0, ["nonexistent_column"]).AsTask());
    }

    [Fact]
    public async Task SweepTest_AllFlatPlainDictSnappyUncompressedFiles()
    {
        var failures = new List<string>();
        var skipped = new List<string>();

        foreach (var filePath in TestData.GetAllParquetFiles())
        {
            var fileName = Path.GetFileName(filePath);

            // Skip encrypted and deliberately malformed files
            if (fileName.Contains("encrypt", StringComparison.OrdinalIgnoreCase) ||
                fileName.Contains("malformed", StringComparison.OrdinalIgnoreCase))
            {
                skipped.Add($"{fileName}: encrypted/malformed");
                continue;
            }

            try
            {
                await using var file = new LocalRandomAccessFile(filePath);
                using var reader = new ParquetFileReader(file, ownsFile: false);
                var metadata = await reader.ReadMetadataAsync();

                if (metadata.RowGroups.Count == 0)
                {
                    skipped.Add($"{fileName}: no row groups");
                    continue;
                }

                // Check if file uses supported codecs and encodings
                var rg = metadata.RowGroups[0];
                bool unsupported = false;
                foreach (var col in rg.Columns)
                {
                    if (col.MetaData == null)
                    {
                        skipped.Add($"{fileName}: missing column metadata");
                        unsupported = true;
                        break;
                    }

                    // Check codec
                    if (col.MetaData.Codec != CompressionCodec.Uncompressed &&
                        col.MetaData.Codec != CompressionCodec.Snappy &&
                        col.MetaData.Codec != CompressionCodec.Gzip &&
                        col.MetaData.Codec != CompressionCodec.Brotli &&
                        col.MetaData.Codec != CompressionCodec.Lz4 &&
                        col.MetaData.Codec != CompressionCodec.Zstd &&
                        col.MetaData.Codec != CompressionCodec.Lz4Raw)
                    {
                        skipped.Add($"{fileName}: unsupported codec {col.MetaData.Codec}");
                        unsupported = true;
                        break;
                    }

                    // Check encodings
                    foreach (var enc in col.MetaData.Encodings)
                    {
                        if (enc != Encoding.Plain &&
                            enc != Encoding.PlainDictionary &&
                            enc != Encoding.RleDictionary &&
                            enc != Encoding.Rle &&
                            enc != Encoding.DeltaBinaryPacked &&
                            enc != Encoding.DeltaLengthByteArray &&
                            enc != Encoding.DeltaByteArray &&
                            enc != Encoding.ByteStreamSplit)
                        {
                            skipped.Add($"{fileName}: unsupported encoding {enc}");
                            unsupported = true;
                            break;
                        }
                    }
                    if (unsupported) break;
                }

                if (unsupported)
                    continue;

                // Check for nested columns (repetition levels > 0)
                var schema = await reader.GetSchemaAsync();
                bool hasNested = false;
                foreach (var col in schema.Columns)
                {
                    if (col.MaxRepetitionLevel > 0)
                    {
                        hasNested = true;
                        break;
                    }
                }

                if (hasNested)
                {
                    // Try reading only flat columns
                    var flatColumns = schema.Columns
                        .Where(c => c.MaxRepetitionLevel == 0)
                        .Select(c => c.DottedPath)
                        .ToList();

                    if (flatColumns.Count == 0)
                    {
                        skipped.Add($"{fileName}: all columns are nested");
                        continue;
                    }

                    var batch = await reader.ReadRowGroupAsync(0, flatColumns);
                    Assert.True(batch.Length >= 0);
                }
                else
                {
                    var batch = await reader.ReadRowGroupAsync(0);
                    Assert.True(batch.Length >= 0);
                }
            }
            catch (NotSupportedException ex)
            {
                skipped.Add($"{fileName}: {ex.Message}");
            }
            catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is NotSupportedException))
            {
                skipped.Add($"{fileName}: {ex.InnerExceptions[0].Message}");
            }
            catch (Exception ex)
            {
                failures.Add($"{fileName}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.True(failures.Count == 0,
            $"Failed on {failures.Count} files:\n" + string.Join("\n", failures));
    }

    [Fact]
    public async Task ZstdCompressedFile_RoundTrips()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-zstd-{Guid.NewGuid():N}.parquet");
        try
        {
            // Write a Zstd-compressed file via Parquet.Net
            int rowCount = 1000;
            var ids = Enumerable.Range(0, rowCount).ToArray();
            var values = Enumerable.Range(0, rowCount).Select(i => (long)i * 100).ToArray();
            var scores = Enumerable.Range(0, rowCount)
                .Select(i => i % 10 == 0 ? (double?)null : i * 1.5)
                .ToArray();

            // Write via ParquetSharp (uses PLAIN/RLE_DICTIONARY encodings we support)
            {
                var columns = new ParquetSharp.Column[]
                {
                    new ParquetSharp.Column<int>("id"),
                    new ParquetSharp.Column<long>("value"),
                    new ParquetSharp.Column<double?>("score"),
                };

                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Compression(ParquetSharp.Compression.Zstd)
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
                using var rowGroup = writer.AppendRowGroup();

                using (var col = rowGroup.NextColumn().LogicalWriter<int>())
                    col.WriteBatch(ids);
                using (var col = rowGroup.NextColumn().LogicalWriter<long>())
                    col.WriteBatch(values);
                using (var col = rowGroup.NextColumn().LogicalWriter<double?>())
                    col.WriteBatch(scores);

                writer.Close();
            }

            // Read it back with EngineeredWood
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var metadata = await reader.ReadMetadataAsync();

            // Verify Zstd codec is actually used
            Assert.Contains(metadata.RowGroups[0].Columns,
                c => c.MetaData!.Codec == CompressionCodec.Zstd);

            var batch = await reader.ReadRowGroupAsync(0);

            Assert.Equal(rowCount, batch.Length);
            Assert.Equal(3, batch.Schema.FieldsList.Count);

            var idArray = (Int32Array)batch.Column("id");
            Assert.Equal(0, idArray.GetValue(0));
            Assert.Equal(999, idArray.GetValue(999));

            var valueArray = (Int64Array)batch.Column("value");
            Assert.Equal(0L, valueArray.GetValue(0));
            Assert.Equal(99900L, valueArray.GetValue(999));

            var scoreArray = (DoubleArray)batch.Column("score");
            Assert.True(scoreArray.IsNull(0)); // index 0: null (0 % 10 == 0)
            Assert.Equal(1.5, scoreArray.GetValue(1));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task ByteStreamSplitExtendedGzip_ReadsAllColumns()
    {
        // 14 columns (Float16, float, double, int32, int64, FLBA5, decimal) × PLAIN + BYTE_STREAM_SPLIT,
        // 200 rows, all Gzip compressed.
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("byte_stream_split_extended.gzip.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(200, batch.Length);
        Assert.Equal(14, batch.Schema.FieldsList.Count);

        // Cross-verify every column against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("byte_stream_split_extended.gzip.parquet"));
        using var rg = psReader.RowGroup(0);

        // Float16 columns (0, 1) — nullable
        for (int c = 0; c < 2; c++)
        {
            using var col = rg.Column(c).LogicalReader<Half?>();
            var expected = col.ReadAll(200);
            var arr = (HalfFloatArray)batch.Column(c);
            for (int i = 0; i < 200; i++)
            {
                if (expected[i] is null)
                    Assert.True(arr.IsNull(i));
                else
                    Assert.Equal(
                        BitConverter.HalfToInt16Bits(expected[i]!.Value),
                        BitConverter.HalfToInt16Bits(arr.GetValue(i)!.Value));
            }
        }

        // Float columns (2, 3) — nullable
        for (int c = 2; c < 4; c++)
        {
            using var col = rg.Column(c).LogicalReader<float?>();
            var expected = col.ReadAll(200);
            var arr = (FloatArray)batch.Column(c);
            for (int i = 0; i < 200; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }

        // Double columns (4, 5) — nullable
        for (int c = 4; c < 6; c++)
        {
            using var col = rg.Column(c).LogicalReader<double?>();
            var expected = col.ReadAll(200);
            var arr = (DoubleArray)batch.Column(c);
            for (int i = 0; i < 200; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }

        // Int32 columns (6, 7) — nullable
        for (int c = 6; c < 8; c++)
        {
            using var col = rg.Column(c).LogicalReader<int?>();
            var expected = col.ReadAll(200);
            var arr = (Int32Array)batch.Column(c);
            for (int i = 0; i < 200; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }

        // Int64 columns (8, 9) — nullable
        for (int c = 8; c < 10; c++)
        {
            using var col = rg.Column(c).LogicalReader<long?>();
            var expected = col.ReadAll(200);
            var arr = (Int64Array)batch.Column(c);
            for (int i = 0; i < 200; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }

        // FLBA5 columns (10, 11) — nullable, verify PLAIN vs BYTE_STREAM_SPLIT match
        {
            var plain = (FixedSizeBinaryArray)batch.Column(10);
            var bss = (FixedSizeBinaryArray)batch.Column(11);
            for (int i = 0; i < 200; i++)
            {
                Assert.Equal(plain.IsNull(i), bss.IsNull(i));
                if (!plain.IsNull(i))
                    Assert.Equal(plain.GetBytes(i).ToArray(), bss.GetBytes(i).ToArray());
            }
        }

        // Decimal columns (12, 13) — nullable, stored as FixedSizeBinary
        for (int c = 12; c < 14; c++)
        {
            using var col = rg.Column(c).LogicalReader<decimal?>();
            var expected = col.ReadAll(200);
            var arr = (FixedSizeBinaryArray)batch.Column(c);
            for (int i = 0; i < 200; i++)
            {
                if (expected[i] is null)
                    Assert.True(arr.IsNull(i));
                else
                    Assert.False(arr.IsNull(i)); // value is present; exact decimal decode not yet supported
            }
        }
    }

    [Theory]
    [InlineData("float16_nonzeros_and_nans.parquet")]
    [InlineData("float16_zeros_and_nans.parquet")]
    public async Task Float16_ReadsTestFiles(string fileName)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.True(batch.Length > 0);

        // Verify the column is a HalfFloatArray
        var col = batch.Column(0);
        Assert.IsType<HalfFloatArray>(col);
        var arr = (HalfFloatArray)col;

        // Cross-verify against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(TestData.GetPath(fileName));
        using var rg = psReader.RowGroup(0);
        long numRows = rg.MetaData.NumRows;

        Assert.Equal(numRows, batch.Length);

        // ParquetSharp reads Float16 as Half? (nullable)
        using var psCol = rg.Column(0).LogicalReader<Half?>();
        var expected = psCol.ReadAll(checked((int)numRows));

        for (int i = 0; i < numRows; i++)
        {
            if (expected[i] is null)
            {
                Assert.True(arr.IsNull(i));
            }
            else
            {
                // Compare bitwise to handle NaN correctly
                Assert.Equal(
                    BitConverter.HalfToInt16Bits(expected[i]!.Value),
                    BitConverter.HalfToInt16Bits(arr.GetValue(i)!.Value));
            }
        }
    }

    [Fact]
    public async Task GzipCompressed_ReadsTestFile()
    {
        // concatenated_gzip_members.parquet: 1 column (UInt64, optional), 513 rows, Gzip compressed
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("concatenated_gzip_members.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Contains(metadata.RowGroups[0].Columns,
            c => c.MetaData!.Codec == CompressionCodec.Gzip);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(513, batch.Length);

        // Cross-verify against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("concatenated_gzip_members.parquet"));
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<ulong?>();
        var expected = col.ReadAll(513);

        var arr = (UInt64Array)batch.Column(0);
        for (int i = 0; i < 513; i++)
            Assert.Equal(expected[i], (ulong?)arr.GetValue(i));
    }

    [Fact]
    public async Task GzipCompressed_RoundTrip()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-gzip-{Guid.NewGuid():N}.parquet");
        try
        {
            int rowCount = 1000;
            var ids = Enumerable.Range(0, rowCount).ToArray();
            var values = Enumerable.Range(0, rowCount).Select(i => i * 3.14).ToArray();

            {
                var columns = new ParquetSharp.Column[]
                {
                    new ParquetSharp.Column<int>("id"),
                    new ParquetSharp.Column<double>("value"),
                };
                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Compression(ParquetSharp.Compression.Gzip)
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
                using var rowGroup = writer.AppendRowGroup();
                using (var col = rowGroup.NextColumn().LogicalWriter<int>())
                    col.WriteBatch(ids);
                using (var col = rowGroup.NextColumn().LogicalWriter<double>())
                    col.WriteBatch(values);
                writer.Close();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var batch = await reader.ReadRowGroupAsync(0);

            Assert.Equal(rowCount, batch.Length);
            var idArr = (Int32Array)batch.Column("id");
            var valArr = (DoubleArray)batch.Column("value");
            for (int i = 0; i < rowCount; i++)
            {
                Assert.Equal(ids[i], idArr.GetValue(i));
                Assert.Equal(values[i], valArr.GetValue(i));
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task BrotliCompressed_RoundTrip()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-brotli-{Guid.NewGuid():N}.parquet");
        try
        {
            int rowCount = 1000;
            var ids = Enumerable.Range(0, rowCount).ToArray();
            var values = Enumerable.Range(0, rowCount).Select(i => (long)i * 42).ToArray();

            {
                var columns = new ParquetSharp.Column[]
                {
                    new ParquetSharp.Column<int>("id"),
                    new ParquetSharp.Column<long>("value"),
                };
                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Compression(ParquetSharp.Compression.Brotli)
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
                using var rowGroup = writer.AppendRowGroup();
                using (var col = rowGroup.NextColumn().LogicalWriter<int>())
                    col.WriteBatch(ids);
                using (var col = rowGroup.NextColumn().LogicalWriter<long>())
                    col.WriteBatch(values);
                writer.Close();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var metadata = await reader.ReadMetadataAsync();
            Assert.Contains(metadata.RowGroups[0].Columns,
                c => c.MetaData!.Codec == CompressionCodec.Brotli);

            var batch = await reader.ReadRowGroupAsync(0);
            Assert.Equal(rowCount, batch.Length);
            var idArr = (Int32Array)batch.Column("id");
            var valArr = (Int64Array)batch.Column("value");
            for (int i = 0; i < rowCount; i++)
            {
                Assert.Equal(ids[i], idArr.GetValue(i));
                Assert.Equal(values[i], valArr.GetValue(i));
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task Lz4RawCompressed_ReadsTestFile()
    {
        // lz4_raw_compressed.parquet: 3 columns (Int64, ByteArray, Double?), 4 rows, LZ4_RAW
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("lz4_raw_compressed.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Contains(metadata.RowGroups[0].Columns,
            c => c.MetaData!.Codec == CompressionCodec.Lz4Raw);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(4, batch.Length);

        // Cross-verify against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("lz4_raw_compressed.parquet"));
        using var rg = psReader.RowGroup(0);

        {
            using var col = rg.Column(0).LogicalReader<long>();
            var expected = col.ReadAll(4);
            var arr = (Int64Array)batch.Column(0);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }
        {
            using var col = rg.Column(1).LogicalReader<byte[]>();
            var expected = col.ReadAll(4);
            var arr = (BinaryArray)batch.Column(1);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetBytes(i).ToArray());
        }
        {
            using var col = rg.Column(2).LogicalReader<double?>();
            var expected = col.ReadAll(4);
            var arr = (DoubleArray)batch.Column(2);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }
    }

    [Fact]
    public async Task Lz4RawCompressed_LargerFile()
    {
        // lz4_raw_compressed_larger.parquet: 1 String column, 10000 rows
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("lz4_raw_compressed_larger.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(10_000, batch.Length);

        // Cross-verify
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("lz4_raw_compressed_larger.parquet"));
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var expected = col.ReadAll(10_000);
        var arr = (StringArray)batch.Column(0);
        for (int i = 0; i < 10_000; i++)
            Assert.Equal(expected[i], arr.GetString(i));
    }

    [Fact]
    public async Task HadoopLz4Compressed_ReadsTestFile()
    {
        // hadoop_lz4_compressed.parquet: 3 columns (Int64, ByteArray, Double?), 4 rows, Hadoop LZ4
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("hadoop_lz4_compressed.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Contains(metadata.RowGroups[0].Columns,
            c => c.MetaData!.Codec == CompressionCodec.Lz4);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(4, batch.Length);

        // Cross-verify against ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("hadoop_lz4_compressed.parquet"));
        using var rg = psReader.RowGroup(0);

        {
            using var col = rg.Column(0).LogicalReader<long>();
            var expected = col.ReadAll(4);
            var arr = (Int64Array)batch.Column(0);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }
        {
            using var col = rg.Column(1).LogicalReader<byte[]>();
            var expected = col.ReadAll(4);
            var arr = (BinaryArray)batch.Column(1);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetBytes(i).ToArray());
        }
        {
            using var col = rg.Column(2).LogicalReader<double?>();
            var expected = col.ReadAll(4);
            var arr = (DoubleArray)batch.Column(2);
            for (int i = 0; i < 4; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }
    }

    [Fact]
    public async Task HadoopLz4Compressed_LargerFile()
    {
        // hadoop_lz4_compressed_larger.parquet: 1 String column, 10000 rows
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("hadoop_lz4_compressed_larger.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(10_000, batch.Length);

        // Cross-verify
        using var psReader = new ParquetSharp.ParquetFileReader(
            TestData.GetPath("hadoop_lz4_compressed_larger.parquet"));
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var expected = col.ReadAll(10_000);
        var arr = (StringArray)batch.Column(0);
        for (int i = 0; i < 10_000; i++)
            Assert.Equal(expected[i], arr.GetString(i));
    }

    /// <summary>
    /// Splits a quoted CSV line into fields, handling commas inside quoted values
    /// and empty (null) fields represented as consecutive commas.
    /// </summary>
    private static List<string> ParseCsvFields(string line)
    {
        line = line.TrimEnd('\r', '\n');
        var fields = new List<string>();
        int i = 0;
        while (i <= line.Length)
        {
            if (i == line.Length)
            {
                // Trailing comma produced an empty field
                fields.Add("");
                break;
            }
            else if (line[i] == '"')
            {
                i++; // skip opening quote
                int start = i;
                while (i < line.Length && line[i] != '"')
                    i++;
                fields.Add(line[start..i]);
                if (i < line.Length) i++; // skip closing quote
                if (i < line.Length && line[i] == ',') i++; // skip delimiter
                else break;
            }
            else if (line[i] == ',')
            {
                // Empty field
                fields.Add("");
                i++;
            }
            else
            {
                int start = i;
                while (i < line.Length && line[i] != ',')
                    i++;
                fields.Add(line[start..i]);
                if (i < line.Length) i++; // skip delimiter
                else break;
            }
        }
        return fields;
    }
}
