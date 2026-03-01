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

        // Verify nulls exist in at least one column (check struct children recursively)
        bool hasNulls = false;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            if (HasNullsRecursive(batch.Column(c)))
            {
                hasNulls = true;
                break;
            }
        }
        Assert.True(hasNulls, "Expected at least one column with null values.");
    }

    private static bool HasNullsRecursive(IArrowArray array)
    {
        if (array.NullCount > 0) return true;
        if (array is StructArray sa)
        {
            for (int i = 0; i < sa.Fields.Count; i++)
            {
                if (HasNullsRecursive(sa.Fields[i]))
                    return true;
            }
        }
        return false;
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

            // Skip encrypted, deliberately malformed, and pathological files
            if (fileName.Contains("encrypt", StringComparison.OrdinalIgnoreCase) ||
                fileName.Contains("malformed", StringComparison.OrdinalIgnoreCase) ||
                fileName == "large_string_map.brotli.parquet") // 2GB+ uncompressed data
            {
                skipped.Add($"{fileName}: encrypted/malformed/pathological");
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

    [Fact]
    public async Task NestedStructsRust_ReadsStructColumns()
    {
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("nested_structs.rust.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.True(batch.Length > 0);
        Assert.True(batch.Schema.FieldsList.Count > 0);

        // Verify at least one field has StructType
        bool hasStruct = false;
        foreach (var field in batch.Schema.FieldsList)
        {
            if (field.DataType is StructType)
            {
                hasStruct = true;
                break;
            }
        }
        Assert.True(hasStruct, "Expected at least one StructType field.");

        // Verify struct arrays have accessible children
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            if (batch.Column(c) is StructArray sa)
            {
                Assert.True(sa.Fields.Count > 0,
                    $"StructArray at column {c} should have children.");
            }
        }
    }

    [Fact]
    public async Task NestedStructsRust_CrossVerifiedWithParquetSharp()
    {
        var path = TestData.GetPath("nested_structs.rust.parquet");

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        // Cross-verify leaf values against ParquetSharp using physical reader API
        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        int numRows = checked((int)rg.MetaData.NumRows);

        Assert.Equal(numRows, batch.Length);

        // Verify some leaf columns against ParquetSharp using physical readers
        int verified = 0;
        for (int c = 0; c < rg.MetaData.NumColumns && verified < 5; c++)
        {
            var desc = psReader.FileMetaData.Schema.Column(c);
            // Only verify plain Int64 physical columns (skip timestamp/date logical types)
            if (desc.PhysicalType != ParquetSharp.PhysicalType.Int64)
                continue;

            try
            {
                var leafArray = FindLeafArray(batch, c);
                using var physCol = rg.Column(c);
                var pr = physCol.LogicalReader<long>();
                var expected = pr.ReadAll(numRows);
                var i64 = (Int64Array)leafArray;
                for (int i = 0; i < numRows; i++)
                    Assert.Equal(expected[i], i64.GetValue(i));
                verified++;
            }
            catch (InvalidCastException)
            {
                // Skip columns whose logical type doesn't match raw Int64 (e.g. DateTime, UInt64)
            }
        }
        Assert.True(verified > 0, "Expected to verify at least one leaf column.");
    }

    [Fact]
    public async Task StructRoundTrip_OptionalStructWithNulls()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-struct-{Guid.NewGuid():N}.parquet");
        try
        {
            int rowCount = 10;

            // Write a file with optional struct via ParquetSharp low-level API:
            // schema: struct_col (optional group) → a (optional int32), b (optional int64)
            {
                var node = new ParquetSharp.Schema.GroupNode("schema", ParquetSharp.Repetition.Required, new ParquetSharp.Schema.Node[]
                {
                    new ParquetSharp.Schema.GroupNode("struct_col", ParquetSharp.Repetition.Optional, new ParquetSharp.Schema.Node[]
                    {
                        new ParquetSharp.Schema.PrimitiveNode("a", ParquetSharp.Repetition.Optional,
                            ParquetSharp.LogicalType.Int(32, true), ParquetSharp.PhysicalType.Int32),
                        new ParquetSharp.Schema.PrimitiveNode("b", ParquetSharp.Repetition.Optional,
                            ParquetSharp.LogicalType.Int(64, true), ParquetSharp.PhysicalType.Int64),
                    }),
                });

                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Encoding(ParquetSharp.Encoding.Plain)
                    .DisableDictionary()
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, node, props);
                using var rowGroup = writer.AppendRowGroup();

                // def levels for struct_col.a (maxDef=2):
                // def=0 → struct null, def=1 → struct present + a null, def=2 → both present
                short[] aDefLevels = [0, 1, 2, 1, 2, 0, 1, 2, 0, 2];
                short[] repLevels = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
                int[] aValues = [10, 40, 70, 90]; // only non-null values
                using (var col = rowGroup.NextColumn())
                {
                    using var pw = (ParquetSharp.ColumnWriter<int>)col;
                    pw.WriteBatch(rowCount, aDefLevels, repLevels, aValues);
                }

                // def levels for struct_col.b (maxDef=2):
                short[] bDefLevels = [0, 1, 2, 1, 2, 0, 1, 2, 0, 2];
                long[] bValues = [100, 400, 700, 900];
                using (var col = rowGroup.NextColumn())
                {
                    using var pw = (ParquetSharp.ColumnWriter<long>)col;
                    pw.WriteBatch(rowCount, bDefLevels, repLevels, bValues);
                }

                writer.Close();
            }

            // Read back with EngineeredWood
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var batch = await reader.ReadRowGroupAsync(0);

            Assert.Equal(rowCount, batch.Length);

            // Should have 1 top-level column: struct_col
            Assert.Single(batch.Schema.FieldsList);
            Assert.IsType<StructType>(batch.Schema.FieldsList[0].DataType);

            var structArray = (StructArray)batch.Column(0);
            Assert.Equal(2, structArray.Fields.Count);

            // Verify struct validity: null at rows 0, 5, 8
            Assert.True(structArray.IsNull(0));
            Assert.False(structArray.IsNull(1));
            Assert.False(structArray.IsNull(2));
            Assert.True(structArray.IsNull(5));
            Assert.True(structArray.IsNull(8));

            // Verify child values
            var intArray = (Int32Array)structArray.Fields[0];
            Assert.True(intArray.IsNull(0)); // struct null
            Assert.True(intArray.IsNull(1)); // struct present, field null
            Assert.Equal(10, intArray.GetValue(2)); // both present
            Assert.True(intArray.IsNull(3)); // struct present, field null
            Assert.Equal(40, intArray.GetValue(4));
            Assert.Equal(90, intArray.GetValue(9));

            var longArray = (Int64Array)structArray.Fields[1];
            Assert.True(longArray.IsNull(0)); // struct null
            Assert.True(longArray.IsNull(1)); // struct present, field null
            Assert.Equal(100, longArray.GetValue(2));
            Assert.Equal(400, longArray.GetValue(4));
            Assert.Equal(700, longArray.GetValue(7));
            Assert.Equal(900, longArray.GetValue(9));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task StructRoundTrip_NestedStructInStruct()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ew-nested-struct-{Guid.NewGuid():N}.parquet");
        try
        {
            int rowCount = 5;

            {
                var node = new ParquetSharp.Schema.GroupNode("schema", ParquetSharp.Repetition.Required, new ParquetSharp.Schema.Node[]
                {
                    new ParquetSharp.Schema.GroupNode("outer", ParquetSharp.Repetition.Optional, new ParquetSharp.Schema.Node[]
                    {
                        new ParquetSharp.Schema.PrimitiveNode("x", ParquetSharp.Repetition.Required,
                            ParquetSharp.LogicalType.Int(32, true), ParquetSharp.PhysicalType.Int32),
                        new ParquetSharp.Schema.GroupNode("inner", ParquetSharp.Repetition.Optional, new ParquetSharp.Schema.Node[]
                        {
                            new ParquetSharp.Schema.PrimitiveNode("y", ParquetSharp.Repetition.Required,
                                ParquetSharp.LogicalType.Int(64, true), ParquetSharp.PhysicalType.Int64),
                        }),
                    }),
                });

                using var props = new ParquetSharp.WriterPropertiesBuilder()
                    .Encoding(ParquetSharp.Encoding.Plain)
                    .DisableDictionary()
                    .Build();
                using var writer = new ParquetSharp.ParquetFileWriter(path, node, props);
                using var rowGroup = writer.AppendRowGroup();

                // outer.x (required under optional outer, maxDef=1):
                // def=0 → outer null, def=1 → outer present + x present (required field)
                short[] xDefLevels = [0, 1, 1, 0, 1];
                short[] repLevels = [0, 0, 0, 0, 0];
                int[] xValues = [10, 20, 40]; // non-null values only
                using (var col = rowGroup.NextColumn())
                {
                    using var pw = (ParquetSharp.ColumnWriter<int>)col;
                    pw.WriteBatch(rowCount, xDefLevels, repLevels, xValues);
                }

                // outer.inner.y (required under optional inner under optional outer, maxDef=2):
                // def=0 → outer null, def=1 → outer present + inner null, def=2 → both present + y present
                short[] yDefLevels = [0, 1, 2, 0, 2];
                long[] yValues = [200, 400]; // non-null values only
                using (var col = rowGroup.NextColumn())
                {
                    using var pw = (ParquetSharp.ColumnWriter<long>)col;
                    pw.WriteBatch(rowCount, yDefLevels, repLevels, yValues);
                }

                writer.Close();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var batch = await reader.ReadRowGroupAsync(0);

            Assert.Equal(rowCount, batch.Length);
            Assert.Single(batch.Schema.FieldsList);

            var outerArray = (StructArray)batch.Column(0);
            Assert.Equal(2, outerArray.Fields.Count);

            // Verify outer struct validity: null at rows 0, 3
            Assert.True(outerArray.IsNull(0));
            Assert.False(outerArray.IsNull(1));
            Assert.False(outerArray.IsNull(2));
            Assert.True(outerArray.IsNull(3));
            Assert.False(outerArray.IsNull(4));

            // outer.x (required): should have values where outer is non-null
            var xArray = (Int32Array)outerArray.Fields[0];
            Assert.Equal(10, xArray.GetValue(1));
            Assert.Equal(20, xArray.GetValue(2));
            Assert.Equal(40, xArray.GetValue(4));

            // outer.inner should be a StructArray
            Assert.IsType<StructArray>(outerArray.Fields[1]);
            var innerArray = (StructArray)outerArray.Fields[1];
            Assert.Single(innerArray.Fields);

            // inner validity: null at rows 0 (outer null), 1 (inner null), 3 (outer null)
            Assert.True(innerArray.IsNull(0));
            Assert.True(innerArray.IsNull(1));
            Assert.False(innerArray.IsNull(2));
            Assert.True(innerArray.IsNull(3));
            Assert.False(innerArray.IsNull(4));

            // inner.y
            var yArray = (Int64Array)innerArray.Fields[0];
            Assert.Equal(200, yArray.GetValue(2));
            Assert.Equal(400, yArray.GetValue(4));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    /// <summary>
    /// Finds a leaf array at a given leaf index by recursively walking the RecordBatch columns.
    /// </summary>
    private static IArrowArray FindLeafArray(RecordBatch batch, int leafIndex)
    {
        int idx = 0;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var result = FindLeafRecursive(batch.Column(c), leafIndex, ref idx);
            if (result != null) return result;
        }
        throw new InvalidOperationException($"Leaf index {leafIndex} not found.");
    }

    private static IArrowArray? FindLeafRecursive(IArrowArray array, int targetIndex, ref int currentIndex)
    {
        if (array is StructArray sa)
        {
            for (int i = 0; i < sa.Fields.Count; i++)
            {
                var result = FindLeafRecursive(sa.Fields[i], targetIndex, ref currentIndex);
                if (result != null) return result;
            }
            return null;
        }

        if (currentIndex == targetIndex)
        {
            currentIndex++;
            return array;
        }
        currentIndex++;
        return null;
    }

    // ---- List and Map column tests ----

    [Fact]
    public async Task ListColumns_ReadsStandard3LevelList()
    {
        // list_columns.parquet: 3 rows, 2 columns: int64_list (list<int64>), utf8_list (list<string>)
        // Row 0: [1,2,3], ["abc","efg","hij"]
        // Row 1: [null,1], null
        // Row 2: [4], ["efg",null,"hij","xyz"]
        await using var file = new LocalRandomAccessFile(TestData.GetPath("list_columns.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(3, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);

        // Verify schema types
        Assert.IsType<ListType>(batch.Schema.FieldsList[0].DataType);
        Assert.IsType<ListType>(batch.Schema.FieldsList[1].DataType);

        var int64List = (ListArray)batch.Column(0);
        var utf8List = (ListArray)batch.Column(1);

        // Row 0: [1, 2, 3]
        Assert.False(int64List.IsNull(0));
        var row0Values = (Int64Array)int64List.GetSlicedValues(0);
        Assert.Equal(3, row0Values.Length);
        Assert.Equal(1L, row0Values.GetValue(0));
        Assert.Equal(2L, row0Values.GetValue(1));
        Assert.Equal(3L, row0Values.GetValue(2));

        // Row 1: [null, 1]
        Assert.False(int64List.IsNull(1));
        var row1Values = (Int64Array)int64List.GetSlicedValues(1);
        Assert.Equal(2, row1Values.Length);
        Assert.True(row1Values.IsNull(0));
        Assert.Equal(1L, row1Values.GetValue(1));

        // Row 2: [4]
        Assert.False(int64List.IsNull(2));
        var row2Values = (Int64Array)int64List.GetSlicedValues(2);
        Assert.Equal(1, row2Values.Length);
        Assert.Equal(4L, row2Values.GetValue(0));

        // utf8_list: Row 0: ["abc","efg","hij"]
        Assert.False(utf8List.IsNull(0));
        var utf8Row0 = (StringArray)utf8List.GetSlicedValues(0);
        Assert.Equal(3, utf8Row0.Length);
        Assert.Equal("abc", utf8Row0.GetString(0));
        Assert.Equal("efg", utf8Row0.GetString(1));
        Assert.Equal("hij", utf8Row0.GetString(2));

        // utf8_list: Row 1: null (null list)
        Assert.True(utf8List.IsNull(1));

        // utf8_list: Row 2: ["efg", null, "hij", "xyz"]
        Assert.False(utf8List.IsNull(2));
        var utf8Row2 = (StringArray)utf8List.GetSlicedValues(2);
        Assert.Equal(4, utf8Row2.Length);
        Assert.Equal("efg", utf8Row2.GetString(0));
        Assert.True(utf8Row2.IsNull(1));
        Assert.Equal("hij", utf8Row2.GetString(2));
        Assert.Equal("xyz", utf8Row2.GetString(3));
    }

    [Fact]
    public async Task NullList_ReadsEmptyList()
    {
        // null_list.parquet: 1 row, 1 column: emptylist (list<null>)
        // Row 0: [] (empty list)
        await using var file = new LocalRandomAccessFile(TestData.GetPath("null_list.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(1, batch.Length);
        var listArray = (ListArray)batch.Column(0);
        Assert.False(listArray.IsNull(0));
        var values = listArray.GetSlicedValues(0);
        Assert.Equal(0, values.Length);
    }

    [Fact]
    public async Task MapNoValue_ReadsMapWithAllNullValues()
    {
        // map_no_value.parquet: 3 rows, 3 columns:
        //   my_map: map<int32, int32> (all values null)
        //   my_map_no_v: map with no value child (pyarrow reads as list)
        //   my_list: list<int32>
        // Row 0: keys=[1,2,3], Row 1: keys=[4,5,6], Row 2: keys=[7,8,9]
        await using var file = new LocalRandomAccessFile(TestData.GetPath("map_no_value.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(3, batch.Length);
        Assert.Equal(3, batch.Schema.FieldsList.Count);

        // First column: my_map
        var mapType = batch.Schema.FieldsList[0].DataType;
        Assert.IsType<MapType>(mapType);

        var mapArray = (MapArray)batch.Column(0);
        Assert.False(mapArray.IsNull(0));

        // Row 0: 3 entries with keys 1,2,3 and null values
        var allKeys = (Int32Array)mapArray.Keys;
        int offset0 = mapArray.ValueOffsets[0];
        int length0 = mapArray.ValueOffsets[1] - offset0;
        Assert.Equal(3, length0);
        Assert.Equal(1, allKeys.GetValue(offset0));
        Assert.Equal(2, allKeys.GetValue(offset0 + 1));
        Assert.Equal(3, allKeys.GetValue(offset0 + 2));

        // Third column: my_list
        var listArray = (ListArray)batch.Column(2);
        Assert.False(listArray.IsNull(0));
        var listRow0 = (Int32Array)listArray.GetSlicedValues(0);
        Assert.Equal(3, listRow0.Length);
        Assert.Equal(1, listRow0.GetValue(0));
        Assert.Equal(2, listRow0.GetValue(1));
        Assert.Equal(3, listRow0.GetValue(2));
    }

    [Fact]
    public async Task RepeatedPrimitiveNoList_ReadsBareRepeatedPrimitive()
    {
        // repeated_primitive_no_list.parquet: 4 rows, bare repeated primitives
        // Row 0: Int32_list=[0,1,2,3], String_list=["foo","zero","one","two"]
        // Row 1: Int32_list=[], String_list=["three"]
        // Row 2: Int32_list=[4], String_list=["four"]
        // Row 3: Int32_list=[5,6,7,8], String_list=["five","six","seven","eight"]
        await using var file = new LocalRandomAccessFile(TestData.GetPath("repeated_primitive_no_list.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(4, batch.Length);

        // Int32_list should be a ListType
        Assert.IsType<ListType>(batch.Schema.FieldsList[0].DataType);

        var intList = (ListArray)batch.Column(0);

        // Row 0: [0,1,2,3]
        var row0 = (Int32Array)intList.GetSlicedValues(0);
        Assert.Equal(4, row0.Length);
        Assert.Equal(0, row0.GetValue(0));
        Assert.Equal(1, row0.GetValue(1));
        Assert.Equal(2, row0.GetValue(2));
        Assert.Equal(3, row0.GetValue(3));

        // Row 1: [] (empty)
        var row1 = (Int32Array)intList.GetSlicedValues(1);
        Assert.Equal(0, row1.Length);

        // Row 2: [4]
        var row2 = (Int32Array)intList.GetSlicedValues(2);
        Assert.Equal(1, row2.Length);
        Assert.Equal(4, row2.GetValue(0));

        // Row 3: [5,6,7,8]
        var row3 = (Int32Array)intList.GetSlicedValues(3);
        Assert.Equal(4, row3.Length);
        Assert.Equal(5, row3.GetValue(0));
        Assert.Equal(8, row3.GetValue(3));

        // String_list
        var stringList = (ListArray)batch.Column(1);
        var strRow0 = (StringArray)stringList.GetSlicedValues(0);
        Assert.Equal(4, strRow0.Length);
        Assert.Equal("foo", strRow0.GetString(0));
    }

    [Fact]
    public async Task OldListStructure_ReadsListOfLists()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("old_list_structure.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(1, batch.Length);

        // Column "a" is LIST<LIST<INT32>>, data: [[1,2],[3,4]]
        var outerList = (ListArray)batch.Column(0);
        Assert.Equal(1, outerList.Length);

        // Outer list row 0 has 2 inner lists
        var innerLists = (ListArray)outerList.GetSlicedValues(0);
        Assert.Equal(2, innerLists.Length);

        // Inner list 0: [1,2]
        var inner0 = (Int32Array)innerLists.GetSlicedValues(0);
        Assert.Equal(2, inner0.Length);
        Assert.Equal(1, inner0.GetValue(0));
        Assert.Equal(2, inner0.GetValue(1));

        // Inner list 1: [3,4]
        var inner1 = (Int32Array)innerLists.GetSlicedValues(1);
        Assert.Equal(2, inner1.Length);
        Assert.Equal(3, inner1.GetValue(0));
        Assert.Equal(4, inner1.GetValue(1));
    }

    [Fact]
    public async Task NestedLists_ReadsCorrectly()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nested_lists.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.True(batch.Length > 0);
    }

    [Fact]
    public async Task NestedMaps_ReadsCorrectly()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nested_maps.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        Assert.True(batch.Length > 0);
    }
}
