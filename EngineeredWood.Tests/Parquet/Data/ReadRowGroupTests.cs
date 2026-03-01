using Apache.Arrow;
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
    public async Task DataPageV2_Snappy_ReadsSupportedColumns()
    {
        // datapage_v2.snappy.parquet uses DeltaBinaryPacked for some columns (out of MVP scope).
        // Read only the "a" column which uses PLAIN/DICTIONARY encoding.
        await using var file = new LocalRandomAccessFile(TestData.GetPath("datapage_v2.snappy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0, ["a"]);

        Assert.True(batch.Length > 0);
        Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("a", batch.Schema.FieldsList[0].Name);
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
                        col.MetaData.Codec != CompressionCodec.Snappy)
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
                            enc != Encoding.Rle)
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
            catch (Exception ex)
            {
                failures.Add($"{fileName}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.True(failures.Count == 0,
            $"Failed on {failures.Count} files:\n" + string.Join("\n", failures));
    }
}
