using Apache.Arrow;
using EngineeredWood.Parquet.Compression;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Reads a single column chunk's pages and produces an Arrow array.
/// </summary>
internal static class ColumnChunkReader
{
    /// <summary>
    /// Reads all pages in a column chunk and returns the resulting Arrow array.
    /// </summary>
    /// <param name="data">Raw bytes of the column chunk (from file offset to end of last page).</param>
    /// <param name="column">The column descriptor (type, levels, schema info).</param>
    /// <param name="columnMeta">The column chunk metadata (codec, num_values, etc.).</param>
    /// <param name="rowCount">Number of rows in the row group.</param>
    /// <param name="arrowField">The Arrow field for this column.</param>
    public static IArrowArray ReadColumn(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        int rowCount,
        Field arrowField)
    {
        if (column.MaxRepetitionLevel > 0)
            throw new NotSupportedException(
                $"Nested/repeated columns are not supported. Column '{column.DottedPath}' has MaxRepetitionLevel={column.MaxRepetitionLevel}.");

        var state = new ColumnBuildState(column.PhysicalType, column.MaxDefinitionLevel, rowCount);
        DictionaryDecoder? dictionary = null;

        int pos = 0;
        long valuesRead = 0;

        while (valuesRead < columnMeta.NumValues && pos < data.Length)
        {
            var pageHeader = PageHeaderDecoder.Decode(data.Slice(pos), out int headerSize);
            pos += headerSize;

            var pageData = data.Slice(pos, pageHeader.CompressedPageSize);
            pos += pageHeader.CompressedPageSize;

            switch (pageHeader.Type)
            {
                case PageType.DictionaryPage:
                    dictionary = ReadDictionaryPage(pageHeader, pageData, column, columnMeta);
                    break;

                case PageType.DataPage:
                    valuesRead += ReadDataPageV1(
                        pageHeader, pageData, column, columnMeta, dictionary, state);
                    break;

                case PageType.DataPageV2:
                    valuesRead += ReadDataPageV2(
                        pageHeader, pageData, column, columnMeta, dictionary, state);
                    break;

                default:
                    // Skip index pages and unknown page types
                    break;
            }
        }

        return ArrowArrayBuilder.Build(state, arrowField, rowCount);
    }

    private static DictionaryDecoder ReadDictionaryPage(
        PageHeader header,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta)
    {
        var dictHeader = header.DictionaryPageHeader
            ?? throw new ParquetFormatException("Dictionary page missing DictionaryPageHeader.");

        ReadOnlySpan<byte> plainData;
        byte[]? decompressedBuffer = null;

        if (columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            plainData = compressedData;
        }
        else
        {
            decompressedBuffer = new byte[header.UncompressedPageSize];
            Decompressor.Decompress(columnMeta.Codec, compressedData, decompressedBuffer);
            plainData = decompressedBuffer;
        }

        var decoder = new DictionaryDecoder(column.PhysicalType);
        decoder.Load(plainData, dictHeader.NumValues, column.TypeLength ?? 0);
        return decoder;
    }

    private static int ReadDataPageV1(
        PageHeader header,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        var dataHeader = header.DataPageHeader
            ?? throw new ParquetFormatException("Data page missing DataPageHeader.");

        int numValues = dataHeader.NumValues;

        // V1: entire payload is compressed together
        ReadOnlySpan<byte> pageData;
        byte[]? decompressedBuffer = null;

        if (columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            pageData = compressedData;
        }
        else
        {
            decompressedBuffer = new byte[header.UncompressedPageSize];
            Decompressor.Decompress(columnMeta.Codec, compressedData, decompressedBuffer);
            pageData = decompressedBuffer;
        }

        int offset = 0;

        // Decode repetition levels (omitted if maxRepLevel == 0)
        var repLevels = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
        offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxRepetitionLevel, numValues, repLevels);

        // Decode definition levels (omitted if maxDefLevel == 0)
        var defLevels = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
        offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxDefinitionLevel, numValues, defLevels);

        state.AddDefLevels(defLevels.Slice(0, numValues));

        // Count non-null values
        int nonNullCount = CountNonNull(defLevels.Slice(0, numValues), column.MaxDefinitionLevel);

        // Decode values
        var valueData = pageData.Slice(offset);
        var encoding = dataHeader.Encoding;
        DecodeValues(valueData, encoding, column, dictionary, nonNullCount, state);

        return numValues;
    }

    private static int ReadDataPageV2(
        PageHeader header,
        ReadOnlySpan<byte> rawData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        var v2Header = header.DataPageHeaderV2
            ?? throw new ParquetFormatException("Data page V2 missing DataPageHeaderV2.");

        int numValues = v2Header.NumValues;
        int offset = 0;

        // V2: repetition and definition levels are uncompressed
        var repLevels = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
        LevelDecoder.DecodeV2(
            rawData.Slice(offset, v2Header.RepetitionLevelsByteLength),
            column.MaxRepetitionLevel, numValues, repLevels);
        offset += v2Header.RepetitionLevelsByteLength;

        var defLevels = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
        LevelDecoder.DecodeV2(
            rawData.Slice(offset, v2Header.DefinitionLevelsByteLength),
            column.MaxDefinitionLevel, numValues, defLevels);
        offset += v2Header.DefinitionLevelsByteLength;

        state.AddDefLevels(defLevels.Slice(0, numValues));

        // Count non-null values
        int nonNullCount = CountNonNull(defLevels.Slice(0, numValues), column.MaxDefinitionLevel);

        // V2: only values portion is compressed (if is_compressed, default true)
        var valuesCompressed = rawData.Slice(offset);

        // All values may be null — nothing to decompress or decode
        if (nonNullCount == 0 || valuesCompressed.IsEmpty)
            return numValues;

        ReadOnlySpan<byte> valueData;
        byte[]? decompressedBuffer = null;

        if (!v2Header.IsCompressed || columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            valueData = valuesCompressed;
        }
        else
        {
            int levelsSize = v2Header.RepetitionLevelsByteLength + v2Header.DefinitionLevelsByteLength;
            int uncompressedValuesSize = header.UncompressedPageSize - levelsSize;
            decompressedBuffer = new byte[uncompressedValuesSize];
            Decompressor.Decompress(columnMeta.Codec, valuesCompressed, decompressedBuffer);
            valueData = decompressedBuffer;
        }

        var encoding = v2Header.Encoding;
        DecodeValues(valueData, encoding, column, dictionary, nonNullCount, state);

        return numValues;
    }

    private static void DecodeValues(
        ReadOnlySpan<byte> data,
        Encoding encoding,
        ColumnDescriptor column,
        DictionaryDecoder? dictionary,
        int nonNullCount,
        ColumnBuildState state)
    {
        bool isDictEncoded = encoding == Encoding.PlainDictionary || encoding == Encoding.RleDictionary;

        if (isDictEncoded)
        {
            if (dictionary == null)
                throw new ParquetFormatException(
                    $"Dictionary-encoded data page but no dictionary page found for column '{column.DottedPath}'.");

            DecodeDictValues(data, column, dictionary, nonNullCount, state);
        }
        else if (encoding == Encoding.Plain)
        {
            DecodePlainValues(data, column, nonNullCount, state);
        }
        else
        {
            throw new NotSupportedException(
                $"Encoding '{encoding}' is not supported for column '{column.DottedPath}'.");
        }
    }

    private static void DecodePlainValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Boolean:
            {
                Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
                PlainDecoder.DecodeBooleans(data, values, count);
                state.AddBoolValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int32:
            {
                Span<int> values = count <= 1024 ? stackalloc int[count] : new int[count];
                PlainDecoder.DecodeInt32s(data, values, count);
                state.AddInt32Values(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int64:
            {
                Span<long> values = count <= 512 ? stackalloc long[count] : new long[count];
                PlainDecoder.DecodeInt64s(data, values, count);
                state.AddInt64Values(values.Slice(0, count));
                break;
            }
            case PhysicalType.Float:
            {
                Span<float> values = count <= 1024 ? stackalloc float[count] : new float[count];
                PlainDecoder.DecodeFloats(data, values, count);
                state.AddFloatValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Double:
            {
                Span<double> values = count <= 512 ? stackalloc double[count] : new double[count];
                PlainDecoder.DecodeDoubles(data, values, count);
                state.AddDoubleValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int96:
            {
                var values = new byte[count * 12];
                PlainDecoder.DecodeInt96s(data, values, count);
                state.AddFixedBytes(values);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.TypeLength ?? throw new ParquetFormatException(
                    "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var values = new byte[count * typeLength];
                PlainDecoder.DecodeFixedLenByteArrays(data, values, count, typeLength);
                state.AddFixedBytes(values);
                break;
            }
            case PhysicalType.ByteArray:
            {
                var offsets = new int[count + 1];
                PlainDecoder.DecodeByteArrays(data, offsets, out byte[] valueData, count);
                state.AddByteArrayValues(offsets, valueData, count);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for PLAIN decoding.");
        }
    }

    private static void DecodeDictValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        DictionaryDecoder dictionary,
        int count,
        ColumnBuildState state)
    {
        if (count == 0)
            return;

        // First byte is the bit width for the RLE-encoded indices
        int bitWidth = data[0];
        var rleData = data.Slice(1);
        var decoder = new RleBitPackedDecoder(rleData, bitWidth);

        var indicesArray = new int[count];
        decoder.ReadBatch(indicesArray);
        ReadOnlySpan<int> indices = indicesArray;

        switch (column.PhysicalType)
        {
            case PhysicalType.Boolean:
            {
                Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
                for (int i = 0; i < count; i++)
                    values[i] = dictionary.GetBoolean(indices[i]);
                state.AddBoolValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int32:
            {
                Span<int> values = count <= 1024 ? stackalloc int[count] : new int[count];
                for (int i = 0; i < count; i++)
                    values[i] = dictionary.GetInt32(indices[i]);
                state.AddInt32Values(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int64:
            {
                Span<long> values = count <= 512 ? stackalloc long[count] : new long[count];
                for (int i = 0; i < count; i++)
                    values[i] = dictionary.GetInt64(indices[i]);
                state.AddInt64Values(values.Slice(0, count));
                break;
            }
            case PhysicalType.Float:
            {
                Span<float> values = count <= 1024 ? stackalloc float[count] : new float[count];
                for (int i = 0; i < count; i++)
                    values[i] = dictionary.GetFloat(indices[i]);
                state.AddFloatValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Double:
            {
                Span<double> values = count <= 512 ? stackalloc double[count] : new double[count];
                for (int i = 0; i < count; i++)
                    values[i] = dictionary.GetDouble(indices[i]);
                state.AddDoubleValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int96:
            case PhysicalType.FixedLenByteArray:
            {
                for (int i = 0; i < count; i++)
                {
                    var bytes = dictionary.GetFixedBytes(indices[i]);
                    state.AddFixedBytes(bytes);
                }
                break;
            }
            case PhysicalType.ByteArray:
            {
                // Build offsets and data for the batch
                var offsets = new int[count + 1];
                int totalLen = 0;
                for (int i = 0; i < count; i++)
                {
                    offsets[i] = totalLen;
                    totalLen += dictionary.GetByteArray(indices[i]).Length;
                }
                offsets[count] = totalLen;

                var valueData = new byte[totalLen];
                int pos = 0;
                for (int i = 0; i < count; i++)
                {
                    var bytes = dictionary.GetByteArray(indices[i]);
                    bytes.CopyTo(valueData.AsSpan(pos));
                    pos += bytes.Length;
                }

                state.AddByteArrayValues(offsets, valueData, count);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for dictionary decoding.");
        }
    }

    private static int CountNonNull(ReadOnlySpan<int> defLevels, int maxDefLevel)
    {
        if (maxDefLevel == 0)
            return defLevels.Length;

        int count = 0;
        for (int i = 0; i < defLevels.Length; i++)
        {
            if (defLevels[i] == maxDefLevel)
                count++;
        }
        return count;
    }
}
