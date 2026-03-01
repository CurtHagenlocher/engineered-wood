using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Parquet.Compression;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Result of reading a single column chunk: the Arrow array and optionally the raw definition/repetition levels.
/// </summary>
internal readonly record struct ColumnResult(IArrowArray Array, int[]? DefinitionLevels, int[]? RepetitionLevels);

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
    /// <param name="preserveDefLevels">
    /// If true, the raw definition levels are returned in the result for struct bitmap derivation.
    /// </param>
    public static ColumnResult ReadColumn(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        int rowCount,
        Field arrowField,
        bool preserveDefLevels = false)
    {
        bool isRepeated = column.MaxRepetitionLevel > 0;
        int capacity = isRepeated ? checked((int)columnMeta.NumValues) : rowCount;

        using var state = new ColumnBuildState(
            column.PhysicalType, column.MaxDefinitionLevel, column.MaxRepetitionLevel, capacity);
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

        int[]? defLevels = null;
        if ((preserveDefLevels || isRepeated) && column.MaxDefinitionLevel > 0)
            defLevels = state.DefLevelSpan.ToArray();

        int[]? repLevels = null;
        if (isRepeated)
            repLevels = state.RepLevelSpan.ToArray();

        IArrowArray array;
        if (isRepeated)
        {
            int numValues = checked((int)columnMeta.NumValues);
            array = ArrowArrayBuilder.BuildDense(state, arrowField, numValues);
        }
        else
        {
            array = ArrowArrayBuilder.Build(state, arrowField, rowCount);
        }

        return new ColumnResult(array, defLevels, repLevels);
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

        // Decode repetition levels
        if (column.MaxRepetitionLevel > 0)
        {
            var repDest = state.ReserveRepLevels(numValues);
            offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxRepetitionLevel, numValues, repDest);
        }
        else
        {
            var repLevels = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
            offset += LevelDecoder.DecodeV1(pageData.Slice(offset), 0, numValues, repLevels);
        }

        // Decode definition levels directly into native buffer
        if (column.MaxDefinitionLevel > 0)
        {
            var defDest = state.ReserveDefLevels(numValues);
            offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxDefinitionLevel, numValues, defDest);
        }
        else
        {
            // Skip def level decoding — no levels to write
            var tempDef = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
            offset += LevelDecoder.DecodeV1(pageData.Slice(offset), 0, numValues, tempDef);
        }

        // Count non-null values
        int nonNullCount = CountNonNull(state, numValues, column.MaxDefinitionLevel);

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
        if (column.MaxRepetitionLevel > 0)
        {
            var repDest = state.ReserveRepLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, v2Header.RepetitionLevelsByteLength),
                column.MaxRepetitionLevel, numValues, repDest);
        }
        else if (v2Header.RepetitionLevelsByteLength > 0)
        {
            var tempRep = numValues <= 1024 ? stackalloc int[numValues] : new int[numValues];
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, v2Header.RepetitionLevelsByteLength),
                column.MaxRepetitionLevel, numValues, tempRep);
        }
        offset += v2Header.RepetitionLevelsByteLength;

        // Decode definition levels directly into native buffer
        if (column.MaxDefinitionLevel > 0)
        {
            var defDest = state.ReserveDefLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, v2Header.DefinitionLevelsByteLength),
                column.MaxDefinitionLevel, numValues, defDest);
        }
        offset += v2Header.DefinitionLevelsByteLength;

        // Count non-null values
        int nonNullCount = CountNonNull(state, numValues, column.MaxDefinitionLevel);

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
        else if (encoding == Encoding.DeltaBinaryPacked)
        {
            DecodeDeltaBinaryPackedValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.DeltaLengthByteArray)
        {
            DecodeDeltaLengthByteArrayValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.DeltaByteArray)
        {
            DecodeDeltaByteArrayValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.ByteStreamSplit)
        {
            DecodeByteStreamSplitValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.Rle)
        {
            DecodeRleBooleanValues(data, column, nonNullCount, state);
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
                var dest = state.ReserveValues<int>(count);
                PlainDecoder.DecodeInt32s(data, dest, count);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                PlainDecoder.DecodeInt64s(data, dest, count);
                break;
            }
            case PhysicalType.Float:
            {
                var dest = state.ReserveValues<float>(count);
                PlainDecoder.DecodeFloats(data, dest, count);
                break;
            }
            case PhysicalType.Double:
            {
                var dest = state.ReserveValues<double>(count);
                PlainDecoder.DecodeDoubles(data, dest, count);
                break;
            }
            case PhysicalType.Int96:
            {
                var dest = state.ReserveFixedBytes(count, 12);
                PlainDecoder.DecodeInt96s(data, dest, count);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.TypeLength ?? throw new ParquetFormatException(
                    "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var dest = state.ReserveFixedBytes(count, typeLength);
                PlainDecoder.DecodeFixedLenByteArrays(data, dest, count, typeLength);
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

    private static void DecodeDeltaBinaryPackedValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Int32:
            {
                var dest = state.ReserveValues<int>(count);
                var decoder = new DeltaBinaryPackedDecoder(data);
                decoder.DecodeInt32s(dest);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                var decoder = new DeltaBinaryPackedDecoder(data);
                decoder.DecodeInt64s(dest);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for DELTA_BINARY_PACKED decoding.");
        }
    }

    private static void DecodeDeltaLengthByteArrayValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.ByteArray)
            throw new NotSupportedException(
                $"Physical type '{column.PhysicalType}' is not supported for DELTA_LENGTH_BYTE_ARRAY decoding.");

        DeltaLengthByteArrayDecoder.Decode(data, count, state);
    }

    private static void DecodeDeltaByteArrayValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.ByteArray &&
            column.PhysicalType != PhysicalType.FixedLenByteArray)
            throw new NotSupportedException(
                $"Physical type '{column.PhysicalType}' is not supported for DELTA_BYTE_ARRAY decoding.");

        DeltaByteArrayDecoder.Decode(data, count, state);
    }

    private static void DecodeByteStreamSplitValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Float:
            {
                var dest = state.ReserveValues<float>(count);
                ByteStreamSplitDecoder.DecodeFloats(data, dest, count);
                break;
            }
            case PhysicalType.Double:
            {
                var dest = state.ReserveValues<double>(count);
                ByteStreamSplitDecoder.DecodeDoubles(data, dest, count);
                break;
            }
            case PhysicalType.Int32:
            {
                var dest = state.ReserveValues<int>(count);
                ByteStreamSplitDecoder.DecodeInt32s(data, dest, count);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                ByteStreamSplitDecoder.DecodeInt64s(data, dest, count);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.TypeLength ?? throw new ParquetFormatException(
                    "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var dest = state.ReserveFixedBytes(count, typeLength);
                ByteStreamSplitDecoder.DecodeFixedLenByteArrays(data, dest, count, typeLength);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for BYTE_STREAM_SPLIT decoding.");
        }
    }

    private static void DecodeRleBooleanValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.Boolean)
            throw new NotSupportedException(
                $"RLE encoding for values is only supported for BOOLEAN columns, not '{column.PhysicalType}'.");

        // RLE boolean values are prefixed with a 4-byte little-endian length
        if (data.Length < 4)
            throw new ParquetFormatException("RLE boolean data too short for length prefix.");
        int rleLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(data);
        var rleData = data.Slice(4, rleLength);

        var decoder = new RleBitPackedDecoder(rleData, bitWidth: 1);
        var ints = new int[count];
        decoder.ReadBatch(ints);

        Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
        for (int i = 0; i < count; i++)
            values[i] = ints[i] != 0;

        state.AddBoolValues(values.Slice(0, count));
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
                var dest = state.ReserveValues<int>(count);
                for (int i = 0; i < count; i++)
                    dest[i] = dictionary.GetInt32(indices[i]);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                for (int i = 0; i < count; i++)
                    dest[i] = dictionary.GetInt64(indices[i]);
                break;
            }
            case PhysicalType.Float:
            {
                var dest = state.ReserveValues<float>(count);
                for (int i = 0; i < count; i++)
                    dest[i] = dictionary.GetFloat(indices[i]);
                break;
            }
            case PhysicalType.Double:
            {
                var dest = state.ReserveValues<double>(count);
                for (int i = 0; i < count; i++)
                    dest[i] = dictionary.GetDouble(indices[i]);
                break;
            }
            case PhysicalType.Int96:
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.PhysicalType == PhysicalType.Int96 ? 12
                    : column.TypeLength ?? throw new ParquetFormatException(
                        "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var dest = state.ReserveFixedBytes(count, typeLength);
                for (int i = 0; i < count; i++)
                {
                    var bytes = dictionary.GetFixedBytes(indices[i]);
                    bytes.CopyTo(dest.Slice(i * typeLength, typeLength));
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

    private static int CountNonNull(ColumnBuildState state, int numValues, int maxDefLevel)
    {
        if (maxDefLevel == 0)
            return numValues;

        // Read the last numValues def levels from the state
        var allDefs = state.DefLevelSpan;
        var pageDefs = allDefs.Slice(allDefs.Length - numValues);

        int count = 0;
        for (int i = 0; i < pageDefs.Length; i++)
        {
            if (pageDefs[i] == maxDefLevel)
                count++;
        }
        return count;
    }
}
