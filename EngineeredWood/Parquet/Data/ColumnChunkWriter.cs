using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Parquet.Compression;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes an Arrow column into Parquet pages (PLAIN or dictionary encoding).
/// Returns the complete column chunk bytes and metadata.
/// </summary>
internal static class ColumnChunkWriter
{
    /// <summary>
    /// Result of encoding a column chunk.
    /// </summary>
    internal readonly struct ColumnChunkResult
    {
        public required ArraySegment<byte> Data { get; init; }
        public required ColumnMetaData MetaData { get; init; }

        /// <summary>
        /// Size of the dictionary page in bytes (including header), or 0 if no dictionary.
        /// Used by the caller to calculate DictionaryPageOffset vs DataPageOffset.
        /// </summary>
        public int DictionaryPageSize { get; init; }
    }

    [ThreadStatic]
    private static byte[]? t_compressBuffer;

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk (flat column, single def level).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        bool isNullable,
        ParquetWriteOptions options)
    {
        int rowCount = array.Length;
        int maxDefLevel = isNullable ? 1 : 0;

        // Extract definition levels for nullable columns
        int[]? defLevels = null;
        int nonNullCount = rowCount;

        if (isNullable)
        {
            defLevels = new int[rowCount];
            nonNullCount = 0;
            for (int i = 0; i < rowCount; i++)
            {
                bool isNull = array.IsNull(i);
                defLevels[i] = isNull ? 0 : 1;
                if (!isNull) nonNullCount++;
            }
        }

        return WriteColumnCore(array, rowCount, pathInSchema, physicalType, typeLength,
            maxDefLevel, 0, defLevels, null, nonNullCount, options);
    }

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk with pre-computed def/rep levels
    /// (for nested columns).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        int[] defLevels,
        int[]? repLevels,
        int nonNullCount,
        int levelCount,
        ParquetWriteOptions options)
    {
        return WriteColumnCore(array, levelCount, pathInSchema, physicalType, typeLength,
            maxDefLevel, maxRepLevel, defLevels, repLevels, nonNullCount, options);
    }

    private static ColumnChunkResult WriteColumnCore(
        IArrowArray array,
        int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        int nonNullCount,
        ParquetWriteOptions options)
    {
        // Normalize def levels for value-encoding and dictionary methods:
        // these check defLevels[i] == 0 for null, which only works when maxDefLevel <= 1.
        int[]? valueDefLevels = NormalizeDefLevels(defLevels, maxDefLevel);

        // Try dictionary encoding (uses normalized levels for null detection)
        var dictResult = DictionaryEncoder.TryEncode(
            array, physicalType, typeLength, valueDefLevels, nonNullCount, options);

        ColumnChunkResult result;

        if (dictResult != null)
        {
            result = WriteDictionaryColumn(
                dictResult.Value, rowCount, pathInSchema, physicalType,
                maxDefLevel, maxRepLevel, defLevels, repLevels, options);
        }
        else
        {
            // Non-dictionary encoding (PLAIN for V1, type-aware for V2)
            result = WriteNonDictionaryColumn(
                array, rowCount, pathInSchema, physicalType, typeLength,
                maxDefLevel, maxRepLevel, defLevels, repLevels, valueDefLevels, nonNullCount, options);
        }

        // Compute column-level statistics (uses normalized levels for null detection)
        var stats = StatisticsCollector.Compute(
            array, physicalType, typeLength, valueDefLevels, nonNullCount, rowCount);
        result.MetaData.Statistics = stats;

        return result;
    }

    private static ColumnChunkResult WriteNonDictionaryColumn(
        IArrowArray array, int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType, int typeLength,
        int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        int[]? valueDefLevels, int nonNullCount,
        ParquetWriteOptions options)
    {
        var output = new MemoryStream(EstimateColumnSize(rowCount, physicalType, typeLength));
        int totalUncompressedSize = 0;
        int totalCompressedSize = 0;
        var encodings = new HashSet<Encoding>();

        if (maxDefLevel > 0 || maxRepLevel > 0)
            encodings.Add(Encoding.Rle);

        int valuesPerPage = EstimateValuesPerPage(array, physicalType, typeLength, options.DataPageSize);
        if (valuesPerPage < 1) valuesPerPage = 1;

        // Reusable encoder for def/rep levels across pages
        var defEncoder = maxDefLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxDefLevel)) : null;
        var repEncoder = maxRepLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxRepLevel)) : null;

        int offset = 0;
        while (offset < rowCount)
        {
            int pageValues = Math.Min(valuesPerPage, rowCount - offset);
            int pageNonNull = maxDefLevel > 0
                ? CountNonNull(defLevels!, offset, pageValues, maxDefLevel)
                : pageValues;

            Encoding pageEncoding;

            if (options.DataPageVersion == DataPageVersion.V2)
            {
                WriteDataPageV2(output, array, offset, pageValues, pageNonNull,
                    physicalType, typeLength, maxDefLevel, maxRepLevel, defLevels, repLevels,
                    valueDefLevels, options, defEncoder, repEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize, out pageEncoding);
            }
            else
            {
                WriteDataPageV1(output, array, offset, pageValues, pageNonNull,
                    physicalType, typeLength, maxDefLevel, maxRepLevel, defLevels, repLevels,
                    valueDefLevels, options, defEncoder, repEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
                pageEncoding = Encoding.Plain;
            }

            encodings.Add(pageEncoding);
            offset += pageValues;
        }

        var metadata = new ColumnMetaData
        {
            Type = physicalType,
            Encodings = encodings.ToArray(),
            PathInSchema = pathInSchema is string[] arr ? arr : pathInSchema.ToArray(),
            Codec = options.Compression,
            NumValues = rowCount,
            TotalUncompressedSize = totalUncompressedSize,
            TotalCompressedSize = totalCompressedSize,
            DataPageOffset = 0, // set by caller
        };

        output.TryGetBuffer(out var buffer);
        return new ColumnChunkResult { Data = buffer, MetaData = metadata };
    }

    private static ColumnChunkResult WriteDictionaryColumn(
        DictionaryEncoder.DictionaryResult dictResult,
        int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        ParquetWriteOptions options)
    {
        var output = new MemoryStream(dictResult.DictionaryPageData.Length + rowCount);
        int totalUncompressedSize = 0;
        int totalCompressedSize = 0;

        int bitWidth = DictionaryEncoder.GetIndexBitWidth(dictResult.DictionaryCount);

        // 1. Encode dictionary page
        int dictionaryPageSize = WriteDictionaryPage(output, dictResult, options,
            ref totalUncompressedSize, ref totalCompressedSize);

        // 2. Encode data pages with RLE dictionary indices
        int bytesPerIndex = Math.Max(1, (bitWidth + 7) / 8);
        int valuesPerPage = Math.Max(1, options.DataPageSize / bytesPerIndex);

        // Reusable encoders across pages
        var defEncoder = maxDefLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxDefLevel)) : null;
        var repEncoder = maxRepLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxRepLevel)) : null;
        var indexEncoder = new RleBitPackedEncoder(bitWidth);

        int offset = 0;
        int indexOffset = 0;
        while (offset < rowCount)
        {
            int pageValues = Math.Min(valuesPerPage, rowCount - offset);
            int pageNonNull = maxDefLevel > 0
                ? CountNonNull(defLevels!, offset, pageValues, maxDefLevel)
                : pageValues;

            var pageIndices = dictResult.Indices.AsSpan(indexOffset, pageNonNull);

            if (options.DataPageVersion == DataPageVersion.V2)
            {
                WriteDictDataPageV2(output, pageIndices, offset, pageValues, pageNonNull,
                    bitWidth, maxDefLevel, maxRepLevel, defLevels, repLevels, options,
                    defEncoder, repEncoder, indexEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }
            else
            {
                WriteDictDataPageV1(output, pageIndices, offset, pageValues, pageNonNull,
                    bitWidth, maxDefLevel, maxRepLevel, defLevels, repLevels, options,
                    defEncoder, repEncoder, indexEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }

            offset += pageValues;
            indexOffset += pageNonNull;
        }

        var encodings = maxDefLevel > 0
            ? new[] { Encoding.Plain, Encoding.RleDictionary, Encoding.Rle }
            : new[] { Encoding.Plain, Encoding.RleDictionary };

        var metadata = new ColumnMetaData
        {
            Type = physicalType,
            Encodings = encodings,
            PathInSchema = pathInSchema is string[] arr ? arr : pathInSchema.ToArray(),
            Codec = options.Compression,
            NumValues = rowCount,
            TotalUncompressedSize = totalUncompressedSize,
            TotalCompressedSize = totalCompressedSize,
            DataPageOffset = 0, // set by caller
            DictionaryPageOffset = 0, // set by caller
        };

        output.TryGetBuffer(out var buffer);
        return new ColumnChunkResult
        {
            Data = buffer,
            MetaData = metadata,
            DictionaryPageSize = dictionaryPageSize,
        };
    }

    private static int WriteDictionaryPage(
        MemoryStream output,
        DictionaryEncoder.DictionaryResult dictResult,
        ParquetWriteOptions options,
        ref int totalUncompressed, ref int totalCompressed)
    {
        byte[] dictData = dictResult.DictionaryPageData;
        int uncompressedSize = dictData.Length;

        int compressedLen = CompressTo(dictData, options.Compression);

        var pageHeader = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = uncompressedSize,
            CompressedPageSize = compressedLen,
            DictionaryPageHeader = new DictionaryPageHeader
            {
                NumValues = dictResult.DictionaryCount,
                Encoding = Encoding.Plain,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);
        int pageSize = headerBytes.Length + compressedLen;

        output.Write(headerBytes);
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedSize;
        totalCompressed += pageSize;

        return pageSize;
    }

    private static void WriteDictDataPageV2(
        MemoryStream output,
        ReadOnlySpan<int> indices, int rowOffset, int numValues, int nonNullCount,
        int bitWidth, int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        RleBitPackedEncoder indexEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Encode indices into reusable buffer: 1-byte bit width + RLE/BP hybrid
        int uncompressedValuesSize = EncodeDictionaryIndicesToBuffer(
            indices, nonNullCount, bitWidth, indexEncoder);

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options.Compression);

        int numRows = maxRepLevel > 0 ? CountRows(repLevels, rowOffset, numValues, maxRepLevel) : numValues;

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numValues - nonNullCount,
                NumRows = numRows,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

        output.Write(headerBytes);
        if (repLevelLen > 0) output.Write(repEncoder!.WrittenSpan);
        if (defLevelLen > 0) output.Write(defEncoder!.WrittenSpan);
        output.Write(t_compressBuffer!, 0, compressedValuesLen);

        int uncompressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + uncompressedValuesSize;
        int compressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + compressedValuesLen;
        totalUncompressed += uncompressedPageSize;
        totalCompressed += compressedPageSize;
    }

    private static void WriteDictDataPageV1(
        MemoryStream output,
        ReadOnlySpan<int> indices, int rowOffset, int numValues, int nonNullCount,
        int bitWidth, int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        RleBitPackedEncoder indexEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Build uncompressed body: rep levels + def levels + values
        // All concatenated and compressed together for V1
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defRleLen = defEncoder.Length;
        }

        int valuesLen = EncodeDictionaryIndicesToBuffer(indices, nonNullCount, bitWidth, indexEncoder);

        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesLen;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        t_valuesBuffer.AsSpan(0, valuesLen).CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options.Compression);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

        output.Write(headerBytes);
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedBodySize;
        totalCompressed += headerBytes.Length + compressedLen;
    }

    /// <summary>
    /// Encodes dictionary indices (1-byte bit width + RLE/BP hybrid) into the
    /// thread-static values buffer. Returns the number of bytes written.
    /// </summary>
    private static int EncodeDictionaryIndicesToBuffer(
        ReadOnlySpan<int> indices, int nonNullCount, int bitWidth,
        RleBitPackedEncoder indexEncoder)
    {
        if (nonNullCount == 0)
        {
            EnsureValuesBuffer(1);
            t_valuesBuffer![0] = (byte)bitWidth;
            return 1;
        }

        indexEncoder.Reset();
        indexEncoder.Encode(indices);

        int totalLen = 1 + indexEncoder.Length;
        EnsureValuesBuffer(totalLen);
        t_valuesBuffer![0] = (byte)bitWidth;
        indexEncoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(1));
        return totalLen;
    }

    [ThreadStatic]
    private static byte[]? t_valuesBuffer;

    private static void EnsureValuesBuffer(int size)
    {
        if (t_valuesBuffer == null || t_valuesBuffer.Length < size)
            t_valuesBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── PLAIN encoding path ─────

    private static int CountNonNull(int[] defLevels, int offset, int count, int maxDefLevel)
    {
        int nonNull = 0;
        for (int i = offset; i < offset + count; i++)
            if (defLevels[i] >= maxDefLevel) nonNull++;
        return nonNull;
    }

    private static void WriteDataPageV2(
        MemoryStream output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        ref int totalUncompressed, ref int totalCompressed,
        out Encoding valueEncoding)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Encode values into reusable buffer with type-aware V2 encoding
        int uncompressedValuesSize = EncodeValuesToBuffer(
            array, offset, numValues, nonNullCount, physicalType, typeLength,
            valueDefLevels, out valueEncoding);

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options.Compression);

        int numNulls = numValues - nonNullCount;
        int numRows = maxRepLevel > 0 ? CountRows(repLevels, offset, numValues, maxRepLevel) : numValues;

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numNulls,
                NumRows = numRows,
                Encoding = valueEncoding,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

        output.Write(headerBytes);
        if (repLevelLen > 0) output.Write(repEncoder!.WrittenSpan);
        if (defLevelLen > 0) output.Write(defEncoder!.WrittenSpan);
        output.Write(t_compressBuffer!, 0, compressedValuesLen);

        int uncompressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + uncompressedValuesSize;
        int compressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + compressedValuesLen;
        totalUncompressed += uncompressedPageSize;
        totalCompressed += compressedPageSize;
    }

    private static void WriteDataPageV1(
        MemoryStream output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Encode levels
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defRleLen = defEncoder.Length;
        }

        // Encode values (use normalized valueDefLevels for null detection)
        byte[] valuesBytes = EncodeValues(array, offset, numValues, nonNullCount,
            physicalType, typeLength, valueDefLevels);

        // Concatenate levels + values, then compress together (V1)
        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesBytes.Length;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        valuesBytes.CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options.Compression);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.Plain,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

        output.Write(headerBytes);
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedBodySize;
        totalCompressed += headerBytes.Length + compressedLen;
    }

    // ───── Compression ─────

    /// <summary>
    /// Compresses data into the thread-static compression buffer.
    /// Returns the number of compressed bytes written.
    /// For Uncompressed codec, copies data into the buffer and returns data.Length.
    /// </summary>
    private static int CompressTo(ReadOnlySpan<byte> data, CompressionCodec codec)
    {
        if (data.Length == 0)
        {
            EnsureCompressBuffer(0);
            return 0;
        }

        int maxLen = Compressor.GetMaxCompressedLength(codec, data.Length);
        EnsureCompressBuffer(maxLen);
        return Compressor.Compress(codec, data, t_compressBuffer!.AsSpan(0, maxLen));
    }

    private static void EnsureCompressBuffer(int size)
    {
        if (t_compressBuffer == null || t_compressBuffer.Length < size)
            t_compressBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── V2 type-aware encoding dispatch ─────

    /// <summary>
    /// Encodes values into <see cref="t_valuesBuffer"/> using type-aware V2 encoding.
    /// Returns the number of bytes written and the encoding used.
    /// </summary>
    private static int EncodeValuesToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels,
        out Encoding encoding)
    {
        if (nonNullCount == 0)
        {
            encoding = GetV2Encoding(physicalType);
            EnsureValuesBuffer(0);
            return 0;
        }

        encoding = GetV2Encoding(physicalType);
        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBooleanValuesRleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int32 => EncodeDeltaInt32ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 => EncodeDeltaInt64ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float => EncodeBssSingleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double => EncodeBssDoubleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeDlbaToBuffer(array, offset, numValues, nonNullCount, defLevels),
            _ => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
        };
    }

    private static Encoding GetV2Encoding(PhysicalType physicalType) => physicalType switch
    {
        PhysicalType.Boolean => Encoding.Rle,
        PhysicalType.Int32 or PhysicalType.Int64 => Encoding.DeltaBinaryPacked,
        PhysicalType.Float or PhysicalType.Double => Encoding.ByteStreamSplit,
        PhysicalType.ByteArray => Encoding.DeltaLengthByteArray,
        _ => Encoding.Plain,
    };

    private static int EncodeBooleanValuesRleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        var values = new int[nonNullCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                values[idx++] = boolArray.GetValue(offset + i) == true ? 1 : 0;
        }

        var encoder = new RleBitPackedEncoder(1);
        encoder.Encode(values);

        // 4-byte LE length prefix + RLE data
        int totalLen = 4 + encoder.Length;
        EnsureValuesBuffer(totalLen);
        BinaryPrimitives.WriteInt32LittleEndian(t_valuesBuffer, encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(4));
        return totalLen;
    }

    private static int EncodeDeltaInt32ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            encoder.EncodeInt32s(span);
        }
        else
        {
            var values = ExtractNonNullValues<int>(array, offset, numValues, nonNullCount, defLevels);
            encoder.EncodeInt32s(values);
        }

        EnsureValuesBuffer(encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer);
        return encoder.Length;
    }

    private static int EncodeDeltaInt64ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, long>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            encoder.EncodeInt64s(span);
        }
        else
        {
            var values = ExtractNonNullValues<long>(array, offset, numValues, nonNullCount, defLevels);
            encoder.EncodeInt64s(values);
        }

        EnsureValuesBuffer(encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer);
        return encoder.Length;
    }

    private static int EncodeBssSingleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(float);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeFloatsTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeFloatsTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeBssDoubleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(double);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeDoublesTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeDoublesTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeDlbaToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Compute lengths and total data size in one pass
        var lengths = new int[nonNullCount];
        int totalDataLen = 0;
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            lengths[idx++] = len;
            totalDataLen += len;
        }

        // Delta-encode lengths
        var deltaEncoder = new DeltaBinaryPackedEncoder();
        deltaEncoder.EncodeInt32s(lengths);

        // Write delta_lengths + compact_data into values buffer
        int totalLen = deltaEncoder.Length + totalDataLen;
        EnsureValuesBuffer(totalLen);
        deltaEncoder.WrittenSpan.CopyTo(t_valuesBuffer);

        // Copy non-null byte data directly
        int pos = deltaEncoder.Length;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            arrowData.Slice(start, len).CopyTo(t_valuesBuffer.AsSpan(pos));
            pos += len;
        }

        return totalLen;
    }

    private static int EncodePlainToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        byte[] valuesBytes = EncodeValues(array, offset, numValues, nonNullCount,
            physicalType, typeLength, defLevels);
        EnsureValuesBuffer(valuesBytes.Length);
        valuesBytes.CopyTo(t_valuesBuffer.AsSpan());
        return valuesBytes.Length;
    }

    private static T[] ExtractNonNullValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);

        if (defLevels == null)
        {
            var result = new T[numValues];
            valueBuffer.Slice(offset, numValues).CopyTo(result);
            return result;
        }

        var values = new T[nonNullCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] != 0)
                values[idx++] = valueBuffer[offset + i];
        }
        return values;
    }

    // ───── PLAIN encoding (V1 and FLBA fallback) ─────

    private static byte[] EncodeValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        if (nonNullCount == 0)
            return [];

        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBooleanValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int32 => EncodeFixedValues<int>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 => EncodeFixedValues<long>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float => EncodeFixedValues<float>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double => EncodeFixedValues<double>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeByteArrayValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.FixedLenByteArray => EncodeFixedLenByteArrayValues(
                array, offset, numValues, nonNullCount, defLevels, typeLength),
            _ => throw new NotSupportedException($"Unsupported physical type: {physicalType}"),
        };
    }

    private static byte[] EncodeBooleanValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        var nonNullValues = new bool[nonNullCount];
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                nonNullValues[idx++] = boolArray.GetValue(offset + i) ?? false;
        }

        var encoded = new byte[(nonNullCount + 7) / 8];
        PlainEncoder.EncodeBooleans(nonNullValues, encoded);
        return encoded;
    }

    private static byte[] EncodeFixedValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        int elementSize = Marshal.SizeOf<T>();

        if (defLevels == null)
        {
            // Required column: direct copy from Arrow value buffer
            var valueSpan = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            var result = new byte[numValues * elementSize];
            MemoryMarshal.AsBytes(valueSpan).CopyTo(result);
            return result;
        }

        // Optional column: extract non-null values
        var nonNullValues = new T[nonNullCount];
        int idx = 0;
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] != 0)
                nonNullValues[idx++] = valueBuffer[offset + i];
        }

        var encoded = new byte[nonNullCount * elementSize];
        MemoryMarshal.AsBytes(nonNullValues.AsSpan()).CopyTo(encoded);
        return encoded;
    }

    private static byte[] EncodeByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets;
        ReadOnlySpan<byte> arrowData;

        if (array is StringArray || array is BinaryArray)
        {
            arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            arrowData = data.Buffers[2].Span;
        }
        else
        {
            throw new NotSupportedException(
                $"Unsupported Arrow array type for BYTE_ARRAY: {array.GetType().Name}");
        }

        // Calculate total size
        int totalSize = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;
            int idx = offset + i;
            int len = arrowOffsets[idx + 1] - arrowOffsets[idx];
            totalSize += 4 + len;
        }

        var result = new byte[totalSize];
        int pos = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;

            int idx = offset + i;
            int start = arrowOffsets[idx];
            int len = arrowOffsets[idx + 1] - start;

            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), len);
            pos += 4;
            arrowData.Slice(start, len).CopyTo(result.AsSpan(pos));
            pos += len;
        }

        return result;
    }

    private static byte[] EncodeFixedLenByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        int[]? defLevels, int typeLength)
    {
        var data = array.Data;
        var valueBuffer = data.Buffers[1].Span;

        if (defLevels == null)
        {
            var result = new byte[numValues * typeLength];
            valueBuffer.Slice(offset * typeLength, numValues * typeLength).CopyTo(result);
            return result;
        }

        var encoded = new byte[nonNullCount * typeLength];
        int pos = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] == 0) continue;
            int idx = offset + i;
            valueBuffer.Slice(idx * typeLength, typeLength).CopyTo(encoded.AsSpan(pos));
            pos += typeLength;
        }

        return encoded;
    }

    private static int EstimateValuesPerPage(
        IArrowArray array, PhysicalType physicalType, int typeLength, int targetPageSize)
    {
        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1, // conservative
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            PhysicalType.ByteArray => EstimateAvgByteArraySize(array),
            _ => 8,
        };

        if (bytesPerValue == 0) bytesPerValue = 1;
        return Math.Max(1, targetPageSize / bytesPerValue);
    }

    private static int EstimateAvgByteArraySize(IArrowArray array)
    {
        if (array.Length == 0) return 32;

        var data = array.Data;
        if (data.Buffers.Length >= 3)
        {
            var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            int totalDataBytes = offsets[array.Length] - offsets[0];
            int avgLen = totalDataBytes / Math.Max(1, array.Length);
            return 4 + avgLen;
        }

        return 32;
    }

    private static int EstimateColumnSize(int rowCount, PhysicalType physicalType, int typeLength)
    {
        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1,
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            _ => 8,
        };
        // Compressed data is typically smaller; estimate 75% of raw size + overhead
        return Math.Max(256, (int)(rowCount * bytesPerValue * 0.75) + 256);
    }

    /// <summary>
    /// For nested columns (maxDefLevel > 1), normalizes def levels to 0/1
    /// so value encoding methods can check != 0 for presence.
    /// Returns null if defLevels is null. Returns defLevels unchanged if maxDefLevel <= 1.
    /// </summary>
    private static int[]? NormalizeDefLevels(int[]? defLevels, int maxDefLevel)
    {
        if (defLevels == null)
            return null;
        if (maxDefLevel == 0)
            return null; // fully required column — no nulls possible
        if (maxDefLevel == 1)
            return defLevels; // already 0/1

        // Multi-level: normalize to 0 (absent) / 1 (present)
        var normalized = new int[defLevels.Length];
        for (int i = 0; i < defLevels.Length; i++)
            normalized[i] = defLevels[i] >= maxDefLevel ? 1 : 0;
        return normalized;
    }

    /// <summary>Minimum bit width to represent values 0..maxValue.</summary>
    private static int BitWidth(int maxValue) =>
        maxValue <= 0 ? 0 : (32 - int.LeadingZeroCount(maxValue));

    /// <summary>
    /// Counts the number of rows in a page by counting entries where repLevel &lt; maxRepLevel
    /// (i.e., rep level 0 starts a new row at the top level).
    /// </summary>
    private static int CountRows(int[]? repLevels, int offset, int numValues, int maxRepLevel)
    {
        if (repLevels == null) return numValues;
        int rows = 0;
        for (int i = offset; i < offset + numValues; i++)
        {
            if (repLevels[i] == 0)
                rows++;
        }
        return rows;
    }
}
