using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Parquet.Compression;
using EngineeredWood.Parquet.Data.Encoders;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Writes a single column chunk: decomposes an Arrow array into Parquet-encoded data pages
/// with optional dictionary page. Returns the raw bytes (page headers + page data) plus
/// the column chunk metadata.
/// </summary>
internal sealed class ColumnWriter
{
    private readonly ColumnDescriptor _column;
    private readonly CompressionCodec _codec;
    private readonly Encoding _encoding;
    private readonly int _targetPageSize;
    private readonly int _maxDictionarySize;
    private readonly DataPageVersion _pageVersion;

    public ColumnWriter(
        ColumnDescriptor column,
        CompressionCodec codec = CompressionCodec.Uncompressed,
        Encoding encoding = Encoding.Plain,
        int targetPageSize = 1 * 1024 * 1024,
        int maxDictionarySize = 1 * 1024 * 1024,
        DataPageVersion pageVersion = DataPageVersion.V1)
    {
        _column = column;
        _codec = codec;
        _encoding = encoding;
        _targetPageSize = targetPageSize;
        _maxDictionarySize = maxDictionarySize;
        _pageVersion = pageVersion;
    }

    /// <summary>
    /// Result of writing a column chunk.
    /// </summary>
    public readonly struct ColumnWriteResult
    {
        /// <summary>Serialized page data (page headers + page bytes).</summary>
        public readonly byte[] Data;

        /// <summary>Metadata for the column chunk.</summary>
        public readonly Metadata.ColumnMetaData Metadata;

        public ColumnWriteResult(byte[] data, Metadata.ColumnMetaData metadata)
        {
            Data = data;
            Metadata = metadata;
        }
    }

    /// <summary>
    /// Writes an Arrow array as a column chunk. Returns the serialized bytes and metadata.
    /// </summary>
    public ColumnWriteResult Write(IArrowArray array)
    {
        int typeLength = _column.TypeLength ?? 0;
        var decomposed = ArrowArrayDecomposer.Decompose(
            array, _column.PhysicalType, _column.MaxDefinitionLevel, typeLength);

        var output = new MemoryStream();
        var encodingsUsed = new HashSet<Encoding>();
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        long? dictionaryPageOffset = null;
        int numValues = array.Length;

        // Try dictionary encoding if requested
        if (_encoding == Encoding.RleDictionary || _encoding == Encoding.PlainDictionary)
        {
            var dictResult = TryWriteDictionary(decomposed, output, encodingsUsed,
                ref totalUncompressedSize, ref totalCompressedSize);

            if (dictResult.HasValue)
            {
                dictionaryPageOffset = 0;
                // Write RLE-encoded index pages
                WriteIndexPages(dictResult.Value.indexData, dictResult.Value.indexBitWidth,
                    decomposed, numValues, output, encodingsUsed,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }
            else
            {
                // Dictionary too large — fall back to plain
                WritePlainPages(decomposed, numValues, output, encodingsUsed,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }
        }
        else
        {
            // Direct encoding (Plain, DeltaBinaryPacked, etc.)
            WriteEncodedPages(decomposed, numValues, output, encodingsUsed,
                ref totalUncompressedSize, ref totalCompressedSize);
        }

        var data = output.ToArray();

        var metadata = new Metadata.ColumnMetaData
        {
            Type = _column.PhysicalType,
            Encodings = encodingsUsed.ToList(),
            PathInSchema = _column.Path.ToList(),
            Codec = _codec,
            NumValues = numValues,
            TotalUncompressedSize = totalUncompressedSize,
            TotalCompressedSize = totalCompressedSize,
            DataPageOffset = dictionaryPageOffset.HasValue
                ? output.Length - data.Length + GetDictionaryPageSize(data)
                : 0,
            DictionaryPageOffset = dictionaryPageOffset,
        };

        return new ColumnWriteResult(data, metadata);
    }

    private static long GetDictionaryPageSize(byte[] data)
    {
        // Read the first page header to find where data pages start
        PageHeaderDecoder.Decode(data, out int headerSize);
        var header = PageHeaderDecoder.Decode(data, out _);
        return headerSize + header.CompressedPageSize;
    }

    private readonly struct DictResult
    {
        public readonly byte[] indexData;
        public readonly int indexBitWidth;

        public DictResult(byte[] indexData, int indexBitWidth)
        {
            this.indexData = indexData;
            this.indexBitWidth = indexBitWidth;
        }
    }

    private DictResult? TryWriteDictionary(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        var dictEncoder = new DictionaryEncoder(_column.PhysicalType);

        // Add values to dictionary
        if (_column.PhysicalType == PhysicalType.Boolean && decomposed.BoolValues != null)
        {
            foreach (bool v in decomposed.BoolValues)
                dictEncoder.AddInt32(v ? 1 : 0);
        }
        else if (decomposed.ValueBytes != null)
        {
            AddFixedWidthToDictionary(dictEncoder, decomposed.ValueBytes, _column.PhysicalType);
        }
        else if (decomposed.ByteArrayData != null && decomposed.ByteArrayOffsets != null)
        {
            for (int i = 0; i < decomposed.NonNullCount; i++)
            {
                int start = decomposed.ByteArrayOffsets[i];
                int len = decomposed.ByteArrayOffsets[i + 1] - start;
                dictEncoder.AddByteArray(decomposed.ByteArrayData.AsSpan(start, len));
            }
        }

        // Check if dictionary is too large
        if (dictEncoder.DictionaryByteSize > _maxDictionarySize)
            return null;

        // Write dictionary page
        byte[] dictPage = dictEncoder.EncodeDictionaryPage();
        WritePage(output, PageType.DictionaryPage, dictPage, dictPage.Length,
            encodingsUsed, ref totalUncompressedSize, ref totalCompressedSize,
            dictPageHeader: new DictionaryPageHeader
            {
                NumValues = dictEncoder.DictionarySize,
                Encoding = Encoding.PlainDictionary,
            });

        encodingsUsed.Add(Encoding.PlainDictionary);

        return new DictResult(dictEncoder.EncodeIndices(), dictEncoder.GetIndexBitWidth());
    }

    private static void AddFixedWidthToDictionary(
        DictionaryEncoder encoder, byte[] valueBytes, PhysicalType physicalType)
    {
        int elementSize = GetElementSize(physicalType);
        int count = valueBytes.Length / elementSize;

        for (int i = 0; i < count; i++)
        {
            var span = valueBytes.AsSpan(i * elementSize, elementSize);
            switch (physicalType)
            {
                case PhysicalType.Int32:
                    encoder.AddInt32(BinaryPrimitives.ReadInt32LittleEndian(span));
                    break;
                case PhysicalType.Int64:
                    encoder.AddInt64(BinaryPrimitives.ReadInt64LittleEndian(span));
                    break;
                case PhysicalType.Float:
                    encoder.AddFloat(BinaryPrimitives.ReadSingleLittleEndian(span));
                    break;
                case PhysicalType.Double:
                    encoder.AddDouble(BinaryPrimitives.ReadDoubleLittleEndian(span));
                    break;
                case PhysicalType.FixedLenByteArray:
                case PhysicalType.Int96:
                    encoder.AddFixedLenByteArray(span);
                    break;
            }
        }
    }

    private void WriteIndexPages(
        byte[] indexData,
        int indexBitWidth,
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        encodingsUsed.Add(Encoding.RleDictionary);

        if (_pageVersion == DataPageVersion.V2)
        {
            WriteIndexPagesV2(indexData, indexBitWidth, decomposed, numValues,
                output, encodingsUsed, ref totalUncompressedSize, ref totalCompressedSize);
            return;
        }

        // V1: def levels + RLE indices go together, then compress everything
        byte[] defLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV1(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        // Build uncompressed page data: def levels + [bit width byte] + index data
        byte[] pageData = new byte[defLevelBytes.Length + 1 + indexData.Length];
        defLevelBytes.CopyTo(pageData.AsSpan());
        pageData[defLevelBytes.Length] = (byte)indexBitWidth;
        indexData.CopyTo(pageData.AsSpan(defLevelBytes.Length + 1));

        WritePage(output, PageType.DataPage, pageData, pageData.Length,
            encodingsUsed, ref totalUncompressedSize, ref totalCompressedSize,
            dataPageHeader: new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            });
    }

    private void WriteIndexPagesV2(
        byte[] indexData,
        int indexBitWidth,
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        byte[] defLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV2(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        // Values = [bit width byte] + index data
        byte[] valuesData = new byte[1 + indexData.Length];
        valuesData[0] = (byte)indexBitWidth;
        indexData.CopyTo(valuesData.AsSpan(1));

        int numNulls = numValues - decomposed.NonNullCount;

        WriteV2Page(output, defLevelBytes, [], valuesData,
            numValues, numNulls, numValues,
            Encoding.RleDictionary, encodingsUsed,
            ref totalUncompressedSize, ref totalCompressedSize);
    }

    private void WritePlainPages(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        WriteEncodedPagesCore(decomposed, numValues, Encoding.Plain, output, encodingsUsed,
            ref totalUncompressedSize, ref totalCompressedSize);
    }

    private void WriteEncodedPages(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        WriteEncodedPagesCore(decomposed, numValues, _encoding, output, encodingsUsed,
            ref totalUncompressedSize, ref totalCompressedSize);
    }

    private void WriteEncodedPagesCore(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        Encoding encoding,
        MemoryStream output,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        encodingsUsed.Add(encoding);
        byte[] encodedValues = EncodeValues(decomposed, encoding);

        if (_pageVersion == DataPageVersion.V2)
        {
            byte[] defLevelBytes = decomposed.DefLevels != null
                ? LevelEncoder.EncodeV2(decomposed.DefLevels, _column.MaxDefinitionLevel)
                : [];

            int numNulls = numValues - decomposed.NonNullCount;

            WriteV2Page(output, defLevelBytes, [], encodedValues,
                numValues, numNulls, numValues,
                encoding, encodingsUsed,
                ref totalUncompressedSize, ref totalCompressedSize);
            return;
        }

        // V1 data page: def levels (with 4-byte length prefix) + encoded values
        byte[] v1DefLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV1(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        byte[] pageData = new byte[v1DefLevelBytes.Length + encodedValues.Length];
        v1DefLevelBytes.CopyTo(pageData.AsSpan());
        encodedValues.CopyTo(pageData.AsSpan(v1DefLevelBytes.Length));

        WritePage(output, PageType.DataPage, pageData, pageData.Length,
            encodingsUsed, ref totalUncompressedSize, ref totalCompressedSize,
            dataPageHeader: new DataPageHeader
            {
                NumValues = numValues,
                Encoding = encoding,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            });
    }

    private byte[] EncodeValues(ArrowArrayDecomposer.DecomposedColumn decomposed, Encoding encoding)
    {
        return encoding switch
        {
            Encoding.Plain => EncodePlain(decomposed),
            Encoding.DeltaBinaryPacked => EncodeDeltaBinaryPacked(decomposed),
            Encoding.DeltaLengthByteArray => EncodeDeltaLengthByteArray(decomposed),
            Encoding.DeltaByteArray => EncodeDeltaByteArray(decomposed),
            Encoding.ByteStreamSplit => EncodeByteStreamSplit(decomposed),
            _ => EncodePlain(decomposed), // fallback to plain
        };
    }

    private byte[] EncodePlain(ArrowArrayDecomposer.DecomposedColumn decomposed)
    {
        if (decomposed.BoolValues != null)
            return PlainEncoder.EncodeBoolean(decomposed.BoolValues);

        if (decomposed.ValueBytes != null)
            return decomposed.ValueBytes;

        if (decomposed.ByteArrayData != null && decomposed.ByteArrayOffsets != null)
            return PlainEncoder.EncodeByteArray(decomposed.ByteArrayData, decomposed.ByteArrayOffsets);

        return [];
    }

    private byte[] EncodeDeltaBinaryPacked(ArrowArrayDecomposer.DecomposedColumn decomposed)
    {
        if (decomposed.ValueBytes == null) return [];

        var encoder = new DeltaBinaryPackedEncoder();
        switch (_column.PhysicalType)
        {
            case PhysicalType.Int32:
                var int32s = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(decomposed.ValueBytes);
                foreach (int v in int32s) encoder.AddValue(v);
                break;
            case PhysicalType.Int64:
                var int64s = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(decomposed.ValueBytes);
                foreach (long v in int64s) encoder.AddValue(v);
                break;
            default:
                return EncodePlain(decomposed); // fallback
        }
        return encoder.Finish();
    }

    private byte[] EncodeDeltaLengthByteArray(ArrowArrayDecomposer.DecomposedColumn decomposed)
    {
        if (decomposed.ByteArrayData == null || decomposed.ByteArrayOffsets == null)
            return EncodePlain(decomposed);

        var encoder = new DeltaLengthByteArrayEncoder();
        encoder.AddValues(decomposed.ByteArrayData, decomposed.ByteArrayOffsets);
        return encoder.Finish();
    }

    private byte[] EncodeDeltaByteArray(ArrowArrayDecomposer.DecomposedColumn decomposed)
    {
        if (decomposed.ByteArrayData == null || decomposed.ByteArrayOffsets == null)
            return EncodePlain(decomposed);

        var encoder = new DeltaByteArrayEncoder();
        encoder.AddValues(decomposed.ByteArrayData, decomposed.ByteArrayOffsets);
        return encoder.Finish();
    }

    private byte[] EncodeByteStreamSplit(ArrowArrayDecomposer.DecomposedColumn decomposed)
    {
        if (decomposed.ValueBytes == null) return [];

        return _column.PhysicalType switch
        {
            PhysicalType.Float => ByteStreamSplitEncoder.EncodeFloat(
                System.Runtime.InteropServices.MemoryMarshal.Cast<byte, float>(decomposed.ValueBytes)),
            PhysicalType.Double => ByteStreamSplitEncoder.EncodeDouble(
                System.Runtime.InteropServices.MemoryMarshal.Cast<byte, double>(decomposed.ValueBytes)),
            PhysicalType.Int32 => ByteStreamSplitEncoder.EncodeInt32(
                System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(decomposed.ValueBytes)),
            PhysicalType.Int64 => ByteStreamSplitEncoder.EncodeInt64(
                System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(decomposed.ValueBytes)),
            _ => EncodePlain(decomposed),
        };
    }

    /// <summary>
    /// Writes a V2 data page: levels uncompressed prefix, values optionally compressed.
    /// </summary>
    private void WriteV2Page(
        MemoryStream output,
        byte[] defLevelBytes,
        byte[] repLevelBytes,
        byte[] valuesData,
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize)
    {
        int uncompressedValuesSize = valuesData.Length;
        byte[] compressedValues;
        int compressedValuesSize;
        bool isCompressed;

        if (_codec == CompressionCodec.Uncompressed)
        {
            compressedValues = valuesData;
            compressedValuesSize = uncompressedValuesSize;
            isCompressed = false;
        }
        else
        {
            int maxLen = Compressor.GetMaxCompressedLength(_codec, uncompressedValuesSize);
            compressedValues = new byte[maxLen];
            compressedValuesSize = Compressor.Compress(_codec, valuesData, compressedValues);
            isCompressed = true;
        }

        // Total page sizes: levels (uncompressed) + values (compressed or not)
        int totalUncompressed = repLevelBytes.Length + defLevelBytes.Length + uncompressedValuesSize;
        int totalCompressed = repLevelBytes.Length + defLevelBytes.Length + compressedValuesSize;

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = totalUncompressed,
            CompressedPageSize = totalCompressed,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numNulls,
                NumRows = numRows,
                Encoding = encoding,
                DefinitionLevelsByteLength = defLevelBytes.Length,
                RepetitionLevelsByteLength = repLevelBytes.Length,
                IsCompressed = isCompressed,
            }
        };

        byte[] headerBytes = PageHeaderEncoder.Encode(pageHeader);

        output.Write(headerBytes);
        if (repLevelBytes.Length > 0) output.Write(repLevelBytes);
        if (defLevelBytes.Length > 0) output.Write(defLevelBytes);
        output.Write(compressedValues, 0, compressedValuesSize);

        totalUncompressedSize += headerBytes.Length + totalUncompressed;
        totalCompressedSize += headerBytes.Length + totalCompressed;

        if (_column.MaxDefinitionLevel > 0)
            encodingsUsed.Add(Encoding.Rle);
    }

    private void WritePage(
        MemoryStream output,
        PageType pageType,
        byte[] uncompressedData,
        int uncompressedSize,
        HashSet<Encoding> encodingsUsed,
        ref long totalUncompressedSize,
        ref long totalCompressedSize,
        DataPageHeader? dataPageHeader = null,
        DictionaryPageHeader? dictPageHeader = null)
    {
        // Compress
        byte[] compressedData;
        int compressedSize;

        if (_codec == CompressionCodec.Uncompressed)
        {
            compressedData = uncompressedData;
            compressedSize = uncompressedSize;
        }
        else
        {
            int maxLen = Compressor.GetMaxCompressedLength(_codec, uncompressedSize);
            compressedData = new byte[maxLen];
            compressedSize = Compressor.Compress(_codec, uncompressedData, compressedData);
        }

        // Build page header
        var pageHeader = new PageHeader
        {
            Type = pageType,
            UncompressedPageSize = uncompressedSize,
            CompressedPageSize = compressedSize,
            DataPageHeader = dataPageHeader,
            DictionaryPageHeader = dictPageHeader,
        };

        byte[] headerBytes = PageHeaderEncoder.Encode(pageHeader);

        // Write header + compressed data
        output.Write(headerBytes);
        output.Write(compressedData, 0, compressedSize);

        totalUncompressedSize += headerBytes.Length + uncompressedSize;
        totalCompressedSize += headerBytes.Length + compressedSize;

        if (_column.MaxDefinitionLevel > 0)
            encodingsUsed.Add(Encoding.Rle);
    }

    private static int GetElementSize(PhysicalType type) => type switch
    {
        PhysicalType.Boolean => 1,
        PhysicalType.Int32 => 4,
        PhysicalType.Int64 => 8,
        PhysicalType.Float => 4,
        PhysicalType.Double => 8,
        PhysicalType.Int96 => 12,
        _ => throw new NotSupportedException($"No fixed element size for {type}"),
    };
}
