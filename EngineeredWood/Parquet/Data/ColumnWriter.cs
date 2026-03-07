using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Compression;
using EngineeredWood.Parquet.Data.Encoders;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Writes a single column chunk: decomposes an Arrow array into Parquet-encoded data pages
/// with optional dictionary page. Streams pages directly to an <see cref="IOutputFile"/>
/// to avoid intermediate buffering.
/// </summary>
internal sealed class ColumnWriter
{
    private readonly ColumnDescriptor _column;
    private readonly CompressionCodec _codec;
    private readonly Encoding _encoding;
    private readonly int _targetPageSize;
    private readonly int _maxDictionarySize;
    private readonly DataPageVersion _pageVersion;
    private readonly EncodingStrategy _strategy;

    public ColumnWriter(
        ColumnDescriptor column,
        CompressionCodec codec = CompressionCodec.Uncompressed,
        Encoding encoding = Encoding.Plain,
        int targetPageSize = 1 * 1024 * 1024,
        int maxDictionarySize = 1 * 1024 * 1024,
        DataPageVersion pageVersion = DataPageVersion.V1,
        EncodingStrategy strategy = EncodingStrategy.None)
    {
        _column = column;
        _codec = codec;
        _strategy = strategy;
        _encoding = strategy != EncodingStrategy.None
            ? EncodingStrategyResolver.Resolve(strategy, encoding, column.PhysicalType)
            : encoding;
        _targetPageSize = targetPageSize;
        _maxDictionarySize = EncodingStrategyResolver.GetMaxDictionarySize(strategy, maxDictionarySize);
        _pageVersion = pageVersion;
    }

    /// <summary>
    /// Result of writing a column chunk (sync API).
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
    /// Writes an Arrow array as a column chunk to the given output, streaming pages directly.
    /// Returns metadata with offsets absolute to the output's position space.
    /// </summary>
    public async ValueTask<Metadata.ColumnMetaData> WriteAsync(
        IArrowArray array, IOutputFile output, CancellationToken ct = default)
    {
        int typeLength = _column.TypeLength ?? 0;
        var decomposed = ArrowArrayDecomposer.Decompose(
            array, _column.PhysicalType, _column.MaxDefinitionLevel, typeLength);

        var state = new WriteState();
        int numValues = array.Length;

        if (_encoding == Encoding.RleDictionary || _encoding == Encoding.PlainDictionary)
        {
            var dictResult = TryBuildDictionary(decomposed, numValues);

            if (dictResult.HasValue)
            {
                state.DictionaryPageOffset = output.Position;
                byte[] dictPage = dictResult.Value.encoder.EncodeDictionaryPage();
                await WritePageAsync(output, PageType.DictionaryPage, dictPage, dictPage.Length,
                    state, ct,
                    dictPageHeader: new DictionaryPageHeader
                    {
                        NumValues = dictResult.Value.encoder.DictionarySize,
                        Encoding = Encoding.PlainDictionary,
                    }).ConfigureAwait(false);
                state.Encodings.Add(Encoding.PlainDictionary);

                byte[] indexData = dictResult.Value.encoder.EncodeIndices();
                int indexBitWidth = dictResult.Value.encoder.GetIndexBitWidth();
                state.DataPageOffset = output.Position;
                await WriteIndexPagesAsync(indexData, indexBitWidth, decomposed, numValues,
                    output, state, ct).ConfigureAwait(false);
            }
            else
            {
                var fallback = EncodingStrategyResolver.GetFallbackEncoding(_strategy, _column.PhysicalType);
                state.DataPageOffset = output.Position;
                await WriteEncodedPagesAsync(decomposed, numValues, fallback,
                    output, state, ct).ConfigureAwait(false);
            }
        }
        else
        {
            state.DataPageOffset = output.Position;
            await WriteEncodedPagesAsync(decomposed, numValues, _encoding,
                output, state, ct).ConfigureAwait(false);
        }

        var statistics = StatisticsCollector.Collect(decomposed, _column.PhysicalType, numValues);

        return new Metadata.ColumnMetaData
        {
            Type = _column.PhysicalType,
            Encodings = state.Encodings.ToList(),
            PathInSchema = _column.Path.ToList(),
            Codec = _codec,
            NumValues = numValues,
            TotalUncompressedSize = state.TotalUncompressedSize,
            TotalCompressedSize = state.TotalCompressedSize,
            DataPageOffset = state.DataPageOffset,
            DictionaryPageOffset = state.DictionaryPageOffset,
            Statistics = statistics,
        };
    }

    /// <summary>
    /// Sync convenience wrapper: writes to an in-memory buffer and returns the bytes.
    /// Kept for backward compatibility with unit tests.
    /// </summary>
    public ColumnWriteResult Write(IArrowArray array)
    {
        var ms = new MemoryStream();
        var adapter = new MemoryOutputAdapter(ms);
        var metadata = WriteAsync(array, adapter).AsTask().GetAwaiter().GetResult();
        ms.TryGetBuffer(out var segment);
        // Copy to exact-sized array for backward compatibility with readers that use array.Length
        var data = segment.Count == segment.Array!.Length
            ? segment.Array
            : segment.AsSpan().ToArray();
        return new ColumnWriteResult(data, metadata);
    }

    // Tracks accumulated state during streaming writes
    private sealed class WriteState
    {
        public readonly HashSet<Encoding> Encodings = new();
        public long TotalUncompressedSize;
        public long TotalCompressedSize;
        public long DataPageOffset;
        public long? DictionaryPageOffset;
    }

    // Minimal IOutputFile adapter for MemoryStream (sync Write() wrapper)
    private sealed class MemoryOutputAdapter : IOutputFile
    {
        private readonly MemoryStream _ms;
        public MemoryOutputAdapter(MemoryStream ms) => _ms = ms;
        public long Position => _ms.Position;
        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            _ms.Write(data.Span);
            return default;
        }
        public ValueTask FlushAsync(CancellationToken ct = default) => default;
        public void Dispose() { }
        public ValueTask DisposeAsync() => default;
    }

    private readonly struct DictBuildResult
    {
        public readonly DictionaryEncoder encoder;
        public DictBuildResult(DictionaryEncoder encoder) => this.encoder = encoder;
    }

    /// <summary>
    /// Builds a dictionary from the decomposed values without writing anything.
    /// Returns null if the dictionary exceeds the size limit.
    /// </summary>
    private DictBuildResult? TryBuildDictionary(ArrowArrayDecomposer.DecomposedColumn decomposed, int valueCount)
    {
        var dictEncoder = new DictionaryEncoder(_column.PhysicalType, estimatedValues: valueCount);

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

        if (dictEncoder.DictionaryByteSize > _maxDictionarySize)
            return null;

        return new DictBuildResult(dictEncoder);
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

    private async ValueTask WriteIndexPagesAsync(
        byte[] indexData,
        int indexBitWidth,
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        IOutputFile output,
        WriteState state,
        CancellationToken ct)
    {
        state.Encodings.Add(Encoding.RleDictionary);

        if (_pageVersion == DataPageVersion.V2)
        {
            await WriteIndexPagesV2Async(indexData, indexBitWidth, decomposed, numValues,
                output, state, ct).ConfigureAwait(false);
            return;
        }

        byte[] defLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV1(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        int pageDataLen = defLevelBytes.Length + 1 + indexData.Length;
        byte[] pageData = ArrayPool<byte>.Shared.Rent(pageDataLen);
        try
        {
            defLevelBytes.CopyTo(pageData.AsSpan());
            pageData[defLevelBytes.Length] = (byte)indexBitWidth;
            indexData.CopyTo(pageData.AsSpan(defLevelBytes.Length + 1));

            await WritePageAsync(output, PageType.DataPage, pageData, pageDataLen,
                state, ct,
                dataPageHeader: new DataPageHeader
                {
                    NumValues = numValues,
                    Encoding = Encoding.RleDictionary,
                    DefinitionLevelEncoding = Encoding.Rle,
                    RepetitionLevelEncoding = Encoding.Rle,
                }).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pageData);
        }
    }

    private async ValueTask WriteIndexPagesV2Async(
        byte[] indexData,
        int indexBitWidth,
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        IOutputFile output,
        WriteState state,
        CancellationToken ct)
    {
        byte[] defLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV2(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        int valuesLen = 1 + indexData.Length;
        byte[] valuesData = ArrayPool<byte>.Shared.Rent(valuesLen);
        try
        {
            valuesData[0] = (byte)indexBitWidth;
            indexData.CopyTo(valuesData.AsSpan(1));

            int numNulls = numValues - decomposed.NonNullCount;
            await WriteV2PageAsync(output, defLevelBytes, [], valuesData, valuesLen,
                numValues, numNulls, numValues,
                Encoding.RleDictionary, state, ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(valuesData);
        }
    }

    private async ValueTask WriteEncodedPagesAsync(
        ArrowArrayDecomposer.DecomposedColumn decomposed,
        int numValues,
        Encoding encoding,
        IOutputFile output,
        WriteState state,
        CancellationToken ct)
    {
        state.Encodings.Add(encoding);
        byte[] encodedValues = EncodeValues(decomposed, encoding);

        if (_pageVersion == DataPageVersion.V2)
        {
            byte[] defLevelBytes = decomposed.DefLevels != null
                ? LevelEncoder.EncodeV2(decomposed.DefLevels, _column.MaxDefinitionLevel)
                : [];

            int numNulls = numValues - decomposed.NonNullCount;
            await WriteV2PageAsync(output, defLevelBytes, [], encodedValues, encodedValues.Length,
                numValues, numNulls, numValues,
                encoding, state, ct).ConfigureAwait(false);
            return;
        }

        // V1 data page: def levels (with 4-byte length prefix) + encoded values
        byte[] v1DefLevelBytes = decomposed.DefLevels != null
            ? LevelEncoder.EncodeV1(decomposed.DefLevels, _column.MaxDefinitionLevel)
            : [];

        int pageDataLen = v1DefLevelBytes.Length + encodedValues.Length;
        byte[] pageData = ArrayPool<byte>.Shared.Rent(pageDataLen);
        try
        {
            v1DefLevelBytes.CopyTo(pageData.AsSpan());
            encodedValues.CopyTo(pageData.AsSpan(v1DefLevelBytes.Length));

            await WritePageAsync(output, PageType.DataPage, pageData, pageDataLen,
                state, ct,
                dataPageHeader: new DataPageHeader
                {
                    NumValues = numValues,
                    Encoding = encoding,
                    DefinitionLevelEncoding = Encoding.Rle,
                    RepetitionLevelEncoding = Encoding.Rle,
                }).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pageData);
        }
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
            _ => EncodePlain(decomposed),
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
                return EncodePlain(decomposed);
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
    private async ValueTask WriteV2PageAsync(
        IOutputFile output,
        byte[] defLevelBytes,
        byte[] repLevelBytes,
        byte[] valuesData,
        int valuesDataLen,
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        WriteState state,
        CancellationToken ct)
    {
        int uncompressedValuesSize = valuesDataLen;
        int compressedValuesSize;
        bool isCompressed;
        byte[]? rentedBuf = null;
        ReadOnlyMemory<byte> compressedValues;

        if (_codec == CompressionCodec.Uncompressed)
        {
            compressedValues = valuesData.AsMemory(0, valuesDataLen);
            compressedValuesSize = uncompressedValuesSize;
            isCompressed = false;
        }
        else
        {
            int maxLen = Compressor.GetMaxCompressedLength(_codec, uncompressedValuesSize);
            rentedBuf = ArrayPool<byte>.Shared.Rent(maxLen);
            compressedValuesSize = Compressor.Compress(
                _codec, valuesData.AsSpan(0, valuesDataLen), rentedBuf);
            compressedValues = rentedBuf.AsMemory(0, compressedValuesSize);
            isCompressed = true;
        }

        try
        {
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

            await output.WriteAsync(headerBytes, ct).ConfigureAwait(false);
            if (repLevelBytes.Length > 0)
                await output.WriteAsync(repLevelBytes, ct).ConfigureAwait(false);
            if (defLevelBytes.Length > 0)
                await output.WriteAsync(defLevelBytes, ct).ConfigureAwait(false);
            await output.WriteAsync(compressedValues, ct).ConfigureAwait(false);

            state.TotalUncompressedSize += headerBytes.Length + totalUncompressed;
            state.TotalCompressedSize += headerBytes.Length + totalCompressed;

            if (_column.MaxDefinitionLevel > 0)
                state.Encodings.Add(Encoding.Rle);
        }
        finally
        {
            if (rentedBuf != null)
                ArrayPool<byte>.Shared.Return(rentedBuf);
        }
    }

    private async ValueTask WritePageAsync(
        IOutputFile output,
        PageType pageType,
        byte[] uncompressedData,
        int uncompressedSize,
        WriteState state,
        CancellationToken ct,
        DataPageHeader? dataPageHeader = null,
        DictionaryPageHeader? dictPageHeader = null)
    {
        int compressedSize;
        byte[]? rentedBuf = null;
        ReadOnlyMemory<byte> compressedData;

        if (_codec == CompressionCodec.Uncompressed)
        {
            compressedData = uncompressedData.AsMemory(0, uncompressedSize);
            compressedSize = uncompressedSize;
        }
        else
        {
            int maxLen = Compressor.GetMaxCompressedLength(_codec, uncompressedSize);
            rentedBuf = ArrayPool<byte>.Shared.Rent(maxLen);
            compressedSize = Compressor.Compress(
                _codec, uncompressedData.AsSpan(0, uncompressedSize), rentedBuf);
            compressedData = rentedBuf.AsMemory(0, compressedSize);
        }

        try
        {
            var pageHeader = new PageHeader
            {
                Type = pageType,
                UncompressedPageSize = uncompressedSize,
                CompressedPageSize = compressedSize,
                DataPageHeader = dataPageHeader,
                DictionaryPageHeader = dictPageHeader,
            };

            byte[] headerBytes = PageHeaderEncoder.Encode(pageHeader);

            await output.WriteAsync(headerBytes, ct).ConfigureAwait(false);
            await output.WriteAsync(compressedData, ct).ConfigureAwait(false);

            state.TotalUncompressedSize += headerBytes.Length + uncompressedSize;
            state.TotalCompressedSize += headerBytes.Length + compressedSize;

            if (_column.MaxDefinitionLevel > 0)
                state.Encodings.Add(Encoding.Rle);
        }
        finally
        {
            if (rentedBuf != null)
                ArrayPool<byte>.Shared.Return(rentedBuf);
        }
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
