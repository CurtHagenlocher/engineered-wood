using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Writes Arrow RecordBatches to a Parquet file.
/// Call <see cref="WriteAsync"/> for each RecordBatch, then <see cref="DisposeAsync"/> to finalize.
/// </summary>
public sealed class ParquetFileWriter : IAsyncDisposable
{
    private static readonly byte[] Magic = "PAR1"u8.ToArray();

    private readonly IOutputFile _output;
    private readonly ParquetWriteOptions _options;
    private readonly bool _ownsOutput;

    private Apache.Arrow.Schema? _arrowSchema;
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private IReadOnlyList<ColumnDescriptor>? _columnDescriptors;
    private readonly List<RowGroup> _rowGroups = new();
    private long _totalRows;
    private bool _headerWritten;
    private bool _disposed;

    public ParquetFileWriter(IOutputFile output, ParquetWriteOptions? options = null, bool ownsOutput = true)
    {
        _output = output;
        _options = options ?? ParquetWriteOptions.Default;
        _ownsOutput = ownsOutput;
    }

    /// <summary>
    /// Writes a RecordBatch as one row group.
    /// </summary>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_headerWritten)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ParquetSchemaConverter.ToParquetSchema(_arrowSchema);
            _columnDescriptors = ParquetSchemaConverter.BuildColumnDescriptors(_parquetSchema);
            await WriteMagicAsync(cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
        }

        await WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Finalizes the Parquet file (writes footer) and optionally disposes the output.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_headerWritten)
        {
            await WriteFooterAsync(default).ConfigureAwait(false);
        }

        if (_ownsOutput)
        {
            await _output.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask WriteMagicAsync(CancellationToken ct)
    {
        await _output.WriteAsync(Magic, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteRowGroupAsync(RecordBatch batch, CancellationToken ct)
    {
        var columns = new List<ColumnChunk>();
        long totalByteSize = 0;
        long totalCompressedSize = 0;
        long rowGroupStart = _output.Position;

        for (int i = 0; i < _columnDescriptors!.Count; i++)
        {
            var descriptor = _columnDescriptors[i];
            string columnPath = descriptor.DottedPath;

            var codec = _options.GetCodecForColumn(columnPath);
            var encoding = _options.GetEncodingForColumn(columnPath);

            var colWriter = new ColumnWriter(
                descriptor,
                codec: codec,
                encoding: encoding,
                targetPageSize: _options.TargetPageSize,
                maxDictionarySize: _options.MaxDictionarySize,
                pageVersion: _options.DataPageVersion,
                strategy: _options.EncodingStrategy);

            var array = batch.Column(i);
            var writeResult = colWriter.Write(array);

            long columnOffset = _output.Position;
            await _output.WriteAsync(writeResult.Data, ct).ConfigureAwait(false);

            // Fix up offsets relative to file position
            var meta = writeResult.Metadata;
            var adjustedMeta = new ColumnMetaData
            {
                Type = meta.Type,
                Encodings = meta.Encodings,
                PathInSchema = meta.PathInSchema,
                Codec = meta.Codec,
                NumValues = meta.NumValues,
                TotalUncompressedSize = meta.TotalUncompressedSize,
                TotalCompressedSize = meta.TotalCompressedSize,
                DataPageOffset = columnOffset + (meta.DictionaryPageOffset.HasValue
                    ? GetFirstDataPageOffset(writeResult.Data)
                    : 0),
                DictionaryPageOffset = meta.DictionaryPageOffset.HasValue
                    ? columnOffset
                    : null,
                Statistics = meta.Statistics,
            };

            columns.Add(new ColumnChunk
            {
                FileOffset = columnOffset,
                MetaData = adjustedMeta,
            });

            totalByteSize += meta.TotalUncompressedSize;
            totalCompressedSize += meta.TotalCompressedSize;
        }

        _rowGroups.Add(new RowGroup
        {
            Columns = columns,
            TotalByteSize = totalByteSize,
            NumRows = batch.Length,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = (short)_rowGroups.Count,
        });

        _totalRows += batch.Length;
    }

    private static long GetFirstDataPageOffset(byte[] data)
    {
        // Skip the dictionary page header + data to find where data pages start
        var header = PageHeaderDecoder.Decode(data, out int headerSize);
        return headerSize + header.CompressedPageSize;
    }

    private async ValueTask WriteFooterAsync(CancellationToken ct)
    {
        var metadata = new FileMetaData
        {
            Version = _options.Version,
            Schema = _parquetSchema!,
            NumRows = _totalRows,
            RowGroups = _rowGroups,
            CreatedBy = _options.CreatedBy,
        };

        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(metadata);
        await _output.WriteAsync(footerBytes, ct).ConfigureAwait(false);

        // Write footer length (4 bytes, little-endian)
        var footerLength = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLength, footerBytes.Length);
        await _output.WriteAsync(footerLength, ct).ConfigureAwait(false);

        // Write trailing magic
        await _output.WriteAsync(Magic, ct).ConfigureAwait(false);

        await _output.FlushAsync(ct).ConfigureAwait(false);
    }
}
