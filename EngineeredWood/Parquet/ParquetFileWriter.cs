using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
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
    private SchemaDescriptor? _schemaDescriptor;
    private bool _hasNestedColumns;
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
            _schemaDescriptor = new SchemaDescriptor(_parquetSchema);
            _columnDescriptors = _schemaDescriptor.Columns;
            _hasNestedColumns = HasNestedTypes(_arrowSchema);
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

        if (_hasNestedColumns)
        {
            // Flatten nested Arrow arrays into leaf columns with pre-computed def/rep levels
            var flattenedColumns = NestedArrayFlattener.Flatten(
                batch, _schemaDescriptor!.Root, _columnDescriptors!);

            for (int i = 0; i < _columnDescriptors!.Count; i++)
            {
                var descriptor = _columnDescriptors[i];
                var flat = flattenedColumns[i];

                var colWriter = CreateColumnWriter(descriptor);
                long columnOffset = _output.Position;
                var meta = await colWriter.WritePreDecomposedAsync(
                    flat.Decomposed, flat.NumValues, _output, ct).ConfigureAwait(false);

                columns.Add(new ColumnChunk { FileOffset = columnOffset, MetaData = meta });
                totalByteSize += meta.TotalUncompressedSize;
                totalCompressedSize += meta.TotalCompressedSize;
            }
        }
        else
        {
            // Flat schema — direct path (no flattening overhead)
            for (int i = 0; i < _columnDescriptors!.Count; i++)
            {
                var descriptor = _columnDescriptors[i];
                var colWriter = CreateColumnWriter(descriptor);
                var array = batch.Column(i);
                long columnOffset = _output.Position;
                var meta = await colWriter.WriteAsync(array, _output, ct).ConfigureAwait(false);

                columns.Add(new ColumnChunk { FileOffset = columnOffset, MetaData = meta });
                totalByteSize += meta.TotalUncompressedSize;
                totalCompressedSize += meta.TotalCompressedSize;
            }
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

    private ColumnWriter CreateColumnWriter(ColumnDescriptor descriptor)
    {
        string columnPath = descriptor.DottedPath;
        return new ColumnWriter(
            descriptor,
            codec: _options.GetCodecForColumn(columnPath),
            encoding: _options.GetEncodingForColumn(columnPath),
            targetPageSize: _options.TargetPageSize,
            maxDictionarySize: _options.MaxDictionarySize,
            pageVersion: _options.DataPageVersion,
            strategy: _options.EncodingStrategy);
    }

    private static bool HasNestedTypes(Apache.Arrow.Schema schema)
    {
        foreach (var field in schema.FieldsList)
        {
            if (field.DataType is StructType or ListType or MapType)
                return true;
        }
        return false;
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
