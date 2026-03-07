using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// Writes Arrow <see cref="RecordBatch"/> data to Parquet files.
/// </summary>
public sealed class ParquetFileWriter : IAsyncDisposable, IDisposable
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetWriteOptions _options;
    private readonly List<RowGroup> _rowGroups = new();
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private Apache.Arrow.Schema? _arrowSchema;
    private bool _headerWritten;
    private bool _closed;
    private bool _disposed;

    /// <summary>
    /// Creates a new Parquet file writer.
    /// </summary>
    /// <param name="file">The sequential file to write to.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this writer is disposed.</param>
    /// <param name="options">Write options. Defaults to <see cref="ParquetWriteOptions.Default"/>.</param>
    public ParquetFileWriter(ISequentialFile file, bool ownsFile = true, ParquetWriteOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? ParquetWriteOptions.Default;
    }

    /// <summary>
    /// Writes a row group from the given <see cref="RecordBatch"/>.
    /// The schema is inferred from the first batch; subsequent batches must have the same schema.
    /// </summary>
    public async ValueTask WriteRowGroupAsync(
        RecordBatch batch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_closed)
            throw new InvalidOperationException("Writer has been closed.");

        cancellationToken.ThrowIfCancellationRequested();

        // First call: write header and convert schema
        if (!_headerWritten)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema);
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
        }

        // Decompose all Arrow columns into leaf columns (flat columns produce 1 leaf each,
        // nested columns produce multiple leaves)
        int arrowColumnCount = batch.ColumnCount;
        var allLeafResults = new List<ColumnChunkWriter.ColumnChunkResult>();

        // Collect leaf column tasks per Arrow column
        var perColumnLeaves = new List<ColumnChunkWriter.ColumnChunkResult>[arrowColumnCount];

        Parallel.For(0, arrowColumnCount, i =>
        {
            var field = _arrowSchema!.FieldsList[i];
            var array = batch.Column(i);

            if (IsNestedType(field.DataType))
            {
                // Nested: decompose into leaf columns with def/rep levels
                var leaves = NestedLevelWriter.Decompose(array, field, batch.Length);
                var results = new List<ColumnChunkWriter.ColumnChunkResult>(leaves.Count);
                foreach (var leaf in leaves)
                {
                    results.Add(ColumnChunkWriter.WriteColumn(
                        leaf.Array,
                        leaf.PathInSchema,
                        leaf.PhysicalType,
                        leaf.TypeLength,
                        leaf.MaxDefLevel,
                        leaf.MaxRepLevel,
                        leaf.DefLevels,
                        leaf.RepLevels,
                        leaf.NonNullCount,
                        leaf.LevelCount,
                        _options));
                }
                perColumnLeaves[i] = results;
            }
            else
            {
                // Flat: resolve physical type from schema
                var element = FindLeafElement(_parquetSchema!, field.Name);
                perColumnLeaves[i] =
                [
                    ColumnChunkWriter.WriteColumn(
                        array,
                        new[] { field.Name },
                        element.Type!.Value,
                        element.TypeLength ?? 0,
                        field.IsNullable,
                        _options)
                ];
            }
        });

        // Flatten results in schema order
        foreach (var results in perColumnLeaves)
            allLeafResults.AddRange(results);

        // Write column data sequentially (to maintain file offsets)
        var columnChunks = new ColumnChunk[allLeafResults.Count];
        long totalByteSize = 0;
        long totalCompressedSize = 0;

        for (int i = 0; i < allLeafResults.Count; i++)
        {
            var result = allLeafResults[i];
            long chunkStart = _file.Position;

            await _file.WriteAsync(result.Data, cancellationToken).ConfigureAwait(false);

            // Calculate offsets: dictionary page comes first if present
            long dataPageOffset = chunkStart + result.DictionaryPageSize;
            long? dictionaryPageOffset = result.DictionaryPageSize > 0 ? chunkStart : null;

            // Update metadata with actual file offset
            var meta = new ColumnMetaData
            {
                Type = result.MetaData.Type,
                Encodings = result.MetaData.Encodings,
                PathInSchema = result.MetaData.PathInSchema,
                Codec = result.MetaData.Codec,
                NumValues = result.MetaData.NumValues,
                TotalUncompressedSize = result.MetaData.TotalUncompressedSize,
                TotalCompressedSize = result.MetaData.TotalCompressedSize,
                DataPageOffset = dataPageOffset,
                DictionaryPageOffset = dictionaryPageOffset,
                Statistics = result.MetaData.Statistics,
            };

            columnChunks[i] = new ColumnChunk
            {
                FileOffset = chunkStart,
                MetaData = meta,
            };

            totalByteSize += result.MetaData.TotalUncompressedSize;
            totalCompressedSize += result.MetaData.TotalCompressedSize;
        }

        _rowGroups.Add(new RowGroup
        {
            Columns = columnChunks,
            TotalByteSize = totalByteSize,
            NumRows = batch.Length,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = checked((short)_rowGroups.Count),
        });
    }

    /// <summary>
    /// Finalizes the file by writing the footer and closing magic.
    /// Must be called before disposing.
    /// </summary>
    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_closed)
            return;

        _closed = true;

        // If no row groups were written, still write a valid empty file
        if (!_headerWritten)
        {
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
            _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];
        }

        // Calculate total rows
        long totalRows = 0;
        foreach (var rg in _rowGroups)
            totalRows += rg.NumRows;

        // Build file metadata
        var fileMetaData = new FileMetaData
        {
            Version = 2,
            Schema = _parquetSchema!,
            NumRows = totalRows,
            RowGroups = _rowGroups,
            CreatedBy = _options.CreatedBy,
            KeyValueMetadata = _options.KeyValueMetadata,
        };

        // Encode footer to Thrift
        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(fileMetaData);

        // Write footer
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        // Write footer length (4 bytes LE)
        var footerLengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLengthBytes, footerBytes.Length);
        await _file.WriteAsync(footerLengthBytes, cancellationToken).ConfigureAwait(false);

        // Write trailing PAR1 magic
        await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);

        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (!_closed)
            await CloseAsync().ConfigureAwait(false);

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            _file.Dispose();
    }

    private static bool IsNestedType(Apache.Arrow.Types.IArrowType type) =>
        type is Apache.Arrow.Types.StructType
            or Apache.Arrow.Types.ListType
            or Apache.Arrow.Types.MapType;

    /// <summary>
    /// Finds the first leaf SchemaElement matching the given top-level field name.
    /// For flat columns, this is at index 1+ in the schema element list.
    /// </summary>
    private static SchemaElement FindLeafElement(IReadOnlyList<SchemaElement> schema, string fieldName)
    {
        // Walk schema elements (index 0 is root "schema"); find matching name
        for (int i = 1; i < schema.Count; i++)
        {
            if (schema[i].Name == fieldName)
                return schema[i];
        }

        throw new InvalidOperationException(
            $"Schema element not found for field '{fieldName}'.");
    }
}
