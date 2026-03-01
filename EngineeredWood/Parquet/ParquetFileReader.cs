using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Reads Parquet file metadata and schema via an <see cref="IRandomAccessFile"/>.
/// </summary>
public sealed class ParquetFileReader : IAsyncDisposable, IDisposable
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();
    private const int MagicSize = 4;
    private const int FooterSuffixSize = 8; // 4-byte footer length + 4-byte magic
    private const int MinFileSize = MagicSize + FooterSuffixSize; // leading PAR1 + trailing 8

    private readonly IRandomAccessFile _file;
    private readonly bool _ownsFile;
    private FileMetaData? _metadata;
    private SchemaDescriptor? _schema;
    private bool _disposed;

    /// <summary>
    /// Creates a new reader over the given file.
    /// </summary>
    /// <param name="file">The random access file to read from.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this reader is disposed.</param>
    public ParquetFileReader(IRandomAccessFile file, bool ownsFile = true)
    {
        _file = file;
        _ownsFile = ownsFile;
    }

    /// <summary>
    /// Reads and caches the file metadata from the Parquet footer.
    /// </summary>
    public async ValueTask<FileMetaData> ReadMetadataAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_metadata != null)
            return _metadata;

        long fileLength = await _file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < MinFileSize)
            throw new ParquetFormatException(
                $"File is too small to be a valid Parquet file ({fileLength} bytes).");

        // Read the last 8 bytes: 4-byte footer length (LE) + 4-byte PAR1 magic
        using var suffixBuffer = await _file.ReadAsync(
            new FileRange(fileLength - FooterSuffixSize, FooterSuffixSize),
            cancellationToken).ConfigureAwait(false);

        var suffix = suffixBuffer.Memory.Span;

        // Validate trailing magic
        if (suffix[4] != Par1Magic[0] || suffix[5] != Par1Magic[1] ||
            suffix[6] != Par1Magic[2] || suffix[7] != Par1Magic[3])
            throw new ParquetFormatException("Invalid Parquet file: missing trailing PAR1 magic.");

        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(suffix);
        if (footerLength <= 0 || footerLength > fileLength - MinFileSize)
            throw new ParquetFormatException(
                $"Invalid Parquet footer length: {footerLength}.");

        // Read the footer (Thrift-encoded FileMetaData)
        long footerOffset = fileLength - FooterSuffixSize - footerLength;
        using var footerBuffer = await _file.ReadAsync(
            new FileRange(footerOffset, footerLength),
            cancellationToken).ConfigureAwait(false);

        _metadata = MetadataDecoder.DecodeFileMetaData(footerBuffer.Memory.Span);
        return _metadata;
    }

    /// <summary>
    /// Gets the schema descriptor, building it from cached metadata.
    /// </summary>
    public async ValueTask<SchemaDescriptor> GetSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_schema != null)
            return _schema;

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        _schema = new SchemaDescriptor(metadata.Schema);
        return _schema;
    }

    /// <summary>
    /// Reads a single row group and returns the data as an Arrow <see cref="RecordBatch"/>.
    /// </summary>
    /// <param name="rowGroupIndex">Zero-based index of the row group to read.</param>
    /// <param name="columnNames">
    /// Optional list of column names to read. If null, reads all columns.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An Arrow RecordBatch containing the requested columns.</returns>
    public async ValueTask<RecordBatch> ReadRowGroupAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        // Read all column chunks in parallel via ReadRangesAsync
        var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var results = new ColumnResult[ctx.Count];
            Parallel.For(0, ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns);
            });

            return AssembleRecordBatch(ctx, results);
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }
    }

    /// <summary>
    /// Reads each column sequentially: read I/O buffer, decode, release buffer before the next.
    /// Only one column's I/O buffer in memory at a time.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupIncrementalAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var results = new ColumnResult[ctx.Count];

        for (int i = 0; i < ctx.Count; i++)
        {
            using var buffer = await _file.ReadAsync(ctx.Ranges[i], cancellationToken)
                .ConfigureAwait(false);

            results[i] = ColumnChunkReader.ReadColumn(
                buffer.Memory.Span, ctx.Columns[i],
                ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                ctx.HasNestedColumns);
        }

        return AssembleRecordBatch(ctx, results);
    }

    /// <summary>
    /// Reads all I/O buffers upfront, then decodes columns in parallel using multiple cores.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupParallelAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var results = new ColumnResult[ctx.Count];

            Parallel.For(0, ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns);
            });

            return AssembleRecordBatch(ctx, results);
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }
    }

    /// <summary>
    /// Reads and decodes columns in parallel with bounded concurrency.
    /// Each iteration reads its I/O buffer, decodes, and releases the buffer immediately.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupIncrementalParallelAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var results = new ColumnResult[ctx.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, ctx.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                CancellationToken = cancellationToken,
            },
            async (i, ct) =>
            {
                using var buffer = await _file.ReadAsync(ctx.Ranges[i], ct)
                    .ConfigureAwait(false);

                results[i] = ColumnChunkReader.ReadColumn(
                    buffer.Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns);
            }).ConfigureAwait(false);

        return AssembleRecordBatch(ctx, results);
    }

    private async ValueTask<RowGroupContext> PrepareRowGroupAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);

        if (rowGroupIndex < 0 || rowGroupIndex >= metadata.RowGroups.Count)
            throw new ArgumentOutOfRangeException(nameof(rowGroupIndex),
                $"Row group index {rowGroupIndex} is out of range (0..{metadata.RowGroups.Count - 1}).");

        var rowGroup = metadata.RowGroups[rowGroupIndex];
        int rowCount = checked((int)rowGroup.NumRows);

        var (selectedColumns, selectedChunks) = ResolveColumns(
            schema, rowGroup, columnNames);

        var ranges = new FileRange[selectedChunks.Count];
        var leafArrowFields = new Field[selectedColumns.Count];

        for (int i = 0; i < selectedChunks.Count; i++)
        {
            var colMeta = selectedChunks[i].MetaData
                ?? throw new ParquetFormatException(
                    $"Column chunk {i} has no inline metadata.");

            long start = colMeta.DictionaryPageOffset is > 0 and long dpo
                ? dpo
                : colMeta.DataPageOffset;
            ranges[i] = new FileRange(start, colMeta.TotalCompressedSize);

            leafArrowFields[i] = ArrowSchemaConverter.ToArrowField(selectedColumns[i]);
        }

        // Detect nested columns: when reading all columns, check if any
        // top-level schema child is a non-leaf (struct, list, or map).
        bool hasNestedColumns = false;
        SchemaNode? schemaRoot = null;
        Field[]? topLevelFields = null;

        if (columnNames == null)
        {
            schemaRoot = schema.Root;
            foreach (var child in schemaRoot.Children)
            {
                if (!child.IsLeaf)
                {
                    hasNestedColumns = true;
                    break;
                }
            }

            // Also check for bare repeated leaves (top-level repeated primitives)
            if (!hasNestedColumns)
            {
                foreach (var child in schemaRoot.Children)
                {
                    if (child.IsLeaf && child.Element.RepetitionType == FieldRepetitionType.Repeated)
                    {
                        hasNestedColumns = true;
                        break;
                    }
                }
            }

            if (hasNestedColumns)
                topLevelFields = ArrowSchemaConverter.ToArrowFields(schemaRoot);
        }

        return new RowGroupContext(selectedColumns, selectedChunks, ranges,
            leafArrowFields, rowCount, hasNestedColumns, schemaRoot, topLevelFields);
    }

    private RecordBatch AssembleRecordBatch(RowGroupContext ctx, ColumnResult[] results)
    {
        if (!ctx.HasNestedColumns)
        {
            // Fast path: flat columns only
            var arrowArrays = new IArrowArray[results.Length];
            for (int i = 0; i < results.Length; i++)
                arrowArrays[i] = results[i].Array;

            return BuildRecordBatch(ctx.LeafArrowFields, arrowArrays, ctx.RowCount);
        }

        // Nested path: group leaf arrays into Struct/List/Map arrays
        var leafArrays = new IArrowArray[results.Length];
        var leafDefLevels = new int[]?[results.Length];
        var leafRepLevels = new int[]?[results.Length];
        for (int i = 0; i < results.Length; i++)
        {
            leafArrays[i] = results[i].Array;
            leafDefLevels[i] = results[i].DefinitionLevels;
            leafRepLevels[i] = results[i].RepetitionLevels;
        }

        var topLevelArrays = NestedAssembler.Assemble(
            ctx.SchemaRoot!, leafArrays, leafDefLevels, leafRepLevels, ctx.RowCount);

        return BuildRecordBatch(ctx.TopLevelFields!, topLevelArrays, ctx.RowCount);
    }

    private static RecordBatch BuildRecordBatch(
        Field[] arrowFields, IArrowArray[] arrowArrays, int rowCount)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < arrowFields.Length; i++)
            builder.Field(arrowFields[i]);

        return new RecordBatch(builder.Build(), arrowArrays, rowCount);
    }

    private sealed record RowGroupContext(
        IReadOnlyList<ColumnDescriptor> Columns,
        IReadOnlyList<ColumnChunk> Chunks,
        FileRange[] Ranges,
        Field[] LeafArrowFields,
        int RowCount,
        bool HasNestedColumns = false,
        SchemaNode? SchemaRoot = null,
        Field[]? TopLevelFields = null)
    {
        public int Count => Columns.Count;
    }

    private static (IReadOnlyList<ColumnDescriptor>, IReadOnlyList<ColumnChunk>) ResolveColumns(
        SchemaDescriptor schema,
        RowGroup rowGroup,
        IReadOnlyList<string>? columnNames)
    {
        if (columnNames == null)
        {
            // All leaf columns (flat, struct, list, map)
            var allColumns = new List<ColumnDescriptor>();
            var allChunks = new List<ColumnChunk>();
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                allColumns.Add(schema.Columns[i]);
                allChunks.Add(rowGroup.Columns[i]);
            }
            return (allColumns, allChunks);
        }

        var columns = new List<ColumnDescriptor>(columnNames.Count);
        var chunks = new List<ColumnChunk>(columnNames.Count);

        foreach (var name in columnNames)
        {
            bool found = false;

            // First, try matching a leaf column by dotted path
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                var col = schema.Columns[i];
                if (col.DottedPath == name)
                {
                    columns.Add(col);
                    chunks.Add(rowGroup.Columns[i]);
                    found = true;
                    break;
                }
            }

            if (found) continue;

            // Second, try matching a top-level group name → resolve to all descendant leaves
            foreach (var child in schema.Root.Children)
            {
                if (child.Name == name && !child.IsLeaf)
                {
                    // Add all descendant leaves
                    for (int i = 0; i < schema.Columns.Count; i++)
                    {
                        var col = schema.Columns[i];
                        if (col.Path.Count > 0 && col.Path[0] == name)
                        {
                            columns.Add(col);
                            chunks.Add(rowGroup.Columns[i]);
                        }
                    }
                    found = true;
                    break;
                }
            }

            if (!found)
                throw new ArgumentException(
                    $"Column '{name}' was not found in the schema.", nameof(columnNames));
        }

        return (columns, chunks);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

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
}
