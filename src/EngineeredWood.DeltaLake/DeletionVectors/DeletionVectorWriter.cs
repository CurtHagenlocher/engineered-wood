// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Creates deletion vectors from sets of deleted row indices.
/// Small DVs are stored inline (Base85-encoded); large DVs are written to files.
/// </summary>
public sealed class DeletionVectorWriter
{
    private readonly ITableFileSystem _fs;

    /// <summary>
    /// Maximum size in bytes for inline DVs. DVs larger than this are written to files.
    /// Default: 1 KB.
    /// </summary>
    public int InlineThreshold { get; init; } = 1024;

    public DeletionVectorWriter(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Creates a deletion vector from the given deleted row indices.
    /// Returns the DV descriptor to include in the <see cref="AddFile"/> action.
    /// </summary>
    public async ValueTask<DeletionVector> CreateAsync(
        IEnumerable<long> deletedRowIndices,
        long cardinality,
        CancellationToken cancellationToken = default)
    {
        byte[] dvBlob = RoaringBitmapWriter.Serialize(deletedRowIndices);

        if (dvBlob.Length <= InlineThreshold)
            return CreateInline(dvBlob, cardinality);

        return await CreateFileAsync(dvBlob, cardinality, cancellationToken)
            .ConfigureAwait(false);
    }

    private DeletionVector CreateInline(byte[] dvBlob, long cardinality)
    {
        // Pad to multiple of 4 for Z85 encoding
        byte[] padded = dvBlob;
        int remainder = dvBlob.Length % 4;
        if (remainder != 0)
        {
            padded = new byte[dvBlob.Length + (4 - remainder)];
            dvBlob.CopyTo(padded, 0);
        }

        string encoded = Base85.Encode(padded);

        return new DeletionVector
        {
            StorageType = "i",
            PathOrInlineDv = encoded,
            SizeInBytes = dvBlob.Length,
            Cardinality = cardinality,
        };
    }

    private async ValueTask<DeletionVector> CreateFileAsync(
        byte[] dvBlob, long cardinality, CancellationToken cancellationToken)
    {
        string uuid = Guid.NewGuid().ToString();
        string fileName = $"deletion_vector_{uuid}.bin";
        string filePath = $"_delta_log/{fileName}";

        await _fs.WriteAllBytesAsync(filePath, dvBlob, cancellationToken)
            .ConfigureAwait(false);

        // Encode UUID for the path reference
        byte[] uuidBytes = Guid.Parse(uuid).ToByteArray();
        // Pad to 16 bytes (already is)
        string encodedUuid = Base85.Encode(uuidBytes);

        return new DeletionVector
        {
            StorageType = "u",
            PathOrInlineDv = encodedUuid,
            Offset = 0,
            SizeInBytes = dvBlob.Length,
            Cardinality = cardinality,
        };
    }
}
