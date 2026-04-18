// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Resolves and reads deletion vectors from inline data or files.
/// </summary>
public sealed class DeletionVectorReader
{
    private readonly ITableFileSystem _fs;

    public DeletionVectorReader(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Reads a deletion vector and returns the set of deleted row indices.
    /// </summary>
    public async ValueTask<HashSet<long>> ReadAsync(
        DeletionVector dv,
        CancellationToken cancellationToken = default)
    {
        byte[] data = dv.StorageType switch
        {
            "i" => ReadInline(dv),
            "u" => await ReadUuidFileAsync(dv, cancellationToken).ConfigureAwait(false),
            "p" => await ReadAbsoluteFileAsync(dv, cancellationToken).ConfigureAwait(false),
            _ => throw new DeltaFormatException(
                $"Unknown deletion vector storage type: '{dv.StorageType}'"),
        };

        return RoaringBitmapReader.Deserialize(data);
    }

    /// <summary>
    /// Reads an inline deletion vector (Base85/Z85-encoded).
    /// </summary>
    private static byte[] ReadInline(DeletionVector dv)
    {
        // The first character of pathOrInlineDv indicates the size encoding
        // For inline DVs, the data is Base85-encoded after a size prefix
        string encoded = dv.PathOrInlineDv;

        if (encoded.Length == 0)
            throw new DeltaFormatException("Empty inline deletion vector.");

        // The first 4 bytes (encoded as 5 Z85 chars) encode the size
        // But in practice, Delta stores the entire DV as Z85-encoded bytes
        return Base85.Decode(encoded);
    }

    /// <summary>
    /// Reads a file-based DV with UUID-relative path.
    /// Path format: {random-prefix}{base85-encoded-uuid}
    /// Resolved to: _delta_log/deletion_vector_{uuid}.bin
    /// </summary>
    private async ValueTask<byte[]> ReadUuidFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        // Decode UUID from the path
        string pathOrUuid = dv.PathOrInlineDv;
        string uuid = DecodeUuidFromPath(pathOrUuid);
        string filePath = $"_delta_log/deletion_vector_{uuid}.bin";

        return await ReadDvFileAsync(filePath, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a file-based DV with an absolute path.
    /// </summary>
    private async ValueTask<byte[]> ReadAbsoluteFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        return await ReadDvFileAsync(dv.PathOrInlineDv, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<byte[]> ReadDvFileAsync(
        string path, int offset, int size,
        CancellationToken cancellationToken)
    {
        byte[] allBytes = await _fs.ReadAllBytesAsync(path, cancellationToken)
            .ConfigureAwait(false);

        if (offset == 0 && size == allBytes.Length)
            return allBytes;

        // Extract the relevant slice
        if (offset + size > allBytes.Length)
            throw new DeltaFormatException(
                $"Deletion vector at {path} offset {offset} size {size} " +
                $"exceeds file length {allBytes.Length}.");

        var result = new byte[size];
        Array.Copy(allBytes, offset, result, 0, size);
        return result;
    }

    /// <summary>
    /// Decodes a UUID from the Z85-encoded path segment.
    /// The path is a Base85-encoded 16-byte UUID.
    /// </summary>
    private static string DecodeUuidFromPath(string encodedPath)
    {
        // The encoded path may have a random prefix before the Z85-encoded UUID
        // Z85-encoded 16 bytes = 20 characters
        if (encodedPath.Length < 20)
            throw new DeltaFormatException(
                $"UUID path too short: '{encodedPath}'");

        // The last 20 characters are the Z85-encoded UUID
        string uuidEncoded = encodedPath[^20..];
        byte[] uuidBytes = Base85.Decode(uuidEncoded);

        if (uuidBytes.Length != 16)
            throw new DeltaFormatException(
                $"Expected 16-byte UUID, got {uuidBytes.Length} bytes.");

        return new Guid(uuidBytes).ToString();
    }
}
