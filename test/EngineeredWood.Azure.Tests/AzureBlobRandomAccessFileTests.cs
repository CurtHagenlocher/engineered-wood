// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Tests;

namespace EngineeredWood.Azure.Tests;

public class AzureBlobRandomAccessFileTests : RandomAccessFileTests
{
    private const string ConnectionStringEnv = "AZURE_STORAGE_CONNECTION_STRING";
    private const string DefaultConnectionString = "UseDevelopmentStorage=true";

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable(ConnectionStringEnv) ?? DefaultConnectionString;

    protected override async Task<(IRandomAccessFile File, byte[] Content, Func<Task> Cleanup)>
        CreateFileAsync(byte[] data, string testId)
    {
        BlobContainerClient containerClient = new(ConnectionString, "random-access-file-tests");
        await containerClient.CreateIfNotExistsAsync();

        var blobName = $"{testId}_{Guid.NewGuid():N}";
        BlobClient blobClient = new(ConnectionString, containerClient.Name, blobName);
        using MemoryStream uploadStream = new(data);
        await blobClient.UploadAsync(uploadStream);

        AzureBlobRandomAccessFile file = new(blobClient);

        async Task Cleanup()
        {
            await blobClient.DeleteIfExistsAsync();
        }

        return (file, data, Cleanup);
    }
}
