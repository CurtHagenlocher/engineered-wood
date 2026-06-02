// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using EngineeredWood.IO;
using EngineeredWood.IO.Tests;

namespace EngineeredWood.AWS.Tests;

public class S3RandomAccessFileTests : RandomAccessFileTests
{
    private const string EndpointEnv = "S3_ENDPOINT";
    private const string AccessKeyEnv = "S3_ACCESS_KEY_ID";
    private const string SecretKeyEnv = "S3_SECRET_ACCESS_KEY";
    private const string BucketEnv = "S3_TEST_BUCKET";

    private static AmazonS3Config StorageConfig => new()
    {
        ServiceURL = Environment.GetEnvironmentVariable(EndpointEnv),
        ForcePathStyle = true,
    };

    private static AWSCredentials Credentials => new BasicAWSCredentials(
        Environment.GetEnvironmentVariable(AccessKeyEnv),
        Environment.GetEnvironmentVariable(SecretKeyEnv));
    
    private static string Bucket =>
        Environment.GetEnvironmentVariable(BucketEnv) ?? "random-access-file-tests";

    protected override async Task<(IRandomAccessFile File, byte[] Content, Func<Task> Cleanup)>
        CreateFileAsync(byte[] data, string testId)
    {
        AmazonS3Client client = new(Credentials, StorageConfig);

        // Ensure the test bucket exists
        try
        {
            await client.PutBucketAsync(Bucket);
        }
        catch
        {
            // Bucket may already exist; ignore.
        }

        var key = $"{testId}_{Guid.NewGuid():N}";
        using var uploadStream = new MemoryStream(data);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            InputStream = uploadStream,
            AutoCloseStream = false,
            AutoResetStreamPosition = false
        });

        S3RandomAccessFile file = new(client, new AmazonS3Uri($"s3://{Bucket}/{key}"));

        async Task Cleanup()
        {
            await client.DeleteObjectAsync(Bucket, key);
        }

        return (file, data, Cleanup);
    }
}
