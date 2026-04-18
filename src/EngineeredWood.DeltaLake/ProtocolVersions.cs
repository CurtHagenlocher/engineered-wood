// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Defines the protocol versions and features supported by this implementation.
/// </summary>
public static class ProtocolVersions
{
    /// <summary>Maximum reader protocol version we can handle.</summary>
    public const int MaxReaderVersion = 3;

    /// <summary>Maximum writer protocol version we can produce.</summary>
    public const int MaxWriterVersion = 7;

    /// <summary>Named reader features supported by this implementation.</summary>
    private static readonly HashSet<string> SupportedReaderFeatures = new(StringComparer.Ordinal)
    {
        "columnMapping",
        "deletionVectors",
        "timestampNtz",
        "typeWidening",
        "v2Checkpoint",
        "vacuumProtocolCheck",
    };

    /// <summary>Named writer features supported by this implementation.</summary>
    private static readonly HashSet<string> SupportedWriterFeatures = new(StringComparer.Ordinal)
    {
        "changeDataFeed",
        "columnMapping",
        "deletionVectors",
        "domainMetadata",
        "icebergCompatV1",
        "icebergCompatV2",
        "identityColumns",
        "inCommitTimestamp",
        "rowTracking",
        "timestampNtz",
        "typeWidening",
        "v2Checkpoint",
        "vacuumProtocolCheck",
    };

    /// <summary>
    /// Validates that vacuum is safe to run on this table.
    /// When the <c>vacuumProtocolCheck</c> feature is present, all reader
    /// and writer features must be supported — otherwise vacuum might
    /// delete files that an unrecognized feature depends on.
    /// </summary>
    public static void ValidateVacuumSupport(Actions.ProtocolAction protocol)
    {
        // Always validate that we can read the table
        ValidateReadSupport(protocol);

        // If vacuumProtocolCheck is enabled, we must also understand all writer features
        bool hasVacuumCheck = false;

        if (protocol.ReaderFeatures is not null)
            hasVacuumCheck |= protocol.ReaderFeatures.Contains("vacuumProtocolCheck");
        if (protocol.WriterFeatures is not null)
            hasVacuumCheck |= protocol.WriterFeatures.Contains("vacuumProtocolCheck");

        if (hasVacuumCheck)
        {
            // Must support ALL features, not just reader features
            ValidateWriteSupport(protocol);
        }
    }

    /// <summary>
    /// Validates that the protocol can be read by this implementation.
    /// Throws <see cref="DeltaFormatException"/> if the protocol requires
    /// features this implementation does not support.
    /// </summary>
    public static void ValidateReadSupport(Actions.ProtocolAction protocol)
    {
        if (protocol.MinReaderVersion > MaxReaderVersion)
        {
            throw new DeltaFormatException(
                $"This table requires reader version {protocol.MinReaderVersion}, " +
                $"but this implementation only supports up to reader version {MaxReaderVersion}.");
        }

        // Reader v3 uses named features — check each one
        if (protocol.ReaderFeatures is { Count: > 0 })
        {
            var unsupported = protocol.ReaderFeatures
                .Where(f => !SupportedReaderFeatures.Contains(f))
                .ToList();

            if (unsupported.Count > 0)
            {
                throw new DeltaFormatException(
                    $"This table requires unsupported reader features: [{string.Join(", ", unsupported)}].");
            }
        }
    }

    /// <summary>
    /// Validates that the protocol allows writing by this implementation.
    /// Throws <see cref="DeltaFormatException"/> if the protocol requires
    /// writer features this implementation does not support.
    /// </summary>
    public static void ValidateWriteSupport(Actions.ProtocolAction protocol)
    {
        ValidateReadSupport(protocol);

        if (protocol.MinWriterVersion > MaxWriterVersion)
        {
            throw new DeltaFormatException(
                $"This table requires writer version {protocol.MinWriterVersion}, " +
                $"but this implementation only supports up to writer version {MaxWriterVersion}.");
        }

        // Writer v7 uses named features — check each one
        if (protocol.WriterFeatures is { Count: > 0 })
        {
            var unsupported = protocol.WriterFeatures
                .Where(f => !SupportedWriterFeatures.Contains(f))
                .ToList();

            if (unsupported.Count > 0)
            {
                throw new DeltaFormatException(
                    $"This table requires unsupported writer features: [{string.Join(", ", unsupported)}].");
            }
        }
    }
}
