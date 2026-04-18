// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Configuration options for Delta table operations.
/// </summary>
public sealed record DeltaTableOptions
{
    /// <summary>Default options.</summary>
    public static DeltaTableOptions Default { get; } = new();

    /// <summary>Parquet write options for data files.</summary>
    public ParquetWriteOptions ParquetWriteOptions { get; init; } = ParquetWriteOptions.Default;

    /// <summary>Parquet read options for data files.</summary>
    public ParquetReadOptions ParquetReadOptions { get; init; } = ParquetReadOptions.Default;

    /// <summary>Target size for individual data files in bytes. Default: 128 MB.</summary>
    public long TargetFileSize { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Number of commits between automatic checkpoints.
    /// Set to 0 to disable automatic checkpointing. Default: 10.
    /// </summary>
    public int CheckpointInterval { get; init; } = 10;

    /// <summary>
    /// Default retention period for vacuum operations. Default: 7 days.
    /// </summary>
    public TimeSpan VacuumRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>Whether to collect per-column statistics on write. Default: true.</summary>
    public bool CollectStats { get; init; } = true;
}
