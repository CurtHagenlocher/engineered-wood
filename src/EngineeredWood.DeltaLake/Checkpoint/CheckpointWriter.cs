// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Writes Delta Lake checkpoint files (Parquet format) from a snapshot.
/// Uses the standard struct-based checkpoint schema expected by all
/// Delta Lake implementations.
/// </summary>
public sealed class CheckpointWriter
{
    private readonly ITableFileSystem _fs;
    private readonly ParquetWriteOptions? _parquetOptions;

    public CheckpointWriter(
        ITableFileSystem fileSystem,
        ParquetWriteOptions? parquetOptions = null)
    {
        _fs = fileSystem;
        _parquetOptions = parquetOptions;
    }

    /// <summary>
    /// Writes a checkpoint Parquet file for the given snapshot,
    /// then updates <c>_last_checkpoint</c>.
    /// </summary>
    public async ValueTask WriteCheckpointAsync(
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        string path = DeltaVersion.CheckpointPath(snapshot.Version);

        var batch = BuildCheckpointBatch(snapshot, out long actionCount);

        await using (var file = await _fs.CreateAsync(path, overwrite: true, cancellationToken)
            .ConfigureAwait(false))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false, _parquetOptions);
            await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        // Write _last_checkpoint
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(new
        {
            version = snapshot.Version,
            size = actionCount,
        });

        await _fs.WriteAllBytesAsync(
            DeltaVersion.LastCheckpointPath, json, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a checkpoint RecordBatch. Used by V2CheckpointWriter for sidecar files.
    /// </summary>
    internal static RecordBatch BuildCheckpointBatchPublic(
        Snapshot.Snapshot snapshot, out long actionCount) =>
        BuildCheckpointBatch(snapshot, out actionCount);

    private static RecordBatch BuildCheckpointBatch(
        Snapshot.Snapshot snapshot, out long actionCount)
    {
        // Collect all actions: 1 protocol + 1 metadata + N adds + N txns + N domainMetadata
        var allActions = new List<DeltaAction>();
        allActions.Add(snapshot.Protocol);
        allActions.Add(snapshot.Metadata);

        foreach (var add in snapshot.ActiveFiles.Values)
            allActions.Add(add);

        foreach (var txn in snapshot.AppTransactions.Values)
            allActions.Add(txn);

        foreach (var dm in snapshot.DomainMetadata.Values)
            allActions.Add(dm);

        actionCount = allActions.Count;
        int count = allActions.Count;

        // Build the struct-based checkpoint schema
        var schema = BuildCheckpointSchema(snapshot.Schema);

        // Build struct arrays for each action type
        var protocolArray = BuildProtocolColumn(allActions, count);
        var metadataArray = BuildMetadataColumn(allActions, count);
        var addArray = BuildAddColumn(allActions, count);
        var removeArray = BuildRemoveColumn(count); // No removes in checkpoints
        var txnArray = BuildTxnColumn(allActions, count);
        var domainMetadataArray = BuildDomainMetadataColumn(allActions, count);

        // Build stats_parsed column from JSON stats in add actions
        var statsParsedArray = StatsParsedBuilder.BuildStatsColumn(
            allActions, count, snapshot.Schema);

        return new RecordBatch(schema,
            [protocolArray, metadataArray, addArray, removeArray, txnArray,
             domainMetadataArray, statsParsedArray],
            count);
    }

    #region Schema Definition

    private static Apache.Arrow.Schema BuildCheckpointSchema(
        Schema.StructType deltaSchema)
    {
        // Protocol struct
        var protocolType = new ArrowStructType(new List<Field>
        {
            new Field("minReaderVersion", Int32Type.Default, true),
            new Field("minWriterVersion", Int32Type.Default, true),
        });

        // Format struct for metaData
        var formatType = new ArrowStructType(new List<Field>
        {
            new Field("provider", StringType.Default, true),
        });

        // MetaData struct
        var metadataType = new ArrowStructType(new List<Field>
        {
            new Field("id", StringType.Default, true),
            new Field("name", StringType.Default, true),
            new Field("description", StringType.Default, true),
            new Field("format", formatType, true),
            new Field("schemaString", StringType.Default, true),
            new Field("partitionColumns", new ListType(new Field("element", StringType.Default, false)), true),
            new Field("createdTime", Int64Type.Default, true),
            new Field("configuration", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
        });

        // Add struct
        var addType = new ArrowStructType(new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("stats", StringType.Default, true),
        });

        // Remove struct
        var removeType = new ArrowStructType(new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("deletionTimestamp", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
        });

        // Txn struct
        var txnType = new ArrowStructType(new List<Field>
        {
            new Field("appId", StringType.Default, true),
            new Field("version", Int64Type.Default, true),
            new Field("lastUpdated", Int64Type.Default, true),
        });

        // DomainMetadata struct
        var domainMetadataType = new ArrowStructType(new List<Field>
        {
            new Field("domain", StringType.Default, true),
            new Field("configuration", StringType.Default, true),
            new Field("removed", BooleanType.Default, true),
        });

        var statsParsedType = StatsParsedBuilder.BuildStatsType(deltaSchema);

        return new Apache.Arrow.Schema.Builder()
            .Field(new Field("protocol", protocolType, true))
            .Field(new Field("metaData", metadataType, true))
            .Field(new Field("add", addType, true))
            .Field(new Field("remove", removeType, true))
            .Field(new Field("txn", txnType, true))
            .Field(new Field("domainMetadata", domainMetadataType, true))
            .Field(new Field("stats_parsed", statsParsedType, true))
            .Build();
    }

    #endregion

    #region Array Builders

    private static StructArray BuildProtocolColumn(List<DeltaAction> actions, int count)
    {
        // Use nullable inner fields; non-protocol rows have null inner values.
        // Struct itself is always non-null to avoid Parquet nested null issues.
        var minReaderBuilder = new Int32Array.Builder();
        var minWriterBuilder = new Int32Array.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is ProtocolAction p)
            {
                minReaderBuilder.Append(p.MinReaderVersion);
                minWriterBuilder.Append(p.MinWriterVersion);
            }
            else
            {
                minReaderBuilder.AppendNull();
                minWriterBuilder.AppendNull();
            }
        }

        var fields = new List<Field>
        {
            new Field("minReaderVersion", Int32Type.Default, true),
            new Field("minWriterVersion", Int32Type.Default, true),
        };

        return new StructArray(
            new ArrowStructType(fields),
            count,
            [minReaderBuilder.Build(), minWriterBuilder.Build()],
            ArrowBuffer.Empty, 0);
    }

    private static StructArray BuildMetadataColumn(List<DeltaAction> actions, int count)
    {
        var idBuilder = new StringArray.Builder();
        var nameBuilder = new StringArray.Builder();
        var descBuilder = new StringArray.Builder();
        var schemaStringBuilder = new StringArray.Builder();
        var createdTimeBuilder = new Int64Array.Builder();

        // Format struct arrays
        var formatProviderBuilder = new StringArray.Builder();

        // partitionColumns list
        var partColOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var partColValues = new StringArray.Builder();
        partColOffsetsBuilder.Append(0);
        int partColOffset = 0;

        // configuration map
        var configOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var configKeys = new StringArray.Builder();
        var configValues = new StringArray.Builder();
        configOffsetsBuilder.Append(0);
        int configOffset = 0;

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is MetadataAction m)
            {
                idBuilder.Append(m.Id);
                nameBuilder.Append(m.Name ?? "");
                descBuilder.Append(m.Description ?? "");
                schemaStringBuilder.Append(m.SchemaString);
                createdTimeBuilder.Append(m.CreatedTime ?? 0);
                formatProviderBuilder.Append(m.Format.Provider);

                foreach (string col in m.PartitionColumns)
                {
                    partColValues.Append(col);
                    partColOffset++;
                }
                partColOffsetsBuilder.Append(partColOffset);

                if (m.Configuration is not null)
                {
                    foreach (var kvp in m.Configuration)
                    {
                        configKeys.Append(kvp.Key);
                        configValues.Append(kvp.Value);
                        configOffset++;
                    }
                }
                configOffsetsBuilder.Append(configOffset);
            }
            else
            {
                idBuilder.AppendNull();
                nameBuilder.AppendNull();
                descBuilder.AppendNull();
                schemaStringBuilder.AppendNull();
                createdTimeBuilder.AppendNull();
                formatProviderBuilder.AppendNull();
                partColOffsetsBuilder.Append(partColOffset);
                configOffsetsBuilder.Append(configOffset);
            }
        }

        var formatFields = new List<Field>
        {
            new Field("provider", StringType.Default, true),
        };
        var formatStruct = new StructArray(
            new ArrowStructType(formatFields),
            count,
            [formatProviderBuilder.Build()],
            ArrowBuffer.Empty, 0);

        var partColList = new ListArray(
            new ListType(new Field("element", StringType.Default, false)),
            count,
            partColOffsetsBuilder.Build(),
            partColValues.Build(),
            ArrowBuffer.Empty);

        var configMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var configKeysArray = configKeys.Build();
        var configValuesArray = configValues.Build();
        var configEntries = new StructArray(
            new ArrowStructType(new List<Field> { configMapType.KeyField, configMapType.ValueField }),
            configKeysArray.Length,
            new IArrowArray[] { configKeysArray, configValuesArray },
            ArrowBuffer.Empty);
        var configMap = new MapArray(configMapType, count,
            configOffsetsBuilder.Build(), configEntries, ArrowBuffer.Empty, 0);

        var fields = new List<Field>
        {
            new Field("id", StringType.Default, true),
            new Field("name", StringType.Default, true),
            new Field("description", StringType.Default, true),
            new Field("format", new ArrowStructType(formatFields), true),
            new Field("schemaString", StringType.Default, true),
            new Field("partitionColumns", new ListType(new Field("element", StringType.Default, false)), true),
            new Field("createdTime", Int64Type.Default, true),
            new Field("configuration", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
        };

        return new StructArray(
            new ArrowStructType(fields),
            count,
            [idBuilder.Build(), nameBuilder.Build(), descBuilder.Build(),
             formatStruct, schemaStringBuilder.Build(), partColList,
             createdTimeBuilder.Build(), configMap],
            ArrowBuffer.Empty, 0);
    }

    private static StructArray BuildAddColumn(List<DeltaAction> actions, int count)
    {
        var pathBuilder = new StringArray.Builder();
        var sizeBuilder = new Int64Array.Builder();
        var modTimeBuilder = new Int64Array.Builder();
        var dataChangeBuilder = new BooleanArray.Builder();
        var statsBuilder = new StringArray.Builder();

        // partitionValues map
        var pvOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var pvKeys = new StringArray.Builder();
        var pvValues = new StringArray.Builder();
        pvOffsetsBuilder.Append(0);
        int pvOffset = 0;

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is AddFile a)
            {
                pathBuilder.Append(a.Path);
                sizeBuilder.Append(a.Size);
                modTimeBuilder.Append(a.ModificationTime);
                dataChangeBuilder.Append(a.DataChange);
                if (a.Stats is not null)
                    statsBuilder.Append(a.Stats);
                else
                    statsBuilder.AppendNull();

                foreach (var kvp in a.PartitionValues)
                {
                    pvKeys.Append(kvp.Key);
                    pvValues.Append(kvp.Value);
                    pvOffset++;
                }
                pvOffsetsBuilder.Append(pvOffset);
            }
            else
            {
                pathBuilder.AppendNull();
                sizeBuilder.AppendNull();
                modTimeBuilder.AppendNull();
                dataChangeBuilder.AppendNull();
                statsBuilder.AppendNull();
                pvOffsetsBuilder.Append(pvOffset);
            }
        }

        var pvMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var pvKeysArray = pvKeys.Build();
        var pvValuesArray = pvValues.Build();
        var pvEntries = new StructArray(
            new ArrowStructType(new List<Field> { pvMapType.KeyField, pvMapType.ValueField }),
            pvKeysArray.Length,
            new IArrowArray[] { pvKeysArray, pvValuesArray },
            ArrowBuffer.Empty);
        var pvMap = new MapArray(pvMapType, count,
            pvOffsetsBuilder.Build(), pvEntries, ArrowBuffer.Empty, 0);

        var fields = new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("stats", StringType.Default, true),
        };

        return new StructArray(
            new ArrowStructType(fields),
            count,
            [pathBuilder.Build(), pvMap, sizeBuilder.Build(),
             modTimeBuilder.Build(), dataChangeBuilder.Build(), statsBuilder.Build()],
            ArrowBuffer.Empty, 0);
    }

    private static StructArray BuildRemoveColumn(int count)
    {
        // Checkpoints don't contain remove actions (they're reconciled away)
        var pathBuilder = new StringArray.Builder();
        var tsBuilder = new Int64Array.Builder();
        var dcBuilder = new BooleanArray.Builder();

        for (int i = 0; i < count; i++)
        {
            pathBuilder.AppendNull();
            tsBuilder.AppendNull();
            dcBuilder.AppendNull();
        }

        var fields = new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("deletionTimestamp", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
        };

        return new StructArray(
            new ArrowStructType(fields), count,
            [pathBuilder.Build(), tsBuilder.Build(), dcBuilder.Build()],
            ArrowBuffer.Empty, 0);
    }

    private static StructArray BuildTxnColumn(List<DeltaAction> actions, int count)
    {
        var appIdBuilder = new StringArray.Builder();
        var versionBuilder = new Int64Array.Builder();
        var lastUpdatedBuilder = new Int64Array.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is TransactionId t)
            {
                appIdBuilder.Append(t.AppId);
                versionBuilder.Append(t.Version);
                lastUpdatedBuilder.Append(t.LastUpdated ?? 0);
            }
            else
            {
                appIdBuilder.AppendNull();
                versionBuilder.AppendNull();
                lastUpdatedBuilder.AppendNull();
            }
        }

        var fields = new List<Field>
        {
            new Field("appId", StringType.Default, true),
            new Field("version", Int64Type.Default, true),
            new Field("lastUpdated", Int64Type.Default, true),
        };

        return new StructArray(
            new ArrowStructType(fields), count,
            [appIdBuilder.Build(), versionBuilder.Build(), lastUpdatedBuilder.Build()],
            ArrowBuffer.Empty, 0);
    }

    private static StructArray BuildDomainMetadataColumn(List<DeltaAction> actions, int count)
    {
        var domainBuilder = new StringArray.Builder();
        var configBuilder = new StringArray.Builder();
        var removedBuilder = new BooleanArray.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is Actions.DomainMetadata dm)
            {
                domainBuilder.Append(dm.Domain);
                configBuilder.Append(dm.Configuration);
                removedBuilder.Append(dm.Removed);
            }
            else
            {
                domainBuilder.AppendNull();
                configBuilder.AppendNull();
                removedBuilder.AppendNull();
            }
        }

        var fields = new List<Field>
        {
            new Field("domain", StringType.Default, true),
            new Field("configuration", StringType.Default, true),
            new Field("removed", BooleanType.Default, true),
        };

        return new StructArray(
            new ArrowStructType(fields), count,
            [domainBuilder.Build(), configBuilder.Build(), removedBuilder.Build()],
            ArrowBuffer.Empty, 0);
    }

    #endregion
}
