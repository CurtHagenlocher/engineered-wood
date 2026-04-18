// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Utilities for in-commit timestamp support.
/// When enabled, the <c>commitInfo</c> action must be the first action
/// in every commit and must include an <c>inCommitTimestamp</c> field
/// (milliseconds since epoch).
/// </summary>
public static class InCommitTimestamp
{
    /// <summary>Table configuration key to enable in-commit timestamps.</summary>
    public const string EnableKey = "delta.enableInCommitTimestamps";

    /// <summary>
    /// Returns true if in-commit timestamps are enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null)
            return false;

        return configuration.TryGetValue(EnableKey, out string? value) &&
               string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Creates a <see cref="CommitInfo"/> action with the <c>inCommitTimestamp</c> field
    /// set to the current time.
    /// </summary>
    public static CommitInfo CreateCommitInfo(
        string operation = "WRITE",
        IDictionary<string, JsonElement>? additionalValues = null)
    {
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return CreateCommitInfo(timestamp, operation, additionalValues);
    }

    /// <summary>
    /// Creates a <see cref="CommitInfo"/> action with the specified timestamp.
    /// </summary>
    public static CommitInfo CreateCommitInfo(
        long inCommitTimestamp,
        string operation = "WRITE",
        IDictionary<string, JsonElement>? additionalValues = null)
    {
        var values = new Dictionary<string, JsonElement>();

        if (additionalValues is not null)
        {
            foreach (var kvp in additionalValues)
                values[kvp.Key] = kvp.Value;
        }

        values["timestamp"] = JsonDocument.Parse(
            inCommitTimestamp.ToString()).RootElement.Clone();
        values["operation"] = JsonDocument.Parse(
            $"\"{operation}\"").RootElement.Clone();
        values["inCommitTimestamp"] = JsonDocument.Parse(
            inCommitTimestamp.ToString()).RootElement.Clone();

        return new CommitInfo { Values = values };
    }

    /// <summary>
    /// Extracts the <c>inCommitTimestamp</c> from a <see cref="CommitInfo"/> action.
    /// Returns null if not present.
    /// </summary>
    public static long? GetTimestamp(CommitInfo commitInfo)
    {
        var ts = commitInfo.GetValue("inCommitTimestamp");
        return ts?.ValueKind == JsonValueKind.Number ? ts.Value.GetInt64() : null;
    }

    /// <summary>
    /// Extracts the <c>inCommitTimestamp</c> from a list of actions.
    /// The <c>commitInfo</c> should be the first action when in-commit timestamps are enabled.
    /// </summary>
    public static long? GetTimestampFromActions(IReadOnlyList<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is CommitInfo ci)
            {
                var ts = GetTimestamp(ci);
                if (ts.HasValue)
                    return ts;
            }
        }

        return null;
    }

    /// <summary>
    /// Prepends a <see cref="CommitInfo"/> with <c>inCommitTimestamp</c> to an action list
    /// if the table has in-commit timestamps enabled and the list doesn't already have one.
    /// </summary>
    public static IReadOnlyList<DeltaAction> EnsureCommitInfo(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<string, string>? configuration,
        string operation = "WRITE")
    {
        if (!IsEnabled(configuration))
            return actions;

        // Check if there's already a CommitInfo with inCommitTimestamp
        if (GetTimestampFromActions(actions).HasValue)
            return actions;

        // Prepend CommitInfo as the first action
        var result = new List<DeltaAction>(actions.Count + 1);
        result.Add(CreateCommitInfo(operation));
        result.AddRange(actions);
        return result;
    }
}
