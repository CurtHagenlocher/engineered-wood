// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Per-file column-level statistics stored in the <c>stats</c> field
/// of <see cref="AddFile"/> actions.
/// </summary>
public sealed record ColumnStats
{
    /// <summary>Total number of records in the file.</summary>
    public long NumRecords { get; init; }

    /// <summary>Minimum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MinValues { get; init; }

    /// <summary>Maximum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MaxValues { get; init; }

    /// <summary>Null count per column (column name → count).</summary>
    public IReadOnlyDictionary<string, long>? NullCount { get; init; }

    /// <summary>
    /// Parses a Delta stats JSON string (the value of <see cref="AddFile.Stats"/>)
    /// into a <see cref="ColumnStats"/>. Returns <c>null</c> if the input is
    /// null, empty, or doesn't parse.
    /// </summary>
    public static ColumnStats? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json!);
            var root = doc.RootElement;

            long numRecords = 0;
            if (root.TryGetProperty("numRecords", out var nr)
                && nr.ValueKind == JsonValueKind.Number)
                numRecords = nr.GetInt64();

            return new ColumnStats
            {
                NumRecords = numRecords,
                MinValues = ReadValueMap(root, "minValues"),
                MaxValues = ReadValueMap(root, "maxValues"),
                NullCount = ReadCountMap(root, "nullCount"),
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static IReadOnlyDictionary<string, JsonElement>? ReadValueMap(
        JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var element)
            || element.ValueKind != JsonValueKind.Object)
            return null;

        var map = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var prop in element.EnumerateObject())
            map[prop.Name] = prop.Value.Clone();
        return map;
    }

    private static IReadOnlyDictionary<string, long>? ReadCountMap(
        JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var element)
            || element.ValueKind != JsonValueKind.Object)
            return null;

        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var prop in element.EnumerateObject())
        {
            if (prop.Value.ValueKind == JsonValueKind.Number)
                map[prop.Name] = prop.Value.GetInt64();
        }
        return map;
    }
}
