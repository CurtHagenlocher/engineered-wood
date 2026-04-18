// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table.Stats;

/// <summary>
/// Collects per-column statistics (numRecords, min/max values, null counts)
/// from Arrow <see cref="RecordBatch"/> data for use in Delta Lake file actions.
/// </summary>
internal static class StatsCollector
{
    /// <summary>
    /// Collects column statistics from a RecordBatch and returns
    /// a JSON-encoded stats string.
    /// </summary>
    public static string? Collect(RecordBatch batch) =>
        Collect([batch]);

    /// <summary>
    /// Collects column statistics aggregated across multiple RecordBatches
    /// and returns a JSON-encoded stats string. Used by compaction when
    /// combining multiple batches into a single output file.
    /// </summary>
    public static string? Collect(IReadOnlyList<RecordBatch> batches)
    {
        long totalRows = 0;
        var minValues = new Dictionary<string, object?>();
        var maxValues = new Dictionary<string, object?>();
        var nullCounts = new Dictionary<string, long>();

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            totalRows += batch.Length;

            for (int col = 0; col < batch.ColumnCount; col++)
            {
                var field = batch.Schema.FieldsList[col];
                var array = batch.Column(col);

                // Sum null counts across batches
                nullCounts.TryGetValue(field.Name, out long existing);
                nullCounts[field.Name] = existing + array.NullCount;

                // Merge min/max across batches
                CollectMinMax(field.Name, array, minValues, maxValues);
            }
        }

        if (totalRows == 0)
            return null;

        return SerializeStats(totalRows, minValues, maxValues, nullCounts);
    }

    private static string SerializeStats(
        long numRecords,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Dictionary<string, long> nullCounts)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();
        writer.WriteNumber("numRecords", numRecords);

        // Write minValues
        writer.WritePropertyName("minValues");
        writer.WriteStartObject();
        foreach (var kvp in minValues)
        {
            if (kvp.Value is not null)
            {
                writer.WritePropertyName(kvp.Key);
                WriteStatValue(writer, kvp.Value);
            }
        }
        writer.WriteEndObject();

        // Write maxValues
        writer.WritePropertyName("maxValues");
        writer.WriteStartObject();
        foreach (var kvp in maxValues)
        {
            if (kvp.Value is not null)
            {
                writer.WritePropertyName(kvp.Key);
                WriteStatValue(writer, kvp.Value);
            }
        }
        writer.WriteEndObject();

        // Write nullCount
        writer.WritePropertyName("nullCount");
        writer.WriteStartObject();
        foreach (var kvp in nullCounts)
            writer.WriteNumber(kvp.Key, kvp.Value);
        writer.WriteEndObject();

        writer.WriteEndObject();
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void CollectMinMax(
        string name, IArrowArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        switch (array)
        {
            case Int64Array int64:
                CollectNumericMinMax(name, int64, minValues, maxValues);
                break;
            case Int32Array int32:
                CollectNumericMinMax(name, int32, minValues, maxValues);
                break;
            case Int16Array int16:
                CollectNumericMinMax(name, int16, minValues, maxValues);
                break;
            case Int8Array int8:
                CollectNumericMinMax(name, int8, minValues, maxValues);
                break;
            case DoubleArray dbl:
                CollectNumericMinMax(name, dbl, minValues, maxValues);
                break;
            case FloatArray flt:
                CollectNumericMinMax(name, flt, minValues, maxValues);
                break;
            case StringArray str:
                CollectStringMinMax(name, str, minValues, maxValues);
                break;
            case LargeStringArray lstr:
                CollectLargeStringMinMax(name, lstr, minValues, maxValues);
                break;
            case BooleanArray bln:
                CollectBooleanMinMax(name, bln, minValues, maxValues);
                break;
            case Date32Array d32:
                CollectNumericMinMax(name, d32, minValues, maxValues);
                break;
            case TimestampArray ts:
                CollectTimestampMinMax(name, ts, minValues, maxValues);
                break;
            // For complex types (struct, list, map), we skip min/max
        }
    }

    private static void CollectNumericMinMax<T>(
        string name, PrimitiveArray<T> array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
        where T : struct, IEquatable<T>, IComparable<T>
    {
        T? min = null;
        T? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            T val = array.GetValue(i)!.Value;

            if (min is null || val.CompareTo(min.Value) < 0) min = val;
            if (max is null || val.CompareTo(max.Value) > 0) max = val;
        }

        // Merge with existing values from previous batches
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var existingMin) && existingMin is T em)
                min = min.Value.CompareTo(em) < 0 ? min : em;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var existingMax) && existingMax is T ex)
                max = max.Value.CompareTo(ex) > 0 ? max : ex;
            maxValues[name] = max;
        }
    }

    private static void CollectStringMinMax(
        string name, StringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        string? min = null;
        string? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            string val = array.GetString(i);

            if (min is null || string.Compare(val, min, StringComparison.Ordinal) < 0) min = val;
            if (max is null || string.Compare(val, max, StringComparison.Ordinal) > 0) max = val;
        }

        MergeStringMinMax(name, min, max, minValues, maxValues);
    }

    private static void CollectLargeStringMinMax(
        string name, LargeStringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        string? min = null;
        string? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            string val = array.GetString(i);

            if (min is null || string.Compare(val, min, StringComparison.Ordinal) < 0) min = val;
            if (max is null || string.Compare(val, max, StringComparison.Ordinal) > 0) max = val;
        }

        MergeStringMinMax(name, min, max, minValues, maxValues);
    }

    private static void MergeStringMinMax(
        string name, string? min, string? max,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var em) && em is string es)
                min = string.Compare(min, es, StringComparison.Ordinal) < 0 ? min : es;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var ex) && ex is string xs)
                max = string.Compare(max, xs, StringComparison.Ordinal) > 0 ? max : xs;
            maxValues[name] = max;
        }
    }

    private static void CollectBooleanMinMax(
        string name, BooleanArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        bool? min = null;
        bool? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            bool val = array.GetValue(i)!.Value;

            if (min is null || (!val && min.Value)) min = val;
            if (max is null || (val && !max.Value)) max = val;
        }

        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var em) && em is bool eb)
                min = !min.Value && !eb ? false : min.Value && eb ? true : min;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var ex) && ex is bool xb)
                max = max.Value || xb;
            maxValues[name] = max;
        }
    }

    private static void CollectTimestampMinMax(
        string name, TimestampArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        // TimestampArray stores values as long (microseconds since epoch)
        long? min = null;
        long? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            long val = array.GetValue(i)!.Value;

            if (min is null || val < min.Value) min = val;
            if (max is null || val > max.Value) max = val;
        }

        // Delta stats stores timestamps as ISO 8601 strings
        if (min.HasValue)
        {
            var tsType = (Apache.Arrow.Types.TimestampType)array.Data.DataType;
            string minStr = FormatTimestamp(min.Value, tsType);
            string maxStr = FormatTimestamp(max!.Value, tsType);
            MergeStringMinMax(name, minStr, maxStr, minValues, maxValues);
        }
    }

    private static string FormatTimestamp(long value, Apache.Arrow.Types.TimestampType tsType)
    {
        // Convert to microseconds
        long micros = tsType.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => value * 1_000_000,
            Apache.Arrow.Types.TimeUnit.Millisecond => value * 1_000,
            Apache.Arrow.Types.TimeUnit.Microsecond => value,
            Apache.Arrow.Types.TimeUnit.Nanosecond => value / 1_000,
            _ => value,
        };

        var dto = DateTimeOffset.FromUnixTimeMilliseconds(micros / 1_000)
            .AddTicks((micros % 1_000) * 10);

        return tsType.Timezone is not null
            ? dto.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")
            : dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff");
    }

    private static void WriteStatValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case long l: writer.WriteNumberValue(l); break;
            case int i: writer.WriteNumberValue(i); break;
            case short s: writer.WriteNumberValue(s); break;
            case sbyte sb: writer.WriteNumberValue(sb); break;
            case double d: writer.WriteNumberValue(d); break;
            case float f: writer.WriteNumberValue(f); break;
            case string str: writer.WriteStringValue(str); break;
            case bool b: writer.WriteBooleanValue(b); break;
            default: writer.WriteNullValue(); break;
        }
    }
}
