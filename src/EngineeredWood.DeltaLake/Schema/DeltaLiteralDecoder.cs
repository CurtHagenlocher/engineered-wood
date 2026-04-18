// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Decodes JSON values from <see cref="Actions.AddFile.Stats"/> and string
/// values from <see cref="Actions.AddFile.PartitionValues"/> into
/// <see cref="LiteralValue"/>, using the Delta primitive type name to choose
/// the encoding.
/// </summary>
internal static class DeltaLiteralDecoder
{
    /// <summary>
    /// Decodes a JSON element from a stats <c>minValues</c>/<c>maxValues</c>
    /// map. Returns null when the element is null, the type is unknown, or
    /// decoding fails (treated as Unknown by the evaluator).
    /// </summary>
    public static LiteralValue? FromJson(JsonElement value, string typeName)
    {
        if (value.ValueKind == JsonValueKind.Null)
            return null;

        try
        {
            switch (typeName)
            {
                case "long":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetInt64()) : null;
                case "integer":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetInt32()) : null;
                case "short":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of((int)value.GetInt16()) : null;
                case "byte":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of((int)value.GetSByte()) : null;
                case "float":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetSingle()) : null;
                case "double":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetDouble()) : null;
                case "boolean":
                    return value.ValueKind switch
                    {
                        JsonValueKind.True => (LiteralValue?)LiteralValue.Of(true),
                        JsonValueKind.False => (LiteralValue?)LiteralValue.Of(false),
                        _ => null,
                    };
                case "string":
                    return value.ValueKind == JsonValueKind.String
                        ? (LiteralValue?)LiteralValue.Of(value.GetString()!) : null;
                case "binary":
                    // Delta stats for binary columns are uncommon; if present, decode as base64.
                    if (value.ValueKind != JsonValueKind.String) return null;
                    try { return LiteralValue.Of(Convert.FromBase64String(value.GetString()!)); }
                    catch { return null; }
                case "date":
                    return value.ValueKind == JsonValueKind.String
                        ? ParseDate(value.GetString()!) : null;
                case "timestamp":
                case "timestamp_ntz":
                    return value.ValueKind == JsonValueKind.String
                        ? ParseTimestamp(value.GetString()!) : null;
                default:
                    if (typeName.StartsWith("decimal(", StringComparison.Ordinal))
                        return ParseDecimal(value, typeName);
                    return null;
            }
        }
        catch (FormatException) { return null; }
        catch (InvalidOperationException) { return null; }
        catch (OverflowException) { return null; }
    }

    /// <summary>
    /// Decodes a partition-column string value (from
    /// <see cref="Actions.AddFile.PartitionValues"/>) per the column's Delta
    /// type. Partition values are always serialized as strings; null partitions
    /// are conventionally <c>null</c> in the dictionary value.
    /// </summary>
    public static LiteralValue? FromPartitionString(string? value, string typeName)
    {
        if (value is null) return LiteralValue.Null;

        try
        {
            switch (typeName)
            {
                case "long":
                    return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l)
                        ? (LiteralValue?)LiteralValue.Of(l) : null;
                case "integer":
                    return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int i)
                        ? (LiteralValue?)LiteralValue.Of(i) : null;
                case "short":
                    return short.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out short s)
                        ? (LiteralValue?)LiteralValue.Of((int)s) : null;
                case "byte":
                    return sbyte.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out sbyte b)
                        ? (LiteralValue?)LiteralValue.Of((int)b) : null;
                case "float":
                    return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float f)
                        ? (LiteralValue?)LiteralValue.Of(f) : null;
                case "double":
                    return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)
                        ? (LiteralValue?)LiteralValue.Of(d) : null;
                case "boolean":
                    return bool.TryParse(value, out bool bo)
                        ? (LiteralValue?)LiteralValue.Of(bo) : null;
                case "string":
                    return LiteralValue.Of(value);
                case "date":
                    return ParseDate(value);
                case "timestamp":
                case "timestamp_ntz":
                    return ParseTimestamp(value);
                default:
                    if (typeName.StartsWith("decimal(", StringComparison.Ordinal))
                    {
                        var (precision, scale) = ParseDecimalSpec(typeName);
                        return ParseDecimalString(value, scale);
                    }
                    return null;
            }
        }
        catch (FormatException) { return null; }
        catch (OverflowException) { return null; }
    }

    private static LiteralValue? ParseDate(string s) =>
        DateTimeOffset.TryParseExact(s, "yyyy-MM-dd",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto)
            ? (LiteralValue?)LiteralValue.Of(dto)
            : (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out dto)
                ? (LiteralValue?)LiteralValue.Of(dto) : null);

    private static LiteralValue? ParseTimestamp(string s) =>
        DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto)
            ? (LiteralValue?)LiteralValue.Of(dto) : null;

    private static LiteralValue? ParseDecimal(JsonElement value, string typeName)
    {
        var (precision, scale) = ParseDecimalSpec(typeName);

        if (value.ValueKind == JsonValueKind.Number)
        {
            // Try System.Decimal first; fall back to high-precision via raw text.
            if (value.TryGetDecimal(out decimal d))
                return LiteralValue.Of(d);
            return ParseDecimalString(value.GetRawText(), scale);
        }

        if (value.ValueKind == JsonValueKind.String)
            return ParseDecimalString(value.GetString()!, scale);

        return null;
    }

    private static LiteralValue? ParseDecimalString(string s, int scale)
    {
        // Accept a textual decimal like "1234.56" or scientific "1.23e4".
        // Parse to BigInteger by tracking the sign and the position of the decimal point,
        // then renormalize to the target scale.
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal d))
            return LiteralValue.Of(d);

        // Fall back to high-precision: split on the decimal point.
        int dot = s.IndexOf('.');
        bool negative = s.Length > 0 && s[0] == '-';
        string digits = dot < 0
            ? s
            : s.Substring(0, dot) + s.Substring(dot + 1);
        if (negative) digits = digits.Substring(1);

        if (!BigInteger.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var unscaled))
            return null;

        if (negative) unscaled = -unscaled;

        int sourceScale = dot < 0 ? 0 : s.Length - dot - 1;
        if (sourceScale < scale)
            unscaled *= BigInteger.Pow(10, scale - sourceScale);
        else if (sourceScale > scale)
            unscaled /= BigInteger.Pow(10, sourceScale - scale);

        return LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
    }

    private static (int Precision, int Scale) ParseDecimalSpec(string typeName)
    {
        // typeName is "decimal(p,s)"
        int open = typeName.IndexOf('(');
        int comma = typeName.IndexOf(',', open);
        int close = typeName.IndexOf(')', comma);
        int precision = int.Parse(typeName.Substring(open + 1, comma - open - 1).Trim(),
            NumberStyles.Integer, CultureInfo.InvariantCulture);
        int scale = int.Parse(typeName.Substring(comma + 1, close - comma - 1).Trim(),
            NumberStyles.Integer, CultureInfo.InvariantCulture);
        return (precision, scale);
    }
}
