// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Tests for the Phase 13 MVP Lance writer. Covers two cases:
/// (1) self-roundtrip — write a file then read it back via our own
/// reader and assert exact value equality; (2) cross-validation —
/// invoke pylance on the written file and confirm it sees the same
/// schema and values.
/// </summary>
public class LanceFileWriterTests
{
    [Fact]
    public async Task SingleColumn_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4, 5 });
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);
            Assert.Single(reader.Schema.FieldsList);
            Assert.Equal("x", reader.Schema.FieldsList[0].Name);
            Assert.IsType<Int32Type>(reader.Schema.FieldsList[0].DataType);

            var arr = (Int32Array)await reader.ReadColumnAsync(0);
            Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, arr.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task TwoColumns_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("a", new[] { 10, 20, 30 });
                await writer.WriteInt32ColumnAsync("b", new[] { 100, 200, 300 });
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);
            Assert.Equal(2, reader.Schema.FieldsList.Count);

            var a = (Int32Array)await reader.ReadColumnAsync(0);
            var b = (Int32Array)await reader.ReadColumnAsync(1);
            Assert.Equal(new int?[] { 10, 20, 30 }, a.ToArray());
            Assert.Equal(new int?[] { 100, 200, 300 }, b.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task SingleColumn_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            // Skip silently when Python/pylance isn't available locally.
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("x", new[] { 7, 11, 13, 17, 19, 23 });
                await writer.FinishAsync();
            }

            // Read back via pylance (LanceFileReader.read_all() → Arrow Table).
            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'columns': t.column_names, 'types': [str(f.type) for f in t.schema], 'rows': len(t), 'values': t['x'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal("x", root.GetProperty("columns")[0].GetString());
            Assert.Equal("int32", root.GetProperty("types")[0].GetString());
            Assert.Equal(6, root.GetProperty("rows").GetInt32());
            var values = new List<int>();
            foreach (var v in root.GetProperty("values").EnumerateArray())
                values.Add(v.GetInt32());
            Assert.Equal(new[] { 7, 11, 13, 17, 19, 23 }, values);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    private static bool IsPythonAvailable()
    {
        try
        {
            var psi = new ProcessStartInfo("python", "-c \"import lance\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi);
            if (proc is null) return false;
            proc.WaitForExit(5_000);
            return proc.ExitCode == 0;
        }
        catch { return false; }
    }

    private static string EscapeArg(string s)
    {
        // Wrap in quotes; escape interior double quotes.
        return "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    }
}
