// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Expressions.Arrow.Tests;

public class ArrowRowEvaluatorTests
{
    private static readonly ArrowRowEvaluator Eval = new();

    private static RecordBatch IntColumn(string name, params int?[] values)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field(name, Int32Type.Default, true))
            .Build();
        var b = new Int32Array.Builder();
        foreach (var v in values)
        {
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        }
        return new RecordBatch(schema, [b.Build()], values.Length);
    }

    private static RecordBatch StringColumn(string name, params string?[] values)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field(name, StringType.Default, true))
            .Build();
        var b = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v is null) b.AppendNull();
            else b.Append(v);
        }
        return new RecordBatch(schema, [b.Build()], values.Length);
    }

    private static RecordBatch TwoIntColumns(string a, string b, int?[] av, int?[] bv)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field(a, Int32Type.Default, true))
            .Field(new Field(b, Int32Type.Default, true))
            .Build();
        var ab = new Int32Array.Builder();
        foreach (var v in av) { if (v.HasValue) ab.Append(v.Value); else ab.AppendNull(); }
        var bb = new Int32Array.Builder();
        foreach (var v in bv) { if (v.HasValue) bb.Append(v.Value); else bb.AppendNull(); }
        return new RecordBatch(schema, [ab.Build(), bb.Build()], av.Length);
    }

    private static List<bool?> ToList(BooleanArray a)
    {
        var list = new List<bool?>(a.Length);
        for (int i = 0; i < a.Length; i++)
            list.Add(a.IsNull(i) ? null : a.GetValue(i)!.Value);
        return list;
    }

    // ── Constants ──

    [Fact]
    public void True_AllRowsTrue()
    {
        var batch = IntColumn("x", 1, 2, 3);
        var r = Eval.EvaluatePredicate(Ex.True, batch);
        Assert.Equal([true, true, true], ToList(r));
    }

    [Fact]
    public void False_AllRowsFalse()
    {
        var batch = IntColumn("x", 1, 2, 3);
        var r = Eval.EvaluatePredicate(Ex.False, batch);
        Assert.Equal([false, false, false], ToList(r));
    }

    // ── Equality ──

    [Fact]
    public void Equal_ColumnVsLiteral()
    {
        var batch = IntColumn("x", 1, 2, 3, 2);
        var r = Eval.EvaluatePredicate(Ex.Equal("x", 2), batch);
        Assert.Equal([false, true, false, true], ToList(r));
    }

    [Fact]
    public void Equal_NullOperand_ProducesNull()
    {
        var batch = IntColumn("x", 1, null, 3);
        var r = Eval.EvaluatePredicate(Ex.Equal("x", 1), batch);
        Assert.Equal([true, null, false], ToList(r));
    }

    [Fact]
    public void NullSafeEqual_NullVsNull_True()
    {
        var batch = IntColumn("x", 1, null, 3);
        var r = Eval.EvaluatePredicate(
            new ComparisonPredicate(
                new UnboundReference("x"), ComparisonOperator.NullSafeEqual,
                new LiteralExpression(LiteralValue.Null)),
            batch);
        Assert.Equal([false, true, false], ToList(r));
    }

    [Fact]
    public void NullSafeEqual_NullVsNonNull_False()
    {
        var batch = IntColumn("x", null, null);
        var r = Eval.EvaluatePredicate(
            new ComparisonPredicate(
                new UnboundReference("x"), ComparisonOperator.NullSafeEqual,
                new LiteralExpression(5)),
            batch);
        Assert.Equal([false, false], ToList(r));
    }

    // ── Range comparisons ──

    [Fact]
    public void GreaterThan_FiltersRows()
    {
        var batch = IntColumn("x", 1, 5, 10, null);
        var r = Eval.EvaluatePredicate(Ex.GreaterThan("x", 4), batch);
        Assert.Equal([false, true, true, null], ToList(r));
    }

    [Fact]
    public void LessThanOrEqual_FiltersRows()
    {
        var batch = IntColumn("x", 1, 5, 10);
        var r = Eval.EvaluatePredicate(Ex.LessThanOrEqual("x", 5), batch);
        Assert.Equal([true, true, false], ToList(r));
    }

    // ── Column vs column ──

    [Fact]
    public void ColumnVsColumn_Comparison()
    {
        var batch = TwoIntColumns("a", "b",
            [1, 5, 10, null],
            [2, 5,  3,    1]);

        var r = Eval.EvaluatePredicate(
            new ComparisonPredicate(
                new UnboundReference("a"), ComparisonOperator.LessThan,
                new UnboundReference("b")),
            batch);

        Assert.Equal([true, false, false, null], ToList(r));
    }

    // ── Boolean combinators ──

    [Fact]
    public void And_BothTrue()
    {
        var batch = IntColumn("x", 1, 5, 10);
        var r = Eval.EvaluatePredicate(
            Ex.And(Ex.GreaterThan("x", 0), Ex.LessThan("x", 7)),
            batch);
        Assert.Equal([true, true, false], ToList(r));
    }

    [Fact]
    public void And_NullAndFalse_IsFalse()
    {
        // SQL: NULL AND FALSE = FALSE.
        var batch = IntColumn("x", new int?[] { null });
        var r = Eval.EvaluatePredicate(
            Ex.And(Ex.GreaterThan("x", 0), Ex.False),
            batch);
        Assert.Equal([false], ToList(r));
    }

    [Fact]
    public void And_NullAndTrue_IsNull()
    {
        var batch = IntColumn("x", new int?[] { null });
        var r = Eval.EvaluatePredicate(
            Ex.And(Ex.GreaterThan("x", 0), Ex.True),
            batch);
        Assert.Equal([null], ToList(r));
    }

    [Fact]
    public void Or_NullOrTrue_IsTrue()
    {
        var batch = IntColumn("x", new int?[] { null });
        var r = Eval.EvaluatePredicate(
            Ex.Or(Ex.GreaterThan("x", 0), Ex.True),
            batch);
        Assert.Equal([true], ToList(r));
    }

    [Fact]
    public void Or_NullOrFalse_IsNull()
    {
        var batch = IntColumn("x", new int?[] { null });
        var r = Eval.EvaluatePredicate(
            Ex.Or(Ex.GreaterThan("x", 0), Ex.False),
            batch);
        Assert.Equal([null], ToList(r));
    }

    [Fact]
    public void Not_TrueFalseNull()
    {
        var batch = IntColumn("x", 1, 5, null);
        var r = Eval.EvaluatePredicate(
            Ex.Not(Ex.GreaterThan("x", 3)),
            batch);
        Assert.Equal([true, false, null], ToList(r));
    }

    // ── IS NULL / IS NOT NULL ──

    [Fact]
    public void IsNull_PicksNullRows()
    {
        var batch = IntColumn("x", 1, null, 3, null);
        var r = Eval.EvaluatePredicate(Ex.IsNull("x"), batch);
        Assert.Equal([false, true, false, true], ToList(r));
    }

    [Fact]
    public void IsNotNull_PicksNonNullRows()
    {
        var batch = IntColumn("x", 1, null, 3);
        var r = Eval.EvaluatePredicate(Ex.IsNotNull("x"), batch);
        Assert.Equal([true, false, true], ToList(r));
    }

    // ── IN ──

    [Fact]
    public void In_FiltersRows()
    {
        var batch = IntColumn("x", 1, 2, 3, 4, 5);
        var r = Eval.EvaluatePredicate(Ex.In("x", 2, 4), batch);
        Assert.Equal([false, true, false, true, false], ToList(r));
    }

    [Fact]
    public void In_NullValue_IsNull()
    {
        var batch = IntColumn("x", null, 1, 2);
        var r = Eval.EvaluatePredicate(Ex.In("x", 1), batch);
        Assert.Equal([null, true, false], ToList(r));
    }

    [Fact]
    public void In_WithNullInList_NoMatch_IsNull()
    {
        // SQL: x IN (1, NULL) where x = 5 → unknown (null).
        var batch = IntColumn("x", 5);
        var r = Eval.EvaluatePredicate(
            new SetPredicate(new UnboundReference("x"),
                [(LiteralValue)1, LiteralValue.Null], SetOperator.In),
            batch);
        Assert.Equal([null], ToList(r));
    }

    // ── String column ──

    [Fact]
    public void StringEqual_FiltersRows()
    {
        var batch = StringColumn("name", "alice", "bob", null, "alice");
        var r = Eval.EvaluatePredicate(Ex.Equal("name", "alice"), batch);
        Assert.Equal([true, false, null, true], ToList(r));
    }

    [Fact]
    public void StartsWith_FiltersRows()
    {
        var batch = StringColumn("name", "alpha", "alphabet", "beta", "alright");
        var r = Eval.EvaluatePredicate(Ex.StartsWith("name", "alp"), batch);
        Assert.Equal([true, true, false, false], ToList(r));
    }

    // ── Cross-type comparison ──

    [Fact]
    public void Compare_Int32ColumnVsInt64Literal()
    {
        var batch = IntColumn("x", 1, 5, 10);
        var r = Eval.EvaluatePredicate(
            new ComparisonPredicate(
                new UnboundReference("x"), ComparisonOperator.GreaterThan,
                new LiteralExpression(3L)), // long literal
            batch);
        Assert.Equal([false, true, true], ToList(r));
    }

    // ── Unknown column ──

    [Fact]
    public void UnknownColumn_Throws()
    {
        var batch = IntColumn("x", 1);
        Assert.Throws<ArgumentException>(() =>
            Eval.EvaluatePredicate(Ex.Equal("y", 1), batch));
    }

    // ── EvaluateExpression ──

    [Fact]
    public void EvaluateExpression_ColumnReference_ReturnsColumn()
    {
        var batch = IntColumn("x", 1, 2, 3);
        var result = Eval.EvaluateExpression(Ex.Ref("x"), batch);
        var arr = Assert.IsType<Int32Array>(result);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.GetValue(0));
        Assert.Equal(2, arr.GetValue(1));
        Assert.Equal(3, arr.GetValue(2));
    }

    [Fact]
    public void EvaluateExpression_Literal_ReturnsConstantArray()
    {
        var batch = IntColumn("x", 1, 2, 3);
        var result = Eval.EvaluateExpression(Ex.Lit(42), batch);
        var arr = Assert.IsType<Int32Array>(result);
        Assert.Equal(3, arr.Length);
        for (int i = 0; i < 3; i++) Assert.Equal(42, arr.GetValue(i));
    }

    // ── Function call with registry ──

    [Fact]
    public void FunctionCall_NoRegistry_Throws()
    {
        var batch = IntColumn("x", 1);
        Assert.Throws<InvalidOperationException>(() =>
            Eval.EvaluatePredicate(
                Ex.Equal(Ex.Call("FOO", Ex.Ref("x")), Ex.Lit(1)),
                batch));
    }

    private sealed class TestRegistry : IFunctionRegistry
    {
        public bool IsRegistered(string name) => name == "DOUBLE_IT";

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount)
        {
            if (name != "DOUBLE_IT") throw new ArgumentException("Unknown function.");
            var src = (Int32Array)args[0];
            var b = new Int32Array.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                if (src.IsNull(i)) b.AppendNull();
                else b.Append(src.GetValue(i)!.Value * 2);
            }
            return b.Build();
        }
    }

    [Fact]
    public void FunctionCall_WithRegistry_Invokes()
    {
        var evalWithFns = new ArrowRowEvaluator(new TestRegistry());
        var batch = IntColumn("x", 1, 2, 3);

        var r = evalWithFns.EvaluatePredicate(
            Ex.Equal(Ex.Call("DOUBLE_IT", Ex.Ref("x")), Ex.Lit(4)),
            batch);

        Assert.Equal([false, true, false], ToList(r));
    }
}
