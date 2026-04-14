using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

public class LiteralValueTests
{
    [Fact]
    public void Null_IsNull()
    {
        var v = LiteralValue.Null;
        Assert.True(v.IsNull);
        Assert.Equal(LiteralValue.Kind.Null, v.Type);
    }

    [Fact]
    public void Default_IsNull()
    {
        LiteralValue v = default;
        Assert.True(v.IsNull);
    }

    [Fact]
    public void Boolean_RoundTrips()
    {
        LiteralValue v = true;
        Assert.Equal(LiteralValue.Kind.Boolean, v.Type);
        Assert.True(v.AsBoolean);
    }

    [Fact]
    public void Int32_RoundTrips()
    {
        LiteralValue v = 42;
        Assert.Equal(LiteralValue.Kind.Int32, v.Type);
        Assert.Equal(42, v.AsInt32);
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        LiteralValue v = 42L;
        Assert.Equal(LiteralValue.Kind.Int64, v.Type);
        Assert.Equal(42L, v.AsInt64);
    }

    [Fact]
    public void Double_RoundTrips()
    {
        LiteralValue v = 3.14;
        Assert.Equal(LiteralValue.Kind.Double, v.Type);
        Assert.Equal(3.14, v.AsDouble);
    }

    [Fact]
    public void String_RoundTrips()
    {
        LiteralValue v = "hello";
        Assert.Equal(LiteralValue.Kind.String, v.Type);
        Assert.Equal("hello", v.AsString);
    }

    [Fact]
    public void Binary_RoundTrips()
    {
        var bytes = new byte[] { 1, 2, 3, 4 };
        LiteralValue v = bytes;
        Assert.Equal(LiteralValue.Kind.Binary, v.Type);
        Assert.Equal(bytes, v.AsBinary);
    }

    [Fact]
    public void Decimal_RoundTrips()
    {
        LiteralValue v = 12.34m;
        Assert.Equal(LiteralValue.Kind.Decimal, v.Type);
        Assert.Equal(12.34m, v.AsDecimal);
    }

    [Fact]
    public void Guid_RoundTrips()
    {
        var g = Guid.NewGuid();
        LiteralValue v = g;
        Assert.Equal(LiteralValue.Kind.Guid, v.Type);
        Assert.Equal(g, v.AsGuid);
    }

    [Fact]
    public void DateTimeOffset_RoundTrips()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 12, 30, 0, TimeSpan.FromHours(-5));
        LiteralValue v = dto;
        Assert.Equal(LiteralValue.Kind.DateTimeOffset, v.Type);
        Assert.Equal(dto, v.AsDateTimeOffset);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void DateOnly_RoundTrips()
    {
        var d = new DateOnly(2024, 1, 15);
        LiteralValue v = d;
        Assert.Equal(LiteralValue.Kind.DateOnly, v.Type);
        Assert.Equal(d, v.AsDateOnly);
    }

    [Fact]
    public void TimeOnly_RoundTrips()
    {
        var t = new TimeOnly(12, 30, 0);
        LiteralValue v = t;
        Assert.Equal(LiteralValue.Kind.TimeOnly, v.Type);
        Assert.Equal(t, v.AsTimeOnly);
    }
#endif

    [Fact]
    public void HighPrecisionDecimal_RoundTrips()
    {
        // Decimal128 with precision exceeding System.decimal: 38 digits
        var unscaled = BigInteger.Parse("12345678901234567890123456789012345678");
        var v = LiteralValue.HighPrecisionDecimalOf(unscaled, 5);

        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal, v.Type);
        var (back, scale) = v.AsHighPrecisionDecimal;
        Assert.Equal(unscaled, back);
        Assert.Equal(5, scale);
    }

    [Fact]
    public void InvalidAccess_Throws()
    {
        LiteralValue v = 42;
        Assert.Throws<InvalidOperationException>(() => v.AsString);
    }

    // ── Equality ──

    [Fact]
    public void Equals_SameKind_SameValue()
    {
        Assert.Equal((LiteralValue)42, (LiteralValue)42);
        Assert.Equal((LiteralValue)"x", (LiteralValue)"x");
        Assert.Equal(LiteralValue.Of(new byte[] { 1, 2 }), LiteralValue.Of(new byte[] { 1, 2 }));
    }

    [Fact]
    public void Equals_DifferentKind_DifferentValue()
    {
        Assert.NotEqual((LiteralValue)42, (LiteralValue)43);
        Assert.NotEqual((LiteralValue)"a", (LiteralValue)"b");
    }

    [Fact]
    public void Equals_CrossTypeNumeric()
    {
        // int and long with same value should be equal via numeric promotion
        Assert.Equal((LiteralValue)42, (LiteralValue)42L);
    }

    [Fact]
    public void Null_EqualsNull()
    {
        Assert.Equal(LiteralValue.Null, LiteralValue.Null);
    }

    // ── Comparison ──

    [Fact]
    public void Compare_Int32_SameType()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)2) < 0);
        Assert.True(((LiteralValue)2).CompareTo((LiteralValue)1) > 0);
        Assert.Equal(0, ((LiteralValue)1).CompareTo((LiteralValue)1));
    }

    [Fact]
    public void Compare_String_Ordinal()
    {
        Assert.True(((LiteralValue)"a").CompareTo((LiteralValue)"b") < 0);
        Assert.True(((LiteralValue)"B").CompareTo((LiteralValue)"a") < 0); // ordinal: uppercase before lowercase
    }

    [Fact]
    public void Compare_CrossType_IntLong()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)2L) < 0);
        Assert.True(((LiteralValue)2L).CompareTo((LiteralValue)1) > 0);
        Assert.Equal(0, ((LiteralValue)42).CompareTo((LiteralValue)42L));
    }

    [Fact]
    public void Compare_CrossType_FloatDouble()
    {
        Assert.True(((LiteralValue)1.0f).CompareTo((LiteralValue)2.0) < 0);
        Assert.Equal(0, ((LiteralValue)1.5f).CompareTo((LiteralValue)1.5));
    }

    [Fact]
    public void Compare_CrossType_IntDouble()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)1.5) < 0);
    }

    [Fact]
    public void Compare_Null_SortsFirst()
    {
        Assert.True(LiteralValue.Null.CompareTo((LiteralValue)1) < 0);
        Assert.True(((LiteralValue)1).CompareTo(LiteralValue.Null) > 0);
        Assert.Equal(0, LiteralValue.Null.CompareTo(LiteralValue.Null));
    }

    [Fact]
    public void Compare_IncompatibleTypes_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ((LiteralValue)"x").CompareTo((LiteralValue)42));
    }

    [Fact]
    public void Compare_Binary_Lexicographic()
    {
        var a = LiteralValue.Of(new byte[] { 1, 2, 3 });
        var b = LiteralValue.Of(new byte[] { 1, 2, 4 });
        var c = LiteralValue.Of(new byte[] { 1, 2 });

        Assert.True(a.CompareTo(b) < 0);
        Assert.True(b.CompareTo(a) > 0);
        Assert.True(c.CompareTo(a) < 0); // shorter prefix sorts first
    }

    [Fact]
    public void Compare_HighPrecisionDecimal_SameScale()
    {
        var a = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("12345"), 2);
        var b = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("67890"), 2);
        Assert.True(a.CompareTo(b) < 0);
    }

    [Fact]
    public void Compare_HighPrecisionDecimal_DifferentScale()
    {
        // 1.23 (123 * 10^-2) vs 1.230 (1230 * 10^-3) — should compare equal
        var a = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("123"), 2);
        var b = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("1230"), 3);
        Assert.Equal(0, a.CompareTo(b));
    }

    // ── Operators ──

    [Fact]
    public void Operators_LessThanGreaterThan()
    {
        Assert.True((LiteralValue)1 < (LiteralValue)2);
        Assert.True((LiteralValue)2 > (LiteralValue)1);
        Assert.True((LiteralValue)1 <= (LiteralValue)1);
        Assert.True((LiteralValue)1 >= (LiteralValue)1);
    }

    // ── Hash ──

    [Fact]
    public void Hash_EqualValues_EqualHashes()
    {
        Assert.Equal(((LiteralValue)42).GetHashCode(), ((LiteralValue)42).GetHashCode());
        Assert.Equal(((LiteralValue)"x").GetHashCode(), ((LiteralValue)"x").GetHashCode());
        Assert.Equal(
            LiteralValue.Of(new byte[] { 1, 2 }).GetHashCode(),
            LiteralValue.Of(new byte[] { 1, 2 }).GetHashCode());
    }

    [Fact]
    public void ToObject_BoxesValue()
    {
        Assert.Equal((object)42, ((LiteralValue)42).ToObject());
        Assert.Equal((object)"x", ((LiteralValue)"x").ToObject());
        Assert.Null(LiteralValue.Null.ToObject());
    }
}
