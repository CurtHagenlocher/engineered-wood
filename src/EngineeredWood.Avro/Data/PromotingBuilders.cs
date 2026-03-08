using Apache.Arrow;
using EngineeredWood.Avro.Encoding;

namespace EngineeredWood.Avro.Data;

/// <summary>Reads int from writer, appends as long to reader builder.</summary>
internal sealed class PromotingIntToLongBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.Int64Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads int from writer, appends as float to reader builder.</summary>
internal sealed class PromotingIntToFloatBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads int from writer, appends as double to reader builder.</summary>
internal sealed class PromotingIntToDoubleBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads long from writer, appends as float to reader builder.</summary>
internal sealed class PromotingLongToFloatBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads long from writer, appends as double to reader builder.</summary>
internal sealed class PromotingLongToDoubleBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads float from writer, appends as double to reader builder.</summary>
internal sealed class PromotingFloatToDoubleBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadFloat());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads string from writer, appends as bytes to reader builder.</summary>
internal sealed class PromotingStringToBytesBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.BinaryArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadStringBytes());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}

/// <summary>Reads bytes from writer, appends as string to reader builder.</summary>
internal sealed class PromotingBytesToStringBuilder : IColumnBuilder
{
    private readonly Apache.Arrow.StringArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader)
        => _builder.Append(System.Text.Encoding.UTF8.GetString(reader.ReadBytes()));
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
}
