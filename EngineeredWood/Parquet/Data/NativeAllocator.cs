using System.Buffers;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// A <see cref="MemoryManager{T}"/> backed by aligned native memory allocated via
/// <see cref="NativeMemory.AlignedAlloc"/>. Disposing frees the native memory.
/// </summary>
internal sealed class NativeMemoryManager : MemoryManager<byte>
{
    private unsafe void* _pointer;
    private readonly int _length;

    public unsafe NativeMemoryManager(int length, int alignment, bool zeroFill)
    {
        _length = length;
        _pointer = NativeMemory.AlignedAlloc((nuint)length, (nuint)alignment);
        if (zeroFill)
            NativeMemory.Clear(_pointer, (nuint)length);
    }

    public override Span<byte> GetSpan()
    {
        unsafe
        {
            return new Span<byte>(_pointer, _length);
        }
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        unsafe
        {
            return new MemoryHandle((byte*)_pointer + elementIndex, pinnable: this);
        }
    }

    public override void Unpin() { }

    protected override void Dispose(bool disposing)
    {
        unsafe
        {
            if (_pointer != null)
            {
                NativeMemory.AlignedFree(_pointer);
                _pointer = null;
            }
        }
    }
}

/// <summary>
/// Arrow <see cref="MemoryAllocator"/> that allocates 64-byte-aligned native memory.
/// </summary>
internal sealed class NativeAllocator : MemoryAllocator
{
    public static readonly NativeAllocator Instance = new();

    private new const int Alignment = 64;

    private static readonly Func<IMemoryOwner<byte>, ArrowBuffer> s_createBuffer = BuildCreateBuffer();

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        return AllocateInternal(length, zeroFill: true, out bytesAllocated);
    }

    /// <summary>
    /// Allocates aligned native memory, optionally skipping zero-fill for buffers
    /// that will be fully overwritten by the caller.
    /// </summary>
    public IMemoryOwner<byte> Allocate(int length, bool zeroFill)
    {
        int aligned = (length + Alignment - 1) & ~(Alignment - 1);
        return new NativeMemoryManager(aligned, Alignment, zeroFill);
    }

    private static IMemoryOwner<byte> AllocateInternal(int length, bool zeroFill, out int bytesAllocated)
    {
        int aligned = (length + Alignment - 1) & ~(Alignment - 1);
        bytesAllocated = aligned;
        return new NativeMemoryManager(aligned, Alignment, zeroFill);
    }

    /// <summary>
    /// Creates an <see cref="ArrowBuffer"/> that owns the given <see cref="IMemoryOwner{T}"/>,
    /// transferring lifetime management to the Arrow buffer. When the Arrow array holding
    /// this buffer is disposed, the native memory is freed.
    /// </summary>
    public static ArrowBuffer CreateBuffer(IMemoryOwner<byte> owner)
    {
        return s_createBuffer(owner);
    }

    private static Func<IMemoryOwner<byte>, ArrowBuffer> BuildCreateBuffer()
    {
        // ArrowBuffer has an internal constructor: ArrowBuffer(IMemoryOwner<byte>)
        // We use a compiled expression to call it efficiently.
        var ctor = typeof(ArrowBuffer).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(IMemoryOwner<byte>)],
            modifiers: null)
            ?? throw new InvalidOperationException(
                "Could not find ArrowBuffer(IMemoryOwner<byte>) constructor.");

        var param = Expression.Parameter(typeof(IMemoryOwner<byte>), "owner");
        var newExpr = Expression.New(ctor, param);
        return Expression.Lambda<Func<IMemoryOwner<byte>, ArrowBuffer>>(newExpr, param).Compile();
    }
}

/// <summary>
/// A pre-sized native-memory buffer for accumulating column values.
/// Values are written into the buffer via <see cref="Span"/>; calling <see cref="Build"/>
/// transfers ownership to an <see cref="ArrowBuffer"/> (zero-copy).
/// </summary>
internal sealed class NativeBuffer<T> : IDisposable where T : struct
{
    private IMemoryOwner<byte>? _owner;
    private int _byteLength;

    /// <summary>Number of <typeparamref name="T"/> elements that fit in the buffer.</summary>
    public int Length { get; private set; }

    /// <summary>Creates a native buffer sized for <paramref name="elementCount"/> elements of <typeparamref name="T"/>.</summary>
    /// <param name="elementCount">Number of elements.</param>
    /// <param name="zeroFill">If true, the buffer is zeroed. Set to false when the caller
    /// will overwrite all bytes (e.g. value buffers for non-nullable columns).</param>
    public NativeBuffer(int elementCount, bool zeroFill = true)
    {
        int elementSize = Unsafe.SizeOf<T>();
        int rawBytes = elementCount * elementSize;
        // Round up to 64-byte boundary
        _byteLength = (rawBytes + 63) & ~63;
        Length = _byteLength / elementSize;
        _owner = NativeAllocator.Instance.Allocate(_byteLength, zeroFill);
    }

    /// <summary>Gets a <see cref="Span{T}"/> over the native buffer.</summary>
    public Span<T> Span
    {
        get
        {
            var byteSpan = _owner!.Memory.Span;
            return MemoryMarshal.Cast<byte, T>(byteSpan);
        }
    }

    /// <summary>Gets a <see cref="Span{T}"/> over the raw bytes of the native buffer.</summary>
    public Span<byte> ByteSpan => _owner!.Memory.Span.Slice(0, _byteLength);

    /// <summary>
    /// Transfers ownership to an <see cref="ArrowBuffer"/>. This instance becomes unusable.
    /// </summary>
    public ArrowBuffer Build(int usedBytes = -1)
    {
        var owner = _owner ?? throw new ObjectDisposedException(nameof(NativeBuffer<T>));
        _owner = null;
        return NativeAllocator.CreateBuffer(owner);
    }

    /// <summary>
    /// Grows the buffer to hold at least <paramref name="newElementCount"/> elements,
    /// preserving existing data.
    /// </summary>
    public void Grow(int newElementCount)
    {
        int elementSize = Unsafe.SizeOf<T>();
        int newBytes = (newElementCount * elementSize + 63) & ~63;
        if (newBytes <= _byteLength)
            return;

        var newOwner = NativeAllocator.Instance.Allocate(newBytes, zeroFill: false);
        _owner!.Memory.Span.CopyTo(newOwner.Memory.Span);
        _owner.Dispose();
        _owner = newOwner;
        _byteLength = newBytes;
        Length = _byteLength / elementSize;
    }

    public void Dispose()
    {
        _owner?.Dispose();
        _owner = null;
    }
}

