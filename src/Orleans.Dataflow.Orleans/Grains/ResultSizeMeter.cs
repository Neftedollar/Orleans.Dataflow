using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Dataflow.Grains;

/// <summary>
/// How many bytes a result value serializes to, measured rather than estimated.
/// </summary>
/// <remarks>
/// <para>
/// The cap has to be about the thing that actually crosses the wire, and for a result that is the value's
/// Orleans-serialized form: a list of a million small records and a single large blob are the same problem
/// and nothing about the CLR object graph says how big either one is. So the value is serialized, through a
/// buffer writer that counts what it is handed and keeps none of it.
/// </para>
/// <para>
/// Counting rather than collecting is the whole point. Building the byte array would allocate the very
/// thing the cap exists to refuse — an oversized result would have to be materialized in full before it
/// could be turned down — while a counting writer's memory is one scratch buffer, sized by the largest
/// single span the serializer asks for and never by the total.
/// </para>
/// <para>
/// The serializer is asked for <see cref="object"/> because that is what the envelope carries: a result is
/// the author's own type and the definition plane never names CLR types, so the polymorphic path is the one
/// the wire will take too, and measuring anything narrower would measure a message nobody sends.
/// </para>
/// </remarks>
internal static class ResultSizeMeter
{
    /// <summary>Measures the serialized size of one result value.</summary>
    /// <param name="serializer">The silo's serializer.</param>
    /// <param name="value">The value a slot resolved to, which may be <see langword="null"/>.</param>
    /// <returns>The number of bytes the value serializes to.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="serializer"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// A serializer failure is not caught here. A result type Orleans cannot serialize is a documented
    /// refusal at first use, and first use is now this measurement rather than the send — the same failure,
    /// one step earlier, with the same message.
    /// </remarks>
    internal static long Measure(Serializer serializer, object? value)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        CountingBufferWriter counter = new();

        serializer.Serialize(value, counter);

        return counter.Written;
    }

    /// <summary>A buffer writer that counts the bytes it is handed and keeps none of them.</summary>
    /// <remarks>
    /// The scratch buffer is reused for every span, which is legal exactly because nothing reads back what
    /// was written: a serializer writes forward into the spans a writer hands it and never asks for one
    /// again. That is what makes the memory a function of the largest single request rather than of the
    /// value's size.
    /// </remarks>
    private sealed class CountingBufferWriter : IBufferWriter<byte>
    {
        /// <summary>The smallest scratch buffer handed out, which is one page's worth.</summary>
        private const int MinimumSpan = 4096;

        private byte[] _scratch = new byte[MinimumSpan];

        /// <summary>Gets how many bytes have been written through this writer.</summary>
        internal long Written { get; private set; }

        /// <inheritdoc/>
        public void Advance(int count) => Written += count;

        /// <inheritdoc/>
        public Memory<byte> GetMemory(int sizeHint = 0) => Grown(sizeHint).AsMemory();

        /// <inheritdoc/>
        public Span<byte> GetSpan(int sizeHint = 0) => Grown(sizeHint).AsSpan();

        /// <summary>Returns a scratch buffer at least as long as a hint asks for.</summary>
        /// <param name="sizeHint">The number of bytes the caller wants room for; zero means "some".</param>
        /// <returns>The scratch buffer.</returns>
        private byte[] Grown(int sizeHint)
        {
            if (sizeHint > _scratch.Length)
            {
                _scratch = new byte[sizeHint];
            }

            return _scratch;
        }
    }
}
