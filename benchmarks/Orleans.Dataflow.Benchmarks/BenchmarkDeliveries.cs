using System.Collections.Concurrent;
using System.Diagnostics;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// What the recording sink writes to, and the only thing in the recovery scenario that outlives the silo
/// whose death is being measured.
/// </summary>
/// <remarks>
/// <para>
/// An ordinary static object in the harness process. The silos of the recovery scenario are in-process, so
/// a ledger they all write to survives any one of them being torn down — which is what makes "the first
/// element the resumed attempt delivered" observable at all. A multi-process cluster would need the shipped
/// answer instead, a commit mark in the checkpoint, and saying so is why this lives in the harness and not
/// in a package.
/// </para>
/// <para>
/// <b>The sink timestamps, the harness does not.</b> The measurement is a latency, and a latency read by a
/// poll is a latency plus the poll interval. So the ledger is armed before the kill and stamps the clock on
/// the very delivery that crosses the armed count; the harness then waits for that stamp at whatever
/// resolution it likes without the wait entering the number.
/// </para>
/// </remarks>
internal static class BenchmarkDeliveries
{
    private static readonly ConcurrentDictionary<string, Ledger> Ledgers = new(StringComparer.Ordinal);

    /// <summary>Writes one delivery down.</summary>
    /// <param name="log">Which ledger.</param>
    /// <param name="element">The element delivered.</param>
    internal static void Record(string log, long element) => Of(log).Record(element);

    /// <summary>Forgets everything one ledger holds.</summary>
    /// <param name="log">Which ledger.</param>
    internal static void Clear(string log) => _ = Ledgers.TryRemove(log, out _);

    /// <summary>Reports how many deliveries one ledger has seen.</summary>
    /// <param name="log">Which ledger.</param>
    /// <returns>The count.</returns>
    internal static long Count(string log) => Of(log).Count;

    /// <summary>Reports the largest element one ledger has seen.</summary>
    /// <param name="log">Which ledger.</param>
    /// <returns>The element, or zero when nothing has been delivered.</returns>
    internal static long Highest(string log) => Of(log).Highest;

    /// <summary>Arms a ledger to stamp the clock on its next delivery.</summary>
    /// <param name="log">Which ledger.</param>
    /// <remarks>
    /// Called after the silo is gone and before anything can resume, so the delivery that trips the stamp
    /// is by construction the resumed attempt's first.
    /// </remarks>
    internal static void Arm(string log) => Of(log).Arm();

    /// <summary>Reads the delivery a ledger was armed for, once it has happened.</summary>
    /// <param name="log">Which ledger.</param>
    /// <returns>
    /// The timestamp and the element of the first delivery after the arming, or <see langword="null"/>
    /// while none has happened.
    /// </returns>
    internal static (long Timestamp, long Element)? Armed(string log) => Of(log).Armed();

    /// <summary>Finds or opens one ledger.</summary>
    /// <param name="log">Which ledger.</param>
    /// <returns>The ledger.</returns>
    private static Ledger Of(string log) => Ledgers.GetOrAdd(log, static _ => new Ledger());

    /// <summary>One run's deliveries.</summary>
    private sealed class Ledger
    {
        private readonly Lock _gate = new();
        private long _count;
        private long _highest;
        private bool _armed;
        private long _armedTimestamp;
        private long _armedElement;

        /// <summary>Gets how many deliveries this ledger has seen.</summary>
        internal long Count
        {
            get
            {
                lock (_gate)
                {
                    return _count;
                }
            }
        }

        /// <summary>Gets the largest element this ledger has seen.</summary>
        internal long Highest
        {
            get
            {
                lock (_gate)
                {
                    return _highest;
                }
            }
        }

        /// <summary>Writes one delivery down, stamping the clock when this is the armed one.</summary>
        /// <param name="element">The element delivered.</param>
        internal void Record(long element)
        {
            lock (_gate)
            {
                _count++;

                if (element > _highest)
                {
                    _highest = element;
                }

                if (_armed)
                {
                    _armed = false;
                    _armedTimestamp = Stopwatch.GetTimestamp();
                    _armedElement = element;
                }
            }
        }

        /// <summary>Arms this ledger to stamp its next delivery.</summary>
        internal void Arm()
        {
            lock (_gate)
            {
                _armed = true;
                _armedTimestamp = 0;
                _armedElement = 0;
            }
        }

        /// <summary>Reads the armed delivery once it has happened.</summary>
        /// <returns>The stamp, or <see langword="null"/> while nothing has tripped it.</returns>
        internal (long Timestamp, long Element)? Armed()
        {
            lock (_gate)
            {
                return _armedTimestamp == 0 ? null : (_armedTimestamp, _armedElement);
            }
        }
    }
}
