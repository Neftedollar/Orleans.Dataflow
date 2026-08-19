using System.Collections.Concurrent;

namespace Orleans.Dataflow.ClusterTests.Provider;

/// <summary>
/// What the recording sink of the test vocabulary has been handed, in order, across every attempt of a run.
/// </summary>
/// <remarks>
/// <para>
/// The vehicle the crash suite measures a duplicate window with. A resume replays the elements between the
/// stored cursor and the moment the silo died, and "exactly those and no others" is a claim about a
/// <em>sequence</em> rather than about a count — so the sink writes down every element it was handed and
/// the test compares the whole list. Nothing here is inferred from a total.
/// </para>
/// <para>
/// <b>It survives the silo on purpose</b>, which is what makes it able to answer the question at all: the
/// log lives in the test process and the silos live inside that same process, so a kill takes the run and
/// leaves the record of what the run had done. That is the same honesty <see cref="TestSignals"/> claims and
/// the same reason it is confined to a test project — in a multi-process cluster this would be a lie, and
/// the shipped answer to "what did this sink commit" is a commit mark rather than a static table.
/// </para>
/// <para>
/// The order within one attempt is the run's own, because the sink is a terminal fold and a terminal sees
/// its elements one at a time in the order they arrive. The order across attempts is the order the attempts
/// happened in, which is what makes a replay visible as a repeated prefix rather than as a bigger number.
/// </para>
/// </remarks>
internal static class TestDeliveries
{
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<long>> Logs =
        new(StringComparer.Ordinal);

    /// <summary>Records that one element reached a sink.</summary>
    /// <param name="log">The name the document gave that sink's log.</param>
    /// <param name="element">The element.</param>
    internal static void Record(string log, long element) =>
        Logs.GetOrAdd(log, static _ => new ConcurrentQueue<long>()).Enqueue(element);

    /// <summary>Reads everything one log has been handed, in order.</summary>
    /// <param name="log">The log's name.</param>
    /// <returns>The elements, oldest first, or an empty list for a log nothing has written to.</returns>
    internal static IReadOnlyList<long> Of(string log) =>
        Logs.TryGetValue(log, out ConcurrentQueue<long>? recorded) ? [.. recorded] : [];

    /// <summary>Forgets one log, so that a test starts from a stated empty rather than from history.</summary>
    /// <param name="log">The log's name.</param>
    internal static void Clear(string log) => _ = Logs.TryRemove(log, out _);
}
