using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What every durability test needs: a store, a run identity, the state codec the durable scan is bound
/// with, and the readers that turn a stored document back into the numbers an assertion is about.
/// </summary>
/// <remarks>
/// <para>
/// The codec here is deliberately the simplest one an author could write — a running total as
/// <c>{"total":n}</c> — because what these tests are about is the seam and not the encoding. It is written
/// once so that two graphs whose scans should agree cannot drift into two encodings that happen to
/// round-trip differently.
/// </para>
/// <para>
/// The store readers exist because a checkpoint is only evidence if a test can read what was actually
/// written. Every number an assertion here names — a cursor, a mark, a scope's state — is read back out of
/// the store rather than out of the run that wrote it.
/// </para>
/// </remarks>
internal static class DurableFixtures
{
    /// <summary>Gets the graph identity every locally authored graph carries.</summary>
    /// <remarks>
    /// A local graph has no author who named it, so every one of them is <c>anonymous</c>; the run identity
    /// is what separates two durable runs, which is exactly why a durable run is named by its author.
    /// </remarks>
    internal static GraphId Anonymous { get; } = GraphId.Create("anonymous");

    /// <summary>Writes a running total as the canonical value a checkpoint carries.</summary>
    /// <param name="total">The state.</param>
    /// <returns>The value.</returns>
    internal static CanonicalJsonValue WriteTotal(long total) =>
        CanonicalJsonValue.Parse(string.Create(CultureInfo.InvariantCulture, $"{{\"total\":{total}}}"));

    /// <summary>Reads a running total back out of the canonical value a checkpoint carried.</summary>
    /// <param name="state">The value.</param>
    /// <returns>The state.</returns>
    internal static long ReadTotal(CanonicalJsonValue state) => state.ToElement().GetProperty("total").GetInt64();

    /// <summary>Builds the durable options a test runs under.</summary>
    /// <param name="store">The store.</param>
    /// <param name="run">What the run is called.</param>
    /// <param name="everyElements">The element bound, or <see langword="null"/> for none.</param>
    /// <param name="interval">The interval, or <see langword="null"/> for none.</param>
    /// <returns>The options.</returns>
    internal static DurableRunOptions Durable(
        InMemoryCheckpointStore store,
        string run,
        int? everyElements = null,
        TimeSpan? interval = null) =>
        new()
        {
            Store = store,
            Run = RunId.Create(run),
            EveryElements = everyElements,
            Interval = interval,
        };

    /// <summary>Reads the checkpoint a store holds for one run.</summary>
    /// <param name="store">The store.</param>
    /// <param name="run">What the run is called.</param>
    /// <param name="cancellationToken">The running test's own token.</param>
    /// <returns>The checkpoint, which the caller has already asserted exists.</returns>
    internal static async Task<LocalCheckpoint> StoredAsync(
        InMemoryCheckpointStore store,
        string run,
        CancellationToken cancellationToken)
    {
        StoredCheckpoint? stored = await store.ReadAsync(Anonymous, RunId.Create(run), cancellationToken);

        Assert.NotNull(stored);
        Assert.True(LocalCheckpointDocument.TryRead(
            stored.Value.Document,
            out LocalCheckpoint? checkpoint,
            out IReadOnlyList<string> violations));
        Assert.Empty(violations);

        return checkpoint!;
    }

    /// <summary>Reads the one cursor a stored checkpoint carries.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <returns>How many elements the source had delivered when the snapshot was taken.</returns>
    internal static long Cursor(LocalCheckpoint checkpoint) =>
        Single(checkpoint.Cursors).ToElement().GetProperty("index").GetInt64();

    /// <summary>Reads the one commit mark a stored checkpoint carries.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <returns>How many elements the sink had committed when the snapshot was taken.</returns>
    internal static long Mark(LocalCheckpoint checkpoint) =>
        Single(checkpoint.Marks).ToElement().GetProperty("committed").GetInt64();

    /// <summary>Reads the running total the one durable scope of a stored checkpoint was holding.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <param name="stage">The position of the scan in the scope's chain, counting from zero.</param>
    /// <returns>The total.</returns>
    internal static long Total(LocalCheckpoint checkpoint, int stage = 0)
    {
        JsonElement stages = Single(checkpoint.States).ToElement().GetProperty("stages");

        return ReadTotal(CanonicalJsonValue.FromElement(stages[stage]));
    }

    /// <summary>Reads the one value a table of a stored checkpoint carries.</summary>
    /// <param name="table">The table.</param>
    /// <returns>The value.</returns>
    private static CanonicalJsonValue Single(IReadOnlyDictionary<NodeId, CanonicalJsonValue> table)
    {
        Assert.Single(table);

        foreach (KeyValuePair<NodeId, CanonicalJsonValue> entry in table)
        {
            return entry.Value;
        }

        throw new InvalidOperationException("unreachable");
    }
}
