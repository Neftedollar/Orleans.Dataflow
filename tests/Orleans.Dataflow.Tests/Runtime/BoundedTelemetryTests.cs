using System.Collections;
using System.Reflection;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Diagnostics;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the diagnostic surface keeps, and what it repeats: two bounds that are the same rule seen twice.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="TelemetryTests"/> says what this library publishes. This says what publishing costs when the
/// deployment is not co-operating — when a document's numbers come from a request, so every request is a
/// fresh fingerprint, and when the data flowing through a graph is the kind of data that must not be
/// written down. Three surfaces answer that, and each is bounded here:
/// </para>
/// <list type="bullet">
/// <item><description>
/// the settled-totals table, which grew one permanent entry per distinct fingerprint and now folds
/// everything past a cap into one bucket;
/// </description></item>
/// <item><description>
/// the key a keyed stage names when it overflows, which is the one place in the runtime where an element's
/// own data reaches a failure message — and a failure message is stored on the run, returned to every
/// caller that polls, and for a durable run written into persistent state;
/// </description></item>
/// <item><description>
/// the state a durable scope refuses, which used to be interpolated whole into an exception that travels
/// the same way and could carry 256 KiB of the author's own export.
/// </description></item>
/// </list>
/// <para>
/// <b>The cap is exercised on a table of this test's own and never on the process's.</b> The naming is
/// permanent by design — a cumulative counter's series may not move — so a test that filled the real table
/// would blur every other test's graphs into the bucket for the rest of the run, and
/// <see cref="TelemetryTests"/> finds its own runs by fingerprint. The fold is therefore proved on an
/// instance built with a capacity of two, and what is asserted about the process's own table is the
/// invariant that holds whatever else the suite is doing: it never grows past the cap.
/// </para>
/// </remarks>
public sealed class BoundedTelemetryTests
{
    /// <summary>The tag value the production code folds every graph past the cap into.</summary>
    /// <remarks>
    /// Spelled as a literal rather than read from the constant, for the reason the rest of the telemetry
    /// suite spells its names out: it is what a dashboard will see, so a test that echoed the constant back
    /// would pass for a rename no subscriber survives.
    /// </remarks>
    private const string OverflowGraph = "(other)";

    [Fact]
    public void ANamingTableKeepsTheFirstGraphsAndFoldsEveryOneAfterThem()
    {
        DataflowDiagnostics.BoundedGraphNames names = new(capacity: 2);

        Assert.Equal("first", names.Name("first"));
        Assert.Equal("second", names.Name("second"));

        // The table is full, so the third graph and every one after it share one series.
        Assert.Equal(OverflowGraph, names.Name("third"));
        Assert.Equal(OverflowGraph, names.Name("fourth"));
        Assert.Equal(2, names.Count);
    }

    [Fact]
    public void ANamedGraphKeepsItsNameAfterTheTableIsFullAndAnOverflowedOneNeverGainsOne()
    {
        DataflowDiagnostics.BoundedGraphNames names = new(capacity: 2);

        _ = names.Name("first");
        _ = names.Name("second");
        _ = names.Name("third");

        // Both directions are permanent, and both have to be. A named graph that lost its name would make
        // its series stop and the bucket's jump; an overflowed graph that gained one would make the
        // bucket's series drop, which a cumulative counter may never do.
        Assert.Equal("first", names.Name("first"));
        Assert.Equal(OverflowGraph, names.Name("third"));
        Assert.Equal(2, names.Count);
    }

    [Fact]
    public void RepeatingOneGraphSpendsNoCapacity()
    {
        DataflowDiagnostics.BoundedGraphNames names = new(capacity: 2);

        for (int repeat = 0; repeat < 100; repeat++)
        {
            Assert.Equal("only", names.Name("only"));
        }

        Assert.Equal(1, names.Count);

        // The capacity counts distinct graphs, so a deployment running one graph a million times still has
        // room for a second one.
        Assert.Equal("other", names.Name("other"));
    }

    [Fact]
    public async Task TheSettledTableNeverHoldsMoreThanTheCapAndTheBucket()
    {
        // A run of its own so the table has certainly been written to during this test, whatever else the
        // suite is doing to it in parallel.
        RunnableGraph graph = Summing([1, 2, 3], out ResultSlot<long> total);

        await using (RunHandle run = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        }

        // The invariant, read off the production field the review named. It holds however many graphs this
        // process has run: at most one entry per named graph, plus the one bucket the rest fold into.
        Assert.True(
            SettledCount() <= DataflowDiagnostics.MaxTaggedGraphs + 1,
            $"the settled table holds {SettledCount()} entries for a cap of {DataflowDiagnostics.MaxTaggedGraphs}");
    }

    [Fact]
    public async Task AKeyThatOverflowsIsQuotedInPartAndItsLengthIsReportedRatherThanItsTail()
    {
        // A key that is really a record: what a group-by over an unbounded field looks like when the field
        // was the wrong one. Its content stands for an account number or an address — something that must
        // not end up in persistent state because a bound was sized wrong.
        string oversized = new('s', 4096);

        RunnableGraph graph = Source.From(["small", oversized])
            .GroupBy(new GroupByOptions { MaxActiveKeys = 1 }, value => value, Flow.For<string>())
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        TrackedKeyOverflowException overflow =
            await Assert.ThrowsAsync<TrackedKeyOverflowException>(() => run.Completion);

        // The diagnosis survives: the bound, the advice, and enough of the key to recognize it.
        Assert.Contains("at most 1 keys", overflow.Message, StringComparison.Ordinal);
        Assert.Contains(new string('s', 64), overflow.Message, StringComparison.Ordinal);

        // The data does not. The whole key is 4096 characters and no rendering of it that long appears.
        Assert.DoesNotContain(oversized, overflow.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(new string('s', 65), overflow.Message, StringComparison.Ordinal);

        // What replaces the tail is a fact about the shape rather than a piece of the data.
        Assert.Contains("4096", overflow.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AKeyShortEnoughToQuoteIsQuotedWhole()
    {
        // The other half of the trade, and the reason the bound is sixty-four rather than eight: the three
        // diagnoses this message exists for are all legible well inside it, so the ordinary case is
        // unchanged.
        TrackedKeyOverflowException overflow = TrackedKeyOverflowException.Active(4, "tenant-17");

        Assert.Contains("'tenant-17'", overflow.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("characters of", overflow.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullKeyIsStillSpelledAsOne()
    {
        TrackedKeyOverflowException overflow = TrackedKeyOverflowException.Active(4, key: null);

        // Without quotation marks, so it cannot be read as a key whose text happens to be that word.
        Assert.Contains(" key null ", overflow.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TruncationNeverSplitsASurrogatePair()
    {
        // The character that must not be cut in half sits exactly on the boundary: sixty-three ASCII
        // characters, then an emoji whose two code units straddle position sixty-four.
        string key = new string('a', 63) + "\U0001F600" + new string('b', 200);

        TrackedKeyOverflowException overflow = TrackedKeyOverflowException.Active(4, key);

        // A message with a lone surrogate in it is no longer text that survives being written down, which
        // is the very trip this message is about to take.
        Assert.True(
            JsonText.IsWellFormed(overflow.Message),
            "the truncated message is not well-formed text");

        Assert.Contains("63 characters of 265", overflow.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStoredStateAScopeCannotReadIsDescribedByItsShapeAndNotQuoted()
    {
        // A durable scope's state is whatever the author's export function wrote. This one stands for an
        // export that carries customer data, stored under a shape the scope cannot read — a checkpoint of a
        // different graph, or one written by hand.
        string secret = "account-4111111111111111-holder-jane-doe";

        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Scoped();
        NodeId scope = graph.Document.Nodes
            .Single(node => node.Stage.ToString() == "local/durable@v1")
            .Id;

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("shape-not-content"),
            LocalCheckpointDocument.Write(
                graph.Fingerprint,
                graph.Document.Revision,
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [scope] = CanonicalJsonValue.Parse($"\"{secret}\""),
                },
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            TestToken);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "shape-not-content", everyElements: 3),
                TestToken));

        // What a reader needs: which scope, what arrived, and how big it was.
        Assert.Contains("durable scope of 1 stages", refused.Message, StringComparison.Ordinal);
        Assert.Contains("it is a string of 42 bytes rather than an object", refused.Message, StringComparison.Ordinal);
        Assert.Contains("stages", refused.Message, StringComparison.Ordinal);

        // What a reader does not need, and what must not be written into a run's remembered failure.
        Assert.DoesNotContain(secret, refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("4111111111111111", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStateWhoseStagesMemberIsTheWrongKindNamesThatKindAndNothingElse()
    {
        string secret = "holder-jane-doe";

        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Scoped();
        NodeId scope = graph.Document.Nodes
            .Single(node => node.Stage.ToString() == "local/durable@v1")
            .Id;

        _ = await store.WriteAsync(
            Anonymous,
            RunId.Create("wrong-member"),
            LocalCheckpointDocument.Write(
                graph.Fingerprint,
                graph.Document.Revision,
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [scope] = CanonicalJsonValue.Parse($"{{\"stages\":\"{secret}\"}}"),
                },
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            TestToken);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeFromCheckpointAsync(
                graph,
                Durable(store, "wrong-member", everyElements: 3),
                TestToken));

        Assert.Contains("member is a string rather than an array", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, refused.Message, StringComparison.Ordinal);
    }

    /// <summary>The graph both scope tests resume: one durable scope over one scan that exports state.</summary>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Scoped() =>
        Source.From(Enumerable.Range(1, 12))
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.Ignore());

    /// <summary>Reads how many entries the production settled-totals table currently holds.</summary>
    /// <returns>The count.</returns>
    /// <remarks>
    /// By reflection, because the table is private and is meant to stay so: what a subscriber sees is the
    /// measurements, and the table is how they are produced. The review measured this field by name, and
    /// reading the same field is what makes this the same claim rather than a different one.
    /// </remarks>
    private static int SettledCount()
    {
        FieldInfo settled = typeof(DataflowDiagnostics)
            .GetField("Settled", BindingFlags.NonPublic | BindingFlags.Static)!;

        return ((ICollection)settled.GetValue(null)!).Count;
    }
}
