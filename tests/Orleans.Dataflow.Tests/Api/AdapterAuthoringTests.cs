using System.Threading.Channels;
using Xunit;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the sources and sinks of the adapter checkpoint check before they build anything, and what their
/// two spellings agree on.
/// </summary>
/// <remarks>
/// <para>
/// Every factory here rejects its arguments before a descriptor exists, so a rejected call leaves the
/// program exactly as it found it. That is the same rule the operators follow, and it is worth restating
/// per factory because each of them takes a different kind of argument: a sequence, a delegate, a reader,
/// a writer, and two option records.
/// </para>
/// <para>
/// The factory spellings are asserted to build the same document as the static ones, because a sink reached
/// through <c>Sink.For&lt;T&gt;()</c> or through a <c>To</c> lambda has to be the same sink and not merely a
/// similar one; the fingerprint is what says so.
/// </para>
/// </remarks>
public sealed class AdapterAuthoringTests
{
    [Fact]
    public void EverySourceFactoryRejectsAnAbsentArgumentAgainstItsOwnParameter()
    {
        Assert.Throws<ArgumentNullException>(
            "elements",
            () => { _ = Source.FromAsyncEnumerable<int>(null!); });
        Assert.Throws<ArgumentNullException>("factory", () => { _ = Source.FromFactory<int>(null!); });
        Assert.Throws<ArgumentNullException>("factory", () => { _ = Source.FromAsyncFactory<int>(null!); });
        Assert.Throws<ArgumentNullException>("elements", () => { _ = Source.Cycle<int>(null!); });
        Assert.Throws<ArgumentNullException>(
            "generator",
            () => { _ = Source.UnfoldAsync<int, int>(0, null!); });
        Assert.Throws<ArgumentNullException>("reader", () => { _ = Source.FromChannel<int>(null!); });
    }

    [Fact]
    public void EverySinkFactoryRejectsAnAbsentArgumentAgainstItsOwnParameter()
    {
        Assert.Throws<ArgumentNullException>("options", () => { _ = Sink.Collect<int>(null!); });
        Assert.Throws<ArgumentNullException>("writer", () => { _ = Sink.ToChannel<int>(null!); });
        Assert.Throws<ArgumentNullException>("options", () => { _ = Sink.For<int>().Collect(null!); });
        Assert.Throws<ArgumentNullException>("writer", () => { _ = Sink.For<int>().ToChannel(null!); });
    }

    [Fact]
    public void BuildingAnyOfTheseGraphsStartsNoWork()
    {
        // A source is a description of where elements come from, and describing one runs nothing: the
        // factories here are the ones an author could most easily expect to fire at authoring time.
        int factories = 0;
        int generators = 0;

        _ = Source.FromFactory(() => ++factories).To(Sink.Ignore<int>());
        _ = Source.FromAsyncFactory(token => Task.FromResult(++factories)).To(Sink.Ignore<int>());
        _ = Source
            .UnfoldAsync<int, int>(0, (state, token) =>
            {
                generators++;

                return Task.FromResult<UnfoldStep<int, int>?>(null);
            })
            .To(Sink.Ignore<int>());

        Assert.Equal(0, factories);
        Assert.Equal(0, generators);
    }

    [Fact]
    public void TheFactorySpellingOfEveryNewSinkBuildsTheSameDocumentAsTheStaticOne()
    {
        Channel<int> channel = Channel.CreateUnbounded<int>();
        CollectOptions bound = new() { MaxElements = 4 };

        Assert.Equal(
            Source.Range(1, 2).To(Sink.Last<int>(), "value", out ResultSlot<int> _).Fingerprint,
            Source.Range(1, 2).To(s => s.Last(), "value", out ResultSlot<int> _).Fingerprint);

        Assert.Equal(
            Source.Range(1, 2).To(Sink.LastOrDefault<int>(), "value", out ResultSlot<int> _).Fingerprint,
            Source.Range(1, 2).To(s => s.LastOrDefault(), "value", out ResultSlot<int> _).Fingerprint);

        Assert.Equal(
            Source.Range(1, 2).To(Sink.Collect<int>(bound), "seen", out ResultSlot<IReadOnlyList<int>> _).Fingerprint,
            Source.Range(1, 2).To(s => s.Collect(bound), "seen", out ResultSlot<IReadOnlyList<int>> _).Fingerprint);

        Assert.Equal(
            Source.Range(1, 2).To(Sink.ToChannel(channel.Writer)).Fingerprint,
            Source.Range(1, 2).To(s => s.ToChannel(channel.Writer)).Fingerprint);
    }

    [Fact]
    public void ALastElementSinksResultCanBeDroppedDeliberately()
    {
        // The one-argument close is the deliberate spelling for running a result-bearing sink and keeping
        // nothing, and it applies to the sinks this checkpoint adds exactly as it does to the older ones.
        RunnableGraph graph = Source.Range(1, 3).To(Sink.Last<int>().ToSink());

        Assert.Empty(graph.ResultSlots);
    }

    [Fact]
    public void TwoUnfoldingGraphsOfOneShapeShareAFingerprintWhateverTheirGeneratorsCompute()
    {
        // The documented limit of a fingerprint: it identifies shape, not behavior. A generator never
        // reaches a document, so these two are the same document and are told apart by the authoring nonce
        // a slot carries instead.
        RunnableGraph counting = Source
            .UnfoldAsync<int, int>(0, (state, token) => Task.FromResult<UnfoldStep<int, int>?>(new(state, state + 1)))
            .To(Sink.Ignore<int>());
        RunnableGraph doubling = Source
            .UnfoldAsync<int, int>(0, (state, token) => Task.FromResult<UnfoldStep<int, int>?>(new(state, state * 2)))
            .To(Sink.Ignore<int>());

        Assert.Equal(counting.Fingerprint, doubling.Fingerprint);
    }
}
