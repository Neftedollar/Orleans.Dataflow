using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The five ways of closing a graph, and the claim that they close the same graph.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0004 section 3 justifies every one of them: the plain form for a graph with no result, the tuple
/// form for the places an <see langword="out"/> parameter is banned outright, the fluent
/// <see langword="out"/> form for the places a tuple would need unpacking, and a sink-factory lambda beside
/// each of the two so that a fold's element type never has to be written down. Four spellings of one
/// operation are only defensible if they are one operation, which is what these tests check — at the level
/// of document bytes, not of shape.
/// </para>
/// <para>
/// The mistakes these overloads exist to prevent are compile errors, and a compile error cannot be a test
/// in a passing suite; the ADR's compile prototypes are their evidence. What is reachable at runtime is
/// asserted here instead.
/// </para>
/// <para>
/// That applies to the two <c>[Obsolete(error: true)]</c> guard overloads in full. They are compile-time
/// surface and nothing else: a call to either is a build error, so no test in this file can invoke one,
/// and their bodies exist only to fail loudly if reflection or a future compiler ever reaches them. What
/// can be asserted about them is asserted below — that the spellings they exist to protect still compile
/// and still produce the documents they always did.
/// </para>
/// </remarks>
public sealed class ToOverloadTests
{
    [Fact]
    public void TheTupleAndOutFormsProduceTheSameDocumentBytes()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        (RunnableGraph Graph, ResultSlot<long> Slot) tuple = Source.From(OrderEvents).To(counting, "processed");

        RunnableGraph fluent = Source.From(OrderEvents)
            .To(counting, "processed", out ResultSlot<long> fluentSlot);

        Assert.Equal(
            GraphDocumentSerializer.Serialize(tuple.Graph.Document),
            GraphDocumentSerializer.Serialize(fluent.Document));
        Assert.Equal(tuple.Graph.Fingerprint, fluent.Fingerprint);

        // Two To calls close two graph instances, so the slots agree on everything except the
        // per-instance authoring nonce that keeps look-alike graphs from resolving each other's results.
        Assert.Equal(tuple.Slot.Id, fluentSlot.Id);
        Assert.Equal(tuple.Slot.Graph, fluentSlot.Graph);
        Assert.NotEqual(tuple.Slot, fluentSlot);
    }

    [Fact]
    public void TheTupleAndOutSinkFactoryFormsProduceTheSameDocumentBytes()
    {
        (RunnableGraph Graph, ResultSlot<long> Slot) tuple = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed");

        RunnableGraph fluent = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> fluentSlot);

        Assert.Equal(
            GraphDocumentSerializer.Serialize(tuple.Graph.Document),
            GraphDocumentSerializer.Serialize(fluent.Document));
        Assert.Equal(tuple.Slot.Id, fluentSlot.Id);
        Assert.Equal(tuple.Slot.Graph, fluentSlot.Graph);
        Assert.NotEqual(tuple.Slot, fluentSlot);
    }

    [Fact]
    public void ASinkValueAndASinkFactoryLambdaProduceTheSameDocument()
    {
        RunnableGraph fromValue = Source.From(OrderEvents)
            .To(Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        RunnableGraph fromLambda = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        Assert.Equal(fromValue.Fingerprint, fromLambda.Fingerprint);
        Assert.Equal(
            GraphDocumentSerializer.Serialize(fromValue.Document),
            GraphDocumentSerializer.Serialize(fromLambda.Document));
    }

    [Fact]
    public void DiscardingAResultIsAlwaysSomethingTheAuthorWrote()
    {
        // The result-bearing sink does not fit the one-argument To without a conversion, so the two ways of
        // saying "run the fold and ignore its value" are both explicit, and both produce a document with no
        // slot in it.
        //
        // This is also the anchor for the guard overloads, which have no test of their own and cannot have
        // one. Writing To(counting) without a slot name binds to Source<T>.To<TResult>(SinkWithResult<T,
        // TResult>), which is [Obsolete(error: true)] and fails the build with a message naming the two
        // correct spellings and this discard. Both spellings below are what that message points at, so a
        // change that broke them would be caught here even though the guard itself cannot be.
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        RunnableGraph viaMethod = Source.From(OrderEvents).To(counting.ToSink());
        RunnableGraph viaCast = Source.From(OrderEvents).To((Sink<OrderCreated>)counting);

        Assert.Empty(viaMethod.ResultSlots);
        Assert.Empty(viaCast.ResultSlots);
        Assert.Equal(viaMethod.Fingerprint, viaCast.Fingerprint);
        Assert.Equal(["from-enumerable", "fold"], StageIds(viaMethod.Document));
    }

    [Fact]
    public void DiscardingAResultKeepsTheFoldAndOnlyDropsTheDeclaration()
    {
        // The document still says a fold happens; it just exposes no name for the value, so nothing can ask
        // for it. Dropping the stage instead would change what the graph does.
        RunnableGraph declared = Source.From(OrderEvents)
            .To(Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        RunnableGraph discarded = Source.From(OrderEvents)
            .To(Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1).ToSink());

        Assert.Equal(StageIds(declared.Document), StageIds(discarded.Document));
        Assert.Equal(Edges(declared.Document), Edges(discarded.Document));
        Assert.NotEqual(declared.Fingerprint, discarded.Fingerprint);
    }

    [Fact]
    public void TheSinkFactoryLambdaIsInvokedExactlyOncePerClosure()
    {
        // The lambda is the author's code; running it twice would run whatever they put in it twice, and
        // would let one call build two different sinks.
        int invocations = 0;

        _ = Source.From(OrderEvents).To(
            factory =>
            {
                invocations++;

                return factory.Aggregate(0L, (count, _) => count + 1);
            },
            "processed",
            out ResultSlot<long> _);

        Assert.Equal(1, invocations);
    }

    [Fact]
    public void TheSinkFactoryCanAlsoChooseADiscardingSink()
    {
        SinkFactory<OrderCreated> factory = SinkFactoryOf<OrderCreated>();

        Assert.Equal(
            Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>()).Fingerprint,
            Source.From(OrderEvents).To(factory.Ignore()).Fingerprint);
        Assert.Equal("sink factory", factory.ToString());
    }

    [Fact]
    public void SinkForIsTheSameFactoryTheLambdaOverloadsHandOut()
    {
        // Sink.For<T>() is the named way to reach the vocabulary a sink-factory lambda receives, so the
        // two have to be the same thing rather than two things that behave alike.
        Assert.Same(SinkFactoryOf<OrderCreated>(), Sink.For<OrderCreated>());
        Assert.Same(Sink.For<OrderCreated>(), Sink.For<OrderCreated>());
        Assert.NotSame((object)Sink.For<OrderCreated>(), Sink.For<OrderDocument>());
    }

    [Fact]
    public void SinkForComposesAFoldWithoutWritingEitherTypeArgument()
    {
        // The inference the type argument list would otherwise cost: Sink.For<int>() pins the element type
        // once, and both lambda parameters are then typed without an annotation. The document is the one
        // the two-type-argument spelling produces, which is what makes this a spelling and not a variant.
        SinkWithResult<int, long> viaFactory = Sink.For<int>().Aggregate(0L, (sum, value) => sum + value);
        SinkWithResult<int, long> viaArguments = Sink.Aggregate<int, long>(0L, (sum, value) => sum + value);

        RunnableGraph fromFactory = Source.From<int>([1, 2, 3])
            .To(viaFactory, "total", out ResultSlot<long> factorySlot);

        RunnableGraph fromArguments = Source.From<int>([1, 2, 3])
            .To(viaArguments, "total", out ResultSlot<long> argumentSlot);

        Assert.Equal(
            GraphDocumentSerializer.Serialize(fromArguments.Document),
            GraphDocumentSerializer.Serialize(fromFactory.Document));
        Assert.Equal(fromArguments.Fingerprint, fromFactory.Fingerprint);
        Assert.Equal(argumentSlot.Id, factorySlot.Id);
        Assert.Equal("sink with result (1 stage)", viaFactory.ToString());
    }

    [Fact]
    public void TheAuthoringValuesDescribeThemselvesForDiagnostics()
    {
        // One is singular and every other count is plural, including zero: these lines are read by people.
        Assert.Equal("source (1 stage)", Source.From(OrderEvents).ToString());
        Assert.Equal("source (2 stages)", Source.From(OrderEvents).Where(o => o.IsValid).ToString());
        Assert.Equal("flow (0 stages)", Flow.For<OrderCreated>().ToString());
        Assert.Equal("flow (1 stage)", Flow.For<OrderCreated>().Where(o => o.IsValid).ToString());
        Assert.Equal("flow (2 stages)", Flow.For<OrderCreated>().Where(o => o.IsValid).Select(o => o.OrderId).ToString());
        Assert.Equal("sink (1 stage)", Sink.Ignore<OrderCreated>().ToString());
        Assert.Equal(
            "sink with result (1 stage)",
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1).ToString());
    }

    [Fact]
    public void AGraphDescribesItselfWithSingularAndPluralCounts()
    {
        RunnableGraph counted = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        RunnableGraph discarded = Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>());

        Assert.EndsWith("(2 nodes, 1 result slot)", counted.ToString(), StringComparison.Ordinal);
        Assert.EndsWith("(2 nodes, 0 result slots)", discarded.ToString(), StringComparison.Ordinal);
    }

    /// <summary>Obtains the sink factory of one element type the way a sink-factory lambda receives it.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <returns>The factory.</returns>
    /// <remarks>
    /// The factory is not constructible by an author: it arrives as the lambda's argument, and
    /// <see cref="Sink.For{T}"/> hands out that same instance. Capturing it out of a lambda is how a test
    /// gets at the one the overload actually passes, which is what makes the claim that the two are the
    /// same instance a claim about the overload rather than about the factory alone.
    /// </remarks>
    private static SinkFactory<T> SinkFactoryOf<T>()
    {
        SinkFactory<T>? captured = null;

        _ = Source.From(Array.Empty<T>()).To(
            factory =>
            {
                captured = factory;
                return factory.Aggregate(0L, (count, _) => count + 1);
            },
            "captured",
            out ResultSlot<long> _);

        return captured ?? throw new InvalidOperationException("The sink factory lambda was never invoked.");
    }
}
