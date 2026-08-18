using System.Diagnostics;
using System.Globalization;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Cluster;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The pipelines the adapter tests run, authored through the ordinary registered surface.
/// </summary>
/// <remarks>
/// Every one of these is written the way a user writes one: bindings declared once and handed both to the
/// silo and to <see cref="OrleansStages"/>, occurrences named, payloads produced by the same helpers, and
/// <c>AsPipeline</c> under a real identity. Nothing here hand-builds a document except the tests that are
/// about a document a user should never write.
/// </remarks>
internal static class AdapterPipelines
{
    /// <summary>The name every counting pipeline exposes its total under.</summary>
    internal const string TotalSlot = "total";

    /// <summary>Addresses a stream nothing else in the suite uses.</summary>
    /// <param name="name">The test's own name for the stream.</param>
    /// <returns>The address.</returns>
    /// <remarks>
    /// One stream per test, so a subscription count is a statement about one test rather than about
    /// whatever ran before it, and so a leftover element cannot reach the wrong run.
    /// </remarks>
    internal static OrleansStreamAddress Stream(string name) =>
        OrleansStreamAddress.Create(AdapterVocabulary.StreamProvider, "adapter-tests", name);

    /// <summary>Builds a pipeline that counts what a stream delivers.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="stream">The stream to subscribe to.</param>
    /// <param name="ingress">The bounded ingress the deliveries land in.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingStream(
        string id,
        OrleansStreamAddress stream,
        BufferOptions ingress,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.StreamSource(AdapterVocabulary.OrderElement),
                "orders",
                OrleansStages.StreamSourceParameters(AdapterVocabulary.OrderElement, stream, ingress))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that counts what a stream delivers, behind a gate.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="stream">The stream to subscribe to.</param>
    /// <param name="ingress">The bounded ingress the deliveries land in.</param>
    /// <param name="entered">The signal the gate raises when its first element reaches it.</param>
    /// <param name="release">The signal that releases the gate.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) GatedStream(
        string id,
        OrleansStreamAddress stream,
        BufferOptions ingress,
        string entered,
        string release,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.StreamSource(AdapterVocabulary.OrderElement),
                "orders",
                OrleansStages.StreamSourceParameters(AdapterVocabulary.OrderElement, stream, ingress))
            .Via(
                RegisteredStage.Flow(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Gate,
                    OrleansStages.Element<AdapterOrder>(),
                    OrleansStages.Element<AdapterOrder>()),
                "gate",
                AdapterVocabulary.GatePayload(entered, release))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds the end-to-end pipeline: a stream in, a grain call, and a stream out.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="orders">The stream of orders to subscribe to.</param>
    /// <param name="prices">The stream of prices to publish to.</param>
    /// <param name="ingress">The bounded ingress the deliveries land in.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// The one that closes phase 1's stated gap. Every element is an author's own record with
    /// <c>[GenerateSerializer]</c>, and it crosses a stream, a grain call, and a stream again, so a passing
    /// run is a statement about the serializer as well as about the path.
    /// </remarks>
    internal static PipelineDefinition StreamThroughGrainCall(
        string id,
        OrleansStreamAddress orders,
        OrleansStreamAddress prices,
        BufferOptions ingress)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.StreamSource(AdapterVocabulary.OrderElement),
                "orders",
                OrleansStages.StreamSourceParameters(AdapterVocabulary.OrderElement, orders, ingress))
            .Via(
                OrleansStages.GrainCall(AdapterVocabulary.Pricing),
                "priced",
                OrleansStages.GrainCallParameters(AdapterVocabulary.Pricing, maxInFlight: 1))
            .To(
                OrleansStages.StreamSink(AdapterVocabulary.PriceElement),
                "published",
                OrleansStages.StreamSinkParameters(AdapterVocabulary.PriceElement, prices));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline that reads a grain enumeration, prices it, and records the prices.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="source">The enumeration binding to open.</param>
    /// <param name="call">The pricing call to make.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once.</param>
    /// <param name="timeout">The per-call timeout, or <see langword="null"/>.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition PricedFeed(
        string id,
        GrainEnumerableBinding<AdapterOrder> source,
        GrainCallBinding<AdapterOrder, AdapterPrice> call,
        int maxInFlight = 1,
        TimeSpan? timeout = null,
        GrainCallSinkBinding<AdapterPrice>? sink = null,
        int sinkMaxInFlight = 1)
    {
        GrainCallSinkBinding<AdapterPrice> recording = sink ?? AdapterVocabulary.Recording;

        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(source),
                "feed",
                OrleansStages.GrainEnumerableParameters(source))
            .Via(
                OrleansStages.GrainCall(call),
                "priced",
                OrleansStages.GrainCallParameters(call, maxInFlight, timeout))
            .To(
                OrleansStages.GrainCallSink(recording),
                "recorded",
                OrleansStages.GrainCallSinkParameters(recording, sinkMaxInFlight));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds the pipeline the commit-mark crash test measures on: a cursored source into a
    /// terminating grain call.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="halt">The signal the source raises after its last element instead of ending.</param>
    /// <param name="gate">The signal the source waits for before one named element.</param>
    /// <param name="gateAt">The element the source waits at.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// <para>
    /// The only shape in this suite that has both halves of a checkpoint: the test vocabulary's range source
    /// declares a <em>cursor</em>, and the Orleans terminating grain call declares a <em>commit mark</em>. The
    /// stage between them exists to join their port contracts and does nothing else — a number becomes the
    /// total of a price and keeps its value — so the callee's log and the source's positions are the same
    /// numbers and a duplicate window is a sequence rather than an arithmetic claim.
    /// </para>
    /// <para>
    /// <b>The bound on calls in flight is one, and that is load-bearing.</b> A mark counts replies that have
    /// been observed, and the window's queue observes the oldest reply when it makes room for a new call, so
    /// a wider bound would let answered calls go uncounted and a capture taken then would store a low-water
    /// number. One makes the mark exactly "the calls whose replies have come back", which is the arrangement
    /// a test may name numbers in.
    /// </para>
    /// </remarks>
    internal static PipelineDefinition MarkedFeed(
        string id,
        int count,
        string halt,
        string gate,
        int gateAt)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(TestVocabulary.Catalog(), TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(count, halt, gate, gateAt))
            .Via(
                RegisteredStage.Flow(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Priced,
                    TestVocabulary.Number,
                    OrleansStages.Element<AdapterPrice>()),
                "priced",
                TestVocabulary.Empty)
            .To(
                OrleansStages.GrainCallSink(AdapterVocabulary.Logging),
                "logged",
                OrleansStages.GrainCallSinkParameters(AdapterVocabulary.Logging, maxInFlight: 1));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline that reads a grain enumeration, prices it by key, and records the prices.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="call">The keyed pricing call to make.</param>
    /// <param name="maxInFlight">The greatest number of calls in flight at once across keys.</param>
    /// <param name="distributed">Whether the keyed stage runs on per-key executor grains.</param>
    /// <param name="timeout">The per-call timeout, or <see langword="null"/>.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// The same three-node shape the unkeyed pricing pipeline has, so the two are comparable: what differs
    /// is the middle stage and its payload, which is the whole of what a document says about being keyed.
    /// </remarks>
    internal static PipelineDefinition KeyedPricedFeed(
        string id,
        KeyedGrainCallBinding<AdapterOrder, AdapterPrice> call,
        int maxInFlight = 1,
        bool distributed = false,
        TimeSpan? timeout = null)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.KeyedFeed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.KeyedFeed))
            .Via(
                OrleansStages.KeyedGrainCall(call),
                "priced",
                OrleansStages.KeyedGrainCallParameters(call, maxInFlight, distributed, timeout))
            .To(
                OrleansStages.GrainCallSink(AdapterVocabulary.Recording),
                "recorded",
                OrleansStages.GrainCallSinkParameters(AdapterVocabulary.Recording, 1));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose keyed grain call carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenKeyedCall(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.KeyedFeed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.KeyedFeed))
            .Via(OrleansStages.KeyedGrainCall(AdapterVocabulary.KeyedPricing), "priced", payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterPrice>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline that counts what a grain enumeration yields.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="source">The enumeration binding to open.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingFeed(
        string id,
        GrainEnumerableBinding<AdapterOrder> source,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(source),
                "feed",
                OrleansStages.GrainEnumerableParameters(source))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that counts the ticks a cluster reminder delivers.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="period">The period between ticks.</param>
    /// <param name="ingress">The bounded ingress the ticks land in.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many ticks are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingReminder(
        string id,
        TimeSpan period,
        BufferOptions ingress,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.ReminderTrigger(),
                "ticks",
                OrleansStages.ReminderTriggerParameters(period, ingress))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<long>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that counts what is pushed at an observer bridge.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="bridge">The bridge binding to publish.</param>
    /// <param name="ingress">The bounded ingress the pushes land in.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many pushes are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingBridge(
        string id,
        ObserverBridgeBinding<AdapterOrder> bridge,
        BufferOptions ingress,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.ObserverBridge(bridge),
                "pushed",
                OrleansStages.ObserverBridgeParameters(bridge, ingress))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline headed by a .NET observable, with no Orleans concept in its source.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="source">The observable binding, registered on the silo and on the local host alike.</param>
    /// <param name="ingress">The bounded ingress the notifications land in.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    /// <remarks>
    /// The cross-runtime claim made checkable: this document names <c>dotnet/observable@v1</c>, which the
    /// main package publishes and which needs no cluster, and a silo runs it because a silo registered the
    /// same binding a local host would.
    /// </remarks>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingObservable(
        string id,
        ObservableBinding<AdapterOrder> source,
        BufferOptions ingress,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                DotnetStages.Observable(source),
                "notes",
                DotnetStages.ObservableParameters(source, ingress))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.DotnetCount,
                    DotnetStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that counts what is pushed at a bridge, behind a gate.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="bridge">The bridge binding to publish.</param>
    /// <param name="ingress">The bounded ingress the pushes land in.</param>
    /// <param name="entered">The signal the gate raises when its first element reaches it.</param>
    /// <param name="release">The signal that releases it.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many pushes are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) GatedBridge(
        string id,
        ObserverBridgeBinding<AdapterOrder> bridge,
        BufferOptions ingress,
        string entered,
        string release,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.ObserverBridge(bridge),
                "pushed",
                OrleansStages.ObserverBridgeParameters(bridge, ingress))
            .Via(
                RegisteredStage.Flow(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Gate,
                    OrleansStages.Element<AdapterOrder>(),
                    OrleansStages.Element<AdapterOrder>()),
                "gate",
                AdapterVocabulary.GatePayload(entered, release))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that reads a grain enumeration and publishes it to a channel.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="channel">The channel to publish to.</param>
    /// <param name="fireAndForgetDelivery">The delivery mode the document declares.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition BroadcastFeed(
        string id,
        OrleansStreamAddress channel,
        bool fireAndForgetDelivery)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.Feed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.Feed))
            .To(
                OrleansStages.BroadcastSink(AdapterVocabulary.BroadcastOrder),
                "published",
                OrleansStages.BroadcastSinkParameters(
                    AdapterVocabulary.BroadcastOrder,
                    channel,
                    fireAndForgetDelivery));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline that counts what a Broadcast Channel publishes at it.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="provider">The broadcast provider whose publications the run consumes.</param>
    /// <param name="channel">The channel's key, within the package's own channel namespace.</param>
    /// <param name="ingress">The bounded ingress the publications land in.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) CountingBroadcast(
        string id,
        string provider,
        string channel,
        BufferOptions ingress,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.BroadcastSource(AdapterVocabulary.BroadcastOrder),
                "published",
                OrleansStages.BroadcastSourceParameters(
                    AdapterVocabulary.BroadcastOrder,
                    provider,
                    channel,
                    ingress))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline that counts what a channel publishes at it, behind a gate.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="provider">The broadcast provider whose publications the run consumes.</param>
    /// <param name="channel">The channel's key, within the package's own channel namespace.</param>
    /// <param name="ingress">The bounded ingress the publications land in.</param>
    /// <param name="entered">The signal the gate raises when its first element reaches it.</param>
    /// <param name="release">The signal that releases it.</param>
    /// <param name="signal">The signal the sink raises once it has seen enough.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The pipeline and the slot its total resolves under.</returns>
    /// <remarks>
    /// The gate is what makes the ingress observable from outside. A run held inside it takes nothing from
    /// its queue, so a test can fill the queue to its declared bound and watch the policy decide what
    /// happens to the element after that.
    /// </remarks>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) GatedBroadcast(
        string id,
        string provider,
        string channel,
        BufferOptions ingress,
        string entered,
        string release,
        string signal,
        int signalAt)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.BroadcastSource(AdapterVocabulary.BroadcastOrder),
                "published",
                OrleansStages.BroadcastSourceParameters(
                    AdapterVocabulary.BroadcastOrder,
                    provider,
                    channel,
                    ingress))
            .Via(
                RegisteredStage.Flow(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Gate,
                    OrleansStages.Element<AdapterOrder>(),
                    OrleansStages.Element<AdapterOrder>()),
                "gate",
                AdapterVocabulary.GatePayload(entered, release))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload(signal, signalAt),
                TotalSlot);

        return Close(graph, id);
    }

    /// <summary>Builds a pipeline whose broadcast source carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenBroadcastSource(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.BroadcastSource(AdapterVocabulary.BroadcastOrder),
                "published",
                payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Addresses a broadcast channel nothing else in the suite uses.</summary>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="name">The test's own name for the channel.</param>
    /// <returns>The address.</returns>
    internal static OrleansStreamAddress Channel(string provider, string name) =>
        OrleansStreamAddress.Create(provider, BroadcastObservations.ChannelNamespace, name);

    /// <summary>Builds a pipeline whose reminder trigger carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenReminder(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(OrleansStages.ReminderTrigger(), "ticks", payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<long>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose observer bridge carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenBridge(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(OrleansStages.ObserverBridge(AdapterVocabulary.OrderBridge), "pushed", payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose broadcast sink carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenBroadcast(string id, CanonicalJsonValue payload)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.Feed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.Feed))
            .To(OrleansStages.BroadcastSink(AdapterVocabulary.BroadcastOrder), "published", payload);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose grain call carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// The only place a document is written outside the helpers, and deliberately so: what these tests are
    /// about is a document naming something a silo does not publish, which the helpers cannot produce
    /// because they take a binding rather than a name.
    /// </remarks>
    internal static PipelineDefinition HandWrittenCall(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.Feed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.Feed))
            .Via(OrleansStages.GrainCall(AdapterVocabulary.Pricing), "priced", payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterPrice>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose terminating grain call carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenCallSink(string id, CanonicalJsonValue payload)
    {
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.GrainEnumerable(AdapterVocabulary.Feed),
                "feed",
                OrleansStages.GrainEnumerableParameters(AdapterVocabulary.Feed))
            .Via(
                OrleansStages.GrainCall(AdapterVocabulary.Pricing),
                "priced",
                OrleansStages.GrainCallParameters(AdapterVocabulary.Pricing, maxInFlight: 1))
            .To(OrleansStages.GrainCallSink(AdapterVocabulary.Recording), "recorded", payload);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose stream source carries a payload the test wrote by hand.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The pipeline.</returns>
    internal static PipelineDefinition HandWrittenStreamSource(string id, CanonicalJsonValue payload)
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(OrleansStages.StreamSource(AdapterVocabulary.OrderElement), "orders", payload)
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.Count,
                    OrleansStages.Element<AdapterOrder>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("unused", int.MaxValue),
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Publishes a run of orders into a stream from a grain's own context.</summary>
    /// <param name="cluster">The deployed cluster.</param>
    /// <param name="stream">The stream to publish to.</param>
    /// <param name="count">How many orders to publish, numbered from one.</param>
    /// <returns>A task that completes when the provider has accepted every one of them.</returns>
    internal static async Task PublishAsync(DataflowCluster cluster, OrleansStreamAddress stream, int count)
    {
        IAdapterStreamGrain producer = cluster.Cluster.Client.GetGrain<IAdapterStreamGrain>("producer");

        for (long index = 1; index <= count; index++)
        {
            await producer.PublishAsync(
                stream.Provider,
                stream.Namespace,
                stream.Key,
                new AdapterOrder(string.Create(CultureInfo.InvariantCulture, $"order-{index}"), index));
        }
    }

    /// <summary>Closes a counting graph under a real identity and recovers the pipeline's own slot.</summary>
    /// <param name="graph">The built graph.</param>
    /// <param name="id">The pipeline's identity.</param>
    /// <returns>The pipeline and its slot.</returns>
    private static (PipelineDefinition Pipeline, ResultSlot<long> Slot) Close(RunnableGraph graph, string id)
    {
        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));

        return (pipeline, pipeline.ResultSlot(TotalSlot, AdapterVocabulary.Total));
    }
}

/// <summary>
/// The one way these tests wait for something that has no signal of its own.
/// </summary>
/// <remarks>
/// A stream delivery is not a thing the run can raise a signal for — it happens inside a provider's pulling
/// agent — so a test that needs to know it has happened polls a condition at the cluster's own configured
/// poll interval. That is a bounded wait on a fact rather than a sleep on a guess: a slow machine takes more
/// turns and asserts the same thing, and the ambient test token is what stops a hung suite.
/// </remarks>
internal static class Poll
{
    /// <summary>The greatest number of turns a poll takes before it reports that it gave up.</summary>
    /// <remarks>
    /// Two minutes of wall clock, which is far longer than anything here takes and far shorter than
    /// forever. The bound exists so that a broken expectation fails with a sentence rather than hanging a
    /// suite: a poll that waits without end turns every regression into a timeout somewhere else. It is
    /// measured in elapsed time rather than in turns, because a turn count silently shrinks under CPU
    /// contention — a suite sharing the machine with two other test hosts once stretched a fifteen-second
    /// test to forty-nine and blew a thirty-second turn budget on a run that was healthy (observed
    /// 2026-08-19, the M7.2 pass) — while a deadline holds still whatever the scheduler is doing.
    /// </remarks>
    private static readonly TimeSpan PollBudget = TimeSpan.FromMinutes(2);

    /// <summary>Waits until a condition holds.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="expectation">What the caller is waiting for, for the message if it never happens.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    internal static Task UntilAsync(Func<bool> condition, string expectation) =>
        UntilAsync(() => Task.FromResult(condition()), expectation);

    /// <summary>Waits until a condition that has to be asked of the cluster holds.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="expectation">What the caller is waiting for, for the message if it never happens.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    internal static async Task UntilAsync(Func<Task<bool>> condition, string expectation)
    {
        CancellationToken token = TestContext.Current.CancellationToken;
        long started = Stopwatch.GetTimestamp();

        while (Stopwatch.GetElapsedTime(started) < PollBudget)
        {
            if (await condition())
            {
                return;
            }

            token.ThrowIfCancellationRequested();

            await Task.Delay(OrleansDataflowClientOptions.DefaultPollInterval, token);
        }

        Assert.Fail(
            $"Waited {Stopwatch.GetElapsedTime(started).TotalSeconds:F0}s and {expectation} never became true.");
    }
}
