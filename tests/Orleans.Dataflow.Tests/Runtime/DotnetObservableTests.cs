using System.Collections.Concurrent;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DotnetFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the observable bridge does with a producer that pushes: admit, drop, complete, fault, and let go.
/// </summary>
/// <remarks>
/// <para>
/// The observable is driven by the test rather than by a clock, so every assertion here is about an order
/// of events and never about a duration. The one probe that has to observe a thread being held says so and
/// proves the claim twice — once by the push not having returned, and once by the order in which things
/// were recorded afterwards.
/// </para>
/// <para>
/// Disposal is asserted on all four terminal paths, because "the subscription is dropped on every terminal
/// path" is the claim a push source most easily gets wrong: a producer nobody unsubscribed keeps pushing at
/// a run that has gone.
/// </para>
/// </remarks>
public sealed class DotnetObservableTests
{
    [Fact]
    public async Task PushedElementsReachTheRunAndOnCompletedEndsIt()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Push("a");
        observable.Push("b");
        observable.Complete();

        await run.Completion;

        Assert.Equal(["a", "b"], await run.GetValueAsync(seen, TestToken));
        await observable.DisposalsReach(1);
    }

    [Fact]
    public async Task OnErrorFaultsTheRunWithTheVeryExceptionItWasHanded()
    {
        InvalidTimeZoneException thrown = new("the producer gave up");
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("failing-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Fail(thrown);

        InvalidTimeZoneException faulted =
            await Assert.ThrowsAsync<InvalidTimeZoneException>(() => run.Completion);

        // The instance and not a copy of it: a bridge that wrapped the producer's exception would make
        // every author's catch block a guess about how many layers to unwrap.
        Assert.Same(thrown, faulted);
        await observable.DisposalsReach(1);
    }

    [Fact]
    public async Task ElementsQueuedBehindAFailureAreAbandoned()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("abandoned-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        Gate gate = new();
        ConcurrentQueue<string> folded = new();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Observable(binding),
                "notes",
                DotnetStages.ObservableParameters(binding, new BufferOptions { Capacity = 4 }))
            .To(
                sink => sink.Aggregate(0L, (count, note) =>
                {
                    gate.Wait();
                    folded.Enqueue(note);

                    return count + 1L;
                }),
                "counted",
                out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Push("a");

        // The run is inside the fold with the first element, so the two that follow are in the queue and
        // nowhere else when the failure arrives.
        await gate.Reached;

        observable.Push("b");
        observable.Push("c");
        observable.Fail(new InvalidTimeZoneException("stop"));

        gate.Open();

        _ = await Assert.ThrowsAsync<InvalidTimeZoneException>(() => run.Completion);

        Assert.Equal(["a"], folded);
    }

    [Fact]
    public async Task TheDeclaredOverflowPolicyDecidesWhatAFullIngressDoesWithAPush()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("dropping-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        Gate gate = new();
        ConcurrentQueue<string> folded = new();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Observable(binding),
                "notes",
                DotnetStages.ObservableParameters(
                    binding,
                    new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest }))
            .To(
                sink => sink.Aggregate(0L, (count, note) =>
                {
                    gate.Wait();
                    folded.Enqueue(note);

                    return count + 1L;
                }),
                "counted",
                out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Push("a");

        // The run is inside the fold with 'a', so the queue is empty: 'b' takes its one place and the two
        // after it arrive at a queue that is full. Nothing here is a race — the gate is what makes the
        // state of the queue at each push a fact.
        await gate.Reached;

        observable.Push("b");
        observable.Push("c");
        observable.Push("d");

        gate.Open();
        observable.Complete();

        await run.Completion;

        Assert.Equal(2L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(["a", "b"], folded);
    }

    [Fact]
    public async Task APushIntoAFullIngressHoldsTheProducersOwnThread()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("backpressured-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        Gate gate = new();
        ConcurrentQueue<string> order = new();
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Observable(binding),
                "notes",
                DotnetStages.ObservableParameters(
                    binding,
                    new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.Backpressure }))
            .To(
                sink => sink.Aggregate(0L, (count, note) =>
                {
                    gate.Wait();
                    order.Enqueue($"folded {note}");

                    return count + 1L;
                }),
                "counted",
                out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Push("a");

        // The run is inside the fold holding 'a', so the queue is empty and 'b' fills its one place.
        await gate.Reached;

        observable.Push("b");

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task pushing = Task.Run(
            () =>
            {
                entered.SetResult();
                observable.Push("c");
                order.Enqueue("pushed c");
            },
            TestToken);

        await entered.Task;

        // The producer's own thread is the one that waits, so the push has not returned while the run is
        // held. The claim is made twice: here, and by the order recorded once the gate opens.
        Assert.False(pushing.IsCompleted);

        gate.Open();

        await pushing;

        observable.Complete();

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal("folded a", order.First());
        Assert.Equal("pushed c", order.Last());
    }

    [Fact]
    public async Task TheSubscriptionIsDisposedWhenTheRunIsCancelled()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("cancelled-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);
        await run.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await observable.DisposalsReach(1);
    }

    [Fact]
    public async Task TheSubscriptionIsDisposedWhenTheRunIsShutDown()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("drained-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> seen);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(1);

        observable.Push("a");

        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // A shutdown stops production, so what the ingress still held is abandoned rather than drained, and
        // the collected list is whatever had already crossed into the graph.
        Assert.True((await run.GetValueAsync(seen, TestToken)).Count <= 1);
        await observable.DisposalsReach(1);
    }

    [Fact]
    public async Task AnObservableThatThrowsFromSubscribeFaultsTheRunAndLeavesNothingSubscribed()
    {
        InvalidTimeZoneException thrown = new("no producer today");
        TestObservable<string> observable = new();

        observable.FailOnSubscribe(thrown);

        ObservableBinding<string> binding = Binding("unsubscribable-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        InvalidTimeZoneException faulted =
            await Assert.ThrowsAsync<InvalidTimeZoneException>(() => run.Completion);

        Assert.Same(thrown, faulted);
        Assert.Equal(1, observable.Subscriptions);
        Assert.Equal(0, observable.Observers);

        // There was no subscription, so there is nothing to dispose and the count stays at zero: a failed
        // subscribe must not look like a leaked one.
        Assert.Equal(0, observable.Disposals);
    }

    [Fact]
    public async Task ABindingThatReturnsNoSequenceFaultsTheRunWithASentence()
    {
        ObservableBinding<string> binding = ObservableBinding.Create<string>(
            "absent-notes",
            NoteContract,
            static () => null!);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        InvalidOperationException faulted =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("absent-notes", faulted.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphSubscribeTwiceAndBothReceive()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> binding = Binding("shared-notes", observable);
        LocalDataflowHost host = HostFor(binding);
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> seen);

        await using RunHandle first = await host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await host.MaterializeAsync(graph, TestToken);

        await observable.SubscriptionsReach(2);

        observable.Push("a");
        observable.Complete();

        await first.Completion;
        await second.Completion;

        // Two subscriptions to one hot producer, so both runs see the element and neither steals it from
        // the other. What a cold observable would do instead is the observable's business and not this
        // stage's.
        Assert.Equal(2, observable.Subscriptions);
        Assert.Equal(["a"], await first.GetValueAsync(seen, TestToken));
        Assert.Equal(["a"], await second.GetValueAsync(seen, TestToken));
        await observable.DisposalsReach(2);
    }

    [Fact]
    public async Task AHostRefusesADocumentNamingAnObservableItDoesNotRegister()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> registered = Binding("registered-notes", observable);
        ObservableBinding<string> authored = Binding("unregistered-notes", new TestObservable<string>());
        LocalDataflowHost host = HostFor(registered);
        RunnableGraph graph = Graph(authored, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        InvalidOperationException refusal = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("unregistered-notes", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("registered-notes", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostRefusesADocumentWrittenAgainstADifferentElementContract()
    {
        TestObservable<string> observable = new();
        ObservableBinding<string> registered = Binding("typed-notes", observable);
        LocalDataflowHost host = HostFor(registered);

        // The same name over a different contract, which is the disagreement no CLR type system can catch
        // across a deployment boundary: the document says one signature and the host publishes another.
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Observable(registered),
                "notes",
                CanonicalJsonValue.Parse(
                    "{\"capacity\":4,\"output\":\"other-note@v1\",\"overflowPolicy\":\"backpressure\",\"source\":\"typed-notes\"}"))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<string>> _);

        InvalidOperationException refusal = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("other-note@v1", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("dotnet-note@v1", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABindingRegisteredTwiceIsRefusedWhenTheHostIsBuilt()
    {
        ObservableBinding<string> binding = Binding("twice", new TestObservable<string>());

        ArgumentException refusal = Assert.Throws<ArgumentException>(
            () => new LocalDataflowHost(dataflow => dataflow.AddObservable(binding).AddObservable(binding)));

        Assert.Contains("'twice' is registered more than once", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostThatPublishedTheVocabularyWithoutBindingsRefusesAtMaterializationRatherThanValidation()
    {
        // A registry with nothing in it checks no names at all, which is the same rule the Orleans adapters'
        // registry states: a process that registered nothing can check the shape of a payload and nothing
        // about which names a deployment publishes. The refusal therefore comes from the factory, and it
        // still comes before the run exists.
        LocalDataflowHost host = TimerHost();
        ObservableBinding<string> binding = Binding("nowhere", new TestObservable<string>());
        RunnableGraph graph = Graph(binding, new BufferOptions { Capacity = 4 }, out ResultSlot<IReadOnlyList<string>> _);

        InvalidOperationException refusal = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("nowhere", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("does not register", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAuthoringCatalogAndAHostsCatalogShareAFingerprint()
    {
        // A parameter validator is behavior and never reaches a fingerprint, so an authoring process
        // resolving these stages against the shared catalog produces a document a host accepts as one of
        // its own vocabulary. That is the property the whole publish-per-host arrangement rests on.
        Assert.Equal(
            Orleans.Dataflow.Serialization.StageCatalogSerializer.Fingerprint(DotnetStages.Catalog),
            Orleans.Dataflow.Serialization.StageCatalogSerializer.Fingerprint(
                DotnetStages.Publish(DotnetAdapterRegistry.Empty)));
    }

    [Fact]
    public void ABindingFactoryRefusesAnEmptyNameAndAnUndeclaredContract()
    {
        _ = Assert.Throws<ArgumentNullException>(
            "name",
            () => ObservableBinding.Create<string>(null!, NoteContract, static () => new TestObservable<string>()));
        _ = Assert.Throws<ArgumentException>(
            "name",
            () => ObservableBinding.Create<string>(" ", NoteContract, static () => new TestObservable<string>()));
        _ = Assert.Throws<ArgumentException>(
            "output",
            () => ObservableBinding.Create("named", default(ElementContract<string>), static () => new TestObservable<string>()));
        _ = Assert.Throws<ArgumentNullException>(
            "open",
            () => ObservableBinding.Create<string>("named", NoteContract, null!));
    }

    /// <summary>Builds the ordinary graph: subscribe to one binding and collect what arrives.</summary>
    /// <param name="binding">The binding the document names.</param>
    /// <param name="ingress">The bounded ingress the pushes land in.</param>
    /// <param name="seen">When this method returns, the slot the collected elements resolve.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Graph(
        ObservableBinding<string> binding,
        BufferOptions ingress,
        out ResultSlot<IReadOnlyList<string>> seen) =>
        Source
            .FromRegistered(
                DotnetStages.Observable(binding),
                "notes",
                DotnetStages.ObservableParameters(binding, ingress))
            .To(sink => sink.Collect(new CollectOptions { MaxElements = 16 }), "seen", out seen);
}
