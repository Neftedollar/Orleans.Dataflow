using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the catalog, the authoring surface, and the run planner do with a supervision policy that is wrong,
/// and what the document says about one that is right.
/// </summary>
/// <remarks>
/// <para>
/// Putting the policy in the document is what makes these tests possible and necessary at once. A policy
/// that lived only in a binding could not be wrong in a document and could not be checked by anything; a
/// policy in a payload can name a form nothing declares, carry an attempt count on a form that never
/// retries, or hold a chain of a shape a scope cannot execute — and each of those has to be a diagnostic
/// rather than a run that quietly supervises something else.
/// </para>
/// <para>
/// The retry members are the interesting half. They are present only for the retrying form, so the
/// unknown-member report is what refuses a resuming scope that carries a ladder: a number nothing reads is a
/// number a reader of the document would have to guess about.
/// </para>
/// </remarks>
public sealed class SupervisionPayloadTests
{
    [Theory]
    [InlineData("""{"scope":[]}""", "the member 'form' is missing")]
    [InlineData("""{"form":4,"scope":[]}""", "one of four form names")]
    [InlineData("""{"form":"stop","scope":[]}""", "a supervision form is one of 'resume', 'restart-stage', 'retry', and 'recover'")]
    [InlineData("""{"form":"resume"}""", "the member 'scope' is missing")]
    [InlineData("""{"form":"resume","scope":{}}""", "an array of the stages the scope is made of")]
    [InlineData("""{"form":"resume","scope":[],"maxAttempts":3}""", "'maxAttempts' is not one this stage declares")]
    [InlineData("""{"form":"resume","scope":[],"backoffTicks":[1]}""", "'backoffTicks' is not one this stage declares")]
    [InlineData("""{"form":"retry","scope":[],"onExhaustion":"fail","backoffTicks":[]}""", "the member 'maxAttempts' is missing")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":0,"onExhaustion":"fail","backoffTicks":[]}""", "is 0, and it is a positive integer")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":2,"backoffTicks":[]}""", "the member 'onExhaustion' is missing")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":2,"onExhaustion":"recover","backoffTicks":[]}""", "an exhaustion answer is one of 'fail', 'resume', and 'restart-stage'")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":2,"onExhaustion":"fail"}""", "the member 'backoffTicks' is missing")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":2,"onExhaustion":"fail","backoffTicks":3}""", "an array of tick counts of zero or more")]
    [InlineData("""{"form":"retry","scope":[],"maxAttempts":2,"onExhaustion":"fail","backoffTicks":[-1]}""", "rung 1 of the member 'backoffTicks' is -1")]
    public async Task AScopePayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(Scoped(payload), TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""[3]""", "stage 1 of the member 'scope' is not an object")]
    [InlineData("""[{"parameters":{}}]""", "has no 'stage' member naming which stage it is")]
    [InlineData("""[{"stage":"local/nope@v1","parameters":{}}]""", "and no local stage is called that")]
    [InlineData("""[{"stage":"local/select-async@v1","parameters":{"maxConcurrency":1}}]""", "a scope owns the execution of its chain element by element, so it holds element stages only")]
    [InlineData("""[{"stage":"local/select-many@v1","parameters":{}}]""", "a scope owns the execution of its chain element by element, so it holds element stages only")]
    [InlineData("""[{"stage":"local/supervised@v1","parameters":{"form":"resume","scope":[]}}]""", "a scope owns the execution of its chain element by element, so it holds element stages only")]
    [InlineData("""[{"stage":"local/select@v1"}]""", "has no 'parameters' member")]
    [InlineData("""[{"stage":"local/take@v1","parameters":{"count":-1}}]""", "carries parameters 'local/take@v1' refuses")]
    [InlineData("""[{"stage":"local/select@v1","parameters":{},"name":"mine"}]""", "carries members a scope stage does not")]
    [InlineData("""[{"stage":"local/select@v1","parameters":{}},{"stage":"local/never@v1","parameters":{}}]""", "stage 2 of the member 'scope'")]
    public async Task AScopeChainThisVocabularyCouldNotHaveWrittenIsRefusedStageByStage(
        string chain,
        string reason)
    {
        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(
                Scoped($$"""{"form":"resume","scope":{{chain}}}"""),
                TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFaultPointIsAdmittedInsideAScopeAndRefusedInsideAGroupFlow()
    {
        // Both readings come from the same reader the runtime uses, so a hand-written document and an
        // authored one are refused — and admitted — for the same reason in the same words.
        await Host.MaterializeAsync(
            Scoped(
                """{"form":"resume","scope":[{"stage":"local/fault-point@v1","parameters":{"mode":"never","firstFailure":1}}]}""",
                LocalStageDescriptor.FaultPoint(
                    LocalFaultMode.Never,
                    1,
                    new object?[] { (Func<long, Exception>)(_ => new FaultInjectedException()), null },
                    controlSlot: null,
                    controlType: null)),
            TestToken);

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(
                Keyed("""[{"stage":"local/fault-point@v1","parameters":{"mode":"never","firstFailure":1}}]"""),
                TestToken));

        Assert.Contains(
            "a group flow runs fused per key, so it holds element stages only",
            rejected.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"mode":"never"}""", "the member 'firstFailure' is missing")]
    [InlineData("""{"firstFailure":1}""", "the member 'mode' is missing")]
    [InlineData("""{"mode":"sometimes","firstFailure":1}""", "a fault-point mode is one of 'never', 'once', and 'always'")]
    [InlineData("""{"mode":2,"firstFailure":1}""", "one of three mode names")]
    [InlineData("""{"mode":"once","firstFailure":0}""", "is 0, and it is a positive integer")]
    [InlineData("""{"mode":"once","firstFailure":1,"seed":2}""", "'seed' is not one this stage declares")]
    public async Task AFaultPointPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "fault-point", "local-fault-point-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.FaultPoint(
                        LocalFaultMode.Never,
                        1,
                        new object?[] { (Func<long, Exception>)(_ => new FaultInjectedException()), null },
                        controlSlot: null,
                        controlType: null)),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException rejected = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", rejected.Message, StringComparison.Ordinal);
        Assert.Contains(reason, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRuntimeReadsThePolicyFromTheDocumentAndNotFromTheBinding()
    {
        // The binding declares three attempts and an escalation to resume, which would swallow the injected
        // failure and let the run finish; the payload declares one attempt and a failing exhaustion, which
        // does not. The run fails, so the document's policy is the one that ran — and an author who mutated
        // an options record after closing a graph would change nothing about a run of it.
        RunnableGraph graph = Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node(
                        "stage-2",
                        "supervised",
                        "local-supervision-parameters",
                        """{"form":"retry","maxAttempts":1,"onExhaustion":"fail","backoffTicks":[],"scope":[{"stage":"local/fault-point@v1","parameters":{"mode":"once","firstFailure":1}}]}"""),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                (
                    "stage-2",
                    LocalStageDescriptor.Supervised(
                        new SupervisionOptions
                        {
                            Form = SupervisionForm.Retry,
                            MaxAttempts = 3,
                            OnExhaustion = RetryExhaustion.Resume,
                        },
                        fallback: null,
                        [
                            LocalStageDescriptor.FaultPoint(
                                LocalFaultMode.Once,
                                1,
                                new object?[]
                                {
                                    (Func<long, Exception>)(_ => new FaultInjectedException()),
                                    null,
                                },
                                controlSlot: null,
                                controlType: null),
                        ])),
                ("stage-3", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        Assert.Equal(1, run.PoisonElements);
    }

    [Fact]
    public void TheDocumentStatesThePolicyAndTheChain()
    {
        RunnableGraph graph = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 3,
                    Backoff = [TimeSpan.FromMilliseconds(1), TimeSpan.Zero],
                    OnExhaustion = RetryExhaustion.RestartStage,
                },
                Flow.For<int>().Take(2).Select(value => value))
            .To(Sink.Ignore<int>());

        Assert.True(GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance).IsValid);

        StageNode scope = graph.Document.Nodes.Single(node => node.Stage.Stage.ToString() == "supervised");

        // Everything a cluster would need to honor the policy, and nothing a document cannot say: the form,
        // the three retry numbers, and one entry per stage with that stage's own reference and payload.
        Assert.Equal(
            """{"backoffTicks":[10000,0],"form":"retry","maxAttempts":3,"onExhaustion":"restart-stage","scope":[{"parameters":{"count":2},"stage":"local/take@v1"},{"parameters":{},"stage":"local/select@v1"}]}""",
            scope.Parameters.ToString());
    }

    [Fact]
    public void AResumingScopeWritesNoRetryMembersAtAll()
    {
        RunnableGraph graph = Source.From([1])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>())
            .To(Sink.Ignore<int>());

        StageNode scope = graph.Document.Nodes.Single(node => node.Stage.Stage.ToString() == "supervised");

        Assert.Equal("""{"form":"resume","scope":[]}""", scope.Parameters.ToString());
    }

    [Fact]
    public void TwoPoliciesAreTwoGraphs()
    {
        RunnableGraph resuming = Source.From([1])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.Resume }, Flow.For<int>())
            .To(Sink.Ignore<int>());

        RunnableGraph restarting = Source.From([1])
            .Supervised(new SupervisionOptions { Form = SupervisionForm.RestartStage }, Flow.For<int>())
            .To(Sink.Ignore<int>());

        RunnableGraph patient = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    Backoff = [TimeSpan.FromSeconds(1)],
                },
                Flow.For<int>())
            .To(Sink.Ignore<int>());

        RunnableGraph patient2 = Source.From([1])
            .Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    Backoff = [TimeSpan.FromSeconds(2)],
                },
                Flow.For<int>())
            .To(Sink.Ignore<int>());

        // A fingerprint changes when the policy does, which is the whole reason the policy is in the payload
        // rather than in the binding table.
        Assert.NotEqual(resuming.Fingerprint, restarting.Fingerprint);
        Assert.NotEqual(patient.Fingerprint, patient2.Fingerprint);
    }

    [Fact]
    public void ARecoveringScopeRefusesTheSpellingThatCarriesNoFallback()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Recover },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains("Use the overload that takes the fallback", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANonRecoveringScopeRefusesTheSpellingThatCarriesAFallback()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>(),
                fallback: -1));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains(
            "The other three forms drop the failing element and have nothing to emit in its place",
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ARetryOnlyMemberIsRefusedOnAFormThatDoesNotRetry()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume, MaxAttempts = 3 },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains("never re-offers an element", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ARetryingScopeRefusesAnAttemptCountBelowOne()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 0 },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
    }

    [Fact]
    public void ARetryingScopeRefusesANegativeRung()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    Backoff = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(-1)],
                },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains("Rung 2 of Backoff is negative", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AScopeRefusesAFormNoMemberDeclares()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions { Form = (SupervisionForm)9 },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
    }

    [Fact]
    public void AScopeRefusesAnExhaustionAnswerNoMemberDeclares()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.From([1]).Supervised(
                new SupervisionOptions
                {
                    Form = SupervisionForm.Retry,
                    MaxAttempts = 2,
                    OnExhaustion = (RetryExhaustion)9,
                },
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
    }

    [Fact]
    public void OptionsRenderTheirOwnSummary()
    {
        Assert.Equal(
            "supervised (Resume)",
            new SupervisionOptions { Form = SupervisionForm.Resume }.ToString());

        Assert.Equal(
            "supervised (Retry, 3 attempts, 2 rungs, RestartStage)",
            new SupervisionOptions
            {
                Form = SupervisionForm.Retry,
                MaxAttempts = 3,
                Backoff = [TimeSpan.Zero, TimeSpan.Zero],
                OnExhaustion = RetryExhaustion.RestartStage,
            }.ToString());
    }

    /// <summary>Builds a chain whose middle node is a scope carrying a payload written by hand.</summary>
    /// <param name="payload">The parameter payload as JSON text.</param>
    /// <param name="scope">The occurrences the binding declares the chain to be.</param>
    /// <returns>The graph, fingerprinted the way closing one would have fingerprinted it.</returns>
    private static RunnableGraph Scoped(string payload, params LocalStageDescriptor[] scope) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "supervised", "local-supervision-parameters", payload),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.Supervised(
                        new SupervisionOptions { Form = SupervisionForm.Resume },
                        fallback: null,
                        scope)),
                ("stage-3", LocalStageDescriptor.Ignore())));

    /// <summary>Builds a chain whose middle node is a keyed stage carrying a group flow written by hand.</summary>
    /// <param name="group">The group flow as JSON text.</param>
    /// <returns>The graph.</returns>
    private static RunnableGraph Keyed(string group) =>
        Graph(
            Document(
                [
                    Node("stage-1", "from-enumerable"),
                    Node(
                        "stage-2",
                        "group-by",
                        "local-group-by-parameters",
                        $$"""{"maxActiveKeys":2,"overflowPolicy":"fail","group":{{group}}}"""),
                    Node("stage-3", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Edge("stage-2", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.GroupBy(
                        new GroupByOptions { MaxActiveKeys = 2 },
                        (Func<int, int>)(value => value),
                        EqualityComparer<int>.Default,
                        [])),
                ("stage-3", LocalStageDescriptor.Ignore())));
}
