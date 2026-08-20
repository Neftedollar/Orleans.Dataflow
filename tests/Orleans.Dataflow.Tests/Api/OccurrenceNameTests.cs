using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The naming combinator: an author can give any local occurrence a name of its own, and a graph whose
/// occurrences are all named stops declaring <c>ephemeral-identity</c>.
/// </summary>
/// <remarks>
/// <para>
/// The token means "this document's node identifiers are positions", and until now it was unconditional for
/// a local graph because a local operator took no name at all. ADR 0009 states why the default must stay a
/// position rather than become a generated name — a random one differs between two runs of the same program
/// and a positional one renames itself when a stage is inserted above it — so what was missing was a
/// spelling, not a default. <c>Named</c> is that spelling.
/// </para>
/// <para>
/// These tests state both halves. A name has to be enough to drop the token, and the absence of one has to
/// keep it, or the token would stop meaning anything; and a name has to reach the document, or dropping the
/// token would be a claim about nothing.
/// </para>
/// </remarks>
public sealed class OccurrenceNameTests
{
    [Fact]
    public void AFullyNamedLocalGraphDeclaresNoEphemeralIdentity()
    {
        RunnableGraph graph = Named();

        // Asserted as an absence rather than as a smaller count: what makes a name worth writing is that
        // the token is gone, and a count that merely went down would pass for a graph that still declares it.
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(["intake", "priced", "queue", "total"], NodeIds(graph.Document));

        // And the other token is untouched, because naming an occurrence says where its identity comes from
        // and nothing about where its behavior lives. This graph is still local, so it is still nondeployable.
        Assert.Equal(["nondeployable"], Capabilities(graph.Document));
    }

    [Fact]
    public void OneUnnamedOccurrenceIsEnoughToKeepTheToken()
    {
        // The same graph with one name removed. The token is still doing its job: it reports the presence of
        // a positional identifier, not the absence of every name.
        RunnableGraph graph = Source.From([1, 2, 3])
            .Named("intake")
            .Select(value => value * 2)
            .Buffer(new BufferOptions { Capacity = 8 })
            .Named("queue")
            .To(s => s.Aggregate(0L, (sum, value) => sum + value).Named("total"), "answer", out ResultSlot<long> _);

        Assert.Contains(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
        Assert.Equal(["intake", "queue", "stage-0002", "total"], NodeIds(graph.Document));
    }

    [Fact]
    public void ANamedGraphIsADifferentDocumentFromTheUnnamedOne()
    {
        // The point of a name, stated as a fact about bytes: a node identifier is document content, so a
        // named graph is not the unnamed one with a label on the side. That is also why a graph that used to
        // declare ephemeral-identity and now does not has a different fingerprint — it says something
        // different about itself.
        RunnableGraph named = Named();
        RunnableGraph anonymous = Anonymous();

        Assert.NotEqual(anonymous.Fingerprint, named.Fingerprint);
        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(anonymous.Document));
        Assert.Equal(["intake", "priced", "queue", "total"], NodeIds(named.Document));

        // Same stages, in the same order, under different identities: nothing but the names moved.
        Assert.Equal(StageIds(anonymous.Document), StageIds(named.Document));
    }

    [Fact]
    public void RenamingOneStageOfAnOtherwiseIdenticalGraphProducesADifferentFingerprint()
    {
        RunnableGraph queue = Named();
        RunnableGraph holdback = Source.From([1, 2, 3])
            .Named("intake")
            .Select(value => value * 2)
            .Named("priced")
            .Buffer(new BufferOptions { Capacity = 8 })
            .Named("holdback")
            .To(s => s.Aggregate(0L, (sum, value) => sum + value).Named("total"), "answer", out ResultSlot<long> _);

        Assert.NotEqual(queue.Fingerprint, holdback.Fingerprint);
        Assert.Equal(["holdback", "intake", "priced", "total"], NodeIds(holdback.Document));
    }

    [Fact]
    public void BuildingTheSameNamedGraphTwiceProducesIdenticalBytes()
    {
        // The other half of the previous test, and the one that makes a name an identity rather than a
        // decoration: two builds of one program are one document, byte for byte, so a checkpoint written
        // against the first anchors to the second. The bytes are compared and not only the fingerprint,
        // because a fingerprint is a hash and this is the claim the hash is standing in for.
        RunnableGraph first = Named();
        RunnableGraph second = Named();

        Assert.Equal(first.Fingerprint, second.Fingerprint);
        Assert.Equal(
            GraphDocumentSerializer.Serialize(first.Document),
            GraphDocumentSerializer.Serialize(second.Document));
    }

    [Fact]
    public void ANamedOccurrenceBindsItsDelegateUnderTheAuthorsName()
    {
        // The binding table is keyed by the identifier the document declares, so a name has to move the key
        // with it. Nothing else in a named graph could tell a runtime which delegate belongs to which node.
        Func<int, int> selector = value => value * 2;

        RunnableGraph graph = Source.From([1])
            .Select(selector)
            .Named("priced")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Same(selector, graph.LocalBindings[NodeId.Create("priced")].Behavior);
        Assert.Equal(LocalStageKind.Ignore, graph.LocalBindings[NodeId.Create("out")].Kind);
        Assert.DoesNotContain(NodeId.Create("stage-0002"), graph.LocalBindings.Keys);
    }

    [Fact]
    public async Task ANamedGraphRunsAndProducesWhatTheUnnamedOneProduces()
    {
        // Naming changes the document and must change nothing else. Both graphs are materialized rather than
        // only closed, because the binding table is keyed by node identifier and a name that reached the
        // document without reaching the table would close fine and fail at run time.
        LocalDataflowHost host = new();
        CancellationToken token = TestContext.Current.CancellationToken;

        RunnableGraph anonymous = Source.From([1, 2, 3])
            .Select(value => value * 2)
            .Buffer(new BufferOptions { Capacity = 8 })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "answer", out ResultSlot<long> unnamedTotal);

        RunnableGraph named = Source.From([1, 2, 3])
            .Named("intake")
            .Select(value => value * 2)
            .Named("priced")
            .Buffer(new BufferOptions { Capacity = 8 })
            .Named("queue")
            .To(s => s.Aggregate(0L, (sum, value) => sum + value).Named("total"), "answer", out ResultSlot<long> namedTotal);

        await using (RunHandle run = await host.MaterializeAsync(anonymous, token))
        {
            Assert.Equal(12L, await run.GetValueAsync(unnamedTotal, token));
            await run.Completion;
        }

        await using (RunHandle run = await host.MaterializeAsync(named, token))
        {
            Assert.Equal(12L, await run.GetValueAsync(namedTotal, token));
            await run.Completion;
        }
    }

    [Fact]
    public void NamingAnAlreadyNamedOccurrenceIsRefusedRatherThanPerformed()
    {
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => Source.From([1]).Named("intake").Named("inlet"));

        Assert.Contains("already named 'intake'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("naming it 'inlet'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("identity rather than a label", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NamingARegisteredOccurrenceIsRefusedByTheNameItAlreadyCarries()
    {
        // A registered occurrence is named where it is attached and is always named, so the general refusal
        // says the right thing here without a case of its own.
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => Source.FromRegistered(OrderSource, "orders-in", SourceParameters).Named("intake"));

        Assert.Contains("already named 'orders-in'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NamingTheIdentityFlowIsRefusedBecauseThereIsNothingToName()
    {
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => Flow.For<int>().Named("nothing"));

        Assert.Contains("no occurrence for 'nothing' to name", refused.Message, StringComparison.Ordinal);
        Assert.Contains("identity flow", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnInvalidNameIsRefusedByTheNodeIdentifierGrammarNamingTheAuthorsParameter()
    {
        // NodeId owns the grammar and the sentence; the only thing corrected is the parameter name, because
        // the author wrote an occurrence name and not a NodeId value. This is the same message and the same
        // parameter a registered attachment produces for the same text.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Named("Not_A_Segment"));

        ArgumentException registered = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "Not_A_Segment", SourceParameters));

        Assert.Equal("occurrenceName", refused.ParamName);
        Assert.Contains("Not_A_Segment", refused.Message, StringComparison.Ordinal);
        Assert.Contains("[a-z0-9]+(-[a-z0-9]+)*", refused.Message, StringComparison.Ordinal);
        Assert.Equal(registered.Message, refused.Message);
    }

    [Fact]
    public void AMultiSegmentPathIsNotAnOccurrenceName()
    {
        // An occurrence names itself; the path structure of a NodeId exists for import scoping, which is the
        // fragment algebra's business and not the author's.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Named("orders/intake"));

        Assert.Equal("occurrenceName", refused.ParamName);
        Assert.Contains("orders/intake", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoOccurrencesUnderOneNameAreRefusedByTheRuleTwoRegisteredOnesMeet()
    {
        // Consistency rather than a second rule: the fragment algebra reports a shared identifier when the
        // shape is composed, and it does not care which kind of occurrence carried it. Both refusals are
        // asserted together so that a change to either would have to change both.
        ArgumentException local = Assert.Throws<ArgumentException>(
            () => Source.From([1])
                .Named("twice")
                .Select(value => value)
                .Named("twice")
                .To(Sink.Ignore<int>()));

        ArgumentException registered = Assert.Throws<ArgumentException>(
            () => Source.FromRegistered(OrderSource, "twice", SourceParameters)
                .Via(Normalize, "twice", NormalizeParameters)
                .To(Sink.Ignore<OrderDocument>()));

        Assert.Contains("share 1 node id", local.Message, StringComparison.Ordinal);
        Assert.Contains("'twice'", local.Message, StringComparison.Ordinal);
        Assert.Contains("share 1 node id", registered.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnAuthorMayWriteAutomaticLookingNameAndCollidesWithOneExactlyAsWithAnyOther()
    {
        // A name in the automatic form is a legal segment and is accepted, because whether it collides is a
        // fact about the whole graph rather than about the text. When it does collide, it collides through
        // the one collision rule and not through a second grammar written to forbid the shape.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1])
                .Named("stage-0002")
                .Select(value => value)
                .To(Sink.Ignore<int>()));

        Assert.Contains("share 1 node id", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'stage-0002'", refused.Message, StringComparison.Ordinal);

        // And it is accepted wherever it does not collide.
        RunnableGraph accepted = Source.From([1])
            .Named("stage-0009")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(["out", "stage-0009"], NodeIds(accepted.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, accepted.Document.Capabilities);
    }

    [Fact]
    public void NamingIsAValueOperationAndLeavesTheReceiverAlone()
    {
        Source<int> plain = Source.From([1]);
        Source<int> named = plain.Named("intake");

        Assert.NotSame(plain, named);
        Assert.Equal(
            ["out", "stage-0001"],
            NodeIds(plain.To(Sink.Ignore<int>().Named("out")).Document));
        Assert.Equal(
            ["intake", "out"],
            NodeIds(named.To(Sink.Ignore<int>().Named("out")).Document));
    }

    [Fact]
    public void ATapNamesItsJunctionAndNotTheBranchTerminal()
    {
        // The reason the rule is "the occurrence this value ends at" rather than "the last occurrence added":
        // a tap appends the branch's stages after the junction, and the branch named its own stages where
        // they were written. The junction is the one occurrence the call contributes with no other spelling.
        RunnableGraph graph = Source.From([1, 2])
            .Named("intake")
            .AlsoTo(Flow.For<int>().To(Sink.Ignore<int>().Named("audit")))
            .Named("tee")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(["audit", "intake", "out", "tee"], NodeIds(graph.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(
            "local/broadcast@v1",
            Assert.Single(graph.Document.Nodes, node => node.Id.Value == "tee").Stage.ToString());
    }

    [Fact]
    public void AFanInNamesItsJunction()
    {
        RunnableGraph graph = Source.From([1])
            .Named("left")
            .Merge(Source.From([2]).Named("right"))
            .Named("both")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(["both", "left", "out", "right"], NodeIds(graph.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(
            "local/merge@v1",
            Assert.Single(graph.Document.Nodes, node => node.Id.Value == "both").Stage.ToString());
    }

    [Fact]
    public void AForksBroadcastIsNamedByAnArgumentBecauseAForkEndsAtTwoOccurrences()
    {
        // A diamond is the shape with the most occurrences an author never wrote by hand, and every one of
        // them has a spelling. The broadcast is not one a combinator can reach — a fork has two open ends,
        // so "the occurrence this value ends at" has two answers — so the fork call takes the name, exactly
        // as a closing fan-out does. The rejoin is a source again and is named the usual way.
        RunnableGraph graph = Source.From([1])
            .Named("intake")
            .Fork(
                "split",
                Flow.For<int>().Select(value => value + 1).Named("left"),
                Flow.For<int>().Select(value => value - 1).Named("right"))
            .Zip()
            .Named("paired")
            .To(Sink.Ignore<(int, int)>().Named("out"));

        Assert.Equal(["intake", "left", "out", "paired", "right", "split"], NodeIds(graph.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(
            "local/broadcast@v1",
            Assert.Single(graph.Document.Nodes, node => node.Id.Value == "split").Stage.ToString());

        // The unnamed spelling still numbers it, so the token still reports the truth.
        Assert.Contains(
            CapabilityToken.EphemeralIdentity,
            Source.From([1])
                .Named("intake")
                .Fork(
                    Flow.For<int>().Select(value => value + 1).Named("left"),
                    Flow.For<int>().Select(value => value - 1).Named("right"))
                .Zip()
                .Named("paired")
                .To(Sink.Ignore<(int, int)>().Named("out"))
                .Document.Capabilities);
    }

    [Fact]
    public void AForkMergeNamesItsBroadcastByArgumentAndItsMergeByCombinator()
    {
        // The one call that adds two junctions. The merge is the occurrence the answering source ends at, so
        // the combinator reaches it; the broadcast is the one with no other spelling, so it takes the
        // argument. Together they leave nothing in a diamond unnamed.
        RunnableGraph graph = Source.From([1])
            .Named("intake")
            .ForkMerge(
                "split",
                Flow.For<int>().Select(value => value + 1).Named("left"),
                Flow.For<int>().Select(value => value - 1).Named("right"))
            .Named("raced")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(["intake", "left", "out", "raced", "right", "split"], NodeIds(graph.Document));
        Assert.Equal(["nondeployable"], Capabilities(graph.Document));
    }

    [Fact]
    public void AClosingFanOutNamesItsJunctionThroughAnArgument()
    {
        // A closing fan-out answers with a document rather than a value, so there is nothing left to write
        // Named on and the name is an argument — which is how the registered fan-out has always spelled it.
        RunnableGraph graph = Source.From([1, 2])
            .Named("intake")
            .BroadcastTo(
                "tee",
                Flow.For<int>().To(Sink.Ignore<int>().Named("first")),
                Flow.For<int>().To(Sink.Ignore<int>().Named("second")));

        Assert.Equal(["first", "intake", "second", "tee"], NodeIds(graph.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(["nondeployable"], Capabilities(graph.Document));
    }

    [Fact]
    public void EveryClosingFanOutTakesTheNameAndTheUnnamedSpellingStillNumbersIt()
    {
        Branch<int> first = Flow.For<int>().To(Sink.Ignore<int>().Named("first"));
        Branch<int> second = Flow.For<int>().To(Sink.Ignore<int>().Named("second"));
        Source<int> head = Source.From([1, 2]).Named("intake");

        Assert.DoesNotContain(
            CapabilityToken.EphemeralIdentity,
            head.BalanceTo("spread", first, second).Document.Capabilities);
        Assert.DoesNotContain(
            CapabilityToken.EphemeralIdentity,
            head.PartitionTo(static value => value % 2, "route", first, second).Document.Capabilities);
        Assert.DoesNotContain(
            CapabilityToken.EphemeralIdentity,
            Source.From<(int Left, int Right)>([(1, 2)])
                .Named("pairs")
                .UnzipTo("split", first, second)
                .Document.Capabilities);

        // And the unnamed spelling still numbers the junction, so the token is still reporting the truth.
        Assert.Contains(
            CapabilityToken.EphemeralIdentity,
            head.BalanceTo(first, second).Document.Capabilities);
    }

    [Fact]
    public void AFullyNamedBranchingGraphDeclaresNoEphemeralIdentity()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Named("intake")
            .DivertTo(
                static value => value < 0,
                Flow.For<int>().Buffer(new BufferOptions { Capacity = 2 }).Named("hold").To(Sink.Ignore<int>().Named("rejects")))
            .Named("classify")
            .Select(value => value * 2)
            .Named("priced")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(
            ["classify", "hold", "intake", "out", "priced", "rejects"],
            NodeIds(graph.Document));
        Assert.Equal(["nondeployable"], Capabilities(graph.Document));
    }

    [Fact]
    public void ANamedStageIsRefusedInsideAGroupFlow()
    {
        // The stages of a group flow are fused into the keyed stage's payload and are not nodes, so a name
        // written on one would name nothing. Dropping it silently would be worse than refusing it: the
        // author would have written a durable identity, watched the graph accept it, and got a document that
        // still declares ephemeral-identity with no statement of why.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1])
                .GroupBy(
                    new GroupByOptions { MaxActiveKeys = 4 },
                    static value => value % 2,
                    Flow.For<int>().Select(value => value + 1).Named("inner")));

        Assert.Equal("group", refused.ParamName);
        Assert.Contains("not nodes of the document", refused.Message, StringComparison.Ordinal);
        Assert.Contains("named 'inner'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("Name the keyed occurrence", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANamedStageIsRefusedInsideASupervisionScope()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1])
                .Supervised(
                    new SupervisionOptions { Form = SupervisionForm.Resume },
                    Flow.For<int>().Select(value => value + 1).Named("inner")));

        Assert.Equal("scope", refused.ParamName);
        Assert.Contains("A supervision scope's stages are not nodes", refused.Message, StringComparison.Ordinal);
        Assert.Contains("named 'inner'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANamedStageIsRefusedInsideADurableScope()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Durable(Flow.For<int>().Select(value => value + 1).Named("inner")));

        Assert.Equal("scope", refused.ParamName);
        Assert.Contains("A durable scope's stages are not nodes", refused.Message, StringComparison.Ordinal);
        Assert.Contains("named 'inner'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANamedGraphOfLocalStagesIsStillRefusedAsAPipeline()
    {
        // The two tokens are independent and this is what that costs: a fully named local graph has dropped
        // one of the two reasons it is not deployable and keeps the other, so AsPipeline still refuses it and
        // names the reason that remains. Making the delegate-free half of the vocabulary drop
        // 'nondeployable' is ADR 0009's other half and not this one.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Named().AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("breaks 1 deployability invariant", refused.Message, StringComparison.Ordinal);
        Assert.Contains("nondeployable", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("ephemeral-identity", refused.Message, StringComparison.Ordinal);

        // And the unnamed twin breaks both, which is the measurement ADR 0009 opens with.
        Assert.Contains(
            "breaks 2 deployability invariants",
            Assert.Throws<ArgumentException>(
                () => Anonymous().AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1))).Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TheElementShorthandsSynthesiseASourceWhoseNameIsWrittenOnTheLongerForm()
    {
        // Prepend and Append have an element-array shorthand that is documented as being
        // `Prepend(Source.From(elements))` and nothing more. That synthesised source is an occurrence like
        // any other and the shorthand has nowhere to name it, so a graph that uses the shorthand keeps
        // 'ephemeral-identity'; the expansion the documentation already names is what an author who wants it
        // named writes. This is the whole of what the combinator cannot reach, and it is stated here rather
        // than left for a reader to find.
        RunnableGraph shorthand = Source.From([2, 3])
            .Named("main")
            .Prepend(1)
            .Named("joined")
            .To(Sink.Ignore<int>().Named("out"));

        RunnableGraph expansion = Source.From([2, 3])
            .Named("main")
            .Prepend(Source.From([1]).Named("header"))
            .Named("joined")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Contains(CapabilityToken.EphemeralIdentity, shorthand.Document.Capabilities);
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, expansion.Document.Capabilities);
        Assert.Equal(["header", "joined", "main", "out"], NodeIds(expansion.Document));
    }

    [Fact]
    public void AControlBearingStageCarriesItsNodeNameAndItsControlNameSeparately()
    {
        // Two names on one occurrence that mean different things: the node identifier is what the document
        // declares this stage as, and the control name is what a run handle resolves its runtime control by.
        // Naming the node must not disturb the control, and the graph must still declare the slot.
        RunnableGraph graph = Source.From([1, 2])
            .Named("intake")
            .Valve("gate")
            .Named("holdback")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.Equal(["holdback", "intake", "out"], NodeIds(graph.Document));
        Assert.Equal("gate", Assert.Single(graph.Document.ResultSlots).Id.Value);
        Assert.Equal("holdback", Assert.Single(graph.Document.ResultSlots).Producer.Node.Value);
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
    }

    [Fact]
    public void ANamedDocumentStillValidatesAgainstTheLocalCatalog()
    {
        // The name reaches a document, and the document is one the compiler accepts: a node identifier is
        // the one thing naming changes, and no catalog rule is about identifiers. Without this the token
        // could have been dropped over a document nothing else would read.
        GraphValidationReport report =
            GraphCompiler.Validate(Named().Document, LocalStageCatalog.Instance);

        Assert.True(report.IsValid);
        Assert.Empty(report.Diagnostics);
    }

    [Fact]
    public void AReusedNamedFlowIsTwoOccurrencesInOneGraphAndCollides()
    {
        // The cost of a name on a reusable value, and it is the registered surface's cost exactly: a name is
        // an identity rather than a position, so a flow carrying one composes into any number of graphs but
        // twice into one graph is a collision. An unnamed flow is still positional and still composes twice.
        Flow<int, int> named = Flow.For<int>().Select(value => value * 2).Named("priced");

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1]).Via(named).Via(named).To(Sink.Ignore<int>()));

        Assert.Contains("share 1 node id", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'priced'", refused.Message, StringComparison.Ordinal);

        Flow<int, int> anonymous = Flow.For<int>().Select(value => value * 2);

        Assert.Equal(
            ["stage-0001", "stage-0002", "stage-0003", "stage-0004"],
            NodeIds(Source.From([1]).Via(anonymous).Via(anonymous).To(Sink.Ignore<int>()).Document));
    }

    [Fact]
    public void ANameSurvivesTheDocumentsOwnSerialisationRoundTrip()
    {
        // A name is document content, so it has to come back out of the bytes: a name that lived only in the
        // authoring value would make the fingerprint a lie.
        GraphDocument round = GraphDocumentSerializer.Deserialize(
            GraphDocumentSerializer.Serialize(Named().Document));

        Assert.Equal(["intake", "priced", "queue", "total"], NodeIds(round));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, round.Capabilities);
    }

    [Fact]
    public void NoOccurrenceOfAWidelyNamedGraphIsLeftPositional()
    {
        // An invariant rather than a fact: the ground truth is re-derived from the document — no identifier
        // in the automatic form, and no automatic-numbering token — instead of being compared against a list
        // written beside the graph. A list would pass for a graph that had quietly lost a stage; this cannot.
        // The chain deliberately spans every shape a linear value can hold, including the three that are not
        // reachable from Source alone: a testing probe's queue source, a fault point inside a flow, and a
        // control-bearing stage.
        RunnableGraph graph = Testing.TestSource.Probe<int>("feed")
            .Named("intake")
            .Where(value => value > 0)
            .Named("positive")
            .Select(value => value * 2)
            .Named("priced")
            .Buffer(new BufferOptions { Capacity = 4 })
            .Named("queue")
            .Take(10)
            .Named("bounded")
            .Skip(1)
            .Named("dropped")
            .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
            .Named("unique")
            .Grouped(2)
            .Named("batched")
            .SelectMany(batch => batch)
            .Named("flattened")
            .Valve("gate")
            .Named("holdback")
            .Via(Testing.TestFlow.FaultPoint<int>(Testing.FaultPointMode.Never, 1).Named("injected"))
            .Delay(TimeSpan.FromMilliseconds(1), new BufferOptions { Capacity = 4 })
            .Named("held")
            .To(Sink.Ignore<int>().Named("out"));

        Assert.DoesNotContain(
            graph.Document.Nodes,
            node => node.Id.Value.StartsWith("stage-", StringComparison.Ordinal));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Equal(["nondeployable"], Capabilities(graph.Document));

        // And the binding table agrees with the document, key for key: a name that reached one and not the
        // other would be a graph that closes and then fails to plan.
        Assert.Equal(
            [.. graph.Document.Nodes.Select(node => node.Id).Order()],
            [.. graph.LocalBindings.Keys.Order()]);
    }

    [Fact]
    public void DiscardingAResultKeepsTheTerminalsName()
    {
        // ToSink discards the result declaration and nothing else, so a name written on the result-bearing
        // carrier has to survive the conversion — otherwise dropping a result would quietly reintroduce
        // 'ephemeral-identity'.
        Sink<int> discarded = Sink.Aggregate<int, long>(0L, (count, _) => count + 1)
            .Named("counted")
            .ToSink();

        RunnableGraph graph = Source.From([1]).Named("intake").To(discarded);

        Assert.Equal(["counted", "intake"], NodeIds(graph.Document));
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities);
        Assert.Empty(graph.Document.ResultSlots);
    }

    /// <summary>Builds the reference graph with every occurrence named.</summary>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// A source, an element stage, a buffer, and a terminal: one occurrence of each shape a chain can hold,
    /// so that "every occurrence" is a claim about four kinds rather than about four calls.
    /// </remarks>
    private static RunnableGraph Named() =>
        Source.From([1, 2, 3])
            .Named("intake")
            .Select(value => value * 2)
            .Named("priced")
            .Buffer(new BufferOptions { Capacity = 8 })
            .Named("queue")
            .To(s => s.Aggregate(0L, (sum, value) => sum + value).Named("total"), "answer", out ResultSlot<long> _);

    /// <summary>Builds the same graph with nothing named.</summary>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Anonymous() =>
        Source.From([1, 2, 3])
            .Select(value => value * 2)
            .Buffer(new BufferOptions { Capacity = 8 })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "answer", out ResultSlot<long> _);
}
