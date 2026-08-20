using System.Runtime.CompilerServices;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a vocabulary written in one place produces, and above all that it produces exactly what the same
/// vocabulary written in three places produced.
/// </summary>
/// <remarks>
/// <para>
/// The convenience is only worth having if it is not a second way of saying something: the catalog it
/// publishes has to be the catalog a hand-written one publishes, the document a graph over it closes to has
/// to be the same document, and the runtime it builds has to be the same runtime. Those three are asserted
/// against a hand-written control rather than against recorded constants, so the comparison re-derives the
/// answer instead of pinning a number that a shared mistake would move.
/// </para>
/// <para>
/// The refusals are the other half. A vocabulary that let a stage of another provider in, or let one
/// reference be declared twice, or let a stage be added after its catalog had been registered would be
/// three new ways to produce the very drift this type exists to prevent.
/// </para>
/// </remarks>
public sealed class StageProviderTests
{
    /// <summary>The provider both vocabularies below declare.</summary>
    private const string ProviderName = "weather";

    /// <summary>The payload member both stages read.</summary>
    private const string CountMember = "n";

    /// <summary>The reference of the reading feed.</summary>
    private static readonly StageRef FeedStage = StageRef.For(ProviderName, "reading-feed");

    /// <summary>The reference of the tallying terminal.</summary>
    private static readonly StageRef TallyStage = StageRef.For(ProviderName, "tally");

    /// <summary>The contract of the elements both stages carry.</summary>
    private static readonly ElementContract<int> Reading = ElementContract.For<int>("weather-reading", 1);

    /// <summary>The contract of the tally the terminal answers with.</summary>
    private static readonly ResultContract<long> Total = ResultContract.For<long>("weather-total", 1);

    /// <summary>The contract of both stages' payloads.</summary>
    private static readonly ContractReference Parameters = ContractReference.For("weather-parameters");

    [Fact]
    public void TheCatalogItPublishesIsTheCatalogAHandWrittenOnePublishes()
    {
        StageCatalog control = StageCatalog.Create(
        [
            StageSpecification.Source(FeedStage, Parameters, Port.Out("out", Reading)),
            StageSpecification.Sink(TallyStage, Parameters, Port.In("in", Reading), Port.Result("total", Total)),
        ]);

        StageCatalog published = Vocabulary().Catalog;

        Assert.Equal(control.Specifications, published.Specifications);
        Assert.Equal(
            StageCatalogSerializer.Fingerprint(control),
            StageCatalogSerializer.Fingerprint(published));
    }

    [Fact]
    public async Task ARunThroughAddProviderProducesTheSameDocumentAndTheSameAnswer()
    {
        StageProvider vocabulary = Vocabulary();

        StageCatalog handWritten = StageCatalog.Create(
        [
            StageSpecification.Source(FeedStage, Parameters, Port.Out("out", Reading)),
            StageSpecification.Sink(TallyStage, Parameters, Port.In("in", Reading), Port.Result("total", Total)),
        ]);

        LocalDataflowHost byProvider = new(builder => builder.AddProvider(vocabulary));
        LocalDataflowHost byHalves = new(builder => builder
            .AddCatalog(handWritten)
            .AddFactory(ProviderId.Create(ProviderName), new HandWrittenFactory()));

        RunnableGraph one = Graph(vocabulary.Catalog, out ResultSlot<long> first);
        RunnableGraph two = Graph(handWritten, out ResultSlot<long> second);

        Assert.Equal(one.Fingerprint, two.Fingerprint);

        await using RunHandle byProviderRun = await byProvider.MaterializeAsync(one, TestToken);
        await using RunHandle byHalvesRun = await byHalves.MaterializeAsync(two, TestToken);

        await byProviderRun.Completion;
        await byHalvesRun.Completion;

        Assert.Equal(6L, await byProviderRun.GetValueAsync(first, TestToken));
        Assert.Equal(6L, await byHalvesRun.GetValueAsync(second, TestToken));
    }

    [Fact]
    public void ItPassesEveryConformanceCheckThatAHandWrittenProviderMust()
    {
        // The conformance kit is the standard a published provider is held to, and it is deliberately run
        // here through the two-argument overload: a vocabulary that carries both halves supplies three of
        // the kit's four arguments, so the check that a convenience produces a conforming provider is
        // itself one call. The payload validators are what the kit insists on and what the overloads
        // taking one exist for.
        ProviderConformance kit = ProviderConformance.Create(
            ValidatingVocabulary(),
            [
                ProviderStageSample.Create(FeedStage, Payload(10)),
                ProviderStageSample.Create(TallyStage, Payload(5)),
            ]);

        foreach (string check in ProviderConformance.Checks)
        {
            kit.Check(check);
        }
    }

    [Fact]
    public void AStageOfAnotherProviderIsRefusedWhereItIsDeclared()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => StageProvider
                .Create(ProviderName)
                .Source(
                    StageRef.For("somebody-else", "reading-feed"),
                    Parameters,
                    Port.Out("out", Reading),
                    Feed));

        Assert.Equal("stage", refused.ParamName);
        Assert.Contains("somebody-else/reading-feed@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OneStageDeclaredTwiceIsRefusedAtTheSecondDeclaration()
    {
        StageProvider vocabulary = StageProvider
            .Create(ProviderName)
            .Source(FeedStage, Parameters, Port.Out("out", Reading), Feed);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => vocabulary.Source(FeedStage, Parameters, Port.Out("out", Reading), Feed));

        Assert.Equal("stage", refused.ParamName);
        Assert.Contains("declared twice", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStageDeclaredAfterTheCatalogWasReadIsRefused()
    {
        StageProvider vocabulary = Vocabulary();

        _ = vocabulary.Catalog;

        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => vocabulary.Source(StageRef.For(ProviderName, "late"), Parameters, Port.Out("out", Reading), Feed));

        Assert.Contains("closed", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStageDeclaredAfterAHostAskedItToBuildOneIsRefused()
    {
        // The other half of the same rule, and the one that makes the thread-safety the factory seam
        // requires true rather than assumed: a host reaching for a stage is a host relying on the table,
        // so the table stops changing at that moment whether or not the catalog was ever read.
        StageProvider vocabulary = Vocabulary();
        IDataflowStageFactory factory = vocabulary;

        _ = factory.Create(new DataflowStageRequest(
            StageNode.Create(NodeId.Create("feed"), FeedStage, Parameters, Payload(1)),
            StageSpecification.Source(FeedStage, Parameters, Port.Out("out", Reading))));

        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => vocabulary.Source(StageRef.For(ProviderName, "late"), Parameters, Port.Out("out", Reading), Feed));

        Assert.Contains("closed", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCatalogIsOneValueHoweverOftenItIsRead()
    {
        StageProvider vocabulary = Vocabulary();

        Assert.Same(vocabulary.Catalog, vocabulary.Catalog);
    }

    [Fact]
    public void ADefaultOrForeignArgumentIsRefused()
    {
        Assert.Equal(
            "provider",
            Assert.Throws<ArgumentException>(() => StageProvider.Create(default(ProviderId))).ParamName);
        Assert.Equal(
            "provider",
            Assert.Throws<ArgumentException>(() => StageProvider.Create("Not A Segment")).ParamName);
        Assert.Throws<ArgumentNullException>(() => StageProvider.Create((string)null!));
        Assert.Equal(
            "stage",
            Assert.Throws<ArgumentException>(
                () => StageProvider.Create(ProviderName).Source(default, Parameters, Port.Out("out", Reading), Feed))
                .ParamName);
        Assert.Throws<ArgumentNullException>(
            () => StageProvider.Create(ProviderName).Source(FeedStage, Parameters, Port.Out("out", Reading), null!));
        Assert.Throws<ArgumentNullException>(
            () => StageProvider.Create(ProviderName).Add(null!, Feed));
    }

    [Fact]
    public void AStageItDoesNotDeclareIsRefusedWhenTheHostAsksForIt()
    {
        IDataflowStageFactory factory = Vocabulary();

        StageNode stranger = StageNode.Create(
            NodeId.Create("stranger"),
            StageRef.For(ProviderName, "no-such-stage"),
            Parameters,
            CanonicalJsonValue.Empty);

        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => factory.Create(new DataflowStageRequest(
                stranger,
                StageSpecification.Source(
                    StageRef.For(ProviderName, "no-such-stage"),
                    Parameters,
                    Port.Out("out", Reading)))));

        Assert.Contains("weather/no-such-stage@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("weather/reading-feed@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("weather/tally@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AVocabularyThatDeclaresNothingSaysSoRatherThanListingAnEmptySet()
    {
        IDataflowStageFactory empty = StageProvider.Create(ProviderName);

        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => empty.Create(new DataflowStageRequest(
                StageNode.Create(NodeId.Create("stranger"), FeedStage, Parameters, CanonicalJsonValue.Empty),
                StageSpecification.Source(FeedStage, Parameters, Port.Out("out", Reading)))));

        Assert.Contains("no stages at all", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Declares the two-stage vocabulary both halves of this suite compare.</summary>
    /// <returns>The vocabulary.</returns>
    private static StageProvider Vocabulary() =>
        StageProvider
            .Create(ProviderName)
            .Source(FeedStage, Parameters, Port.Out("out", Reading), Feed)
            .Sink(TallyStage, Parameters, Port.In("in", Reading), Port.Result("total", Total), Tally);

    /// <summary>Declares the same two stages with the payload checks a published provider owes.</summary>
    /// <returns>The vocabulary.</returns>
    private static StageProvider ValidatingVocabulary() =>
        StageProvider
            .Create(ProviderName)
            .Source(FeedStage, Parameters, Port.Out("out", Reading), CountValidator.Instance, Feed)
            .Sink(
                TallyStage,
                Parameters,
                Port.In("in", Reading),
                Port.Result("total", Total),
                CountValidator.Instance,
                Tally);

    /// <summary>Builds the reading feed of one node.</summary>
    /// <param name="request">The node and its specification.</param>
    /// <returns>The runtime.</returns>
    private static DataflowStageRuntime Feed(DataflowStageRequest request)
    {
        int count = Count(request);

        return DataflowStageRuntime.Source(_ => Readings(count));
    }

    /// <summary>Builds the tallying terminal of one node.</summary>
    /// <param name="request">The node and its specification.</param>
    /// <returns>The runtime.</returns>
    private static DataflowStageRuntime Tally(DataflowStageRequest request)
    {
        int least = Count(request);

        return DataflowStageRuntime.Terminal(
            static () => 0L,
            (state, element) => (int)element! >= least ? (long)state! + 1L : state,
            finish: null,
            producesResult: true);
    }

    /// <summary>Reads the one number both stages carry.</summary>
    /// <param name="request">The node and its specification.</param>
    /// <returns>The number.</returns>
    private static int Count(DataflowStageRequest request) =>
        request.Node.Parameters.ToElement().GetProperty(CountMember).GetInt32();

    /// <summary>Writes the payload both stages carry.</summary>
    /// <param name="value">The number.</param>
    /// <returns>The canonical payload.</returns>
    private static CanonicalJsonValue Payload(int value) =>
        StageParameters.Create().Add(CountMember, value).Build();

    /// <summary>Builds the one graph both hosts run.</summary>
    /// <param name="catalog">The catalog the handles resolve through.</param>
    /// <param name="counted">The slot the tally is read from.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Graph(IStageCatalog catalog, out ResultSlot<long> counted) =>
        Source
            .FromRegistered(RegisteredStage.Source(catalog, FeedStage, Reading), "feed", Payload(10))
            .To(
                RegisteredStage.SinkWithResult(catalog, TallyStage, Reading, Total),
                "tally",
                Payload(5),
                "total",
                out counted);

    /// <summary>Emits the first readings of the feed.</summary>
    /// <param name="count">How many to emit.</param>
    /// <param name="cancellationToken">The enumeration's own token.</param>
    /// <returns>The readings.</returns>
    private static async IAsyncEnumerable<object?> Readings(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int index = 1; index <= count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return index;

            await Task.Yield();
        }
    }

    /// <summary>The payload check both stages declare: one member, a whole number, and nothing else.</summary>
    /// <remarks>
    /// The smallest reader that satisfies the conformance kit, which requires that a stage refuse a member
    /// it never heard of rather than let it reach its factory.
    /// </remarks>
    private sealed class CountValidator : IStageParameterValidator
    {
        /// <summary>Gets the one instance, because the check holds no state.</summary>
        internal static CountValidator Instance { get; } = new();

        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
        {
            if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
            {
                return ["the payload is not a JSON object"];
            }

            List<string> violations = [];
            JsonElement payload = parameters.ToElement();

            foreach (JsonProperty member in payload.EnumerateObject())
            {
                if (!string.Equals(member.Name, CountMember, StringComparison.Ordinal))
                {
                    violations.Add($"the payload carries the member '{member.Name}', which this stage does not declare");
                }
            }

            if (!payload.TryGetProperty(CountMember, out JsonElement count))
            {
                violations.Add($"the payload has no '{CountMember}'");
            }
            else if (count.ValueKind is not JsonValueKind.Number || !count.TryGetInt32(out int _))
            {
                violations.Add($"the payload's '{CountMember}' is not a whole number");
            }

            return violations;
        }
    }

    /// <summary>The control: the same two stages, built by a factory written the long way.</summary>
    /// <remarks>
    /// Deliberately the shape the tutorial teaches — one type, one dispatch on the node's stage reference,
    /// one throw for a stranger — so that what is compared is two spellings of one vocabulary rather than
    /// two vocabularies.
    /// </remarks>
    private sealed class HandWrittenFactory : IDataflowStageFactory
    {
        /// <inheritdoc/>
        public DataflowStageRuntime Create(DataflowStageRequest request)
        {
            StageNode node = request.Node;

            if (node.Stage == FeedStage)
            {
                return Feed(request);
            }

            if (node.Stage == TallyStage)
            {
                return Tally(request);
            }

            throw new InvalidOperationException($"'{node.Stage}' is not a stage this provider implements.");
        }
    }
}
