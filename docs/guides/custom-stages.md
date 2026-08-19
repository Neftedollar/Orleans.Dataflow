# Writing a custom stage

You have work that belongs in a pipeline, and you want a
[silo](../reference/glossary.md#silo) to run it — which means the pipeline's
description cannot carry your delegate.

A `Select(x => ...)` is a [local stage](../reference/glossary.md#local-stage): it
runs in your process and marks its graph nondeployable. A
[registered stage](../reference/glossary.md#registered-stage) is the same work
split in two — a **name and a payload** that travel in the
[graph document](../reference/glossary.md#graph-document), and **code that stays
behind** on every host that may run it. This page writes one of each shape, end
to end.

## The two halves

| Half | What it is | Where it lives | Who needs it |
|---|---|---|---|
| **Definition** | Which stages exist, what their ports carry, what their payloads mean. | A [catalog](../reference/glossary.md#catalog): `StageCatalog` of `StageSpecification`. | Anyone authoring a pipeline, and anyone validating one. |
| **Runtime** | What a stage *does*. | An `IDataflowStageFactory`. | Only a host that will run the graph. |

A silo registers both. A silo that registered a catalog without the matching
factory accepts a document at the coordinator and refuses it at materialization,
naming the missing provider. That split is deliberate: a validator needs to know
that `sales/discount@v1` exists and takes a `percent`; it does not need to know
how discounting works.

## The definition half

```csharp
public static class SalesVocabulary
{
    public static ProviderId Provider { get; } = ProviderId.Create("sales");

    public static StageRef FeedStage { get; } =
        StageRef.Create(Provider, StageId.Create("order-feed"), StageRef.FirstMajorVersion);

    public static StageRef DiscountStage { get; } =
        StageRef.Create(Provider, StageId.Create("discount"), StageRef.FirstMajorVersion);

    public static StageRef TallyStage { get; } =
        StageRef.Create(Provider, StageId.Create("tally"), StageRef.FirstMajorVersion);

    // A contract identifier and a major version — deliberately not a CLR type name. What makes two
    // stages connectable is that they agree on this, which is a fact a document can state and a silo
    // in another process can check.
    public static ElementContract<OrderEvent> OrderEventContract { get; } =
        ElementContract.For<OrderEvent>("sales-order-event", 1);

    public static ElementContract<OrderDocument> OrderDocumentContract { get; } =
        ElementContract.For<OrderDocument>("sales-order-document", 1);

    public static ResultContract<long> TallyContract { get; } = ResultContract.For<long>("sales-tally", 1);

    public static ContractReference FeedParameterContract { get; } =
        ContractReference.Create(ContractId.Create("sales-order-feed-parameters"), 1);

    // … one per stage.

    public const string CountMember = "count";
    public const string PercentMember = "percent";
    public const string LabelMember = "label";
    public const string MinimumAmountMember = "minimum-amount";

    public static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Create(
                FeedStage,
                [],                                                   // no input ports: this is a source
                [OutputPortSpecification.Create(PortId.Create("out"), OrderEventContract.Reference)],
                [],                                                   // no result ports
                FeedParameterContract,
                [],                                                   // no required capabilities
                new PayloadValidator("order feed", (CountMember, JsonValueKind.Number))),
            StageSpecification.Create(
                DiscountStage,
                [InputPortSpecification.Create(PortId.Create("in"), OrderEventContract.Reference)],
                [OutputPortSpecification.Create(PortId.Create("out"), OrderDocumentContract.Reference)],
                [],
                DiscountParameterContract,
                [],
                new PayloadValidator("discounting flow", (PercentMember, JsonValueKind.Number))),
            StageSpecification.Create(
                TallyStage,
                [InputPortSpecification.Create(PortId.Create("in"), OrderDocumentContract.Reference)],
                [],                                                   // no output ports: this is a terminal
                [ResultPortSpecification.Create(PortId.Create("total"), TallyContract.Reference)],
                TallyParameterContract,
                [],
                new PayloadValidator(
                    "tallying terminal",
                    (LabelMember, JsonValueKind.String),
                    (MinimumAmountMember, JsonValueKind.Number))),
        ]);

    // Typed handles, resolved against the catalog so a typo is an authoring-time diagnostic rather than
    // a deployment-time refusal.
    private static readonly IStageCatalog Authoring = Catalog();

    public static RegisteredSource<OrderEvent> Feed { get; } =
        RegisteredStage.Source(Authoring, FeedStage, OrderEventContract);

    public static RegisteredFlow<OrderEvent, OrderDocument> Discount { get; } =
        RegisteredStage.Flow(Authoring, DiscountStage, OrderEventContract, OrderDocumentContract);

    public static RegisteredSinkWithResult<OrderDocument, long> Tally { get; } =
        RegisteredStage.SinkWithResult(Authoring, TallyStage, OrderDocumentContract, TallyContract);

    // Payload writers. One per stage, and the only place the member names are spelled for writing.
    public static CanonicalJsonValue FeedParameters(int count) =>
        CanonicalJsonValue.Parse(
            string.Create(CultureInfo.InvariantCulture, $"{{\"{CountMember}\":{count}}}"));

    // … and one reader per member, which is what the factory below calls.
}
```

The shape to copy is the sample's own vocabulary,
[`samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs)
— a catalog is a published artifact rather than a language artifact, which is why
the sample's lives in the F# project and is consumed from C#.

### The parameter validator

Every specification carries one, and it is the narrowest piece of behaviour in
the whole model: it sees a canonical payload and returns violations.

```csharp
public sealed class PayloadValidator(string stage, params (string Member, JsonValueKind Kind)[] members)
    : IStageParameterValidator
{
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
    {
        if (parameters.IsDefault || parameters.ToElement().ValueKind != JsonValueKind.Object)
        {
            return [$"the {stage}'s payload is not a JSON object"];
        }

        List<string> violations = [];
        JsonElement payload = parameters.ToElement();

        foreach ((string member, JsonValueKind kind) in members)
        {
            if (!payload.TryGetProperty(member, out JsonElement value))
            {
                violations.Add($"the member '{member}' is missing");
            }
            else if (value.ValueKind != kind)
            {
                violations.Add($"the member '{member}' is {value.ValueKind} rather than {kind}");
            }
        }

        foreach (JsonProperty property in payload.EnumerateObject())
        {
            if (!members.Any(declared => string.Equals(declared.Member, property.Name, StringComparison.Ordinal)))
            {
                violations.Add($"the member '{property.Name}' is not one this stage declares");
            }
        }

        return violations;
    }
}
```

Four rules the type demands and the [conformance kit](#proving-it-with-the-conformance-kit) checks:

- **Return violations rather than throwing.** An invalid payload is an expected
  outcome of validating an untrusted document, not an exceptional one.
- **Report every violation, not the first.** A caller fixing one problem per run
  learns the shape of the contract one rejection at a time.
- **Refuse a member you never declared.** This is what stops a payload written
  against a newer version of your stage from reaching a factory that will ignore
  half of it.
- **Be pure and fast.** No I/O, no clock, no ambient culture, no mutable state,
  and the same answer for the same payload in every process — a report has to be
  reproducible from the document and the catalog alone.

A validator is behaviour, so it is never serialised and never contributes to the
catalog fingerprint. Two catalogs whose specifications agree but whose validators
differ share a fingerprint, and that limit is worth knowing before you rely on
the fingerprint to mean "these silos will refuse the same documents".

## The runtime half

```csharp
public sealed class SalesStageFactory : IDataflowStageFactory
{
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == SalesVocabulary.FeedStage)
        {
            int count = SalesVocabulary.ReadFeedCount(node.Parameters);

            return DataflowStageRuntime.Source(tokens => Orders(count, tokens));
        }

        if (node.Stage == SalesVocabulary.DiscountStage)
        {
            decimal percent = SalesVocabulary.ReadDiscountPercent(node.Parameters);

            return DataflowStageRuntime.Element(element =>
            {
                OrderEvent order = (OrderEvent)element!;

                return new OrderDocument(
                    order.Sequence,
                    order.OrderId,
                    order.Region,
                    order.Amount - (order.Amount * percent / 100m));
            });
        }

        if (node.Stage == SalesVocabulary.TallyStage)
        {
            decimal minimum = SalesVocabulary.ReadTallyMinimum(node.Parameters);

            // Every terminal is a fold. The seed is made once per run rather than handed over as a
            // value, so two runs of one pipeline never share it.
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) => ((OrderDocument)element!).Amount >= minimum ? (long)state! + 1L : state,
                finish: null,
                producesResult: true);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }
}
```

That is
[`samples/Orleans.Dataflow.Samples/SampleStageFactory.cs`](../../samples/Orleans.Dataflow.Samples/SampleStageFactory.cs)
almost line for line, and three things about it are the whole discipline.

**One factory per provider, dispatching on the node's stage reference.** The
final `throw` is not defensive noise — the conformance kit asserts that a factory
refuses a stage its catalog does not declare, by name.

**Nothing here reads the graph, the run, or the cluster.** A stage is handed its
own node and answers with its own behaviour, which is what lets the same three
stages be composed into a pipeline this factory has never seen.

**A stage runtime is built once per node per run**, so whatever your closures
capture is fresh per run. That is why a terminal is given a seed *factory* rather
than a seed: a mutable accumulator handed over as a value would be one object
that two runs both wrote into.

The values are untyped because a document never names an element type, so the
engine works in `object` and the factory is the one place that knows what your
elements are.

### The shapes available

`DataflowStageRuntime` has six shapes and refuses to grow past what the engine
runs — a stage that wants a seventh is asking for a new engine primitive.

| Shape | Factory method | Notes |
|---|---|---|
| Source | `Source(open)` / `Source(open, cursor)` | Enumerated once per run, disposed on every terminal path. |
| Synchronous element | `Element(map)` | Runs inside the run's pull loop. **Must not block** — a stage that waits belongs in the next row. |
| Asynchronous element | `ElementAsync(map, maxConcurrency, ordered)` | The bound *is* the backpressure: a call in flight is credit spent. |
| Terminal | `Terminal(seed, fold, finish, producesResult)` | Plus overloads taking `DataflowRunTokens` and/or a `DataflowSinkMark`. |
| Fan-out junction | `Broadcast()`, `Balance()`, `Partition(route)`, `Unzip(parts)` | Ports in the specification's own order, which is ordinal by port name. |
| Fan-in junction | `Merge()`, `Concat()`, `Interleave(size)`, `Zip(combine)`, `CombineLatest(combine)` | Same ordering rule. |

Registering a junction is what lets a branching graph be a
[pipeline](../reference/glossary.md#pipeline) rather than nondeployable.

### The two tokens

A source's opener and a terminal's seed receive `DataflowRunTokens`, and **which
end of the graph you are writing decides which token to watch**.

- `RunToken` is cancelled when the run is cancelled and when anything in it
  fails. This is the token a **sink's** own work should carry.
- `StopToken` is cancelled for all of that *and* when a graceful
  [shutdown](../reference/glossary.md#shutdown) is asked for.

A **source** watches both and tells them apart: released by `StopToken` alone it
ends its sequence as if it had run out, which is what makes a shutdown
[drain](../reference/glossary.md#drain) the run instead of abandoning it. A
source that watches only `RunToken` is correct but blunt — it turns every
shutdown into a wait for its next yield.

A **terminal** is the thing a shutdown drains *into*, so a sink's own work
carries `RunToken` and nothing else. Abandoning on `StopToken` would drop exactly
the elements a graceful stop set out to deliver.

`RunIdentity` is what the run is called in this deployment — the run grain's key
`{graph}/{run}` on a silo, a fresh per-run identifier in an in-process host. A
source that has to be addressable from outside the run composes its address from
that and its binding's name; every other source ignores it.

## Parameters that travel, code that stays behind

The line is absolute and worth stating twice, because it is the one thing that
makes a document portable.

**Travels in the document:** the stage reference, the occurrence name, and a
canonical JSON payload — numbers, strings, booleans, objects and arrays of them.

**Never travels:** a delegate, a closure, a CLR type name, a connection string, a
grain reference, a service. If a stage needs one of those, it is a *registration*
— declared once on every host that may run the stage, and named by the payload.

The conformance kit checks the negative half directly: `NoParameterPayloadNamesAClrType`
fails a provider whose payload smuggles an assembly-qualified name through as a
string.

Two consequences worth planning for:

- **Every number in a payload is part of the
  [fingerprint](../reference/glossary.md#fingerprint).** A stage whose payload
  takes a value from a request mints a fresh fingerprint per request, which is
  what the metric tag cardinality bound exists to survive. Parameterise from a
  fixed set where you can.
- **Bump the stage's major version when the payload's meaning changes.**
  `StageRef` carries one, and a document written against the old meaning names
  the old version.

## Cursors and marks for durability

A stage that wants to take part in a [durable run](../reference/glossary.md#durable-run)
declares one of two things. Both are the provider's own halves of one object —
the seam does not join them for you.

**A source declares a [cursor](../reference/glossary.md#cursor).**

```csharp
DataflowStageRuntime.Source(tokens => Open(cursor, tokens), cursor);
```

`DataflowSourceCursor` has `Position` (what a checkpoint stores), `Delivered()`
(the engine calls it when the *run* has delivered the element the sequence last
yielded), and `RestoreTo(position)` (a resume hands the stored position back
before the first element). Nothing in the seam reads a position, so your opener
closes over the very cursor instance you hand over and decides for itself whether
a restored position is an index to skip, a token to subscribe at, or an offset to
seek to.

The requirement the engine cannot check and you must state: **reopening at a
stored position has to land on the elements after it.** Where you cannot promise
that, declare no cursor — the source then contributes nothing to a checkpoint and
resumes from now, which is a row in your table rather than a silent
approximation.

**A sink declares a [mark](../reference/glossary.md#mark).**

```csharp
DataflowStageRuntime.Terminal(seed, fold, finish, producesResult, mark);
```

`DataflowSinkMark` has `Mark` and `RestoreTo(mark)`, and no `Delivered()` —
because when the number moves is your business, not the engine's. The engine
calls the fold and reads the mark; whether a commit happened after an
acknowledgement landed, after a transaction committed, or after a flush returned
is something only your adapter knows.

The rule: **the mark advances after the effect, never before it.** A mark that
led its effect would make a resume skip an element whose commit never happened,
turning a duplicate window into a *loss* window. A mark that lags — because an
effect is still in flight when a capture reads it — costs a wider replay and
loses nothing. Lean that way when the two moments cannot be separated exactly.

## Registering it

On a silo:

```csharp
silo.AddOrleansDataflow(dataflow => dataflow
    .AddCatalog(SalesVocabulary.Catalog())
    .AddFactory(SalesVocabulary.Provider, new SalesStageFactory()));
```

On a local host — **the same two calls**, which is what makes "a provider's stages
run in either runtime" a checkable claim rather than an intention:

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(SalesVocabulary.Catalog())
    .AddFactory(SalesVocabulary.Provider, new SalesStageFactory()));
```

`AddCatalog` is callable more than once and the host's catalog is the union — a
deployment composes vocabularies from several packages. Registering one stage
reference twice, or one provider twice, is refused when the host is built, because
two specifications for one reference are two answers to one question rather than a
merge.

Authoring against it is ordinary:

```csharp
(RunnableGraph graph, ResultSlot<long> accepted) = Source
    .FromRegistered(SalesVocabulary.Feed, "feed", SalesVocabulary.FeedParameters(12))
    .Via(SalesVocabulary.Discount, "discount", SalesVocabulary.DiscountParameters(10))
    .To(SalesVocabulary.Tally, "tally", SalesVocabulary.TallyParameters("accepted-orders", 20), "accepted");

PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("sales-orders"), GraphRevision.Create(1));
```

Running it prints:

```text
graph       sha256:7b05dd25bf86934073ee2e71ef92be6829c341924bc5fd46a98d3203b25ae854
pipeline    sha256:45fab1eb59877959653cac593a61c585becb2ff707676de64c2db9de7518ab3b
accepted    10
```

The two fingerprints differ **by design**: declaring an identity re-closes the
document under that identity, so a pipeline's fingerprint is the fingerprint of
the deployable document. It is also why a graph's result slot and a pipeline's
are recovered differently — a closed graph's slot binds to that built instance, a
pipeline's binds to the fingerprint and the lineage, which is what lets a run
started by one process be read by another.

## Proving it with the conformance kit

`ProviderConformance`, in `Orleans.Dataflow.Testing`, is nine structural checks
over the two halves. Point it at your provider with one accepted payload per
stage, written by **your own parameter writers** — a sample written as literal
JSON would be a second spelling of the payload maintained beside the first, and
the first thing to drift.

```csharp
ProviderConformance kit = ProviderConformance.Create(
    SalesVocabulary.Provider,
    SalesVocabulary.Catalog(),
    new SalesStageFactory(),
    [
        ProviderStageSample.Create(SalesVocabulary.FeedStage, SalesVocabulary.FeedParameters(4)),
        ProviderStageSample.Create(SalesVocabulary.DiscountStage, SalesVocabulary.DiscountParameters(10)),
        ProviderStageSample.Create(SalesVocabulary.TallyStage, SalesVocabulary.TallyParameters("t", 20)),
    ]);

foreach (string check in ProviderConformance.Checks)
{
    kit.Check(check);
}
```

```text
ok          EveryPortCarriesADeclaredContractInCanonicalOrder
ok          EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare
ok          TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages
ok          TheFactoryAnswersForEveryStageTheCatalogDeclares
ok          TheFactoryRefusesAStageTheCatalogDoesNotDeclare
ok          EveryRuntimeHasTheShapeItsSpecificationDeclares
ok          EveryStageHasATypedHandleThatRefusesTheWrongShape
ok          NoParameterPayloadNamesAClrType
ok          NoCoreOptionTypeNamesAnythingOfThisProvider
```

Drive it as a theory with `ProviderConformance.Checks` as the data, one row per
check, so a failure reads as the sentence that stopped being true. `CheckAll()`
runs the lot as one and reports every violation it found, numbered.

Two things the kit is strict about, which is the point of running it:

- **A catalog declaring no stage of your provider is refused** rather than
  passing every check vacuously. A green suite that measured nothing reads
  exactly like a green suite that measured everything.
- **Every declared stage needs a sample and every sample needs a declared stage.**
  A stage without one would be skipped silently; a sample without one is a stage
  you believe you registered and did not.

Where your stages need a host to build — because building one resolves a stream
provider, or reads a cluster option — run the kit *inside* your test cluster,
against the very container `AddOrleansDataflow` would hand its factory. That is
what
[`tests/Orleans.Dataflow.OrleansTests/Cluster/OrleansVocabularyConformanceTests.cs`](../../tests/Orleans.Dataflow.OrleansTests/Cluster/OrleansVocabularyConformanceTests.cs)
does for the shipped Orleans vocabulary.

**What the kit does not prove.** It is structural. No test can check that an
acknowledgement boundary is where its documentation says it is, that a cursor
reopens on the right side of its position, or that a mark advances after its
effect. Those are semantics, and they belong in your adapter's own row in the
[adapter reference](../reference/adapters.md), stated in prose, plus a test you
write that crashes a run and counts what was delivered twice.

## Next

- [Provider SDK](../reference/provider-sdk.md) — every type on the seam.
- [Orleans streams and grains](orleans-integration.md) — the shipped adapters, written against this same seam.
- [Durable runs](durable-runs.md) — what a cursor and a mark buy.
- [Testing and observability](testing-and-observability.md) — testing a custom stage without a cluster.
