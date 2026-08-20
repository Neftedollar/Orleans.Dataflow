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

| Half | What it is | Who needs it |
|---|---|---|
| **Definition** | Which stages exist, what their ports carry, what their payloads mean. A [catalog](../reference/glossary.md#catalog). | Anyone authoring a pipeline, and anyone validating one. |
| **Runtime** | What a stage *does*. | Only a host that will run the graph. |

The split is what makes a document portable: a validator needs to know that
`sales/discount@v1` exists and takes a `percent`; it does not need to know how
discounting works. A host that has the definition and not the runtime accepts a
document and then cannot build it, which it says by name.

**You state each stage once and get both halves.** `StageProvider` holds a
provider's whole vocabulary — each stage's declaration beside the code that
builds it — and hands out the definition half and the runtime half from the same
value. Declaring a stage in the catalog and forgetting to implement it, or
implementing one you never declared, are the same mistake, and stating the fact
once is what removes it.

## Declaring a vocabulary

This is `sales`: a feed of orders, a discount applied to each, and a tally of the
ones worth keeping.

```csharp
public static class SalesVocabulary
{
    // A name a machine that never saw your assembly can resolve: who published the stage, which of
    // their stages it is, and which generation of it. `For` starts at the first generation.
    public static StageRef FeedStage { get; } = StageRef.For("sales", "order-feed");
    public static StageRef DiscountStage { get; } = StageRef.For("sales", "discount");
    public static StageRef TallyStage { get; } = StageRef.For("sales", "tally");

    // A contract identifier and a major version — deliberately not a CLR type name. What makes two
    // stages connectable is that they agree on this, which is a fact a document can state and a silo
    // in another process can check.
    public static ElementContract<OrderEvent> OrderEventContract { get; } =
        ElementContract.For<OrderEvent>("sales-order-event", 1);

    public static ElementContract<OrderDocument> OrderDocumentContract { get; } =
        ElementContract.For<OrderDocument>("sales-order-document", 1);

    public static ResultContract<long> TallyContract { get; } = ResultContract.For<long>("sales-tally", 1);

    // A payload binds to no CLR type, so its contract has no <T>: an identifier and a version.
    public static ContractReference FeedParameterContract { get; } =
        ContractReference.For("sales-order-feed-parameters");

    public static ContractReference DiscountParameterContract { get; } =
        ContractReference.For("sales-discount-parameters");

    public static ContractReference TallyParameterContract { get; } =
        ContractReference.For("sales-tally-parameters");

    // The member names, spelled once, read and written through these constants only.
    public const string CountMember = "count";
    public const string PercentMember = "percent";
    public const string LabelMember = "label";
    public const string MinimumAmountMember = "minimum-amount";

    public static StageProvider Vocabulary { get; } = StageProvider.Create("sales")
        .Source(
            FeedStage,
            FeedParameterContract,
            Port.Out("out", OrderEventContract),
            new PayloadValidator("order feed", (CountMember, JsonValueKind.Number)),
            request =>
            {
                int count = ReadFeedCount(request.Node.Parameters);

                return DataflowStageRuntime.Source(_ => Orders(count));
            })
        .Flow(
            DiscountStage,
            DiscountParameterContract,
            Port.In("in", OrderEventContract),
            Port.Out("out", OrderDocumentContract),
            new PayloadValidator("discounting flow", (PercentMember, JsonValueKind.Number)),
            request =>
            {
                decimal percent = ReadDiscountPercent(request.Node.Parameters);

                return DataflowStageRuntime.Element(element =>
                {
                    OrderEvent order = (OrderEvent)element!;

                    return new OrderDocument(
                        order.Sequence,
                        order.OrderId,
                        order.Region,
                        order.Amount - (order.Amount * percent / 100m));
                });
            })
        .Sink(
            TallyStage,
            TallyParameterContract,
            Port.In("in", OrderDocumentContract),
            Port.Result("total", TallyContract),
            new PayloadValidator(
                "tallying terminal",
                (LabelMember, JsonValueKind.String),
                (MinimumAmountMember, JsonValueKind.Number)),
            request =>
            {
                decimal minimum = ReadTallyMinimum(request.Node.Parameters);

                // Every terminal is a fold. The seed is made once per run rather than handed over as
                // a value, so two runs of one pipeline never share it.
                return DataflowStageRuntime.Terminal(
                    static () => 0L,
                    (state, element) => ((OrderDocument)element!).Amount >= minimum ? (long)state! + 1L : state,
                    finish: null,
                    producesResult: true);
            });

    // Typed handles, resolved against this vocabulary's own catalog so a typo is an authoring-time
    // diagnostic rather than a deployment-time refusal.
    public static RegisteredSource<OrderEvent> Feed { get; } =
        RegisteredStage.Source(Vocabulary.Catalog, FeedStage, OrderEventContract);

    public static RegisteredFlow<OrderEvent, OrderDocument> Discount { get; } =
        RegisteredStage.Flow(Vocabulary.Catalog, DiscountStage, OrderEventContract, OrderDocumentContract);

    public static RegisteredSinkWithResult<OrderDocument, long> Tally { get; } =
        RegisteredStage.SinkWithResult(Vocabulary.Catalog, TallyStage, OrderDocumentContract, TallyContract);

    // Payload writers, one per stage, and the only place a member name is spelled for writing.
    public static CanonicalJsonValue FeedParameters(int count) =>
        StageParameters.Create().Add(CountMember, count).Build();

    public static CanonicalJsonValue DiscountParameters(int percent) =>
        StageParameters.Create().Add(PercentMember, percent).Build();

    public static CanonicalJsonValue TallyParameters(string label, int minimum) =>
        StageParameters.Create().Add(LabelMember, label).Add(MinimumAmountMember, minimum).Build();

    // … and one reader per member, which is what the build delegates above call.
    public static int ReadFeedCount(CanonicalJsonValue parameters) =>
        parameters.ToElement().GetProperty(CountMember).GetInt32();

    private static async IAsyncEnumerable<object?> Orders(int count)
    {
        for (long index = 1; index <= count; index++)
        {
            yield return new OrderEvent(index, $"order-{index}", "eu", index * 5m);
        }
    }
}
```

Four things about that are the whole discipline.

**Each build delegate is handed one node and answers with one behaviour.** It is
never told about the graph, the run, or the cluster — which is what lets these
three stages be composed into a pipeline the provider has never seen.

**A stage is built once per occurrence per run**, so whatever a delegate's
closure captures is fresh per run. That is why a terminal is given a seed
*factory* rather than a seed: a mutable accumulator handed over as a value would
be one object that two runs both wrote into.

**Values arrive as `object`.** A document never names an element type, so the
engine works untyped and a build delegate is the one place that knows what your
elements are.

**A vocabulary closes when it is first used** — when its catalog is read, or when
a host asks it to build a node. Declaring another stage after that is refused,
because a deployment registers the vocabulary it declared and a stage added
afterwards would leave the registration describing something that no longer
exists. Declaring every stage in one expression, as above, means you never meet
the rule.

### Declaring a stage

A specification always has six things in it — input ports, output ports, result
ports, a parameter contract, required capabilities, and an optional payload check
— because that is what a catalog stores. An *author* almost never has six things
to say, so the method you call names the shape you are declaring and asks for
only the ports that shape has.

| Shape | Method | Ports it asks for |
|---|---|---|
| Source | `.Source(stage, parameters, out, build)` | one output |
| Flow | `.Flow(stage, parameters, in, out, build)` | one input, one output |
| Sink | `.Sink(stage, parameters, in, build)` | one input |
| Sink with a result | `.Sink(stage, parameters, in, result, build)` | one input, one result |
| Anything else | `.Add(specification, build)` | whatever the specification declares |

Each takes an `IStageParameterValidator` before the build delegate when the stage
checks its payloads, which is what the vocabulary above does at every stage.

`Port.In`, `Port.Out`, and `Port.Result` take the port name as plain text and the
contract as the `ElementContract<T>` or `ResultContract<T>` you already declared,
so a port costs one call. They are typed on purpose: `Port.In` will not take a
result contract and `Port.Result` will not take an element one. Overloads taking
a `ContractReference` are there for a provider whose ports carry whatever a
deployment binds to them — the shipped Orleans adapters are written that way —
and an optional input or an ignorable output says so with a third argument:
`Port.In("side", contract, isOptional: true)`.

**`.Add` is the general form**, and the escape hatch for everything the named
shapes do not cover — a junction, a stage that requires a capability of its host,
one that declares several result ports. Its specification is built by
`StageSpecification.Create`, where everything after the stage and its parameter
contract is optional and written by name:

```csharp
provider.Add(
    StageSpecification.Create(
        DurableSinkStage,
        DurableSinkParameterContract,
        inputPorts: [Port.In("in", OrderDocumentContract)],
        requiredCapabilities: [CapabilityToken.Create("durable-state")]),
    request => /* … */);
```

An omitted collection declares none of that kind, so nothing has to be written
just to say that a stage has no result ports. `StageSpecification.FanOut` and
`StageSpecification.FanIn` are the junction shapes, and take a collection of
ports where the linear shapes take one.

`StageSpecification`'s own named shapes — `Source`, `Flow`, `Sink` — are the same
set without a delegate, and they are what a catalog published on its own is built
from. [When the halves ship apart](#when-the-halves-ship-apart) is where that
happens.

### Writing a payload

A payload is a JSON object: the numbers, words, and flags that configure one
*use* of a stage. `StageParameters` writes one a member at a time.

```csharp
StageParameters.Create()
    .Add("label", "accepted-orders")
    .Add("minimum-amount", 20)
    .Build();
```

`Add` takes whole numbers, words, flags, nested builders, ordered lists of any of
those, and `AddNull` for a member whose value is JSON `null`. A stage that takes
no parameters at all carries `CanonicalJsonValue.Empty` rather than a builder
with nothing in it.

Composing the JSON as a string is still possible and is the wrong default. The
builder cannot express a trailing comma, a fraction the
[canonical form](../reference/glossary.md#canonical-json) does not admit, or a
number formatted under whatever culture the machine happens to be set to. What it
produces goes through the very parse a hand-written string would have gone
through, so the stored bytes are the same either way — the builder removes the
ways of getting them wrong, not a step.

Two members of the API are worth knowing about because they are narrow on
purpose. Numbers are `long` and there is no floating-point sibling: the canonical
form admits integers and nothing else, so a value that is genuinely fractional is
written as the units it is counted in. And `Add(name, CanonicalJsonValue)` is the
escape hatch for a stage whose payload embeds another payload whole — a scope
holding the chain inside it, a policy read from somewhere else.

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
  against a newer version of your stage from reaching a build delegate that will
  ignore half of it.
- **Be pure and fast.** No I/O, no clock, no ambient culture, no mutable state,
  and the same answer for the same payload in every process — a report has to be
  reproducible from the document and the catalog alone.

A validator is behaviour, so it is never serialised and never contributes to the
catalog fingerprint. Two catalogs whose specifications agree but whose validators
differ share a fingerprint, and that limit is worth knowing before you rely on
the fingerprint to mean "these silos will refuse the same documents".

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

They map onto the declaration shapes one for one, except that `Flow` covers both
the synchronous and the asynchronous element stage — whether a transformation
awaits is a property of the code that stays behind, not of the ports a document
connects — and a terminal is a `Sink` whether or not it yields a result.

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
  the old version. `StageRef.For("sales", "tally", 2)` is the second generation.

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
silo.AddOrleansDataflow(dataflow => dataflow.AddProvider(SalesVocabulary.Vocabulary));
```

On a local host — **the same call**, which is what makes "a provider's stages run
in either runtime" a checkable claim rather than an intention:

```csharp
LocalDataflowHost host = new(builder => builder.AddProvider(SalesVocabulary.Vocabulary));
```

A host composes vocabularies from several packages: `AddProvider` is callable
once per provider, and the host's catalog is the union of everything registered.
Registering one stage reference twice, or one provider twice, is refused when the
host is built, because two specifications for one reference are two answers to
one question rather than a merge.

A silo with **no vocabulary at all** is refused at startup, by name. It could
resolve no stage reference, so every document it was handed would be refused, and
saying so when the silo starts is better than saying it once per document.

Authoring against it is ordinary — `Feed` produces order events, `Discount` turns
each into a document, and `Tally` counts the ones worth keeping. Each is a typed
handle, so the chain below is checked the same way an ordinary pipeline is; what
a registered stage adds is the occurrence name — your name for *this* use of it —
and the parameters that use carries into the document.

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
accepted    8
```

Twelve orders at five, ten, fifteen and up; a tenth off each; eight of them still
at twenty or above.

The two fingerprints differ **by design**: declaring an identity re-closes the
document under that identity, so a pipeline's fingerprint is the fingerprint of
the deployable document. It is also why a graph's result slot and a pipeline's
are recovered differently — a closed graph's slot binds to that built instance, a
pipeline's binds to the fingerprint and the lineage, which is what lets a run
started by one process be read by another.

Neither fingerprint depends on the code behind the names. Rewrite `Orders` to
emit different amounts and both lines above stay the same, because behaviour
reaches no document — which is the portability being bought and the limit that
comes with it.

## When the halves ship apart

A vocabulary published for other people to use sometimes travels as two packages:
a contracts package that authors and validators reference, and a deployment
package that implements it. The halves are then genuinely separate artifacts, and
they are registered separately.

```csharp
// In the contracts package: the definition half, with no code behind it. The names, the contracts,
// and the payload writers and readers are declared here too — everything from the vocabulary above
// except the build delegates.
public static class SalesContracts
{
    public static ProviderId Provider { get; } = ProviderId.Create("sales");

    // … the stage references, the contracts, the member names, and the payload writers.

    public static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Source(FeedStage, FeedParameterContract, Port.Out("out", OrderEventContract)),
            StageSpecification.Flow(
                DiscountStage,
                DiscountParameterContract,
                Port.In("in", OrderEventContract),
                Port.Out("out", OrderDocumentContract)),
            StageSpecification.Sink(
                TallyStage,
                TallyParameterContract,
                Port.In("in", OrderDocumentContract),
                Port.Result("total", TallyContract)),
        ]);
}
```

```csharp
// In the deployment package: the runtime half, one factory per provider,
// dispatching on the node's stage reference.
public sealed class SalesStageFactory : IDataflowStageFactory
{
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == SalesContracts.FeedStage)
        {
            return DataflowStageRuntime.Source(_ => Orders(SalesContracts.ReadFeedCount(node.Parameters)));
        }

        // … one branch per stage.

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }
}
```

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(SalesContracts.Catalog())
    .AddFactory(SalesContracts.Provider, new SalesStageFactory()));
```

The final `throw` is not defensive noise — the conformance kit asserts that a
factory refuses a stage its catalog does not declare, by name. A `StageProvider`
throws the same way and lists what it does declare.

`AddCatalog` and `AddFactory` are what `AddProvider` does, and they take the same
values; `StageProvider.Catalog` is an ordinary `StageCatalog`, publishable on its
own and fingerprinted like any other. So this is the same seam with the two ends
written in two places, and the reason to choose it is that the two ends really do
ship in two places. When one deployment does both, saying it once is strictly
better.

The pipeline at the top of this section, authored against this split vocabulary
and run on this host, prints
`graph sha256:7b05dd25bf86934073ee2e71ef92be6829c341924bc5fd46a98d3203b25ae854`
and `accepted 8` — the same document and the same answer as the single
declaration above it. Which registration you choose is a fact about your source
tree and about nothing else.

The samples are written this way on purpose, and demonstrate a second thing while
they are at it: their catalog is declared in F# and consumed from C#, because a
catalog is a published artifact rather than a language artifact. The vocabulary
is in
[`samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs)
and the factory in
[`samples/Orleans.Dataflow.Samples/SampleStageFactory.cs`](../../samples/Orleans.Dataflow.Samples/SampleStageFactory.cs).

## Proving it with the conformance kit

`ProviderConformance`, in `Orleans.Dataflow.Testing`, is nine structural checks
over the two halves. Point it at your provider with one accepted payload per
stage, written by **your own parameter writers** — a sample written as literal
JSON would be a second spelling of the payload maintained beside the first, and
the first thing to drift.

```csharp
ProviderConformance kit = ProviderConformance.Create(
    SalesVocabulary.Vocabulary.Provider,
    SalesVocabulary.Vocabulary.Catalog,
    SalesVocabulary.Vocabulary,
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
[`tests/Orleans.Dataflow.ClusterTests/Cluster/OrleansVocabularyConformanceTests.cs`](../../tests/Orleans.Dataflow.ClusterTests/Cluster/OrleansVocabularyConformanceTests.cs)
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
