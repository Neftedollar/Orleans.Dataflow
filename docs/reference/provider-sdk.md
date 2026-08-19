# Provider SDK

The seam for writing a stage the runtime can run anywhere — in your process or on
a silo — addressed by name rather than by delegate.

Everything the ordinary authoring surface builds carries a delegate, and a
delegate is not something a [graph document](glossary.md#graph-document) can
carry: those graphs declare themselves nondeployable and a cluster refuses them by
name. A **[registered stage](glossary.md#registered-stage)** is the other kind. It
is a stage reference, a major version, and a canonical payload — so the silo that
runs it needs nothing from the process that authored it.

**A provider ships two halves**, and they are registered separately because
different processes need different halves:

| Half | What it is | Who needs it |
|---|---|---|
| a **catalog** | which stages exist, what their ports carry, what their payloads mean | anything that validates a document |
| a **factory** | what each stage *does* when a run is materialized | only a host that will run the graph |

A host with the catalog and no factory validates a document and refuses it at
materialization, naming the provider that has nothing to build it.

Both go in through the same two calls, on
[either builder](hosting.md#the-local-builder):

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(providerCatalog)
    .AddFactory(providerId, new MyStageFactory()));
```

**The claim this seam makes: a provider writes its stages once and they run in
either runtime.** The very catalog and the very factory a silo is given can be
given to a `LocalDataflowHost`.

**Examples on this page** are lifted verbatim from `samples/`, where they compile
and run in continuous integration: the vocabulary from
`samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs` and the factory from
`samples/Orleans.Dataflow.Samples/SampleStageFactory.cs`. **Two blocks are
illustrative** and say so by naming things you supply — the registration below
that names `MyStageFactory`, and the conformance test that names `MyStages`.
Everything else on the page was compiled and executed.

---

## Names, contracts, and payloads

A provider is identified by a `ProviderId`; each of its stages by a `StageRef`,
which is the provider, a `StageId`, and a major version.

```fsharp
let Provider = ProviderId.Create "samples"
let FeedStage = StageRef.Create(Provider, StageId.Create "order-feed", StageRef.FirstMajorVersion)
```

**A document may not name a CLR type**, so what makes two stages connectable is
that they agree on a *contract reference* — an identifier and a major version —
which is a fact a document can state and a silo in another process can check. An
`ElementContract<T>` is the process-local assertion binding one to a CLR type:

```fsharp
let OrderEventContract = Orleans.Dataflow.ElementContract.For<OrderEvent>("samples-order-event", 1)
let TallyContract = Orleans.Dataflow.ResultContract.For<int64>("samples-tally", 1)
```

(The F# snippets on this page are qualified because the sample they come from
deliberately does not `open Orleans.Dataflow`: its `Source`, `Flow`, and `Sink`
would then shadow the F# frontend's. An F# file that opens neither, or opens them
in the other order, writes these names plain.)

Declaring that says "in this process, contract `samples-order-event@v1` is
`OrderEvent`". The document stores only the reference. **Two processes agreeing on
the reference but binding different CLR types is a deployment error the definition
plane cannot see** — stated, not hidden.

Both contract types are readonly structs with value equality and three readings:

| Member | On | What it is |
|---|---|---|
| `Reference` | both | the `ContractReference` a document carries |
| `ElementType` | `ElementContract<T>` | the CLR type this process bound to it |
| `ResultType` | `ResultContract<TResult>` | the same, for a result |
| `IsDefault` | both | whether this is the default value, which binds nothing |

A stage's parameters travel as a `CanonicalJsonValue` validated against the
specification's declared parameter contract.

---

## The catalog

A `StageCatalog` of `StageSpecification`s. Each specification names the stage, its
input ports, its output ports, its result ports, its parameter contract, and the
capabilities it requires. A stage is written through the factory that names its
shape, which asks for only the ports that shape has:

```fsharp
let Catalog () : StageCatalog =
    StageCatalog.Create
        [
            StageSpecification.Source(FeedStage, FeedParameterContract, Port.Out("out", OrderEventContract))
            StageSpecification.Flow(
                DiscountStage,
                DiscountParameterContract,
                Port.In("in", OrderEventContract),
                Port.Out("out", OrderDocumentContract)
            )
            StageSpecification.Sink(
                TallyStage,
                TallyParameterContract,
                Port.In("in", OrderDocumentContract),
                Port.Result("total", TallyContract)
            )
        ]
```

| Factory | Ports | Runtime shapes it declares |
|---|---|---|
| `Source(stage, parameters, out)` | one output | `Source` |
| `Flow(stage, parameters, in, out)` | one input, one output | `Element`, `ElementAsync` |
| `Sink(stage, parameters, in)` | one input | `Terminal` |
| `Sink(stage, parameters, in, result)` | one input, one result | `Terminal` yielding a result |
| `FanOut(stage, parameters, in, outs)` | one input, a collection of outputs | `Broadcast`, `Balance`, `Partition`, `Unzip` |
| `FanIn(stage, parameters, ins, out)` | a collection of inputs, one output | `Merge`, `Concat`, `Interleave`, `Zip`, `CombineLatest` |

Each takes an `IStageParameterValidator` as a last argument when the stage checks
its payloads. `Port.In`, `Port.Out`, and `Port.Result` take the port name as text
and the contract as an `ElementContract<T>` or a `ResultContract<T>`; overloads
taking a `ContractReference` serve a provider whose ports carry whatever a
deployment binds to them, which is how the shipped Orleans vocabulary declares
its own. `Port` lives in `Orleans.Dataflow.Definition` beside the port
specifications it builds, and ships in the `Orleans.Dataflow` package, because the
typed contracts it accepts are an authoring-plane assertion the language-neutral
package cannot make.

`StageSpecification.Create` is the general form, for the shapes these do not
cover — a stage requiring a capability, one with several result ports. Everything
after the stage and its parameter contract is optional and written by name, so
nothing has to be written to say that a stage has no ports of some kind.

A specification sorts its ports at construction, so canonical port order is the
same in every process that resolves it. That one order is read by three places:
the authoring side wires branches in it, the planner allocates legs in it, and a
provider's own router or combiner answers in it.

**A catalog has a fingerprint**, over the set of stages one host knows, and a host
refuses a pipeline naming a stage it cannot resolve — before running anything, by
name, rather than failing halfway through.

`AddCatalog` is callable more than once and the host's catalog is the union, so a
deployment composes vocabularies from several packages. Registering one stage
reference twice is refused, because two specifications for one reference are two
answers to one question.

**The local vocabulary is a catalog too.** `LocalStageCatalog.Instance` is the
`IStageCatalog` of every stage the [operator](operators.md) surface builds —
`local/buffer@v1`, `local/select@v1`, and the rest — and it is what a bare
`LocalDataflowHost` resolves against. It is exposed so a tool that validates a
document knows the whole vocabulary rather than only the registered half. Local
stages as a class are nondeployable: a buffer carries no delegate, but
`local/buffer@v1` resolves in this process's provider and nowhere else.

---

## Typed handles

A registered stage becomes a typed authoring value by pairing its specification
with element contracts. `RegisteredStage` has seven factories:

| Factory | Produces | Shape it checks |
|---|---|---|
| `RegisteredStage.Source<TOut>(catalog, stage, output)` | `RegisteredSource<TOut>` | 0 inputs, 1 output, 0 results |
| `RegisteredStage.Flow<TIn, TOut>(catalog, stage, input, output)` | `RegisteredFlow<TIn, TOut>` | 1 input, 1 output, 0 results |
| `RegisteredStage.Sink<TIn>(catalog, stage, input)` | `RegisteredSink<TIn>` | 1 input, 0 outputs, 0 results |
| `RegisteredStage.SinkWithResult<TIn, TResult>(catalog, stage, input, result)` | `RegisteredSinkWithResult<TIn, TResult>` | 1 input, 0 outputs, 1 result |
| `RegisteredStage.FanOut<TIn, TOut>(catalog, stage, input, output)` | `RegisteredFanOut<TIn, TOut>` | 1 input, ≥ 2 outputs of one contract, 0 results |
| `RegisteredStage.FanOut<TIn, TLeft, TRight>(catalog, stage, input, left, right)` | `RegisteredFanOut<TIn, TLeft, TRight>` | 1 input, exactly 2 unlike outputs, 0 results |
| `RegisteredStage.FanIn<TIn, TOut>(catalog, stage, input, output)` | `RegisteredFanIn<TIn, TOut>` | ≥ 2 inputs of one contract, 1 output, 0 results |
| `RegisteredStage.FanIn<TFirst, TSecond, TOut>(catalog, stage, first, second, output)` | `RegisteredFanIn<TFirst, TSecond, TOut>` | exactly 2 unlike inputs, 1 output, 0 results |

**Construction validates against the catalog immediately**: the stage must exist,
must have the shape the handle claims, and the element contracts must equal the
specification's port contracts. A mismatch is an `ArgumentException` at the
author's own line, not a diagnostic when the graph closes.

Every handle exposes `Stage` — the `StageRef` — and `Specification`, plus the
contracts of its own ports:

| Handle | Contract members | Arity member |
|---|---|---|
| `RegisteredSource<TOut>` | `Output` | — |
| `RegisteredFlow<TIn, TOut>` | `Input`, `Output` | — |
| `RegisteredSink<TIn>` | `Input` | — |
| `RegisteredSinkWithResult<TIn, TResult>` | `Input`, `Result` | — |
| `RegisteredFanOut<TIn, TOut>` | `Input`, `Output` | `Legs` |
| `RegisteredFanOut<TIn, TLeft, TRight>` | `Input`, `Left`, `Right` | — (exactly two) |
| `RegisteredFanIn<TIn, TOut>` | `Input`, `Output` | `Inputs` |
| `RegisteredFanIn<TFirst, TSecond, TOut>` | `First`, `Second`, `Output` | — (exactly two) |

**The arity is read from the specification rather than asked for.** How many legs
a junction has is a fact about the stage a provider registered; a handle that let
an author restate it would let the two disagree. A call with the wrong number of
branches is refused naming both numbers.

**A junction declares no result port.** A result is read from a terminal and a
junction is not one; requiring none rather than ignoring them keeps a stage from
declaring a result nothing in a graph could expose.

```fsharp
let Feed: Orleans.Dataflow.RegisteredSource<OrderEvent> =
    Orleans.Dataflow.RegisteredStage.Source(authoring, FeedStage, OrderEventContract)

let Discount: Orleans.Dataflow.RegisteredFlow<OrderEvent, OrderDocument> =
    Orleans.Dataflow.RegisteredStage.Flow(authoring, DiscountStage, OrderEventContract, OrderDocumentContract)

let Tally: Orleans.Dataflow.RegisteredSinkWithResult<OrderDocument, int64> =
    Orleans.Dataflow.RegisteredStage.SinkWithResult(authoring, TallyStage, OrderDocumentContract, TallyContract)
```

`authoring` there is a catalog the module built for itself — reading a catalog at
authoring time is what turns a typo into a diagnostic at the author's line rather
than a refusal at deployment time.

Attaching one takes an occurrence name and a payload — see
[operators](operators.md#reusing-and-composing) for every spelling.

---

## The factory

`Orleans.Dataflow.Hosting.IDataflowStageFactory`. One method, one factory per
provider, asked for every node of that provider.

```csharp
DataflowStageRuntime Create(DataflowStageRequest request);
```

`DataflowStageRequest` is a two-member record: `Node`, the node as the document
declares it, and `Specification`, the entry it resolved to in the host's catalog.
**Nothing else.** No document, no sibling node, no run identity, no services
beyond what the factory was constructed with. That is what lets the same stages be
composed into a pipeline the factory has never seen.

### Junction shapes

`DataflowStageRuntime` is the executable form, in the shapes the engine runs and
no others. **Four linear and nine junctions.** A provider that wants a shape this
type does not have is asking for a new engine primitive rather than a new stage.

| Factory | Shape |
|---|---|
| `DataflowStageRuntime.Source(open)` | a source: opens an `IAsyncEnumerable<object?>` given the run's tokens |
| `DataflowStageRuntime.Source(open, cursor)` | the same, declaring a [cursor](#cursors-and-marks) |
| `DataflowStageRuntime.Element(map)` | a synchronous one-in-one-out flow |
| `DataflowStageRuntime.ElementAsync(map, maxConcurrency, ordered)` | an asynchronous flow with a declared bound and ordering |
| `DataflowStageRuntime.Terminal(seed, fold, finish, producesResult)` | a sink; four overloads, crossing "seed sees the run's tokens" with "declares a [mark](#cursors-and-marks)" |
| `DataflowStageRuntime.Broadcast()` | one in, every element to every leg |
| `DataflowStageRuntime.Balance()` | one in, each element to one leg |
| `DataflowStageRuntime.Partition(route)` | one in, each element to the leg a function names |
| `DataflowStageRuntime.Unzip(parts)` | one in, each projection to a leg of its own |
| `DataflowStageRuntime.Merge()` | several in, whichever has an element |
| `DataflowStageRuntime.Concat()` | several in, in order |
| `DataflowStageRuntime.Interleave(segmentSize)` | several in, a declared number from each in turn |
| `DataflowStageRuntime.Zip(combine)` | several in, one row from one element of each |
| `DataflowStageRuntime.CombineLatest(combine)` | several in, a row on every arrival once all have produced |

**Every terminal is a fold**, including the ones that look like something else: a
seed, a step applied to each element, and an optional finish. The seed is made
once per run rather than handed over as a value, so two runs of one pipeline never
share it.

**A registered junction's semantics are the local junction's**, literally — the
same planning and the same strategy value — so the memory bounds, the pause
discipline, the drain-versus-abandon split, and the completion rules are the same
code.

### The run's tokens

`DataflowRunTokens` is handed to a source opener, and to a terminal's seed in the
overloads that take it. Three members:

| Member | What it is |
|---|---|
| `RunIdentity` | the identity of the run being opened |
| `RunToken` | cancelled when the run is cancelled |
| `StopToken` | signalled when the run is asked to stop gracefully |

A factory still receives no run identity: a stage *request* says what a stage is,
and these say which run is opening it.

### A complete factory

```csharp
internal sealed class SampleStageFactory : IDataflowStageFactory
{
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;

        if (node.Stage == SampleVocabulary.FeedStage)
        {
            int count = SampleVocabulary.ReadFeedCount(node.Parameters);

            return DataflowStageRuntime.Source(tokens => Orders(count, tokens));
        }

        if (node.Stage == SampleVocabulary.DiscountStage)
        {
            decimal percent = SampleVocabulary.ReadDiscountPercent(node.Parameters);

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

        if (node.Stage == SampleVocabulary.TallyStage)
        {
            decimal minimum = SampleVocabulary.ReadTallyMinimum(node.Parameters);

            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) => ((OrderDocument)element!).Amount >= minimum ? (long)state! + 1L : state,
                finish: null,
                producesResult: true);
        }

        throw new InvalidOperationException(
            $"The node '{node.Id}' is an occurrence of '{node.Stage}', which this provider does not implement.");
    }

    private static async IAsyncEnumerable<object?> Orders(
        int count,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = tokens;

        foreach (OrderEvent order in SampleOrders.Take(count))
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return order;

            await Task.Yield();
        }
    }
}
```

Two things in that code are the shape rather than the sample: the dispatch is on
`node.Stage`, and the fallthrough **throws naming the stage** rather than
returning null or dereferencing a missing key. A conformance check asserts exactly
that.

---

## Payloads: the three-views pattern

A provider's payload lives in exactly three places, and the pattern is that they
are three views of one statement:

| Place | What it is |
|---|---|
| the member names and one reader | an internal payload class with `const string` members, a `Write`, and a `TryRead` |
| a typed writer per stage | `public static CanonicalJsonValue XxxParameters(…)` on the vocabulary type |
| a validator over the reader | an `IStageParameterValidator` that runs `TryRead` and answers its violations |

and the factory that executes the stage reads the node's payload through **the
same `TryRead`**, so a member renamed in one place stops compiling in the other
three.

```fsharp
let FeedParameters (count: int) : CanonicalJsonValue =
    CanonicalJsonValue.Parse(
        System.String.Format(CultureInfo.InvariantCulture, "{{\"{0}\":{1}}}", CountMember, count)
    )

let ReadFeedCount (parameters: CanonicalJsonValue) : int =
    match (payloadOf "order feed" parameters).TryGetProperty CountMember with
    | true, counted ->
        match counted.TryGetInt32() with
        | true, count when count >= 0 -> count
        | _ -> invalidOp $"The order feed's '{CountMember}' is not a count of zero or more: {parameters}."
    | false, _ -> invalidOp $"The order feed's payload has no '{CountMember}': {parameters}."
```

**What the typed writer buys is a refusal at the line the author wrote** — a
period of zero, a backpressuring ingress for a clock, a mode this vocabulary does
not have. **What it cannot buy is the check**, and that is why the validator is not
optional: a document reaching a silo was not necessarily written through the
builder. It may be hand-authored, from another version, or from another provider
entirely, and the reader is the only thing standing between it and the factory.

The builders are sugar over the raw payload and nothing more, which is what makes
them safe to adopt: a builder writes byte for byte what the literal wrote, so
documents and fingerprints are unchanged. The definition plane never learns that a
builder exists.

**No string in a payload may resolve to a CLR type**, and none may be
assembly-qualified. A document causes no code loading; a conformance check asserts
it.

---

## Cursors and marks

Two abstract classes, and they are the only two externally derivable types in the
whole public surface. Everything else hand-written is sealed or static.

### `DataflowSourceCursor`

How a registered source says *where it is*, so a checkpoint can store the position
and a resume can reopen at it. A source **declares** a cursor by being built
through the `Source(open, cursor)` overload; every other registered source
contributes nothing to a checkpoint and **resumes from now**.

| Member | What it does |
|---|---|
| `abstract CanonicalJsonValue Position { get; }` | Where this source has reached. It means "handed over and delivered through its segment", never "committed at a sink". |
| `abstract void Delivered()` | Called once per element the run took, *after* that element travelled through the segment it entered. |
| `abstract void RestoreTo(position)` | Takes back a position reported earlier. Throws `InvalidOperationException` for a value this cursor does not understand. |

**`Delivered` is called by the run and never by the sequence**, and that
difference is the whole reason a stored position is exact. A sequence learns its
element was wanted only when the *next* one is asked for, and the moment between
those two is exactly where a capture's hold lands — a cursor that counted what it
had yielded would be one ahead at every capture. Exactly one element is ever
outstanding between a yield and its report.

**The cursor is the provider's own object and the opener closes over it.** Nothing
in the seam opens anything: an adapter restored to a position reads that position
from its own cursor instance when the run asks for its sequence, because only the
adapter knows whether a position is an index to skip, a token to subscribe at, or
an offset to seek to. One cursor is built per node per materialization, so two
runs of one pipeline never share one.

**The position is canonical JSON, and that is the seam's requirement rather than a
preference.** A checkpoint is read by a process that is not the one that wrote it.
**An adapter whose position cannot be said in that plane declares no cursor at
all** — which is a better answer than a position the engine could not honor.

**Threading.** `Delivered` is called from the source segment's own thread;
`Position` is read from the capture loop's while the run is held quiescent;
`RestoreTo` is called on the thread that materializes the run, before any segment
has started. An implementation whose position is more than one word writes it so
the reading is a fact rather than a race the quiescence happened to close.

### `DataflowSinkMark`

How a registered sink says *how far its side effect has actually got*. A sink
declares one by being built through a `Terminal(…, mark)` overload.

| Member | What it does |
|---|---|
| `abstract CanonicalJsonValue Mark { get; }` | How far the effect has committed. |
| `abstract void RestoreTo(mark)` | Takes back a mark reported earlier. |

A cursor answers "where did the source get to"; a mark answers "what did the sink
finish with". **A checkpoint carries both**, because they are different questions
and a resume needs both answers.

The shipped example is the
[grain-call sink](adapters.md#orleansgrain-call-sinkv1), whose mark is
`{"acknowledged":n}` and can lag the truth by up to its in-flight bound — which
widens a resume's replay and never narrows it.

**The 1.x policy for these two types**: additions arrive as *virtual* members with
working defaults, never as new abstract members, because an abstract addition
would break every provider compiled before it.

---

## The conformance kit

A provider ships a catalog and a factory, and everything that can go wrong between
them goes wrong quietly. `Orleans.Dataflow.Testing.ProviderConformance` is the
mechanical half of this SDK: point it at your own registration plus one accepted
payload per stage, and it answers nine checks over the pair.

```csharp
public static TheoryData<string> Checks => [.. ProviderConformance.Checks];

[Theory]
[MemberData(nameof(Checks))]
public void TheProviderConforms(string check) => Kit().Check(check);

private static ProviderConformance Kit() =>
    ProviderConformance.Create(
        MyStages.Provider,
        MyStages.Catalog,
        new MyStageFactory(registry),
        [ProviderStageSample.Create(MyStages.ReadStage, MyStages.ReadParameters(…))]);
```

One theory over `Checks` is the whole of what an author writes, and a check added
to the kit becomes a test in every provider's suite without that file changing.
Nothing in the kit names a test framework: a failure is a
[`ProviderConformanceException`](errors.md#the-testing-package) carrying every
violation the check found.

`ProviderConformance` has four members: `Checks` and `Create` are static,
`Check(name)` runs one, and `CheckAll()` runs every one.
`ProviderStageSample` has `Create` in two forms — the second taking
`optionalMembers`, whose names the payload check is then allowed to remove — and
reads back as `Stage`, `Parameters`, and `OptionalMembers`.

### The nine checks

| Check | What it asserts |
|---|---|
| `EveryPortCarriesADeclaredContractInCanonicalOrder` | Every port declares a created contract, names are unique across the stage, each port list is in ordinal order of its names, and a stage declares at least one port. |
| `EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare` | The stage has a reader; it accepts the sample; it refuses an added member, each removed required member, each retyped member, and a payload that is not an object — naming the member in single quotes each time; and it accepts a removed *optional* member. |
| `TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages` | Registration order does not change the fingerprint, two reads of the catalog do not, and a changed parameter contract does. |
| `TheFactoryAnswersForEveryStageTheCatalogDeclares` | The factory builds a non-null runtime for every declared stage. |
| `TheFactoryRefusesAStageTheCatalogDoesNotDeclare` | An unknown stage id and an unregistered major version are refused by *throwing, naming the stage* — not by a null reference, an index, a missing key, or a cast. |
| `EveryRuntimeHasTheShapeItsSpecificationDeclares` | Port counts imply a shape and the built runtime is that shape; a terminal produces a result exactly when the stage declares a result port; an unzip's projections match the leg count. |
| `EveryStageHasATypedHandleThatRefusesTheWrongShape` | The handle the specification implies is creatable, a handle of another shape is refused, and a contract no port declares is refused. |
| `NoParameterPayloadNamesAClrType` | No string in a payload resolves to a `Type`, and none is assembly-qualified. |
| `NoCoreOptionTypeNamesAnythingOfThisProvider` | No public `*Options` type of the core packages names a type of the provider's assembly or namespace. |

**The kit refuses to measure nothing.** A catalog declaring no stage of the named
provider, a declared stage with no sample, and a sample naming a stage the catalog
does not declare are all refused at `Create` — because a green suite that measured
nothing reads exactly like a green suite that measured everything.

### What the kit does not check

Stated here rather than discovered later.

- **Semantics.** Whether a source really ends its sequence on a stop token,
  whether a terminal's fold is associative, whether an adapter's acknowledgement
  boundary is where its documentation says it is: none of that is derivable from a
  catalog and a factory. [Adapters](adapters.md) is where those answers are stated
  and a provider's own tests are what prove them.
- **The runtime it builds is never run.** The factory is asked to build and the
  shape of what it built is read; nothing is opened, pulled, folded, or disposed.
  A source that throws on its first `MoveNextAsync` passes every check here.
- **The samples are the provider's own claim.** The kit mutates one payload per
  stage, so a member the sample omits is a member nothing is checked about. The
  sample should be the fullest payload the stage accepts, with its genuinely
  optional members named as such.

---

## What a mixed graph costs

A chain may hold registered and lambda stages, and closing it works. But every
*local* port declares an opaque contract while a registered port declares a real
one, so **every lambda-to-registered seam edge is an element-contract mismatch
under the graph compiler**, against any catalog. Weakening the contract rule to
treat the opaque contract as a wildcard would blunt contract checking for every
document to buy an authoring convenience.

Mixing is therefore an authoring and materialization affordance rather than a
definition-plane one. A graph you intend to deploy is registered end to end, and
`AsPipeline` tells you so by name.

**Capability tokens are conditional and causal.** `nondeployable` appears exactly
when a local-provider stage is present — local stages as a class are
nondeployable, since a buffer carries no delegate but resolves in this process's
provider and nowhere else. `ephemeral-identity` appears exactly when an occurrence
is auto-named. A document's capabilities are the union of every occurrence's
declared requirements, so a registered stage requiring a capability closes into a
document that declares it, and `AsPipeline` refuses any capability the target
catalog does not know.

---

## Related

- [Hosting](hosting.md) — where a catalog and a factory are registered.
- [Adapters](adapters.md) — the two vocabularies this library ships, written
  against this seam.
- [Operators](operators.md#registered-junctions) — attaching a registered stage.
- [Writing a custom stage](../guides/custom-stages.md) — the same material as a
  working program.
- [Graphs and identity](../concepts/graphs-and-identity.md) — why a document names
  stages rather than types.
