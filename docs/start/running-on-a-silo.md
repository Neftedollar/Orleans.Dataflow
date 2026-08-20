# Running on a silo

After this page you have a pipeline executing inside an Orleans
[silo](../reference/glossary.md#silo) instead of in your own process — started
through the ordinary hosting API, watched to its end from a client, and read
through a [result slot](../reference/glossary.md#result-slot). You will also
know why the pipeline from the last page cannot go, and what to write instead.
About twenty minutes.

**What to write instead is a different job, and this page says so up front rather
than letting you discover it.** On the last two pages you were an *author*: you
composed values — a source, a flow, a sink, a result slot — and the library kept
the rest to itself. From Step 2 you are also a *publisher*: you give your steps
names, declare what those steps carry, and register the code behind the names on
the host that will run them. That is a second vocabulary arriving in one sitting,
and a reader who was not warned tends to conclude they have stopped understanding
the library. They have not — the job changed.

It is not ceremony you are being charged for nothing, either. A lambda cannot
travel. A pipeline that runs where it was not written therefore has to name its
steps, and naming them is precisely what buys it the ability to go. You publish
once per provider, and the authoring you already know comes back unchanged in
Step 5.

## Before you start

- [ ] You have finished [Your first pipeline](first-pipeline.md).
- [ ] A project that references `Orleans.Dataflow.Cluster`. That one reference
      brings `Orleans.Dataflow`, the abstractions, `Microsoft.Orleans.Server
      10.2.2` and the .NET generic host with it — you name none of them
      separately:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../orleans-dataflow/src/Orleans.Dataflow.Cluster/Orleans.Dataflow.Cluster.csproj" />
  </ItemGroup>
</Project>
```

## Step 1 — find out why your pipeline cannot go

A graph that runs in your process needs no name: you hold it in a variable, and
the variable is how you refer to it. A pipeline that runs somewhere else needs
two things your variable cannot supply, so `AsPipeline` asks for both.

- **`GraphId`** is the pipeline's *name*, and it stays the same across every
  version of it. `GraphId.Create("weather-daily")` validates the text
  immediately rather than at first use: lowercase ASCII letters, digits, and
  interior hyphens, refused by name if you hand it anything else. It is a type
  and not a `string` for the ordinary reason — the call takes two identifiers,
  and two strings side by side is a mistake waiting to happen.
- **`GraphRevision`** is the version of that pipeline's *shape*, starting at 1.
  You increase it when you change what the pipeline does.

Together they answer "which pipeline, in which version". A
[durable run](../reference/glossary.md#durable-run) is the reason both exist
separately: it continues the same name at the same revision, so the name is what
lets a stored position find its pipeline again, and the revision is what stops a
position taken of one shape being handed to another.

Here is a graph of the ordinary kind — ten readings, keep the ones at six or
above, count them — and an attempt to give it an identity:

```csharp
using Orleans.Dataflow;
using Orleans.Dataflow.Identity;

RunnableGraph local = Source.Range(1, 10)
    .Where(reading => reading >= 6)
    .To(s => s.Count(), "seen", out ResultSlot<long> _);

try
{
    _ = local.AsPipeline(GraphId.Create("weather-daily"), GraphRevision.Create(1));

    Console.WriteLine("no refusal, which would be a bug");
}
catch (ArgumentException refusal)
{
    Console.WriteLine(refusal.Message);
}
```

```console
dotnet run
```

```
This graph cannot become a PipelineDefinition because it breaks 2 deployability invariants:
1. it declares the capability 'ephemeral-identity', which says its node identifiers are positions rather than names, so nothing durable could be anchored to them; every occurrence of a pipeline is named by its author.
2. it declares the capability 'nondeployable', which says a stage's behavior is bound in this process and reaches no document, so nothing else could ever materialize it; every stage of a pipeline resolves from a catalog.
```

That is the whole obstacle, and it is worth understanding rather than working
around. `reading => reading >= 6` is a delegate. A delegate lives in your process
and a [graph document](../reference/glossary.md#graph-document) is JSON, so a
graph holding one is a [local stage](../reference/glossary.md#local-stage)
graph: perfect for your own process, impossible anywhere else. The library
refuses it here, by name, rather than shipping a document a silo could not
resolve and failing halfway through a run.

The fix is not to smuggle the lambda across. It is to give the stage a *name*,
and register the code behind that name on the silo. A stage a host knows by name
is a [registered stage](../reference/glossary.md#registered-stage), and the set
of them one host knows is its [catalog](../reference/glossary.md#catalog).

By the end of Step 6 you will have that same computation — ten readings, count
the ones at six or above — running inside a silo and handing the answer back.
Two stages will do it, a feed and a tally. Composing stages was the last page's
lesson and nothing about composition changes out here, so this page spends its
budget on the parts that do.

## Step 2 — give the two stages names

Steps 2 to 6 build one `Program.cs`. Start it with these directives — the
`Definition`, `Grains`, `Hosting`, `Identity` and `Serialization` namespaces all
appear before you are done:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Dataflow;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
```

**A name a machine that never saw your assembly can resolve.** That is the first
thing publishing needs. It is spelled in three parts — who published the stage,
which of their stages it is, and which generation of it — and the value that
holds all three is a `StageRef`.

```csharp
StageRef feedStage = StageRef.For("weather", "reading-feed");
StageRef tallyStage = StageRef.For("weather", "tally");
```

`weather/reading-feed@v1` is what a document says about which code to run. A silo
reads that, looks it up in its own catalog, and builds what it finds — no type
name, no assembly, nothing loaded.

The generation is the `v1` and `For` starts you at it, because a first stage is
always a first version. It rides *inside* the reference rather than beside it
because a stage that changes what it means is a different stage, and a document
already deployed should go on naming the one it was written against. When the day
comes to publish a second generation, name it: `StageRef.For("weather", "tally",
2)`.

The three parts have types of their own — `ProviderId`, `StageId`, and the
version — and `StageRef.Create` takes them when you are holding them already,
which is what a vocabulary published as a library does. For a program that spells
its names once, `For` says the same thing in one line and validates the same
text.

**A contract two sides can agree on without sharing a type.** That is the second
thing publishing needs, and it is what these two declarations are:

```csharp
ElementContract<int> reading = ElementContract.For<int>("weather-reading", 1);
ResultContract<long> total = ResultContract.For<long>("weather-total", 1);
```

Look at what a contract is: `"weather-reading"` and a major version. Not
`System.Int32`. What makes two stages connectable is that they agree on a
contract *identifier*, which is a fact a document can state and a silo in another
process can check without ever seeing your assembly. The `<int>` is this
process's private half of the deal — "here, that contract is an `int`" — and it
is what lets the authoring in Step 5 be typed at all.

**A name for the payload each stage takes.** A payload binds to no CLR type, so
its contract has no `<T>` — it is an identifier and a version, and that pair is a
`ContractReference`. Both stages here take a single number, so one name describes
both payloads:

```csharp
ContractReference parameters = ContractReference.For("weather-parameters");
```

`For` starts at version 1 the way `StageRef.For` does. That is every name this
program needs.

## Step 3 — declare the stages and what they do

A stage is two facts: what it *is* — its ports, and the contracts those ports
carry — and what it *does*. Write them together:

```csharp
StageProvider weather = StageProvider.Create("weather")
    .Source(
        feedStage,
        parameters,
        Port.Out("out", reading),
        request =>
        {
            int count = request.Node.Parameters.ToElement().GetProperty("n").GetInt32();

            return DataflowStageRuntime.Source(_ => Readings(count));
        })
    .Sink(
        tallyStage,
        parameters,
        Port.In("in", reading),
        Port.Result("total", total),
        request =>
        {
            int least = request.Node.Parameters.ToElement().GetProperty("n").GetInt32();

            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) => (int)element! >= least ? (long)state! + 1L : state,
                finish: null,
                producesResult: true);
        });
```

`Source` and `Sink` name the shapes the engine runs, and each asks only for the
ports its shape has, which is why nothing above declares a port it does not have.
[Declaring a stage](../guides/custom-stages.md#declaring-a-stage) has the rest of
the set — `Flow` for one in and one out, junctions for branching, and a general
form for anything they do not cover — along with the
[parameter validator](../guides/custom-stages.md#the-parameter-validator) a stage
adds when it wants its payloads refused before its code ever sees them.

**The delegates are still delegates.** They just live on the silo now, reached by
name, instead of travelling inside the document. What travels is the number:
`{"n":10}` in the document becomes `count` here, and `{"n":6}` becomes `least`.
That split has a section of its own in
[parameters that travel, code that stays behind](../guides/custom-stages.md#parameters-that-travel-code-that-stays-behind).

**Each delegate is handed one node and answers with one behaviour.** It is never
told about the graph, the run, or the cluster — which is what lets these two
stages end up in a pipeline this program has never seen. It runs once per
occurrence per run, so whatever the lambda captured is that run's own.
`DataflowStageRuntime.Terminal` is the shape of every sink: a seed, a fold, an
optional finish, and whether it produces a result. The seed is a *function*
rather than a value, so two runs of one pipeline never share it. It is one of
[six shapes](../guides/custom-stages.md#the-shapes-available) a stage may take;
a source that wants to be told when the run is stopping takes
[the two tokens](../guides/custom-stages.md#the-two-tokens) the ignored `_`
discards here, and a source that must resume where a crash left it declares
[a cursor](../guides/custom-stages.md#cursors-and-marks-for-durability). None of
those is needed to get a pipeline onto a silo, which is why none of them is on
this page.

The source pulls from this, a local function; put it at the bottom of the file,
below every statement, where it stays out of the way of the program's story:

```csharp
static async IAsyncEnumerable<object?> Readings(int count)
{
    for (int index = 1; index <= count; index++)
    {
        yield return index;
    }
}
```

It is declared `async` because that is what writing an `IAsyncEnumerable`
iterator takes. This one has nothing to await; a real source awaits whatever it
is reading from.

**What `weather` now holds is a seam with two sides.** `weather.Catalog` is the
data half — which stages exist, what their ports carry — and it is what Step 5
authors against and what a validating process would publish on its own. The
provider itself is the code half. They cannot disagree, because you stated each
stage once. [Writing a custom stage](../guides/custom-stages.md#when-the-halves-ship-apart)
has the case where they genuinely do ship apart, in two packages, and how to
register them separately then.

## Step 4 — start a silo that knows them

This is an ordinary generic host. No test facility is involved.

```csharp
HostApplicationBuilder builder = Host.CreateApplicationBuilder();

builder.Logging.ClearProviders();

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

    silo.AddOrleansDataflow(dataflow => dataflow.AddProvider(weather));

    silo.Services.AddOrleansDataflowClient();
});

using IHost silo = builder.Build();

await silo.StartAsync();
```

Four lines are the deployment story and the rest is Orleans.

`AddOrleansDataflow` registers the vocabulary this silo can run — both halves of
it, from the one value that holds both. A silo with no vocabulary at all is
refused when it starts, by name, because it could resolve no stage reference and
would refuse every document it was handed. `AddMemoryGrainStorage` under
`OrleansDataflowStorage.CoordinatorProviderName` is where the
[coordinator](../reference/glossary.md#coordinator) — the grain that owns a
pipeline's identity — keeps its state; a real deployment names a real provider
there and changes nothing else. `AddOrleansDataflowClient` registers the client
side, and it is on the silo's own services here because this one process is
both; when your clients are separate processes, they write that same line.

`ClearProviders` is only so the output below is readable. Keep your logging.

That is the last of the publishing. A provider shipping stages for other people
to use also has to prove its two halves agree, and the library ships that check
rather than leaving it to be rewritten per provider:
[the conformance kit](../guides/custom-stages.md#proving-it-with-the-conformance-kit)
is nine structural checks over a catalog and its factory.

## Step 5 — author the pipeline and give it an identity

You are back to being an author. This is the last page's surface, unchanged,
with names where the lambdas were:

```csharp
RegisteredSource<int> feed = RegisteredStage.Source(weather.Catalog, feedStage, reading);
RegisteredSinkWithResult<int, long> tally =
    RegisteredStage.SinkWithResult(weather.Catalog, tallyStage, reading, total);

PipelineDefinition pipeline = Source
    .FromRegistered(feed, "feed", StageParameters.Create().Add("n", 10).Build())
    .To(tally, "tally", StageParameters.Create().Add("n", 6).Build(), "total", out ResultSlot<long> _)
    .AsPipeline(GraphId.Create("weather-daily"), GraphRevision.Create(1));

ResultSlot<long> counted = pipeline.ResultSlot("total", total);
```

`RegisteredStage.Source` and its sibling resolve a name against the catalog and
hand back a *typed* handle, so a typo is a diagnostic while you are authoring
rather than a refusal at deployment time.

`StageParameters` writes the payload a member at a time. `{"n":10}` is short
enough to be tempting to type, and typing it is how a payload acquires a comma in
the wrong place, a decimal point the canonical form does not admit, or a number
formatted under whatever culture the machine happens to be set to. The builder
cannot express any of the three, and it ends in the very parse a hand-written
string would have gone through, so what is stored is the same bytes either way. A
stage that takes no parameters at all writes `CanonicalJsonValue.Empty`.

Both stages now carry a name of their own — `"feed"`, `"tally"`. Those are
[occurrence](../reference/glossary.md#occurrence) names: one *use* of a stage in
one graph. Two feeds in one pipeline would be two occurrences of one kind, and
the names are what make a failure legible — it can say `"feed"` rather than "the
first lambda".

`AsPipeline` is what a graph becomes when it has an identity and a
[revision](../reference/glossary.md#revision). Declaring an identity re-closes
the document under that identity, so a pipeline's
[fingerprint](../reference/glossary.md#fingerprint) is the fingerprint of the
deployable document and not of the anonymous graph it came from. Notice too that
the slot is recovered from the *pipeline* rather than kept from the closing call,
which is why that `out` parameter is discarded: a closed graph's slot binds to
that built instance, and a pipeline's binds to the fingerprint and the identity —
which is exactly what lets a run started by one process be read by another.

## Step 6 — materialize through the cluster and watch it end

```csharp
OrleansDataflowHost cluster = silo.Services.GetRequiredService<OrleansDataflowHost>();

await using (OrleansRunHandle run = await cluster.MaterializeAsync(pipeline))
{
    RunEnding ending = await run.WatchTermination;

    Console.WriteLine($"ending:                 {ending.Kind}");
    Console.WriteLine($"readings at 6 or above: {await run.GetValueAsync(counted)}");
}

await silo.StopAsync();
```

The handle is scoped to a block so that it is disposed while the silo is still
up. `await using` at the top level of a program disposes *after* your last
statement, which would be after `StopAsync`.

```console
dotnet run
```

```
ending:                 Completed
readings at 6 or above: 5
```

Six, seven, eight, nine and ten. It is the same answer the graph in Step 1 would
have given you in your own process, and it came back from a run your process did
not execute.

**`WatchTermination` is how a client learns a remote run is over.** It answers
with the [ending](../reference/glossary.md#ending) the run reached — `Completed`
or `Failed` — as a *value*, so a completed run and a failed one are told apart
by reading rather than by catching. `Completion` still exists and still faults
if you would rather inherit the failure; pick the one that matches what you are
writing. A monitor wants an ending; a caller that should die with the run wants
the completion.

## When it does not work

| What you see | What it means |
|---|---|
| `This graph cannot become a PipelineDefinition because it breaks 2 deployability invariants` | A lambda somewhere in the graph. The message numbers every violation it found, so one call names them all. Replace the local stages with registered ones. |
| `Orleans.Runtime.SiloUnavailableException: The local Orleans host is shutting down and can no longer process the request` on `DisposeAsync` | The run handle outlived the silo. `await using` at the top level of a program disposes *after* your last statement, so a bare `await using OrleansRunHandle run = …` followed by `await silo.StopAsync()` disposes into a stopped silo. Scope the handle in a block, as above. |
| `The node '…' is an occurrence of '…', which the provider '…' does not implement` | A document names a stage this vocabulary does not declare. The message ends by listing what it does declare, which is usually enough to spot the typo. |
| `A silo running Orleans.Dataflow publishes at least one vocabulary` | The silo started with no `AddProvider`, no `AddCatalog`, and none of the shipped vocabularies. It could resolve no stage reference, so it would refuse every document. |
| `The provider '…' was closed when it was first used` | A stage was declared after the vocabulary was registered or its catalog read. Declare every stage before either happens. |
| The run never ends | Your source never ends. A silo will happily run a pipeline for ever; if you meant it to finish, the source has to stop producing. |

## What you learned

- A graph carrying a delegate cannot be deployed, and the refusal names every
  reason.
- Deploying one means taking on a second job: naming the steps, declaring what
  they carry, and registering the code behind the names.
- A registered stage is a name a host knows plus code registered behind it, and
  `StageProvider` is where you write both at once.
- A contract identifies elements by name and version, not by CLR type.
- `AddOrleansDataflow` on a silo, `AddOrleansDataflowClient` on whatever starts
  runs — that is the whole registration.
- `AsPipeline` gives a graph an identity, and re-fingerprints it in doing so.
- `WatchTermination` reads an ending; `Completion` inherits a failure.
- Dispose the run handle before you stop the silo.

Next: [Surviving a crash](surviving-a-crash.md) — give a run a name, kill the
process, and continue it somewhere else.

## Where to look next

- [Writing a custom stage](../guides/custom-stages.md) — the publishing half of
  this page as its own subject, at the depth a provider needs: every runtime
  shape, the parameter validator, cursors and marks, and the conformance kit.
- [Orleans integration](../guides/orleans-integration.md) — the registration
  above with the options a real deployment sets.

The repository's samples run a larger version of this scenario twice, once in
each language, and compare the two documents on every build:
[`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs)
and
[`samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs),
with the silo in
[`samples/Orleans.Dataflow.Samples/SampleCluster.cs`](../../samples/Orleans.Dataflow.Samples/SampleCluster.cs)
and the catalog in
[`samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs).
Run them with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only cluster`.
