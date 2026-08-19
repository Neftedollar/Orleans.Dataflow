# Running on a silo

After this page you have a pipeline executing inside an Orleans
[silo](../reference/glossary.md#silo) instead of in your own process — started
through the ordinary hosting API, watched to its end from a client, and read
through a [result slot](../reference/glossary.md#result-slot). You will also
know why the pipeline from the last page cannot go, and what to write instead.
About twenty-five minutes.

## Before you start

- [ ] You have finished [Your first pipeline](first-pipeline.md).
- [ ] A project that references `Orleans.Dataflow.Orleans`. That one reference
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
    <ProjectReference Include="../orleans-dataflow/src/Orleans.Dataflow.Orleans/Orleans.Dataflow.Orleans.csproj" />
  </ItemGroup>
</Project>
```

## Step 1 — find out why your pipeline cannot go

Take the graph from the last page and try to give it an identity:

```csharp
using Orleans.Dataflow;
using Orleans.Dataflow.Identity;

RunnableGraph local = Source.Range(1, 10)
    .Select(reading => reading * 3)
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
around. `reading => reading * 3` is a delegate. A delegate lives in your process
and a [graph document](../reference/glossary.md#graph-document) is JSON, so a
graph holding one is a [local stage](../reference/glossary.md#local-stage)
graph: perfect for your own process, impossible anywhere else. The library
refuses it here, by name, rather than shipping a document a silo could not
resolve and failing halfway through a run.

The fix is not to smuggle the lambda across. It is to give the stage a *name*,
and register the code behind that name on the silo. A stage a host knows by name
is a [registered stage](../reference/glossary.md#registered-stage), and the set
of them one host knows is its [catalog](../reference/glossary.md#catalog).

## Step 2 — name the stages

Steps 2 to 6 build one `Program.cs`. Start it with these directives — the
`Hosting`, `Definition`, `Identity` and `Serialization` namespaces all appear
before you are done:

```csharp
using System.Globalization;
using System.Runtime.CompilerServices;
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

A catalog is data: for each stage, its reference, its input and output ports,
the contracts those ports carry, and the contract of the JSON payload it takes.

```csharp
ProviderId provider = ProviderId.Create("weather");

StageRef feedStage = StageRef.Create(provider, StageId.Create("reading-feed"), StageRef.FirstMajorVersion);
StageRef scaleStage = StageRef.Create(provider, StageId.Create("scale"), StageRef.FirstMajorVersion);
StageRef tallyStage = StageRef.Create(provider, StageId.Create("tally"), StageRef.FirstMajorVersion);

ElementContract<int> reading = ElementContract.For<int>("weather-reading", 1);
ResultContract<long> total = ResultContract.For<long>("weather-total", 1);
ContractReference parameters = ContractReference.Create(ContractId.Create("weather-parameters"), 1);

StageCatalog catalog = StageCatalog.Create(
[
    StageSpecification.Create(
        feedStage,
        [],
        [OutputPortSpecification.Create(PortId.Create("out"), reading.Reference)],
        [],
        parameters,
        []),
    StageSpecification.Create(
        scaleStage,
        [InputPortSpecification.Create(PortId.Create("in"), reading.Reference)],
        [OutputPortSpecification.Create(PortId.Create("out"), reading.Reference)],
        [],
        parameters,
        []),
    StageSpecification.Create(
        tallyStage,
        [InputPortSpecification.Create(PortId.Create("in"), reading.Reference)],
        [],
        [ResultPortSpecification.Create(PortId.Create("total"), total.Reference)],
        parameters,
        []),
]);
```

Look at what a contract is: `"weather-reading"` and a major version. Not
`System.Int32`. What makes two stages connectable is that they agree on a
contract *identifier*, which is a fact a document can state and a silo in
another process can check without ever seeing your assembly.

The empty lists are the ports a stage does not have — a source has no inputs, a
terminal has no outputs — and the last one is the capabilities a stage requires,
which none of these do.

## Step 3 — say what those names do

The catalog is half of a seam. The other half is a factory: given a node from a
document, build the thing that runs.

This one is a type, so in a top-level-statements file it goes at the **bottom**,
after every statement. Put it above them and the compiler says `error CS8803:
Top-level statements must precede namespace and type declarations.`

```csharp
internal sealed class WeatherStages(StageRef feed, StageRef scale, StageRef tally) : IDataflowStageFactory
{
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageNode node = request.Node;
        int n = node.Parameters.ToElement().GetProperty("n").GetInt32();

        if (node.Stage == feed)
        {
            return DataflowStageRuntime.Source(_ => Readings(n));
        }

        if (node.Stage == scale)
        {
            return DataflowStageRuntime.Element(element => (int)element! * n);
        }

        if (node.Stage == tally)
        {
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                (state, element) => (int)element! >= n ? (long)state! + 1L : state,
                finish: null,
                producesResult: true);
        }

        throw new InvalidOperationException($"'{node.Stage}' is not a stage this provider implements.");
    }

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
}
```

The delegates are still delegates — they just live on the silo now, reached by
name, instead of travelling inside the document. The payload is where the
numbers travel: `{"n":10}` in the document becomes `n` here.

`DataflowStageRuntime.Terminal` is the shape of every sink: a seed, a fold, an
optional finish, and whether it produces a result. The seed is a *function*
rather than a value, so two runs of one pipeline never share it.

## Step 4 — start a silo that knows them

This is an ordinary generic host. No test facility is involved.

```csharp
HostApplicationBuilder builder = Host.CreateApplicationBuilder();

builder.Logging.ClearProviders();

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

    silo.AddOrleansDataflow(dataflow => dataflow
        .AddCatalog(catalog)
        .AddFactory(provider, new WeatherStages(feedStage, scaleStage, tallyStage)));

    silo.Services.AddOrleansDataflowClient();
});

using IHost silo = builder.Build();

await silo.StartAsync();
```

Four lines are the deployment story and the rest is Orleans.

`AddOrleansDataflow` registers the vocabulary this silo can run: the catalog of
names, and the factory that builds them. `AddMemoryGrainStorage` under
`OrleansDataflowStorage.CoordinatorProviderName` is where the
[coordinator](../reference/glossary.md#coordinator) — the grain that owns a
pipeline's identity — keeps its state; a real deployment names a real provider
there and changes nothing else. `AddOrleansDataflowClient` registers the client
side, and it is on the silo's own services here because this one process is
both; when your clients are separate processes, they write that same line.

`ClearProviders` is only so the output below is readable. Keep your logging.

## Step 5 — author the pipeline and hand it over

```csharp
RegisteredSource<int> feed = RegisteredStage.Source(catalog, feedStage, reading);
RegisteredFlow<int, int> scale = RegisteredStage.Flow(catalog, scaleStage, reading, reading);
RegisteredSinkWithResult<int, long> tally = RegisteredStage.SinkWithResult(catalog, tallyStage, reading, total);

RunnableGraph graph = Source.FromRegistered(feed, "feed", N(10))
    .Via(scale, "scale", N(3))
    .To(tally, "tally", N(15), "total", out ResultSlot<long> _);

PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("weather-daily"), GraphRevision.Create(1));
ResultSlot<long> counted = pipeline.ResultSlot("total", total);

static CanonicalJsonValue N(int value) =>
    CanonicalJsonValue.Parse(string.Format(CultureInfo.InvariantCulture, "{{\"n\":{0}}}", value));
```

`RegisteredStage.Source` and its siblings resolve a name against the catalog and
hand back a *typed* handle, so a typo is a diagnostic while you are authoring
rather than a refusal at deployment time.

Every stage now carries a name of its own — `"feed"`, `"scale"`, `"tally"`.
Those are [occurrence](../reference/glossary.md#occurrence) names: one *use* of
a stage in one graph. Two `scale` stages in one pipeline would be two
occurrences of one kind, and the names are what make a failure legible — it can
say `"scale"` rather than "the second lambda".

`AsPipeline` is what a graph becomes when it has an identity and a
[revision](../reference/glossary.md#revision). Notice the slot is recovered from
the *pipeline* rather than kept from the closing call, which is why that `out`
parameter is discarded. A closed graph's slot binds to that built instance; a
pipeline's binds to the fingerprint and the identity — which is exactly what
lets a run started by one process be read by another.

## Step 6 — materialize through the cluster and watch it end

```csharp
OrleansDataflowHost cluster = silo.Services.GetRequiredService<OrleansDataflowHost>();

// The handle is disposed inside this block, while the silo is still up.
await using (OrleansRunHandle run = await cluster.MaterializeAsync(pipeline))
{
    RunEnding ending = await run.WatchTermination;
    long above = await run.GetValueAsync(counted);
    RunSnapshot snapshot = await run.SnapshotAsync();

    Console.WriteLine($"ending:               {ending.Kind}");
    Console.WriteLine($"status:               {snapshot.Status}");
    Console.WriteLine($"readings 15 or above: {above}");
    Console.WriteLine($"graph:                {graph.Fingerprint}");
    Console.WriteLine($"pipeline:             {pipeline.Fingerprint}");
}

await silo.StopAsync();
```

```console
dotnet run
```

```
ending:               Completed
status:               Completed
readings 15 or above: 6
graph:                sha256:8d74d7eb4708b85dbf3fe9e06e470d7b8468675b8b40d2e6b8e9468900c4b781
pipeline:             sha256:2af4a08d062fdf2b5355debd3df4cd954fab2960dfe15ad8d991089d58f43a6e
```

Ten readings, tripled, and six of them land at 15 or above (5×3 through 10×3).
The arithmetic is the least interesting line.

**`WatchTermination` is how a client learns a remote run is over.** It answers
with the [ending](../reference/glossary.md#ending) the run reached — `Completed`
or `Failed` — as a *value*, so a completed run and a failed one are told apart
by reading rather than by catching. `Completion` still exists and still faults
if you would rather inherit the failure; pick the one that matches what you are
writing. A monitor wants an ending; a caller that should die with the run wants
the completion.

**The two fingerprints differ, and that is correct.** Declaring an identity
re-closes the document under that identity, so a pipeline's fingerprint is the
fingerprint of the deployable document rather than of the anonymous graph it
came from.

## When it does not work

| What you see | What it means |
|---|---|
| `This graph cannot become a PipelineDefinition because it breaks 2 deployability invariants` | A lambda somewhere in the graph. The message numbers every violation it found, so one call names them all. Replace the local stages with registered ones. |
| `Orleans.Runtime.SiloUnavailableException: The local Orleans host is shutting down and can no longer process the request` on `DisposeAsync` | The run handle outlived the silo. `await using` at the top level of a program disposes *after* your last statement, so a bare `await using OrleansRunHandle run = …` followed by `await silo.StopAsync()` disposes into a stopped silo. Scope the handle in a block, as above. |
| `'…' is not a stage this provider implements.` | Your factory was handed a node it does not build. Usually a stage in the catalog with no branch in `Create`. |
| The run never ends | Your source never ends. A silo will happily run a pipeline for ever; if you meant it to finish, the source has to stop producing. |

## What you learned

- A graph carrying a delegate cannot be deployed, and the refusal names every
  reason.
- A registered stage is a name in a catalog plus code a host registers behind it.
- A contract identifies elements by name and version, not by CLR type.
- `AddOrleansDataflow` on a silo, `AddOrleansDataflowClient` on whatever starts
  runs — that is the whole registration.
- `AsPipeline` gives a graph an identity, and re-fingerprints it in doing so.
- `WatchTermination` reads an ending; `Completion` inherits a failure.
- Dispose the run handle before you stop the silo.

Next: [Surviving a crash](surviving-a-crash.md) — give a run a name, kill the
process, and continue it somewhere else.

## Where to look next

The repository's samples run this same scenario twice, once in each language, and
compare the two documents on every build:
[`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs)
and
[`samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs),
with the silo in
[`samples/Orleans.Dataflow.Samples/SampleCluster.cs`](../../samples/Orleans.Dataflow.Samples/SampleCluster.cs)
and the catalog in
[`samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Vocabulary.fs).
Run them with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only cluster`.
