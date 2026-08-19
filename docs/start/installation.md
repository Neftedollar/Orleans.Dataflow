# Installation

After this page you have a project that references Orleans.Dataflow, builds, and
runs a ten-element [pipeline](../reference/glossary.md#pipeline) that prints a
count and a [fingerprint](../reference/glossary.md#fingerprint). It takes about
ten minutes, and most of that is reading the table of five assemblies so you
never have to guess which one a type came from.

## Before you start

- [ ] **.NET 10 SDK.** Run `dotnet --version`; you want `10.0.x`. Orleans.Dataflow
      targets `net10.0` only.
- [ ] **A clone of this repository.** The packages are not on NuGet yet — see
      [Getting the code](#getting-the-code) below — so you reference the projects
      by path.
- [ ] **For F#: no extra package.** The SDK's own `FSharp.Core` is newer than the
      floor the F# frontend declares (`10.1.201`), so you add none. You *will*
      have to edit the generated project file, for a reason that has nothing to
      do with this library — see [Step 5](#step-5--the-same-thing-in-f).
- [ ] **For a cluster: nothing extra either.** `Microsoft.Orleans.Server 10.2.2`
      arrives through the hosting assembly, and it brings the .NET generic host
      with it.

In one table:

| | Version |
|---|---|
| .NET | `net10.0`, and only `net10.0` |
| C# | 14 |
| Orleans | `10.2.2` — the version built and tested against |
| FSharp.Core | `10.1.201` or later; the SDK's own is newer, so you add nothing |

[Compatibility](../reference/compatibility.md) has the rest: what the API
guarantee covers, which assemblies can see Orleans at all, and what is not
claimed.

## Getting the code

There is no published package. Asking for one gets you this:

```console
$ dotnet add try package Orleans.Dataflow
info :   GET https://api.nuget.org/v3/registration5-gz-semver2/orleans.dataflow/index.json
info :   NotFound https://api.nuget.org/v3/registration5-gz-semver2/orleans.dataflow/index.json
error: There are no versions available for the package 'Orleans.Dataflow'.
```

That is expected, not a broken feed. Until the packages ship, you build against
the repository: clone it, and add a `ProjectReference` to the assembly you need.
Everything on this page and in the pages after it works that way, and the day a
package appears the only line that changes is the reference.

What this costs you in the meantime is worth saying plainly. You are pinned to
whatever commit you cloned; there is no version number to write down, no
restore-time reproducibility, and no way to hand a colleague a project file that
resolves on its own. If that matters for what you are building, wait for the
packages. If you are evaluating, a `ProjectReference` is fine and is exactly how
the repository's own samples are wired.

## The five assemblies

The library ships as five assemblies. Only one of them names Orleans, and that
is the whole point of the split: the parts that describe and run a pipeline
cannot break when Orleans releases, because they cannot see Orleans.

| Assembly | What is in it | What it brings with it |
|---|---|---|
| `Orleans.Dataflow.Abstractions` | The language-neutral contracts: [graph documents](../reference/glossary.md#graph-document), identities, [canonical JSON](../reference/glossary.md#canonical-json), the validator. Namespaces `Definition`, `Identity`, `Serialization`, `Compilation`. | Nothing. |
| `Orleans.Dataflow` | Authoring (`Source`, `Flow`, `Sink`) and the local engine (`LocalDataflowHost`, `RunHandle`). Namespaces `Orleans.Dataflow`, `Authoring`, `Adapters`, `Hosting`. | `Orleans.Dataflow.Abstractions`. |
| `Orleans.Dataflow.Orleans` | Cluster hosting: `AddOrleansDataflow`, `OrleansDataflowHost`, `OrleansRunHandle`, the grains behind them. | `Orleans.Dataflow` and the Orleans packages. |
| `Orleans.Dataflow.FSharp` | The F# frontend: the `Source`, `Flow`, `Sink`, `Branch`, `Pipeline` and `Run` modules. | `Orleans.Dataflow` and `FSharp.Core`. |
| `Orleans.Dataflow.Testing` | Probes for tests: `TestSource`, `TestSink`, `TestFlow`, `TestClock`, `InMemoryCheckpointStore`, and the provider conformance kit. | `Orleans.Dataflow`. |

Two things about that table catch people out.

**A namespace does not name its package.** `Orleans.Dataflow.Hosting` holds
`ICheckpointStore` and the stage-factory seam from the core assembly, *and*
`OrleansDataflowHost` and the silo builders from the Orleans one. Same for
`Orleans.Dataflow.Adapters`. If a type will not resolve, the namespace is not
the clue — this table is.

**References are transitive, so you name one.** Referencing
`Orleans.Dataflow.Orleans` gets you the core and the abstractions for free. You
never need to list all three.

## Which one you need

| What you are doing | Reference |
|---|---|
| Building and running pipelines in your own process | `Orleans.Dataflow` |
| Running pipelines on a [silo](../reference/glossary.md#silo), or writing the client that starts them | `Orleans.Dataflow.Orleans` |
| Authoring in F# | `Orleans.Dataflow.FSharp` (add `Orleans.Dataflow.Orleans` too if you also host) |
| Writing tests for a pipeline | `Orleans.Dataflow.Testing`, in the test project only |
| Writing a custom stage that others deploy | `Orleans.Dataflow` — the seam lives in the core assembly, so a stage provider never references Orleans |

## Step 1 — make a project

```console
dotnet new console -o Readings
cd Readings
```

## Step 2 — reference the library

Open `Readings.csproj` and add the reference, with the path pointing at your
clone:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../orleans-dataflow/src/Orleans.Dataflow/Orleans.Dataflow.csproj" />
  </ItemGroup>
</Project>
```

## Step 3 — write something that runs

Replace `Program.cs` with this:

```csharp
using Orleans.Dataflow;

RunnableGraph graph = Source.Range(1, 10)
    .To(s => s.Count(), "seen", out ResultSlot<long> seen);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

Console.WriteLine($"elements:    {await run.GetValueAsync(seen)}");
Console.WriteLine($"fingerprint: {graph.Fingerprint}");

await run.Completion;
```

## Step 4 — verify

```console
dotnet run
```

```
elements:    10
fingerprint: sha256:8d867140b6cd699f44c7927c154ada71efbc4cf980357890d492d0b2b7bc7e2f
```

Two lines, and both of them mean something. The count came out of a
[result slot](../reference/glossary.md#result-slot) — a named, typed place a
sink's value appears. The fingerprint is the SHA-256 of the graph's canonical
bytes, and it is the same on your machine as it is here: if you got a different
one, you did not write the same graph.

## Step 5 — the same thing in F#

The F# frontend is not a wrapper. It builds the same documents, so the
fingerprints match, which you will see for yourself on the
[next page](first-pipeline.md).

```console
dotnet new console -lang F# -o Readings.FSharp
```

**Fix the project file before you do anything else.** On the .NET 10 SDK the F#
console template does not build as generated — not with this library, not at
all:

```console
$ dotnet build
error NETSDK1022: Duplicate 'Compile' items were included. The .NET SDK includes 'Compile' items
from your project directory by default. You can either remove these items from your project file,
or set the 'EnableDefaultCompileItems' property to 'false' if you want to explicitly include them
in your project file. … The duplicate items were: 'Program.fs'
```

The SDK now globs `.fs` files by default, and the template also lists them. Take
the second option rather than the first: compile order is load-bearing in F#, and
you want to own it. Here is the whole working file:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <EnableDefaultCompileItems>false</EnableDefaultCompileItems>
  </PropertyGroup>
  <ItemGroup>
    <Compile Include="Program.fs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="../orleans-dataflow/src/Orleans.Dataflow.FSharp/Orleans.Dataflow.FSharp.fsproj" />
  </ItemGroup>
</Project>
```

One more thing surprises people: the SDK's implicit `FSharp.Core` is *newer* than
the floor the frontend declares, so do **not** add your own `PackageReference` to
it. If you do, you get `warning NU1504: Duplicate 'PackageReference' items found.
… The duplicate 'PackageReference' items are: FSharp.Core 10.1.400, FSharp.Core
10.1.201.` The version in the file above is deliberately absent.

## When it does not work

| What you see | What it means |
|---|---|
| `error: There are no versions available for the package 'Orleans.Dataflow'.` | You tried `dotnet add package`. There is no package; use a `ProjectReference`. |
| `error NETSDK1045: The current .NET SDK does not support targeting .NET 10.0` | Your SDK is older than 10. `dotnet --list-sdks` will say so. |
| `error CS0246: The type or namespace name 'OrleansDataflowHost' could not be found` | You referenced `Orleans.Dataflow` but that type lives in `Orleans.Dataflow.Orleans`. See the table above — the namespace they share is `Orleans.Dataflow.Hosting`. |
| `warning NU1504: Duplicate 'PackageReference' items ... FSharp.Core` | An F# project with its own `FSharp.Core` reference. Remove it; the SDK supplies a newer one. |
| `error NETSDK1022: Duplicate 'Compile' items were included` | The F# console template as generated. It both lists `Program.fs` and lets the SDK glob it, and it fails on an empty project before this library is involved. Set `EnableDefaultCompileItems` to `false`. |

## What you learned

- The library is five assemblies; one of them sees Orleans and four do not.
- A namespace does not tell you which assembly a type is in; the table does.
- References are transitive, so you name exactly one.
- There is no package yet, and what that costs you.
- A graph is a value with a fingerprint, and running it is a separate step.

Next: [Your first pipeline](first-pipeline.md) — fifteen minutes, and the same
pipeline in both languages producing the same fingerprint.
