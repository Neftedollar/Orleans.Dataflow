using System.Reflection;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Tests.Api;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredJunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// A branching pipeline of registered stages, materialized and run by the in-process host through the
/// public provider seam.
/// </summary>
/// <remarks>
/// <para>
/// The claim under test is not that a junction pump works — the engine's own junction suite proves that on
/// documents built directly — but that a provider can reach it. Every stage of every graph here is
/// registered, named, resolved from a catalog the host was handed, and executed by a factory the host was
/// handed, with nothing bound in this process and no local stage anywhere in the document.
/// </para>
/// <para>
/// The host is built exactly the way a deployment would build one: <c>AddCatalog</c> and <c>AddFactory</c>
/// on <see cref="ILocalDataflowBuilder"/>, mirroring what a silo writes. Since the same
/// <see cref="IDataflowStageFactory"/> value is what a silo registers, a provider written once runs in
/// either runtime, and this suite is the local half of that claim.
/// </para>
/// </remarks>
public sealed class RegisteredJunctionRunTests
{
    /// <summary>The executable shapes the seam publishes, by the name of the factory that builds each.</summary>
    private static readonly string[] Shapes =
    [
        "Source",
        "Element",
        "ElementAsync",
        "Terminal",
        "Broadcast",
        "Balance",
        "Partition",
        "Unzip",
        "Merge",
        "Concat",
        "Interleave",
        "Zip",
        "CombineLatest",
    ];

    [Fact]
    public async Task ARegisteredFanOutPipelineRunsOnTheLocalHost()
    {
        // Three events, broadcast to two legs, counted separately: the shape M4.2 could author and could
        // not deploy, and could not run through a provider at all.
        (LocalDataflowHost host, RegisteredJunctionProvider provider) = Host();
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> left, out ResultSlot<long> right);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(left, TestToken));
        Assert.Equal(3L, await run.GetValueAsync(right, TestToken));
        Assert.Equal(
            ["count-left", "count-right", "normalize", "orders-in", "split"],
            provider.Built.Keys.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ThePayloadDecidesWhatTheRegisteredJunctionDoes()
    {
        // The junction reads its own occurrence's payload, so the same stage under the same handle behaves
        // differently in two documents — and the two documents have two fingerprints, which is what makes
        // that honest. A balance delivers each element once, so the two legs sum to the stream's length
        // instead of each seeing all of it.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> left, out ResultSlot<long> right, BalanceParameters);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(left, TestToken) + await run.GetValueAsync(right, TestToken));
    }

    [Fact]
    public async Task ARegisteredFanInPipelineRunsOnTheLocalHost()
    {
        // Two registered sources, one registered junction, one registered terminal: the joining direction,
        // and the arithmetic that only a real join produces.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredFanIn(out ResultSlot<long> total);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AConcatenatingRegisteredFanInReadsOneInputToItsEndFirst()
    {
        // The same graph under the other payload. A concat reads its inputs in the specification's own port
        // order, so the count is the same and the difference is invisible in a count — which is why the
        // assertion is that both modes run and produce the whole stream, and the ordering claims stay with
        // the engine's own junction suite where they can be measured.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredFanIn(out ResultSlot<long> total, ConcatParameters);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AnUnlikeLeggedRegisteredFanOutRunsOnTheLocalHost()
    {
        // The unzip shape, whose three ports carry three contracts: each leg receives its own part of the
        // row and the two legs advance in lockstep, so both counts are the stream's length.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredUnzip(out ResultSlot<long> documents, out ResultSlot<long> keys);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(documents, TestToken));
        Assert.Equal(3L, await run.GetValueAsync(keys, TestToken));
    }

    [Fact]
    public async Task AnUnlikeInputRegisteredFanInRunsOnTheLocalHost()
    {
        // The zip shape: one row per element from each input, built by the provider's own combiner over the
        // columns the junction read in the specification's port order.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredZip(out ResultSlot<long> total);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AThreeLeggedRegisteredFanOutRunsOnTheLocalHost()
    {
        // Arity is read from the specification and wired positionally, so a junction of three has to run as
        // well as close. A broadcast delivers to all three, so each leg counts the whole stream.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredSpread(
            out ResultSlot<long> a,
            out ResultSlot<long> b,
            out ResultSlot<long> c);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(a, TestToken));
        Assert.Equal(3L, await run.GetValueAsync(b, TestToken));
        Assert.Equal(3L, await run.GetValueAsync(c, TestToken));
    }

    [Fact]
    public async Task AThreeInputRegisteredFanInRunsOnTheLocalHost()
    {
        // Three sources of three elements, merged: nine, which no arity but three could produce.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = RegisteredGather(out ResultSlot<long> total);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(9L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TheFactoryIsAskedExactlyOncePerNodePerMaterialization()
    {
        // The seam's contract, and it is load-bearing for a junction rather than incidental: the planner
        // asks whether a node is a fan-in before it walks the graph and asks again while it builds it, so a
        // planner that did not remember the answer would build two runtimes for one node — two lots of
        // per-run state, one of them executed and one of them silently discarded.
        (LocalDataflowHost host, RegisteredJunctionProvider provider) = Host();
        RunnableGraph graph = RegisteredFanIn(out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(graph.Document.Nodes.Count, provider.Built.Count);
        Assert.All(provider.Built, built => Assert.Equal(1, built.Value));
    }

    [Fact]
    public async Task TwoRunsOfOneGraphAreBuiltTwiceAndCountSeparately()
    {
        // The other half of "once per node per materialization": a second run asks the factory again, so
        // whatever a provider's closures hold is that run's own. A terminal that shared a counter across
        // runs would answer six here.
        (LocalDataflowHost host, RegisteredJunctionProvider provider) = Host();
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> left, out ResultSlot<long> _);

        await using (RunHandle first = await host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;

            Assert.Equal(3L, await first.GetValueAsync(left, TestToken));
        }

        await using RunHandle second = await host.MaterializeAsync(graph, TestToken);

        await second.Completion;

        Assert.Equal(3L, await second.GetValueAsync(left, TestToken));
        Assert.All(provider.Built, built => Assert.Equal(2, built.Value));
    }

    [Fact]
    public async Task AHostThatRegistersTheCatalogWithoutTheFactoryRefusesTheGraph()
    {
        // The two halves of the provider boundary are registered separately because different processes need
        // different halves, and this is what that costs when only one is present: the document validates —
        // the catalog resolves every stage — and materialization refuses it, naming the provider that has
        // nothing to build it.
        LocalDataflowHost host = new(builder => builder.AddCatalog(Catalog));
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("runtime factory", refused.Message, StringComparison.Ordinal);
        Assert.Contains(RegisteredFixtures.Provider, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostWithNeitherHalfRefusesTheGraphAtValidation()
    {
        // And with neither half it never reaches materialization at all: the graph compiler answers first,
        // naming every unresolvable node rather than the first one.
        LocalDataflowHost host = new();
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _, out ResultSlot<long> _);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("unknown-stage", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFactoryThatBuildsAJunctionWhereTheDocumentWiresAChainIsRefused()
    {
        // The catalog cannot catch this and is not supposed to: a specification describes ports and says
        // nothing about what a factory will build. So the planner is the check, and what it refuses is a
        // fan-out with one leg — which is a chain written the long way and not a junction at all.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = Source
            .FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .Via(Miscast, "miscast", RegisteredFixtures.NormalizeParameters)
            .To(RegisteredFixtures.IndexSink, "index-out", RegisteredFixtures.IndexParameters);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("miscast", refused.Message, StringComparison.Ordinal);
        Assert.Contains("routes to at least", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFanOutThatProjectsFewerPartsThanTheDocumentWiresLegsIsRefused()
    {
        // The other disagreement a junction can have with its own document, and the one only a run could
        // discover: three legs wired, two projections built. It is reached through the payload rather than
        // through a second stage, which is what makes it a statement about the seam.
        (LocalDataflowHost host, RegisteredJunctionProvider _) = Host();
        RunnableGraph graph = Source
            .FromRegistered(OrderSource, "orders-in", RegisteredFixtures.SourceParameters)
            .Via(Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .FanOutTo(
                Spread,
                "spread",
                HalvesParameters,
                Flow.For<OrderDocument>().To(CountSink, "count-a", RegisteredFixtures.CountParameters, "a", out ResultSlot<long> _),
                Flow.For<OrderDocument>().To(CountSink, "count-b", RegisteredFixtures.CountParameters, "b", out ResultSlot<long> _),
                Flow.For<OrderDocument>().To(CountSink, "count-c", RegisteredFixtures.CountParameters, "c", out ResultSlot<long> _));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await host.MaterializeAsync(graph, TestToken));

        Assert.Contains("splits a row into 2 parts and connects 3 outputs", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSeamAProviderNeedsIsPublicApiOfTheCorePackage()
    {
        // What "through the public seam only" means, checked rather than asserted in prose. The test
        // assembly is a friend of the core package, so nothing stops a fixture from reaching an internal;
        // what this pins is that it never had to — every type a provider states its vocabulary in is public,
        // and it is public in Orleans.Dataflow rather than in the Orleans hosting package, so a provider
        // that never references Orleans can still write one.
        Assembly core = typeof(LocalDataflowHost).Assembly;

        Assert.All(
            new[]
            {
                typeof(IDataflowStageFactory),
                typeof(DataflowStageRequest),
                typeof(DataflowStageRuntime),
                typeof(DataflowRunTokens),
                typeof(ILocalDataflowBuilder),
            },
            type =>
            {
                Assert.True(type.IsPublic, type.Name);
                Assert.Same(core, type.Assembly);
            });

        // Every executable shape a provider may build, including the two junctions, is a public factory on
        // the public runtime type. A seam that published four of them and kept the junctions internal would
        // pass every other test in this file, because this assembly is a friend. The names are looked up
        // across every public static method rather than by a single-match lookup, because a shape may be
        // spelled by more than one overload — a source that declares a cursor is still a source — and a
        // check that broke when one gained an overload would be a check on arity rather than on the seam.
        MethodInfo[] published = typeof(DataflowStageRuntime)
            .GetMethods(BindingFlags.Public | BindingFlags.Static);

        Assert.All(
            Shapes,
            name => Assert.Contains(published, shape => shape.Name == name));
    }

    [Fact]
    public void TheFixtureProvidersOwnSurfaceNamesNoInternalOfTheCorePackage()
    {
        // What can be checked of "public surface only" without reading IL: every type the provider derives
        // from, implements, or names in a member signature is public. It does not prove that no method body
        // reaches an internal — this assembly is a friend of the core package and nothing in the language
        // could stop one — so the claim this file makes is a discipline plus this check plus the one above,
        // and the residual gap is stated rather than implied.
        Type provider = typeof(RegisteredJunctionProvider);
        Assembly core = typeof(LocalDataflowHost).Assembly;

        List<Type> named = [.. provider.GetInterfaces(), provider.BaseType!];

        foreach (MemberInfo member in provider.GetMembers(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            if (member is MethodInfo method)
            {
                named.Add(method.ReturnType);
                named.AddRange(method.GetParameters().Select(parameter => parameter.ParameterType));
            }
            else if (member is PropertyInfo property)
            {
                named.Add(property.PropertyType);
            }
        }

        Assert.All(
            named.Where(type => type.Assembly == core),
            type => Assert.True(type.IsPublic || type.IsNestedPublic, type.FullName));
    }

    /// <summary>Builds the host a deployment of the fixture provider would build, and its factory.</summary>
    /// <returns>The host and the factory it was given, so a test can read what the factory was asked for.</returns>
    /// <remarks>
    /// The factory is returned rather than resolved from the host afterwards, because a host publishes no
    /// way to reach one — which is correct, and is why the counting lives on the fixture provider.
    /// </remarks>
    private static (LocalDataflowHost Host, RegisteredJunctionProvider Provider) Host()
    {
        RegisteredJunctionProvider provider = new();

        return (
            new LocalDataflowHost(builder => builder
                .AddCatalog(Catalog)
                .AddFactory(RegisteredJunctionProvider.Provider, provider)),
            provider);
    }
}
