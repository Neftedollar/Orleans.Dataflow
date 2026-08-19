namespace Orleans.Dataflow.FSharpTests

open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Xunit
open Orleans.Dataflow.FSharpTests.Fixtures
open RegisteredVocabulary

/// <summary>
/// A pipeline of registered stages, authored in F# and run by the in-process host through the public provider
/// seam.
/// </summary>
/// <remarks>
/// <para>
/// The claim is not that the engine runs registered stages — the C# suite proves that on documents of its own
/// — but that a graph an F# author closed reaches them. Every stage of every graph here is registered, named,
/// resolved from a catalog the host was handed, and executed by a factory the host was handed, with nothing
/// bound in this process and no local stage anywhere in the document.
/// </para>
/// <para>
/// The host is built exactly the way a deployment builds one: <c>AddCatalog</c> and <c>AddFactory</c> on the
/// in-process builder, which is member for member what a silo writes. Since the same
/// <see cref="T:Orleans.Dataflow.Hosting.IDataflowStageFactory"/> value is what a silo registers, a provider
/// written once runs in either runtime, and this suite is the local half of that claim from F#.
/// </para>
/// <para>
/// Handles are bound with <c>use!</c>, for the reason <c>MaterializationTests</c> gives: a handle is
/// <see cref="T:System.IAsyncDisposable"/>, and <c>use!</c> disposes it at the end of the scope whether the
/// scope ends in a return or in a failed assertion. Every run here completes on its own before that.
/// </para>
/// </remarks>
type RegisteredRunTests() =

    [<Fact>]
    member _.``A registered chain authored in F# runs and resolves its result``() : Task =
        task {
            let host, provided = hostWithProvider ()

            let graph, total =
                Source.ofRegistered numberSource "numbers-in" sourceParameters
                |> Source.viaRegistered scale "scale-up" scaleParameters
                |> Source.toRegisteredResult "total" sumSink "sum-out" sumParameters

            use! run = host.MaterializeAsync(graph, token ())

            // Four elements from the source's own payload, tripled by the flow's own payload: 3+6+9+12.
            let! sum = run |> Run.value total (token ())

            Assert.Equal(30L, sum)

            do! run.Completion

            // Every node was built through the factory the host was handed, and named by the author.
            Assert.Equal<string>(
                [ "numbers-in"; "scale-up"; "sum-out" ],
                provided.Built.Keys |> Seq.sort |> Seq.toList)
        }

    [<Fact>]
    member _.``A registered terminal that declares no result still does its work``() : Task =
        task {
            let host, provided = hostWithProvider ()

            let graph =
                Source.ofRegistered numberSource "numbers-in" sourceParameters
                |> Source.viaRegistered scale "scale-up" scaleParameters
                |> Source.toRegistered labelSink "log-out" labelParameters

            use! run = host.MaterializeAsync(graph, token ())

            do! run.Completion

            Assert.Equal<int>([ 3; 6; 9; 12 ], provided.Observed |> Seq.toList)
        }

    [<Fact>]
    member _.``The payload of an occurrence decides what that occurrence does``() : Task =
        task {
            let host, _ = hostWithProvider ()

            let doubled = Orleans.Dataflow.Serialization.CanonicalJsonValue.Parse """{"factor":2}"""

            let graph, total =
                Source.ofRegistered numberSource "numbers-in" sourceParameters
                |> Source.viaRegistered scale "scale-up" doubled
                |> Source.toRegisteredResult "total" sumSink "sum-out" sumParameters

            use! run = host.MaterializeAsync(graph, token ())
            let! sum = run |> Run.value total (token ())

            // The same stage under the same handle, and a different stream, because a payload is document
            // content: 2+4+6+8 rather than 3+6+9+12.
            Assert.Equal(20L, sum)

            do! run.Completion
        }

    [<Fact>]
    member _.``A registered fan-out authored in F# runs and every leg sees the stream``() : Task =
        task {
            let host, _ = hostWithProvider ()

            let leftBranch, left =
                Flow.identity |> Branch.toRegisteredResult "left" sumSink "sum-left" sumParameters

            let rightBranch, right =
                Flow.identity |> Branch.toRegisteredResult "right" sumSink "sum-right" sumParameters

            let graph =
                Source.ofRegistered numberSource "numbers-in" sourceParameters
                |> Source.fanOutToRegistered split "split" junctionParameters [ leftBranch; rightBranch ]

            use! run = host.MaterializeAsync(graph, token ())
            let! leftSum = run |> Run.value left (token ())
            let! rightSum = run |> Run.value right (token ())

            // The provider's junction broadcasts, so both legs see all four elements: 1+2+3+4 twice.
            Assert.Equal(10L, leftSum)
            Assert.Equal(10L, rightSum)

            do! run.Completion
        }

    [<Fact>]
    member _.``A registered fan-in authored in F# runs and joins both streams``() : Task =
        task {
            let host, _ = hostWithProvider ()

            let graph, total =
                Source.ofRegistered numberSource "numbers-a" sourceParameters
                |> Source.fanInRegistered join "join" junctionParameters
                    [ Source.ofRegistered numberSource "numbers-b" sourceParameters ]
                |> Source.toRegisteredResult "total" sumSink "sum-out" sumParameters

            use! run = host.MaterializeAsync(graph, token ())
            let! sum = run |> Run.value total (token ())

            // Both sources reach the junction, so the join is the two streams' elements: 10 + 10.
            Assert.Equal(20L, sum)

            do! run.Completion
        }

    [<Fact>]
    member _.``A host that did not register this vocabulary refuses the graph``() : Task =
        task {
            let graph =
                Source.ofRegistered numberSource "numbers-in" sourceParameters
                |> Source.toRegistered labelSink "log-out" labelParameters

            // The lambda-only host of every other suite here: it resolves the local vocabulary and nothing
            // else, so a document naming a provider's stage is refused rather than run with a guess.
            let refused =
                Assert.ThrowsAsync<System.InvalidOperationException>(fun () ->
                    task {
                        // Never bound: the host refuses the document. `use!` is still what a caller who got
                        // a handle would want, and it costs nothing to write the refusable call that way.
                        use! _run = host.MaterializeAsync(graph, token ())

                        ()
                    }
                    :> Task)

            let! failure = refused

            Assert.Contains("fsharp-test/number-source", failure.Message)
        }

/// <summary>
/// The shipped conformance kit, pointed at the F# provider this suite's registered tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// The kit is the provider seam's own statement of what a provider owes a deployment, and it is written
/// against the two public halves — a catalog and a factory — with nothing about F# in it. Running it here is
/// therefore worth more than the tests it duplicates: it checks the parts of this vocabulary that no test of
/// the F# frontend would ever exercise, such as whether the readers refuse a member the stages do not declare
/// and whether a payload ever names a CLR type.
/// </para>
/// <para>
/// One test per check rather than one for all of them, because a failure then reads as the sentence that
/// stopped being true.
/// </para>
/// </remarks>
type ProviderConformanceTests() =

    /// <summary>The kit, pointed at this suite's provider.</summary>
    static let kit () =
        let provided = newProvider ()

        Orleans.Dataflow.Testing.ProviderConformance.Create(provider, catalog, provided.Factory, samples)

    /// <summary>The name of every check the kit runs, in the order it runs them.</summary>
    static member Checks: TheoryData<string> =
        let data = TheoryData<string>()

        for check in Orleans.Dataflow.Testing.ProviderConformance.Checks do
            data.Add check

        data

    [<Theory; MemberData(nameof ProviderConformanceTests.Checks)>]
    member _.``The F# provider conforms``(check: string) = kit().Check check
