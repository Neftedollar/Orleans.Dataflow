namespace Orleans.Dataflow.FSharpTests

open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Identity
open Xunit
open RegisteredVocabulary

/// <summary>
/// The M7.4 half of the M7 invariant: an F#-authored <em>deployable</em> graph and its C#-authored twin are
/// one document, and the pipeline each closes into is one pipeline.
/// </summary>
/// <remarks>
/// <para>
/// The registered vocabulary is where the two frontends could most easily disagree without either being
/// obviously wrong: the ports are the provider's, the occurrence names are the author's, and the arity is
/// the stage's, so a frontend that read any of the three from somewhere else would still build a document.
/// Fingerprint equality against a twin built from the very same handles is what makes "the same graph" a
/// measurement rather than a reading of two sources side by side.
/// </para>
/// <para>
/// The pipeline cases are the only place in this suite where a fingerprint is deliberately <em>not</em> the
/// graph's: <c>AsPipeline</c> re-closes the content under a real identity, so a pipeline's fingerprint differs
/// from the graph's and equals the twin pipeline's. Both facts are asserted, because only the pair of them
/// says the identity is document content.
/// </para>
/// </remarks>
type RegisteredParityTests() =

    /// <summary>The lambda graph the deployability guard is pointed at, in each frontend's own spelling.</summary>
    static let lambdaGraphs () =
        let fsharpGraph = Source.ofSeq [ 1; 2; 3 ] |> Source.toSink Sink.ignore

        let csharpGraph =
            Orleans.Dataflow.Source.From([ 1; 2; 3 ]).To(Orleans.Dataflow.Sink.Ignore<int>())

        fsharpGraph, csharpGraph

    [<Fact>]
    member _.``A registered F#-authored chain and its C#-authored twin share one fingerprint``() =
        let fsharpGraph, _ =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.viaRegistered scale "scale-up" scaleParameters
            |> Source.toRegisteredResult "total" sumSink "sum-out" sumParameters

        let mutable slot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-in", sourceParameters)
                .Via(scale, "scale-up", scaleParameters)
                .To(sumSink, "sum-out", sumParameters, "total", &slot)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered chain closed with no result is the same document from either frontend``() =
        let fsharpGraph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.viaRegistered scale "scale-up" scaleParameters
            |> Source.toRegistered labelSink "log-out" labelParameters

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-in", sourceParameters)
                .Via(scale, "scale-up", scaleParameters)
                .To(labelSink, "log-out", labelParameters)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered chain becomes a pipeline, and the two frontends' pipelines are one document``() =
        let fsharpGraph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.toRegistered labelSink "log-out" labelParameters

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-in", sourceParameters)
                .To(labelSink, "log-out", labelParameters)

        let fsharpPipeline = fsharpGraph |> Pipeline.define "numbers" 3

        let csharpPipeline =
            csharpGraph.AsPipeline(GraphId.Create "numbers", GraphRevision.Create 3)

        Assert.Equal(csharpPipeline.Fingerprint, fsharpPipeline.Fingerprint)

        // The identity is document content, so the pipeline is not the graph re-labelled: the two
        // fingerprints differ, and that difference is what makes a pipeline's fingerprint the deployable
        // document's rather than the anonymous graph's.
        Assert.NotEqual(fsharpGraph.Fingerprint, fsharpPipeline.Fingerprint)
        Assert.Equal(GraphId.Create "numbers", fsharpPipeline.Id)
        Assert.Equal(GraphRevision.Create 3, fsharpPipeline.Revision)

    [<Fact>]
    member _.``A lambda graph is refused as a pipeline with the C# guard's own message``() =
        let fsharpGraph, csharpGraph = lambdaGraphs ()

        let refusedFSharp =
            Assert.Throws<System.ArgumentException>(fun () -> fsharpGraph |> Pipeline.define "numbers" 1 |> ignore)

        let refusedCSharp =
            Assert.Throws<System.ArgumentException>(fun () ->
                csharpGraph.AsPipeline(GraphId.Create "numbers", GraphRevision.Create 1) |> ignore)

        // Not "a message that mentions the same things": the same message. The guard is the C# package's and
        // this frontend calls it rather than restating why a lambda graph is not deployable.
        Assert.Equal(refusedCSharp.Message, refusedFSharp.Message)
        Assert.Contains("nondeployable", refusedFSharp.Message)

    [<Fact>]
    member _.``An invalid pipeline identity is refused with the identifier's own message``() =
        let graph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.toRegistered labelSink "log-out" labelParameters

        let refusedFSharp =
            Assert.Throws<System.ArgumentException>(fun () -> graph |> Pipeline.define "Not_A_Segment" 1 |> ignore)

        let refusedCSharp =
            Assert.Throws<System.ArgumentException>(fun () -> GraphId.Create "Not_A_Segment" |> ignore)

        Assert.Equal(refusedCSharp.Message, refusedFSharp.Message)

        // And the revision has a guard of its own, which is an out-of-range rather than an argument failure.
        Assert.Throws<System.ArgumentOutOfRangeException>(fun () -> graph |> Pipeline.define "numbers" 0 |> ignore)
        |> ignore

    [<Fact>]
    member _.``An invalid occurrence name is refused with the shared attachment's own message``() =
        let refusedFSharp =
            Assert.Throws<System.ArgumentException>(fun () ->
                Source.ofRegistered numberSource "Not A Name" sourceParameters |> ignore)

        let refusedCSharp =
            Assert.Throws<System.ArgumentException>(fun () ->
                Orleans.Dataflow.Source.FromRegistered(numberSource, "Not A Name", sourceParameters)
                |> ignore)

        // Both frontends attach through the one shared attachment, which owns the grammar and the diagnostic,
        // so neither of them restates what a node identifier is.
        Assert.Equal(refusedCSharp.Message, refusedFSharp.Message)
        Assert.Equal(refusedCSharp.ParamName, refusedFSharp.ParamName)

    [<Fact>]
    member _.``Two occurrences of one graph may not share a name, and the close says so``() =
        let refused =
            Assert.Throws<System.ArgumentException>(fun () ->
                Source.ofRegistered numberSource "shared" sourceParameters
                |> Source.viaRegistered scale "shared" scaleParameters
                |> Source.toRegistered labelSink "log-out" labelParameters
                |> ignore)

        // A name is an identity rather than a position, so the second use is a collision reported where the
        // whole chain is first visible. Nothing in the F# frontend checks this, and that is correct: one
        // defect with two diagnostics is two diagnostics that can drift apart.
        Assert.Contains("shared", refused.Message)

    [<Fact>]
    member _.``An F#-authored registered document names the provider's own ports and node names``() =
        let graph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.viaRegistered scale "scale-up" scaleParameters
            |> Source.toRegistered labelSink "log-out" labelParameters

        // Derived from the document rather than from the twin: a fingerprint equality says the two frontends
        // agree, and this says what they agree on. The ports are the provider's — 'numbers', 'scaled', and
        // 'elements' — and none of them is the local vocabulary's 'in' or 'out'.
        Assert.Equal<string>(
            [ "log-out"; "numbers-in"; "scale-up" ],
            graph.Document.Nodes |> Seq.map (fun node -> string node.Id) |> Seq.sort |> Seq.toList)

        Assert.Equal<string>(
            [ "numbers-in#numbers -> scale-up#elements"; "scale-up#scaled -> log-out#elements" ],
            graph.Document.Edges |> Seq.map string |> Seq.sort |> Seq.toList)

        // And it is deployable by construction: no lambda binds behavior in this process, and every node is
        // named by its author rather than by its position.
        Assert.Empty graph.Document.Capabilities

    [<Fact>]
    member _.``A graph mixing registered and lambda stages is refused as a pipeline``() =
        let mixed =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.map (fun value -> value + 1)
            |> Source.toRegistered labelSink "log-out" labelParameters

        let refused =
            Assert.Throws<System.ArgumentException>(fun () -> mixed |> Pipeline.define "numbers" 1 |> ignore)

        // Both refusals at once, because one lambda anywhere costs a document both of them: its behavior is
        // bound in this process, and its identifier is a position rather than a name.
        Assert.Contains("nondeployable", refused.Message)
        Assert.Contains("ephemeral-identity", refused.Message)

    [<Fact>]
    member _.``A fully registered branching graph authored in F# is a pipeline``() =
        let leftBranch = Flow.identity |> Branch.toRegistered labelSink "log-left" labelParameters

        let rightBranch, _ =
            Flow.identity |> Branch.toRegisteredResult "right" sumSink "sum-right" sumParameters

        let graph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.fanOutToRegistered split "split" junctionParameters [ leftBranch; rightBranch ]

        let pipeline = graph |> Pipeline.define "numbers" 1

        // The claim the registered junction exists for: a branching graph closed entirely from registered
        // handles carries no capability that denies it a durable identity, so it becomes a pipeline. A local
        // junction in the same position would make it nondeployable however registered its stages were.
        Assert.Empty graph.Document.Capabilities
        Assert.Equal(4, pipeline.Document.Nodes.Count)

        let recovered = pipeline.ResultSlot("right", totalContract)

        Assert.False recovered.IsDefault

    [<Fact>]
    member _.``A registered fan-out and its C#-authored twin share one fingerprint``() =
        let leftBranch, _ =
            Flow.identity |> Branch.toRegisteredResult "left" sumSink "sum-left" sumParameters

        let rightBranch, _ =
            Flow.identity |> Branch.toRegisteredResult "right" sumSink "sum-right" sumParameters

        let fsharpGraph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.fanOutToRegistered split "split" junctionParameters [ leftBranch; rightBranch ]

        let mutable leftSlot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
        let mutable rightSlot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

        let csharpLeft =
            Orleans.Dataflow.Flow
                .For<int>()
                .To(sumSink, "sum-left", sumParameters, "left", &leftSlot)

        let csharpRight =
            Orleans.Dataflow.Flow
                .For<int>()
                .To(sumSink, "sum-right", sumParameters, "right", &rightSlot)

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-in", sourceParameters)
                .FanOutTo(split, "split", junctionParameters, csharpLeft, csharpRight)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered fan-in and its C#-authored twin share one fingerprint``() =
        let fsharpGraph =
            Source.ofRegistered numberSource "numbers-a" sourceParameters
            |> Source.fanInRegistered join "join" junctionParameters
                [ Source.ofRegistered numberSource "numbers-b" sourceParameters ]
            |> Source.toRegistered labelSink "log-out" labelParameters

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-a", sourceParameters)
                .FanIn(
                    join,
                    "join",
                    junctionParameters,
                    Orleans.Dataflow.Source.FromRegistered(numberSource, "numbers-b", sourceParameters)
                )
                .To(labelSink, "log-out", labelParameters)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered junction with two unlike legs is the same document from either frontend``() =
        let numbersBranch, _ =
            Flow.identity |> Branch.toRegisteredResult "numbers" sumSink "sum-numbers" sumParameters

        let labelsBranch = Flow.identity |> Branch.toSink Sink.ignore

        let fsharpGraph =
            Source.ofSeq [ struct (1, "a"); struct (2, "b") ]
            |> Source.fanOutToRegisteredPair unzip "unzip" junctionParameters numbersBranch labelsBranch

        let mutable numbersSlot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

        let csharpNumbers =
            Orleans.Dataflow.Flow
                .For<int>()
                .To(sumSink, "sum-numbers", sumParameters, "numbers", &numbersSlot)

        let csharpLabels =
            Orleans.Dataflow.Flow.For<string>().To(Orleans.Dataflow.Sink.Ignore<string>())

        let csharpGraph =
            Orleans.Dataflow.Source
                .From([ struct (1, "a"); struct (2, "b") ])
                .FanOutTo(unzip, "unzip", junctionParameters, csharpNumbers, csharpLabels)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered junction joining two unlike streams is the same document from either frontend``() =
        let fsharpGraph =
            Source.ofSeq [ 1; 2 ]
            |> Source.fanInRegisteredPair attach "attach" junctionParameters (Source.ofSeq [ "a"; "b" ])
            |> Source.toSink Sink.ignore

        let csharpGraph =
            Orleans.Dataflow.Source
                .From([ 1; 2 ])
                .FanIn(attach, "attach", junctionParameters, Orleans.Dataflow.Source.From([ "a"; "b" ]))
                .To(Orleans.Dataflow.Sink.Ignore<struct (int * string)>())

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A registered flow inside a leg is the same document from either frontend``() =
        let leg =
            Flow.identity
            |> Flow.andThenRegistered scale "scale-leg" scaleParameters
            |> Branch.toRegistered labelSink "log-leg" labelParameters

        let fsharpGraph =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.alsoTo leg
            |> Source.toRegistered labelSink "log-out" labelParameters

        let csharpLeg =
            Orleans.Dataflow.Flow
                .For<int>()
                .Via(scale, "scale-leg", scaleParameters)
                .To(labelSink, "log-leg", labelParameters)

        let csharpGraph =
            Orleans.Dataflow.Source
                .FromRegistered(numberSource, "numbers-in", sourceParameters)
                .AlsoTo(csharpLeg)
                .To(labelSink, "log-out", labelParameters)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``Both frontends refuse a registered fan-out of the wrong arity in the same words``() =
        // Both bounds, because a restated sentence can agree at one arity and not at another: the count the
        // author asked for is in the message, and it is the part a restatement is most likely to get wrong.
        let refusals (count: int) =
            let branch () = Flow.identity |> Branch.toRegistered labelSink "log-leg" labelParameters

            let refusedFSharp =
                Assert.Throws<System.ArgumentException>(fun () ->
                    Source.ofRegistered numberSource "numbers-in" sourceParameters
                    |> Source.fanOutToRegistered split "split" junctionParameters
                        [ for _ in 1..count -> branch () ]
                    |> ignore)

            let csharpBranches =
                [| for _ in 1..count -> Orleans.Dataflow.Flow.For<int>().To(labelSink, "log-leg", labelParameters) |]

            let refusedCSharp =
                Assert.Throws<System.ArgumentException>(fun () ->
                    Orleans.Dataflow.Source
                        .FromRegistered(numberSource, "numbers-in", sourceParameters)
                        .FanOutTo(split, "split", junctionParameters, csharpBranches)
                    |> ignore)

            refusedCSharp.Message, refusedFSharp.Message

        // The arity sentence is restated in F# rather than called, because the guard that owns it is typed to
        // the C# facade's own branch value. This is what keeps the restatement honest.
        let cases = [ refusals 1; refusals 3 ]

        Assert.Equal<string>(cases |> List.map fst, cases |> List.map snd)

    [<Fact>]
    member _.``Both frontends refuse a registered fan-in of the wrong arity in the same words``() =
        let refusals (others: int) =
            let named (index: int) = $"numbers-{index}"

            let refusedFSharp =
                Assert.Throws<System.ArgumentException>(fun () ->
                    Source.ofRegistered numberSource "numbers-a" sourceParameters
                    |> Source.fanInRegistered join "join" junctionParameters
                        [ for index in 1..others -> Source.ofRegistered numberSource (named index) sourceParameters ]
                    |> ignore)

            let csharpOthers =
                [| for index in 1..others ->
                       Orleans.Dataflow.Source.FromRegistered(numberSource, named index, sourceParameters) |]

            let refusedCSharp =
                Assert.Throws<System.ArgumentException>(fun () ->
                    Orleans.Dataflow.Source
                        .FromRegistered(numberSource, "numbers-a", sourceParameters)
                        .FanIn(join, "join", junctionParameters, csharpOthers)
                    |> ignore)

            refusedCSharp.Message, refusedFSharp.Message

        // Zero others is one stream counting the receiver, and two others is three: the junction declares two,
        // so both are refused, and the arithmetic that counts the receiver has to agree in both frontends.
        let cases = [ refusals 0; refusals 2 ]

        Assert.Equal<string>(cases |> List.map fst, cases |> List.map snd)

    [<Fact>]
    member _.``A pipeline recovers its result slot by name and contract, and refuses another contract``() =
        let graph, _ =
            Source.ofRegistered numberSource "numbers-in" sourceParameters
            |> Source.toRegisteredResult "total" sumSink "sum-out" sumParameters

        let pipeline = graph |> Pipeline.define "numbers" 1

        // The direct member call, which is why this package wraps no recovery of its own: the contract
        // argument fixes the type parameter, so F# needs neither a type annotation nor a type application.
        let recovered = pipeline.ResultSlot("total", totalContract)

        Assert.False recovered.IsDefault
        Assert.Equal(pipeline.Fingerprint, recovered.Graph)

        // A slot of a pipeline is not the slot the closing call handed back: that one binds to the built
        // graph's instance, and this one to the deployable document.
        Assert.NotEqual(graph.Fingerprint, recovered.Graph)

        let refused =
            Assert.Throws<System.ArgumentException>(fun () ->
                pipeline.ResultSlot("total", Orleans.Dataflow.ResultContract.For<int64>("other-total", 1))
                |> ignore)

        Assert.Contains("number-total", refused.Message)
