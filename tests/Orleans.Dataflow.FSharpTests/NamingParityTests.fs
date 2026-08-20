namespace Orleans.Dataflow.FSharpTests

open System
open Orleans.Dataflow.Definition
open Orleans.Dataflow.FSharp
open Xunit

/// <summary>
/// The naming combinator, in the frontend that has to be able to spell it too.
/// </summary>
/// <remarks>
/// <para>
/// The combinator exists in this shape rather than as a name parameter on every operator largely because of
/// this package: a parameter would have to be curried into forty-five functions or made optional, and F#
/// module functions have neither a comfortable spelling for it. <c>named</c> is one function per authoring
/// type, it composes under <c>|&gt;</c> where the stage was written, and it changes no existing signature.
/// </para>
/// <para>
/// Parity is asserted as byte identity of the closed document, through fingerprint equality, because the two
/// frontends are equal frontends over one algebra and a name is document content. A twin that diverged would
/// mean one of them had grown a private spelling of identity, which is the thing this suite exists to make
/// impossible.
/// </para>
/// </remarks>
type NamingParityTests() =

    let capabilities (graph: Orleans.Dataflow.RunnableGraph) =
        graph.Document.Capabilities |> Seq.map (fun token -> token.Value) |> List.ofSeq

    let nodeIds (graph: Orleans.Dataflow.RunnableGraph) =
        graph.Document.Nodes |> Seq.map (fun node -> node.Id.Value) |> List.ofSeq

    [<Fact>]
    member _.``A fully named F# graph of local stages declares no ephemeral identity``() =
        let graph =
            Source.ofSeq [ 1; 2; 3 ]
            |> Source.named "intake"
            |> Source.via (Flow.map (fun value -> value * 2) |> Flow.named "priced")
            |> Source.buffer (Fixtures.bounded 8)
            |> Source.named "queue"
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities)
        Assert.Equal<string list>([ "nondeployable" ], capabilities graph)
        Assert.Equal<string list>([ "intake"; "out"; "priced"; "queue" ], nodeIds graph)

    [<Fact>]
    member _.``One unnamed occurrence keeps the token``() =
        let graph =
            Source.ofSeq [ 1; 2; 3 ]
            |> Source.named "intake"
            |> Source.map (fun value -> value * 2)
            |> Source.buffer (Fixtures.bounded 8)
            |> Source.named "queue"
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        Assert.Contains(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities)
        Assert.Equal<string list>([ "ephemeral-identity"; "nondeployable" ], capabilities graph)
        Assert.Equal<string list>([ "intake"; "out"; "queue"; "stage-0002" ], nodeIds graph)

    [<Fact>]
    member _.``An F#-named chain and its C#-named twin are one document``() =
        let fsharpGraph, _ =
            Source.ofSeq [ 1; 2; 3 ]
            |> Source.named "intake"
            |> Source.map (fun value -> value * 2)
            |> Source.named "priced"
            |> Source.buffer (Fixtures.bounded 8)
            |> Source.named "queue"
            |> Source.toResult
                "answer"
                (Sink.aggregate 0L (fun state value -> state + int64 value) |> Sink.namedResult "total")

        let mutable slot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

        let csharpGraph =
            Orleans.Dataflow.Source
                .From([ 1; 2; 3 ])
                .Named("intake")
                .Select(fun value -> value * 2)
                .Named("priced")
                .Buffer(Orleans.Dataflow.BufferOptions(Capacity = 8))
                .Named("queue")
                .To(
                    Orleans.Dataflow.Sink
                        .Aggregate<int, int64>(0L, fun state value -> state + int64 value)
                        .Named("total"),
                    "answer",
                    &slot
                )

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``Naming changes the document in both frontends alike``() =
        // The named twins agree, the unnamed twins agree, and the two pairs differ from each other. Without
        // the last assertion the first two would pass for a pair of frontends that both ignored the name.
        let fsharpNamed =
            Source.ofSeq [ 1 ]
            |> Source.named "intake"
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        let fsharpAnonymous = Source.ofSeq [ 1 ] |> Source.toSink Sink.ignore

        let csharpNamed =
            Orleans.Dataflow.Source
                .From([ 1 ])
                .Named("intake")
                .To(Orleans.Dataflow.Sink.Ignore<int>().Named("out"))

        let csharpAnonymous =
            Orleans.Dataflow.Source.From([ 1 ]).To(Orleans.Dataflow.Sink.Ignore<int>())

        Assert.Equal(csharpNamed.Fingerprint, fsharpNamed.Fingerprint)
        Assert.Equal(csharpAnonymous.Fingerprint, fsharpAnonymous.Fingerprint)
        Assert.NotEqual(fsharpAnonymous.Fingerprint, fsharpNamed.Fingerprint)

    [<Fact>]
    member _.``Building the same named graph twice produces one fingerprint``() =
        let build () =
            Source.ofSeq [ 1; 2 ]
            |> Source.named "intake"
            |> Source.map (fun value -> value + 1)
            |> Source.named "priced"
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        Assert.Equal((build ()).Fingerprint, (build ()).Fingerprint)

    [<Fact>]
    member _.``Renaming one stage produces a different fingerprint``() =
        let under name =
            Source.ofSeq [ 1; 2 ]
            |> Source.named "intake"
            |> Source.buffer (Fixtures.bounded 4)
            |> Source.named name
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        Assert.NotEqual((under "queue").Fingerprint, (under "holdback").Fingerprint)

    [<Fact>]
    member _.``Every named junction spelling matches its C# twin``() =
        // The junctions no combinator can reach, because the call they belong to answers with a document or
        // with two open ends. Both frontends take the name as an argument of that call and must write the
        // same node.
        let branchesFSharp () =
            [ Flow.identity |> Branch.toSink (Sink.ignore |> Sink.named "first")
              Flow.identity |> Branch.toSink (Sink.ignore |> Sink.named "second") ]

        let branchesCSharp () =
            [| Orleans.Dataflow.Flow
                   .For<int>()
                   .To(Orleans.Dataflow.Sink.Ignore<int>().Named("first"))
               Orleans.Dataflow.Flow
                   .For<int>()
                   .To(Orleans.Dataflow.Sink.Ignore<int>().Named("second")) |]

        let headFSharp () = Source.ofSeq [ 1; 2 ] |> Source.named "intake"
        let headCSharp () = Orleans.Dataflow.Source.From([ 1; 2 ]).Named("intake")

        Fixtures.assertParity
            [ "broadcastToNamed",
              (fun () -> headFSharp () |> Source.broadcastToNamed "tee" (branchesFSharp ())),
              (fun () -> (headCSharp ()).BroadcastTo("tee", branchesCSharp ()))
              "balanceToNamed",
              (fun () -> headFSharp () |> Source.balanceToNamed "spread" (branchesFSharp ())),
              (fun () -> (headCSharp ()).BalanceTo("spread", branchesCSharp ()))
              "partitionToNamed",
              (fun () ->
                  headFSharp ()
                  |> Source.partitionToNamed (fun value -> value % 2) "route" (branchesFSharp ())),
              (fun () ->
                  (headCSharp ()).PartitionTo((fun value -> value % 2), "route", branchesCSharp ()))
              "forkNamed",
              (fun () ->
                  headFSharp ()
                  |> Source.forkNamed
                      "split"
                      (Flow.map (fun value -> value + 1) |> Flow.named "left")
                      (Flow.map (fun value -> value - 1) |> Flow.named "right")
                  |> Fork.zip
                  |> Source.named "paired"
                  |> Source.toSink (Sink.ignore |> Sink.named "out")),
              (fun () ->
                  (headCSharp ())
                      .Fork(
                          "split",
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value + 1).Named("left"),
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value - 1).Named("right")
                      )
                      .Zip()
                      .Named("paired")
                      .To(Orleans.Dataflow.Sink.Ignore<struct (int * int)>().Named("out")))
              "forkMergeNamed",
              (fun () ->
                  headFSharp ()
                  |> Source.forkMergeNamed
                      "split"
                      (Flow.map (fun value -> value + 1) |> Flow.named "left")
                      (Flow.map (fun value -> value - 1) |> Flow.named "right")
                  |> Source.named "raced"
                  |> Source.toSink (Sink.ignore |> Sink.named "out")),
              (fun () ->
                  (headCSharp ())
                      .ForkMerge(
                          "split",
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value + 1).Named("left"),
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value - 1).Named("right")
                      )
                      .Named("raced")
                      .To(Orleans.Dataflow.Sink.Ignore<int>().Named("out"))) ]

    [<Fact>]
    member _.``A named unzip matches its C# twin``() =
        // Written apart from the sweep because its element type is a struct tuple, so the branches are not
        // the ones every other junction takes.
        let fsharpGraph =
            Source.ofSeq [ struct (1, "a") ]
            |> Source.named "pairs"
            |> Source.unzipToNamed
                "split"
                (Flow.identity |> Branch.toSink (Sink.ignore |> Sink.named "numbers"))
                (Flow.identity |> Branch.toSink (Sink.ignore |> Sink.named "words"))

        let csharpPairs =
            Orleans.Dataflow.Source.From([ struct (1, "a") ]).Named("pairs")

        let csharpGraph =
            Orleans.Dataflow.Source.UnzipTo(
                csharpPairs,
                "split",
                Orleans.Dataflow.Flow.For<int>().To(Orleans.Dataflow.Sink.Ignore<int>().Named("numbers")),
                Orleans.Dataflow.Flow
                    .For<string>()
                    .To(Orleans.Dataflow.Sink.Ignore<string>().Named("words"))
            )

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, fsharpGraph.Document.Capabilities)

    [<Fact>]
    member _.``A tap names its junction and not the branch terminal``() =
        let graph =
            Source.ofSeq [ 1; 2 ]
            |> Source.named "intake"
            |> Source.alsoTo (Flow.identity |> Branch.toSink (Sink.ignore |> Sink.named "audit"))
            |> Source.named "tee"
            |> Source.toSink (Sink.ignore |> Sink.named "out")

        Assert.Equal<string list>([ "audit"; "intake"; "out"; "tee" ], nodeIds graph)
        Assert.DoesNotContain(CapabilityToken.EphemeralIdentity, graph.Document.Capabilities)

    [<Fact>]
    member _.``An invalid occurrence name is refused by name``() =
        let refused =
            Assert.Throws<ArgumentException>(fun () ->
                Source.ofSeq [ 1 ] |> Source.named "Not_A_Segment" |> ignore)

        Assert.Equal("occurrenceName", refused.ParamName)
        Assert.Contains("Not_A_Segment", refused.Message)

        // The same text through the other authoring types produces the same sentence, because one guard owns
        // the grammar for all of them.
        let onFlow =
            Assert.Throws<ArgumentException>(fun () -> Flow.map id |> Flow.named "Not_A_Segment" |> ignore)

        let onSink =
            Assert.Throws<ArgumentException>(fun () -> Sink.ignore<int> |> Sink.named "Not_A_Segment" |> ignore)

        Assert.Equal(refused.Message, onFlow.Message)
        Assert.Equal(refused.Message, onSink.Message)

    [<Fact>]
    member _.``Naming an already named occurrence is refused``() =
        let refused =
            Assert.Throws<InvalidOperationException>(fun () ->
                Source.ofSeq [ 1 ] |> Source.named "intake" |> Source.named "inlet" |> ignore)

        Assert.Contains("already named 'intake'", refused.Message)
        Assert.Contains("naming it 'inlet'", refused.Message)

    [<Fact>]
    member _.``Naming the identity flow is refused because there is nothing to name``() =
        let refused =
            Assert.Throws<InvalidOperationException>(fun () ->
                Flow.identity<int> |> Flow.named "nothing" |> ignore)

        Assert.Contains("no occurrence for 'nothing' to name", refused.Message)
        Assert.Contains("identity flow", refused.Message)

    [<Fact>]
    member _.``A named stage inside a scope is refused because a scope's stages are not nodes``() =
        let refused =
            Assert.Throws<ArgumentException>(fun () ->
                Source.ofSeq [ 1 ]
                |> Source.durable (Flow.map (fun value -> value + 1) |> Flow.named "inner")
                |> ignore)

        Assert.Equal("scope", refused.ParamName)
        Assert.Contains("not nodes of the document", refused.Message)
        Assert.Contains("named 'inner'", refused.Message)

    [<Fact>]
    member _.``A named graph runs and produces what the unnamed one produces``() =
        // Naming changes the document and must change nothing else. The graph is materialized rather than
        // only closed, because the binding table is keyed by node identifier: a name that reached the
        // document without reaching the table would close fine and fail at run time.
        task {
            let! named =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.named "intake"
                |> Source.map (fun value -> value * 2)
                |> Source.named "priced"
                |> Source.buffer (Fixtures.bounded 8)
                |> Source.named "queue"
                |> Fixtures.elementsOf

            Assert.Equal<int list>([ 2; 4; 6 ], List.ofSeq named)
        }
