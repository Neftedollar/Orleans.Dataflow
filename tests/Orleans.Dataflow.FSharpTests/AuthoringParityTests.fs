namespace Orleans.Dataflow.FSharpTests

open Orleans.Dataflow.FSharp
open Xunit

/// <summary>
/// The M7 invariant: an F#-authored graph and its C#-authored twin are one document.
/// </summary>
/// <remarks>
/// Byte identity of the canonical document — asserted through fingerprint equality, which is a hash of
/// exactly those bytes — is what "equal frontends over one algebra" means as a test. It holds because both
/// frontends funnel through the one graph builder and the one descriptor vocabulary; nothing here would
/// survive either frontend growing a private spelling. Delegates never enter a document, so the twins use
/// different lambda instances on purpose: the shape is the identity, the behavior is not.
/// </remarks>
type AuthoringParityTests() =

    [<Fact>]
    member _.``An F#-authored slice and its C#-authored twin share one fingerprint``() =
        let fsharpGraph, _ =
            Source.ofSeq [ 1; 2; 3; 4 ]
            |> Source.map (fun value -> value + 1)
            |> Source.filter (fun value -> value % 2 = 0)
            |> Source.toResult "total" (Sink.aggregate 0L (fun state value -> state + int64 value))

        let mutable slot = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

        let csharpSink =
            Orleans.Dataflow.Sink.Aggregate<int, int64>(0L, fun state value -> state + int64 value)

        let csharpGraph =
            Orleans.Dataflow.Source
                .From([ 1; 2; 3; 4 ])
                .Select(fun value -> value + 1)
                .Where(fun value -> value % 2 = 0)
                .To(csharpSink, "total", &slot)

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A resultless close is the same document from either frontend``() =
        let fsharpGraph =
            Source.ofSeq [ 10; 20 ]
            |> Source.via (Flow.map string)
            |> Source.toSink Sink.ignore

        let csharpGraph =
            Orleans.Dataflow.Source
                .From([ 10; 20 ])
                .Select(fun value -> string value)
                .To(Orleans.Dataflow.Sink.Ignore<string>())

        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``The identity flow contributes nothing to a document``() =
        let plain, _ =
            Source.ofSeq [ 1; 2 ]
            |> Source.toResult "total" (Sink.aggregate 0 (fun state value -> state + value))

        let threaded, _ =
            Source.ofSeq [ 1; 2 ]
            |> Source.via Flow.identity
            |> Source.toResult "total" (Sink.aggregate 0 (fun state value -> state + value))

        Assert.Equal(plain.Fingerprint, threaded.Fingerprint)

    [<Fact>]
    member _.``Composed flows and inline shorthands spell one document``() =
        let composed =
            Flow.filter (fun value -> value > 0)
            |> Flow.andThen (Flow.map (fun value -> value * 2))

        let viaComposed, _ =
            Source.ofSeq [ -1; 1; 2 ]
            |> Source.via composed
            |> Source.toResult "total" (Sink.aggregate 0 (fun state value -> state + value))

        let viaShorthands, _ =
            Source.ofSeq [ -1; 1; 2 ]
            |> Source.filter (fun value -> value > 0)
            |> Source.map (fun value -> value * 2)
            |> Source.toResult "total" (Sink.aggregate 0 (fun state value -> state + value))

        Assert.Equal(viaComposed.Fingerprint, viaShorthands.Fingerprint)

    [<Fact>]
    member _.``A reused flow value is read, not consumed``() =
        let double = Flow.map (fun value -> value * 2)

        let first = Source.ofSeq [ 1 ] |> Source.via double |> Source.toSink Sink.ignore
        let second = Source.ofSeq [ 1 ] |> Source.via double |> Source.toSink Sink.ignore

        // Two graphs from one flow value, and the flow is unchanged by either: same shape, same bytes.
        Assert.Equal(first.Fingerprint, second.Fingerprint)

    [<Fact>]
    member _.``An invalid slot name is refused by name at the close``() =
        let refused =
            Assert.Throws<System.ArgumentException>(fun () ->
                Source.ofSeq [ 1 ]
                |> Source.toResult "Not_A_Segment" (Sink.aggregate 0 (fun state value -> state + value))
                |> ignore)

        Assert.Contains("Not_A_Segment", refused.Message)
