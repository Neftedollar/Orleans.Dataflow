namespace Orleans.Dataflow.FSharpTests

open System.Collections.Generic
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.FSharpTests.Fixtures
open Xunit

/// <summary>
/// The M7 invariant, read over the junction vocabulary: every F# junction and its C#-authored twin are one
/// document.
/// </summary>
/// <remarks>
/// <para>
/// A junction is where a document stops being a list and becomes a graph, so this is the first place where
/// byte identity is a claim about edges rather than about order alone: a fan-out's legs, a fan-in's inputs,
/// and the numbering both of them impose. Nothing here asserts an edge directly — the other frontend's
/// authoring suite does that, node by node — and the claim made instead is stronger for being indirect. Two
/// documents with one fingerprint agree on every edge there is.
/// </para>
/// <para>
/// Branch order is identity-bearing, so the suite asserts it twice over: once by showing that the two
/// frontends agree on a given order, and once by showing that swapping two branches changes the F# document.
/// A parity suite that only did the first would pass for a frontend that ignored order entirely.
/// </para>
/// <para>
/// Delegates never enter a document, so the twins use different lambda instances on purpose. The branches
/// are built inside each case rather than shared across them, because a branch that declares a result closes
/// exactly one graph and sharing one would make the second case fail for a reason that is not parity.
/// </para>
/// </remarks>
type JunctionParityTests() =

    static let fsharpInts () = Source.ofSeq [ 1; 2; 3 ]

    static let csharpInts () = Orleans.Dataflow.Source.From([ 1; 2; 3 ])

    /// <summary>A second stream of a shape the first does not share.</summary>
    /// <remarks>
    /// Which input of a fan-in reaches <c>in-0</c> is only observable in a document when the two inputs are
    /// distinguishable stages: a local document states which stage stands where and never what a sequence
    /// contains, so two <c>from-enumerable</c> sources exchanged are byte-for-byte the graph they were. Every
    /// fan-in case below therefore joins streams of different shapes, and would fail for a frontend that
    /// wired its inputs backwards.
    /// </remarks>
    static let fsharpOther () = Source.range 4 2 |> Source.map (fun value -> value * 10)

    static let csharpOther () =
        Orleans.Dataflow.Source.Range(4, 2).Select(fun (value: int) -> value * 10)

    static let fsharpThird () = Source.single 7

    static let csharpThird () = Orleans.Dataflow.Source.Single(7)

    static let fsharpText () =
        Source.ofSeq [ "a"; "b" ] |> Source.filter (fun text -> text <> "")

    static let csharpText () =
        Orleans.Dataflow.Source.From([ "a"; "b" ]).Where(fun (text: string) -> text <> "")

    static let fsharpPairs () =
        Source.ofSeq [ struct ("ada", 36); struct ("alan", 41) ]

    static let csharpPairs () =
        Orleans.Dataflow.Source.From([ struct ("ada", 36); struct ("alan", 41) ])

    /// <summary>A leg that keeps nothing, in each frontend's spelling.</summary>
    static let ignoring () : Branch<int> = Flow.identity<int> |> Branch.toSink Sink.ignore

    static let ignoringCSharp () =
        Orleans.Dataflow.Flow.For<int>().To(Orleans.Dataflow.Sink.Ignore<int>())

    /// <summary>A leg with a stage of its own, so that two legs of one junction are distinguishable.</summary>
    static let doubling () : Branch<int> =
        Flow.map (fun (value: int) -> value * 2) |> Branch.toSink Sink.ignore

    static let doublingCSharp () =
        Orleans.Dataflow.Flow
            .For<int>()
            .Select(fun (value: int) -> value * 2)
            .To(Orleans.Dataflow.Sink.Ignore<int>())

    [<Fact>]
    member _.``The fan-in junctions are one document from either frontend``() =
        assertParity
            [ "merge",
              (fun () -> fsharpInts () |> Source.merge (fsharpOther ()) |> closeFSharp),
              (fun () -> csharpInts().Merge(csharpOther ()) |> closeCSharp)

              "merge3",
              (fun () ->
                  fsharpInts ()
                  |> Source.merge3 (fsharpOther ()) (fsharpThird ())
                  |> closeFSharp),
              (fun () -> csharpInts().Merge(csharpOther (), csharpThird ()) |> closeCSharp)

              // Two junctions rather than one, and a different document from the three-input form: the F#
              // arity set is honest about that only if the chained spelling is asserted as its own case.
              "merge of a merge",
              (fun () ->
                  fsharpInts ()
                  |> Source.merge (fsharpOther ())
                  |> Source.merge (fsharpThird ())
                  |> closeFSharp),
              (fun () -> csharpInts().Merge(csharpOther()).Merge(csharpThird ()) |> closeCSharp)

              "concat",
              (fun () -> fsharpInts () |> Source.concat (fsharpOther ()) |> closeFSharp),
              (fun () -> csharpInts().Concat(csharpOther ()) |> closeCSharp)

              // Two distinguishable inputs, written both ways round, so that the frontends are asserted to
              // agree on which input reaches in-0 and not merely on the pair of nodes present.
              "concat of a range and a sequence",
              (fun () -> Source.range 1 3 |> Source.concat (Source.ofSeq [ 9 ]) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.Range(1, 3).Concat(Orleans.Dataflow.Source.From([ 9 ]))
                  |> closeCSharp)

              "concat of a sequence and a range",
              (fun () -> Source.ofSeq [ 9 ] |> Source.concat (Source.range 1 3) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.From([ 9 ]).Concat(Orleans.Dataflow.Source.Range(1, 3))
                  |> closeCSharp)

              "interleave",
              (fun () -> fsharpInts () |> Source.interleave (fsharpOther ()) 2 |> closeFSharp),
              (fun () -> csharpInts().Interleave(csharpOther (), 2) |> closeCSharp)

              // The segment size is the one number a junction writes into its document, so a second size is
              // a second document and this case is what proves the number reaches the bytes at all.
              "interleave of another segment",
              (fun () -> fsharpInts () |> Source.interleave (fsharpOther ()) 3 |> closeFSharp),
              (fun () -> csharpInts().Interleave(csharpOther (), 3) |> closeCSharp)

              "zip",
              (fun () -> fsharpInts () |> Source.zip (fsharpText ()) |> closeFSharp),
              (fun () -> csharpInts().Zip(csharpText ()) |> closeCSharp)

              "zipWith",
              (fun () ->
                  fsharpInts ()
                  |> Source.zipWith (fsharpText ()) (fun value text -> $"{text}{value}")
                  |> closeFSharp),
              (fun () ->
                  csharpInts().Zip(csharpText (), (fun value text -> $"{text}{value}")) |> closeCSharp)

              "combineLatest",
              (fun () ->
                  fsharpInts ()
                  |> Source.combineLatest (fsharpText ()) (fun value text -> $"{text}{value}")
                  |> closeFSharp),
              (fun () ->
                  csharpInts().CombineLatest(csharpText (), (fun value text -> $"{text}{value}"))
                  |> closeCSharp)

              "prepend",
              (fun () -> fsharpInts () |> Source.prepend (fsharpOther ()) |> closeFSharp),
              (fun () -> csharpInts().Prepend(csharpOther ()) |> closeCSharp)

              // The element overload this package deliberately does not mirror: it is Prepend over a
              // sequence source and nothing else, which is what this case asserts rather than assumes.
              "prepend of a fixed run",
              (fun () -> fsharpInts () |> Source.prepend (Source.ofSeq [ 0 ]) |> closeFSharp),
              (fun () -> csharpInts().Prepend([| 0 |]) |> closeCSharp)

              "append",
              (fun () -> fsharpInts () |> Source.append (fsharpOther ()) |> closeFSharp),
              (fun () -> csharpInts().Append(csharpOther ()) |> closeCSharp)

              "append of a fixed run",
              (fun () -> fsharpInts () |> Source.append (Source.ofSeq [ 9 ]) |> closeFSharp),
              (fun () -> csharpInts().Append([| 9 |]) |> closeCSharp) ]

    [<Fact>]
    member _.``The taps and the diamonds are one document from either frontend``() =
        assertParity
            // The branch has a stage the main line does not, so that which leg of the junction the branch
            // hangs off is observable: with both legs ending in the same terminal, a frontend that tapped
            // onto leg 0 and continued on leg 1 would build the very same document.
            [ "alsoTo",
              (fun () -> fsharpInts () |> Source.alsoTo (doubling ()) |> closeFSharp),
              (fun () -> csharpInts().AlsoTo(doublingCSharp ()) |> closeCSharp)

              "alsoTo of a result-bearing branch",
              (fun () ->
                  let audited, _ = Flow.identity<int> |> Branch.toResult "audited" Sink.count

                  fsharpInts () |> Source.alsoTo audited |> closeFSharp),
              (fun () ->
                  let mutable audited = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                  let branch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(Orleans.Dataflow.Sink.Count<int>(), "audited", &audited)

                  csharpInts().AlsoTo(branch) |> closeCSharp)

              "two taps in a row",
              (fun () ->
                  fsharpInts ()
                  |> Source.alsoTo (ignoring ())
                  |> Source.alsoTo (doubling ())
                  |> closeFSharp),
              (fun () -> csharpInts().AlsoTo(ignoringCSharp()).AlsoTo(doublingCSharp ()) |> closeCSharp)

              "divertTo",
              (fun () ->
                  fsharpInts ()
                  |> Source.divertTo (fun value -> value > 1) (doubling ())
                  |> closeFSharp),
              (fun () ->
                  csharpInts().DivertTo((fun value -> value > 1), doublingCSharp ()) |> closeCSharp)

              "divertTo of a result-bearing branch",
              (fun () ->
                  let rejected, _ = Flow.identity<int> |> Branch.toResult "rejected" Sink.count

                  fsharpInts ()
                  |> Source.divertTo (fun value -> value > 1) rejected
                  |> closeFSharp),
              (fun () ->
                  let mutable rejected = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                  let branch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(Orleans.Dataflow.Sink.Count<int>(), "rejected", &rejected)

                  csharpInts().DivertTo((fun value -> value > 1), branch) |> closeCSharp)

              "fork and zip",
              (fun () ->
                  fsharpInts ()
                  |> Source.fork Flow.identity<int> (Flow.map (fun (value: int) -> value * 2))
                  |> Fork.zip
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Fork(
                          Orleans.Dataflow.Flow.For<int>(),
                          Orleans.Dataflow.Flow.For<int>().Select(fun (value: int) -> value * 2))
                      .Zip()
                  |> closeCSharp)

              "fork and zipWith",
              (fun () ->
                  fsharpInts ()
                  |> Source.fork Flow.identity<int> (Flow.map (fun (value: int) -> value * 2))
                  |> Fork.zipWith (fun left right -> left + right)
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Fork(
                          Orleans.Dataflow.Flow.For<int>(),
                          Orleans.Dataflow.Flow.For<int>().Select(fun (value: int) -> value * 2))
                      .Zip(fun left right -> left + right)
                  |> closeCSharp)

              "forkMerge",
              (fun () ->
                  fsharpInts ()
                  |> Source.forkMerge Flow.identity<int> (Flow.map (fun (value: int) -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .ForkMerge(
                          Orleans.Dataflow.Flow.For<int>(),
                          Orleans.Dataflow.Flow.For<int>().Select(fun (value: int) -> value * 2))
                  |> closeCSharp) ]

    [<Fact>]
    member _.``The closing fan-outs are one document from either frontend``() =
        assertParity
            [ "broadcastTo",
              (fun () -> fsharpInts () |> Source.broadcastTo [ ignoring (); doubling () ]),
              (fun () -> csharpInts().BroadcastTo(ignoringCSharp (), doublingCSharp ()))

              "broadcastTo of result-bearing branches",
              (fun () ->
                  let counting, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count

                  let summing, _ =
                      Flow.identity<int>
                      |> Branch.toResult "summed" (Sink.aggregate 0 (fun state value -> state + value))

                  fsharpInts () |> Source.broadcastTo [ counting; summing ]),
              (fun () ->
                  let mutable counted = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
                  let mutable summed = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int>>

                  let counting =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(Orleans.Dataflow.Sink.Count<int>(), "counted", &counted)

                  let summing =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(
                              Orleans.Dataflow.Sink.Aggregate<int, int>(0, fun state value -> state + value),
                              "summed",
                              &summed)

                  csharpInts().BroadcastTo(counting, summing))

              "broadcastTo of three branches",
              (fun () -> fsharpInts () |> Source.broadcastTo [ ignoring (); doubling (); ignoring () ]),
              (fun () -> csharpInts().BroadcastTo(ignoringCSharp (), doublingCSharp (), ignoringCSharp ()))

              "balanceTo",
              (fun () -> fsharpInts () |> Source.balanceTo [ ignoring (); doubling () ]),
              (fun () -> csharpInts().BalanceTo(ignoringCSharp (), doublingCSharp ()))

              "partitionTo",
              (fun () ->
                  fsharpInts ()
                  |> Source.partitionTo (fun value -> value % 2) [ ignoring (); doubling () ]),
              (fun () ->
                  csharpInts()
                      .PartitionTo((fun value -> value % 2), ignoringCSharp (), doublingCSharp ()))

              "unzipTo",
              (fun () ->
                  let names, _ = Flow.identity<string> |> Branch.toResult "names" (collecting ())
                  let ages, _ = Flow.identity<int> |> Branch.toResult "ages" (collecting ())

                  fsharpPairs () |> Source.unzipTo names ages),
              (fun () ->
                  let mutable names =
                      Unchecked.defaultof<Orleans.Dataflow.ResultSlot<IReadOnlyList<string>>>

                  let mutable ages = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<IReadOnlyList<int>>>

                  let namesBranch =
                      Orleans.Dataflow.Flow.For<string>().To(collectingCSharp (), "names", &names)

                  let agesBranch =
                      Orleans.Dataflow.Flow.For<int>().To(collectingCSharp (), "ages", &ages)

                  Orleans.Dataflow.Source.UnzipTo(csharpPairs (), namesBranch, agesBranch))

              // The composition claim: a junction call answers an ordinary source, so what follows it is the
              // whole vocabulary and not a restricted one. A fan-in feeds an operator which feeds a fan-out.
              // Legs of unequal length, both declaring a result, written both ways round. The position a
              // slot's producer is read from is arithmetic over the lengths of the legs before it, so a leg
              // list built in the wrong order — or an off-by-one in that arithmetic — would put a slot on
              // the wrong occurrence. Every case above has legs of one stage each and could not see it.
              "broadcastTo of unequal result-bearing legs",
              (fun () ->
                  let short, _ = Flow.identity<int> |> Branch.toResult "short" Sink.count

                  let long, _ =
                      Flow.map (fun (value: int) -> value * 2)
                      |> Flow.andThen (Flow.filter (fun value -> value > 2))
                      |> Branch.toResult "long" Sink.count

                  fsharpInts () |> Source.broadcastTo [ short; long ]),
              (fun () ->
                  let mutable short = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
                  let mutable long = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                  let shortBranch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(Orleans.Dataflow.Sink.Count<int>(), "short", &short)

                  let longBranch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .Select(fun (value: int) -> value * 2)
                          .Where(fun value -> value > 2)
                          .To(Orleans.Dataflow.Sink.Count<int>(), "long", &long)

                  csharpInts().BroadcastTo(shortBranch, longBranch))

              "broadcastTo of unequal result-bearing legs, the long one first",
              (fun () ->
                  let long, _ =
                      Flow.map (fun (value: int) -> value * 2)
                      |> Flow.andThen (Flow.filter (fun value -> value > 2))
                      |> Branch.toResult "long" Sink.count

                  let short, _ = Flow.identity<int> |> Branch.toResult "short" Sink.count

                  fsharpInts () |> Source.broadcastTo [ long; short ]),
              (fun () ->
                  let mutable long = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
                  let mutable short = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                  let longBranch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .Select(fun (value: int) -> value * 2)
                          .Where(fun value -> value > 2)
                          .To(Orleans.Dataflow.Sink.Count<int>(), "long", &long)

                  let shortBranch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .To(Orleans.Dataflow.Sink.Count<int>(), "short", &short)

                  csharpInts().BroadcastTo(longBranch, shortBranch))

              // The same hazard on the junction whose legs are differently typed and whose slot arithmetic
              // the other frontend writes out by hand rather than through the shared guard.
              "unzipTo of unequal result-bearing legs",
              (fun () ->
                  let names, _ =
                      Flow.map (fun (name: string) -> name.ToUpperInvariant())
                      |> Branch.toResult "names" (collecting ())

                  let ages, _ = Flow.identity<int> |> Branch.toResult "ages" (collecting ())

                  fsharpPairs () |> Source.unzipTo names ages),
              (fun () ->
                  let mutable names =
                      Unchecked.defaultof<Orleans.Dataflow.ResultSlot<IReadOnlyList<string>>>

                  let mutable ages = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<IReadOnlyList<int>>>

                  let namesBranch =
                      Orleans.Dataflow.Flow
                          .For<string>()
                          .Select(fun (name: string) -> name.ToUpperInvariant())
                          .To(collectingCSharp (), "names", &names)

                  let agesBranch =
                      Orleans.Dataflow.Flow.For<int>().To(collectingCSharp (), "ages", &ages)

                  Orleans.Dataflow.Source.UnzipTo(csharpPairs (), namesBranch, agesBranch))

              // A tap reads its slot's producer off the end of the shape rather than by arithmetic, which is
              // only right while the branch is the last thing the split appended. A multi-stage branch is
              // what would show it otherwise.
              "alsoTo of a multi-stage result-bearing branch",
              (fun () ->
                  let audited, _ =
                      Flow.map (fun (value: int) -> value * 2)
                      |> Flow.andThen (Flow.filter (fun value -> value > 2))
                      |> Branch.toResult "audited" Sink.count

                  fsharpInts ()
                  |> Source.alsoTo audited
                  |> Source.map (fun value -> value + 1)
                  |> closeFSharp),
              (fun () ->
                  let mutable audited = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                  let branch =
                      Orleans.Dataflow.Flow
                          .For<int>()
                          .Select(fun (value: int) -> value * 2)
                          .Where(fun value -> value > 2)
                          .To(Orleans.Dataflow.Sink.Count<int>(), "audited", &audited)

                  csharpInts().AlsoTo(branch).Select(fun value -> value + 1) |> closeCSharp)

              // A head that is itself a pipeline, so that prepend is asserted to place a whole shape rather
              // than only a bare source on the junction's first input.
              "prepend of a composed head",
              (fun () ->
                  fsharpInts ()
                  |> Source.prepend (Source.range 0 2 |> Source.map (fun value -> value - 1))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Prepend(Orleans.Dataflow.Source.Range(0, 2).Select(fun value -> value - 1))
                  |> closeCSharp)

              "a fan-in feeding a fan-out",
              (fun () ->
                  fsharpInts ()
                  |> Source.merge (Source.ofSeq [ 10; 20 ])
                  |> Source.filter (fun value -> value <> 2)
                  |> Source.broadcastTo [ ignoring (); doubling () ]),
              (fun () ->
                  csharpInts()
                      .Merge(Orleans.Dataflow.Source.From([ 10; 20 ]))
                      .Where(fun value -> value <> 2)
                      .BroadcastTo(ignoringCSharp (), doublingCSharp ())) ]

    [<Fact>]
    member _.``Branch order is identity-bearing``() =
        let ordered = fsharpInts () |> Source.broadcastTo [ ignoring (); doubling () ]
        let swapped = fsharpInts () |> Source.broadcastTo [ doubling (); ignoring () ]

        // The legs are attached in argument order and the occurrences are numbered in that same order, so
        // two branches exchanged is a different graph. A frontend that sorted its branches, or that wired
        // them by anything other than position, would pass every other case in this suite and fail this one.
        Assert.NotEqual<Orleans.Dataflow.Definition.GraphFingerprint>(ordered.Fingerprint, swapped.Fingerprint)

    [<Fact>]
    member _.``The order of a fan-in's inputs is identity-bearing``() =
        // The two inputs have to be distinguishable stages for the claim to be observable at all. A local
        // document states which stage stands where and never what a sequence contains, so a concat of two
        // `from-enumerable` sources exchanged is byte-for-byte the graph it was — in either frontend, and
        // correctly: the elements are behavior, and behavior never enters a document. A range and a
        // sequence are two stages, so exchanging them moves a real edge.
        let forward = Source.range 1 3 |> Source.concat (Source.ofSeq [ 9 ]) |> closeFSharp
        let backward = Source.ofSeq [ 9 ] |> Source.concat (Source.range 1 3) |> closeFSharp

        Assert.NotEqual<Orleans.Dataflow.Definition.GraphFingerprint>(
            forward.Fingerprint,
            backward.Fingerprint)

    [<Fact>]
    member _.``Two indistinguishable fan-in inputs exchanged are one document in either frontend``() =
        // The other half of the statement above, asserted rather than assumed, and asserted across the two
        // frontends so that it reads as a fact about the algebra rather than as an F# accident.
        let fsharpForward = Source.ofSeq [ 1 ] |> Source.concat (Source.ofSeq [ 9 ]) |> closeFSharp
        let fsharpBackward = Source.ofSeq [ 9 ] |> Source.concat (Source.ofSeq [ 1 ]) |> closeFSharp

        let csharpForward =
            Orleans.Dataflow.Source.From([ 1 ]).Concat(Orleans.Dataflow.Source.From([ 9 ]))
            |> closeCSharp

        Assert.Equal(fsharpForward.Fingerprint, fsharpBackward.Fingerprint)
        Assert.Equal(csharpForward.Fingerprint, fsharpForward.Fingerprint)

    [<Fact>]
    member _.``Two builds of one junction program are byte-identical``() =
        let build () =
            let counting, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count

            Source.ofSeq [ 1; 2; 3 ]
            |> Source.merge (Source.ofSeq [ 4 ])
            |> Source.alsoTo (ignoring ())
            |> Source.fork Flow.identity<int> (Flow.map (fun (value: int) -> value * 2))
            |> Fork.zipWith (fun left right -> left + right)
            |> Source.broadcastTo [ counting; doubling () ]

        // Nothing in a document may come from the run that built it: two builds of one program, each with
        // its own branch values and its own lambda instances, are the same bytes.
        Assert.Equal((build ()).Fingerprint, (build ()).Fingerprint)

    [<Fact>]
    member _.``A result-bearing branch closes exactly one graph``() =
        let counting, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count
        let first = fsharpInts () |> Source.broadcastTo [ counting; ignoring () ]

        let refused =
            Assert.Throws<System.InvalidOperationException>(fun () ->
                fsharpInts () |> Source.broadcastTo [ counting; ignoring () ] |> ignore)

        // The second graph is refused rather than silently repointing the first graph's slot at itself.
        Assert.NotEmpty(first.ResultSlots)
        Assert.Contains("closes exactly one graph", refused.Message)

    [<Fact>]
    member _.``A branch that declares no result is reusable``() =
        let side = ignoring ()

        let first = fsharpInts () |> Source.broadcastTo [ side; doubling () ]
        let second = fsharpInts () |> Source.broadcastTo [ side; doubling () ]

        Assert.Equal(first.Fingerprint, second.Fingerprint)

    [<Fact>]
    member _.``A fan-out of one branch is refused by name in either frontend``() =
        let fsharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () ->
                fsharpInts () |> Source.broadcastTo [ ignoring () ] |> ignore)

        let csharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () ->
                csharpInts().BroadcastTo(ignoringCSharp ()) |> ignore)

        // The bound is the shared vocabulary's, and both frontends refuse the same call under the same
        // parameter name with the same sentence.
        Assert.Equal("branches", fsharpRefusal.ParamName)
        Assert.Equal("branches", csharpRefusal.ParamName)
        Assert.Equal(csharpRefusal.Message, fsharpRefusal.Message)

    [<Fact>]
    member _.``A fan-out past the declared legs is refused by name in either frontend``() =
        let nine = List.init 9 (fun _ -> ignoring ())
        let nineCSharp = Array.init 9 (fun _ -> ignoringCSharp ())

        let fsharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () ->
                fsharpInts () |> Source.broadcastTo nine |> ignore)

        let csharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () -> csharpInts().BroadcastTo(nineCSharp) |> ignore)

        // The upper bound and the count are both in the sentence, so an equal message is an equal bound as
        // well as an equal wording — which is what a restated diagnostic has to be checked for.
        Assert.Equal(csharpRefusal.Message, fsharpRefusal.Message)

    [<Fact>]
    member _.``Two branches declaring one name are refused in either frontend``() =
        let fsharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () ->
                let first, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count
                let second, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count

                fsharpInts () |> Source.broadcastTo [ first; second ] |> ignore)

        let csharpRefusal =
            Assert.Throws<System.ArgumentException>(fun () ->
                let mutable first = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
                let mutable second = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

                let firstBranch =
                    Orleans.Dataflow.Flow
                        .For<int>()
                        .To(Orleans.Dataflow.Sink.Count<int>(), "counted", &first)

                let secondBranch =
                    Orleans.Dataflow.Flow
                        .For<int>()
                        .To(Orleans.Dataflow.Sink.Count<int>(), "counted", &second)

                csharpInts().BroadcastTo(firstBranch, secondBranch) |> ignore)

        // Both frontends reach the one closure, so the refusal is the algebra's own and reads identically.
        Assert.Equal(csharpRefusal.Message, fsharpRefusal.Message)

    [<Fact>]
    member _.``An interleave of no segment is refused by name in either frontend``() =
        let fsharpRefusal =
            Assert.Throws<System.ArgumentOutOfRangeException>(fun () ->
                fsharpInts () |> Source.interleave (Source.ofSeq [ 4 ]) 0 |> ignore)

        let csharpRefusal =
            Assert.Throws<System.ArgumentOutOfRangeException>(fun () ->
                csharpInts().Interleave(Orleans.Dataflow.Source.From([ 4 ]), 0) |> ignore)

        Assert.Equal("segmentSize", fsharpRefusal.ParamName)
        Assert.Equal(csharpRefusal.Message, fsharpRefusal.Message)

    [<Fact>]
    member _.``A tap's result and a fan-out's results are one graph's declarations``() =
        let audited, _ = Flow.identity<int> |> Branch.toResult "audited" Sink.count
        let counting, _ = Flow.identity<int> |> Branch.toResult "counted" Sink.count
        let summing, _ = Flow.identity<int> |> Branch.toResult "summed" Sink.count

        let fsharpGraph =
            fsharpInts ()
            |> Source.alsoTo audited
            |> Source.broadcastTo [ counting; summing ]

        let csharpGraph =
            let mutable audited = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
            let mutable counted = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>
            let mutable summed = Unchecked.defaultof<Orleans.Dataflow.ResultSlot<int64>>

            let auditBranch =
                Orleans.Dataflow.Flow
                    .For<int>()
                    .To(Orleans.Dataflow.Sink.Count<int>(), "audited", &audited)

            let countBranch =
                Orleans.Dataflow.Flow
                    .For<int>()
                    .To(Orleans.Dataflow.Sink.Count<int>(), "counted", &counted)

            let sumBranch =
                Orleans.Dataflow.Flow
                    .For<int>()
                    .To(Orleans.Dataflow.Sink.Count<int>(), "summed", &summed)

            csharpInts().AlsoTo(auditBranch).BroadcastTo(countBranch, sumBranch)

        // A tap's request travels on the shape and the fan-out's are handed to the close, so a graph with
        // both is where the two lists meet. Three results, one document, and the same one either way round.
        Assert.Equal(3, fsharpGraph.ResultSlots.Count)
        Assert.Equal(csharpGraph.Fingerprint, fsharpGraph.Fingerprint)

    [<Fact>]
    member _.``A branch names its result by the same grammar a close does``() =
        let refused =
            Assert.Throws<System.ArgumentException>(fun () ->
                Flow.identity<int> |> Branch.toResult "Not_A_Segment" Sink.count |> ignore)

        Assert.Equal("slotName", refused.ParamName)
        Assert.Contains("Not_A_Segment", refused.Message)
