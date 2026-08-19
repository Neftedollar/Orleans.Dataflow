namespace Orleans.Dataflow.FSharpTests

open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.FSharpTests.Fixtures
open Xunit

/// <summary>Every junction this frontend adds actually runs, and carries the elements it promises.</summary>
/// <remarks>
/// <para>
/// A fingerprint says two frontends wrote one document. It says nothing about whether the router an F#
/// partition stored is a shape the runtime's delegate adapter can read, whether the projections an unzip
/// carries reach the halves they are named after, or whether a zip's combiner builds the row the author
/// wrote — a count comes out the same when two projections are swapped. That is what these tests are for.
/// </para>
/// <para>
/// Every element type is chosen so that a mix-up cannot pass: the halves of a pair are a string and an
/// integer, and the two sides of a zip are distinguishable in the row they build. The arrangements are the
/// other frontend's own runtime suite, deliberately, so that a divergence in behavior between the two shows
/// as one suite passing and the other failing on the same program.
/// </para>
/// <para>
/// The results are exact wherever the semantics are exact. Where they are not — a merge, a balance, a
/// merging diamond — what is asserted is the multiset or the total, because the order is genuinely undefined
/// and an assertion that fixed it would be asserting a timing. Handles are bound with <c>use!</c>: a handle
/// is <see cref="T:System.IAsyncDisposable"/>, and <c>use!</c> disposes it at the end of the scope — after a
/// failed assertion as much as after a passing one.
/// </para>
/// </remarks>
type JunctionBehaviorTests() =

    /// <summary>A leg that keeps nothing, for the junctions that need a second branch and no second claim.</summary>
    static let ignoringBranch () : Branch<int> = Flow.identity<int> |> Branch.toSink Sink.ignore

    [<Fact>]
    member _.``merge delivers every element of both inputs``() : Task =
        task {
            let! observed = Source.ofSeq [ 1; 2; 3 ] |> Source.merge (Source.ofSeq [ 10; 20 ]) |> elementsOf

            // A merge emits what has arrived, so the order between the inputs is a scheduling fact and the
            // claim that can be made is the multiset.
            Assert.Equal<int>([ 1; 2; 3; 10; 20 ], observed |> Seq.sort)
        }

    [<Fact>]
    member _.``merge3 delivers every element of all three inputs``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.merge3 (Source.ofSeq [ 10 ]) (Source.ofSeq [ 100; 200 ])
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 10; 100; 200 ], observed |> Seq.sort)
        }

    [<Fact>]
    member _.``concat emits its first input to the end before its second``() : Task =
        task {
            let! observed = Source.ofSeq [ 1; 2; 3 ] |> Source.concat (Source.ofSeq [ 10; 20 ]) |> elementsOf

            Assert.Equal<int>([ 1; 2; 3; 10; 20 ], observed)
        }

    [<Fact>]
    member _.``prepend emits its head first and append its tail last``() : Task =
        task {
            let! headed = Source.ofSeq [ 1; 2 ] |> Source.prepend (Source.ofSeq [ 0 ]) |> elementsOf
            let! tailed = Source.ofSeq [ 1; 2 ] |> Source.append (Source.ofSeq [ 9 ]) |> elementsOf

            Assert.Equal<int>([ 0; 1; 2 ], headed)
            Assert.Equal<int>([ 1; 2; 9 ], tailed)
        }

    [<Fact>]
    member _.``interleave takes its declared segment from each input in turn``() : Task =
        task {
            // The segment size is the one number a junction writes into its document, and this is the
            // sequence it buys: two from the left, two from the right, and on until both run out. An input
            // that ends is dropped from the rotation and the remaining one carries on.
            let! observed =
                Source.ofSeq [ 1; 2; 3; 4; 5; 6 ]
                |> Source.interleave (Source.ofSeq [ 10; 20; 30 ]) 2
                |> elementsOf

            Assert.Equal<int>([ 1; 2; 10; 20; 3; 4; 30; 5; 6 ], observed)
        }

    [<Fact>]
    member _.``zip pairs the inputs in the order they were written``() : Task =
        task {
            // The first member is the source the call was written on and the second is the argument. Nothing
            // else in this suite would notice them being exchanged, because both halves are present anyway.
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.zip (Source.ofSeq [ "a"; "b" ])
                |> Source.map (fun struct (value, text) -> $"{value}{text}")
                |> elementsOf

            Assert.Equal<string>([ "1a"; "2b" ], observed)
        }

    [<Fact>]
    member _.``zip ends as soon as either input does``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.zip (Source.ofSeq [ "a" ])
                |> Source.map (fun struct (value, text) -> $"{value}{text}")
                |> elementsOf

            Assert.Equal<string>([ "1a" ], observed)
        }

    [<Fact>]
    member _.``zipWith builds the row the author wrote``() : Task =
        task {
            // 10 x 3 and 20 x 4, paired positionally: the answer is a statement about which price met which
            // quantity and not only about how many rows there were.
            let! observed =
                Source.ofSeq [ 10; 20 ]
                |> Source.zipWith (Source.ofSeq [ 3; 4 ]) (fun price quantity -> price * quantity)
                |> elementsOf

            Assert.Equal<int>([ 30; 80 ], observed)
        }

    [<Fact>]
    member _.``combineLatest builds a row from each arrival and the other side's latest``() : Task =
        task {
            // One element on the left and three on the right, so every row has to carry the one left
            // element: that is the whole difference from a zip, which would have produced one row and
            // stopped.
            let! observed =
                Source.ofSeq [ "setting" ]
                |> Source.combineLatest (Source.ofSeq [ 1; 2; 3 ]) (fun setting value -> $"{setting}:{value}")
                |> elementsOf

            // How many rows a combine-latest emits depends on how the two inputs interleave in time, which
            // is a scheduling fact and not a contract. What is a contract: every row pairs the one left
            // element with a right element, the last right element is represented, and none is invented.
            Assert.NotEmpty(observed)
            Assert.All(observed, fun row -> Assert.StartsWith("setting:", row, System.StringComparison.Ordinal))
            Assert.Contains("setting:3", observed)
            Assert.All(observed, fun row -> Assert.Contains(row, [| "setting:1"; "setting:2"; "setting:3" |]))
        }

    [<Fact>]
    member _.``alsoTo sees every element the main line sees``() : Task =
        task {
            // A broadcast delivers to every leg, so a tap is not a sample: it is the same stream, and the
            // main line's own filtering happens downstream of the junction rather than before it.
            let audited, auditedSlot = Flow.identity<int> |> Branch.toResult "audited" (collecting ())

            let graph, keptSlot =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.alsoTo audited
                |> Source.filter (fun value -> value > 2)
                |> Source.toResult "kept" (collecting ())

            let! tapped, kept = graph |> bothResultsOf auditedSlot keptSlot

            Assert.Equal<int>([ 1; 2; 3 ], tapped)
            Assert.Equal<int>([ 3 ], kept)
        }

    [<Fact>]
    member _.``Two taps in a row are two junctions and both see everything``() : Task =
        task {
            let first, firstSlot = Flow.identity<int> |> Branch.toResult "first" Sink.count
            let second, secondSlot = Flow.identity<int> |> Branch.toResult "second" Sink.count

            let graph, _ =
                Source.ofSeq [ 1; 2 ]
                |> Source.alsoTo first
                |> Source.alsoTo second
                |> Source.toResult "kept" Sink.count

            let! firstCount, secondCount = graph |> bothResultsOf firstSlot secondSlot

            Assert.Equal(
                2,
                graph.Document.Nodes |> Seq.filter (fun node -> node.Stage.Stage.Value = "broadcast") |> Seq.length)

            Assert.Equal(2L, firstCount)
            Assert.Equal(2L, secondCount)
        }

    [<Fact>]
    member _.``divertTo sends the accepted elements aside and keeps the rest``() : Task =
        task {
            // Unlike a tap, a divert never duplicates: an element goes one way or the other and never both.
            let diverted, divertedSlot = Flow.identity<int> |> Branch.toResult "diverted" (collecting ())

            let graph, keptSlot =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.divertTo (fun value -> value % 2 = 0) diverted
                |> Source.toResult "kept" (collecting ())

            let! sidelined, kept = graph |> bothResultsOf divertedSlot keptSlot

            Assert.Equal<int>([ 2; 4 ], sidelined)
            Assert.Equal<int>([ 1; 3 ], kept)
        }

    [<Fact>]
    member _.``partitionTo sends each element to the branch its router names``() : Task =
        task {
            // The router's answer is the leg's position, so a partition that ignored it — or read it as
            // something else — shows up as elements on the wrong side rather than as a different count.
            let even, evenSlot = Flow.identity<int> |> Branch.toResult "even" (collecting ())
            let odd, oddSlot = Flow.identity<int> |> Branch.toResult "odd" (collecting ())

            let graph =
                Source.ofSeq [ 1; 2; 3; 4; 5 ]
                |> Source.partitionTo (fun value -> value % 2) [ even; odd ]

            let! evens, odds = graph |> bothResultsOf evenSlot oddSlot

            Assert.Equal<int>([ 2; 4 ], evens)
            Assert.Equal<int>([ 1; 3; 5 ], odds)
        }

    [<Fact>]
    member _.``broadcastTo delivers every element to every branch``() : Task =
        task {
            let counting, countedSlot = Flow.identity<int> |> Branch.toResult "counted" Sink.count

            let summing, summedSlot =
                Flow.identity<int>
                |> Branch.toResult "summed" (Sink.aggregate 0 (fun state value -> state + value))

            let graph = Source.ofSeq [ 1; 2; 3 ] |> Source.broadcastTo [ counting; summing ]
            let! counted, summed = graph |> bothResultsOf countedSlot summedSlot

            Assert.Equal(3L, counted)
            Assert.Equal(6, summed)
        }

    [<Fact>]
    member _.``balanceTo gives every element to exactly one branch``() : Task =
        task {
            // Which leg an element takes is not defined — a balance hands it to whichever is ready — so the
            // claim that can be made is the one that matters: nothing is lost and nothing is duplicated.
            let left, leftSlot = Flow.identity<int> |> Branch.toResult "left" Sink.count
            let right, rightSlot = Flow.identity<int> |> Branch.toResult "right" Sink.count

            let graph = Source.range 0 100 |> Source.balanceTo [ left; right ]
            let! leftCount, rightCount = graph |> bothResultsOf leftSlot rightSlot

            Assert.Equal(100L, leftCount + rightCount)
        }

    [<Fact>]
    member _.``unzipTo sends the left half left and the right half right``() : Task =
        task {
            // Two differently typed halves collected rather than counted, because a count comes out the same
            // when the two projections are swapped and the values do not.
            let names, namesSlot = Flow.identity<string> |> Branch.toResult "names" (collecting ())
            let ages, agesSlot = Flow.identity<int> |> Branch.toResult "ages" (collecting ())

            let graph =
                Source.ofSeq [ struct ("ada", 36); struct ("alan", 41) ]
                |> Source.unzipTo names ages

            let! collectedNames, collectedAges = graph |> bothResultsOf namesSlot agesSlot

            Assert.Equal<string>([ "ada"; "alan" ], collectedNames)
            Assert.Equal<int>([ 36; 41 ], collectedAges)
        }

    [<Fact>]
    member _.``A zipped stream unzips again with nothing in between``() : Task =
        task {
            // The struct-tuple decision, asserted as behavior: what a zip produces is exactly what an unzip
            // consumes, so a round trip through both needs no conversion stage and loses no half.
            let names, namesSlot = Flow.identity<string> |> Branch.toResult "names" (collecting ())
            let ages, agesSlot = Flow.identity<int> |> Branch.toResult "ages" (collecting ())

            let graph =
                Source.ofSeq [ "ada"; "alan" ]
                |> Source.zip (Source.ofSeq [ 36; 41 ])
                |> Source.unzipTo names ages

            let! collectedNames, collectedAges = graph |> bothResultsOf namesSlot agesSlot

            Assert.Equal<string>([ "ada"; "alan" ], collectedNames)
            Assert.Equal<int>([ 36; 41 ], collectedAges)
        }

    [<Fact>]
    member _.``A fork through two identity flows pairs each element with itself``() : Task =
        task {
            // Both legs contribute no occurrence at all, so the broadcast's own leg ports are wired straight
            // to the zip's inputs. That is the smallest diamond expressible.
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.fork Flow.identity<int> Flow.identity<int>
                |> Fork.zipWith (fun left right -> left + right)
                |> elementsOf

            Assert.Equal<int>([ 2; 4 ], observed)
        }

    [<Fact>]
    member _.``A fork rejoined by zip names its halves by the order it was written``() : Task =
        task {
            let! observed =
                Source.ofSeq [ 1; 2 ]
                |> Source.fork (Flow.map (fun (value: int) -> value * 10)) (Flow.map (fun (value: int) -> string value))
                |> Fork.zip
                |> Source.map (fun struct (scaled, text) -> $"{scaled}:{text}")
                |> elementsOf

            Assert.Equal<string>([ "10:1"; "20:2" ], observed)
        }

    [<Fact>]
    member _.``forkMerge emits both derivations of every element``() : Task =
        task {
            // One element in produces two elements out — one per path — in whatever order the paths finish.
            // That is a merge and not a zip, so what is asserted is the multiset.
            let! observed =
                Source.ofSeq [ 1 ]
                |> Source.forkMerge
                    (Flow.map (fun (value: int) -> value * 10))
                    (Flow.map (fun (value: int) -> value * 100))
                |> elementsOf

            Assert.Equal<int>([ 10; 100 ], observed |> Seq.sort)
        }

    [<Fact>]
    member _.``A fan-in feeds a fan-out through one graph``() : Task =
        task {
            // The composition claim, run rather than fingerprinted: a junction call answers an ordinary
            // source, so what follows it is the whole vocabulary and not a restricted one.
            let counting, countedSlot = Flow.identity<int> |> Branch.toResult "counted" Sink.count

            let summing, summedSlot =
                Flow.identity<int>
                |> Branch.toResult "summed" (Sink.aggregate 0 (fun state value -> state + value))

            let graph =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.merge (Source.ofSeq [ 10; 20 ])
                |> Source.filter (fun value -> value <> 2)
                |> Source.broadcastTo [ counting; summing ]

            let! counted, summed = graph |> bothResultsOf countedSlot summedSlot

            Assert.Equal(4L, counted)
            Assert.Equal(34, summed)
        }

    [<Fact>]
    member _.``A tap on a joined source keeps its slot on its own branch``() : Task =
        task {
            // The positions of everything the right-hand source carries move when it is placed beside the
            // left, and a result its tap already asked for moves with them. If it did not, the slot would
            // resolve whatever occurrence happens to stand at the old position — which, in a graph this
            // size, is a different terminal that also produces a number.
            let tapped, tappedSlot = Flow.identity<int> |> Branch.toResult "tapped" Sink.count

            let graph, joinedSlot =
                Source.ofSeq [ 1; 2; 3; 4 ]
                |> Source.merge (
                    Source.ofSeq [ 10; 20 ]
                    |> Source.alsoTo tapped
                    |> Source.map (fun value -> value + 1)
                )
                |> Source.toResult "joined" Sink.count

            let! tappedCount, joinedCount = graph |> bothResultsOf tappedSlot joinedSlot

            Assert.Equal(2L, tappedCount)
            Assert.Equal(6L, joinedCount)
        }

    [<Fact>]
    member _.``A tap's result and a fan-out's results all resolve from one run``() : Task =
        task {
            // A tap's request travels on the shape and a fan-out's are handed to the closing call, so this
            // is where the two lists meet. All three have to resolve against the one run, and the tap's has
            // to point at the tap's own terminal rather than at whichever occurrence stands at its position.
            let audited, auditedSlot = Flow.identity<int> |> Branch.toResult "audited" (collecting ())
            let counting, countedSlot = Flow.identity<int> |> Branch.toResult "counted" Sink.count

            let summing, summedSlot =
                Flow.identity<int>
                |> Branch.toResult "summed" (Sink.aggregate 0 (fun state value -> state + value))

            let graph =
                Source.ofSeq [ 1; 2; 3 ]
                |> Source.alsoTo audited
                |> Source.filter (fun value -> value > 1)
                |> Source.broadcastTo [ counting; summing ]

            use! run = host.MaterializeAsync(graph, token ())
            let! tapped = run.GetValueAsync(auditedSlot, token ())
            let! counted = run.GetValueAsync(countedSlot, token ())
            let! summed = run.GetValueAsync(summedSlot, token ())

            do! run.Completion

            Assert.Equal<int>([ 1; 2; 3 ], tapped)
            Assert.Equal(2L, counted)
            Assert.Equal(5, summed)
        }

    [<Fact>]
    member _.``A run refuses the slot of a branch that closed no graph``() : Task =
        task {
            // The practical end of the branch-slot rule: a slot whose junction call never ran names no
            // graph, so a run cannot be asked for it. It fails where the slot is read rather than resolving
            // something that happens to share its name.
            let counting, countedSlot = Flow.identity<int> |> Branch.toResult "counted" Sink.count
            let graph = Source.ofSeq [ 1; 2 ] |> Source.broadcastTo [ counting; ignoringBranch () ]

            let _, unclosed = Flow.identity<int> |> Branch.toResult "counted" Sink.count

            use! run = host.MaterializeAsync(graph, token ())

            let! refused =
                Assert.ThrowsAsync<System.InvalidOperationException>(fun () ->
                    run.GetValueAsync(unclosed, token ()) :> Task)

            let! counted = run.GetValueAsync(countedSlot, token ())

            do! run.Completion

            Assert.NotNull(refused)
            Assert.Equal(2L, counted)
        }

    [<Fact>]
    member _.``Two runs of one junction graph resolve independently``() : Task =
        task {
            // A junction graph is a description like every other: materializing it twice starts two runs,
            // and the slots of the one graph resolve against either. A shared accumulator would show as a
            // count that continued.
            let counting, countedSlot = Flow.identity<int> |> Branch.toResult "counted" Sink.count
            let summing, summedSlot = Flow.identity<int> |> Branch.toResult "summed" Sink.count

            let graph = Source.ofSeq [ 1; 2 ] |> Source.broadcastTo [ counting; summing ]

            let! firstCounted, _ = graph |> bothResultsOf countedSlot summedSlot
            let! secondCounted, secondSummed = graph |> bothResultsOf countedSlot summedSlot

            Assert.Equal(2L, firstCounted)
            Assert.Equal(2L, secondCounted)
            Assert.Equal(2L, secondSummed)
        }
