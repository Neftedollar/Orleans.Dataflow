namespace Orleans.Dataflow.FSharpTests

open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.Identity
open Xunit
open Orleans.Dataflow.FSharpTests.Fixtures

/// <summary>
/// A durable run of an F#-authored graph: the checkpoints it writes, the attempt that continues it, and the
/// duplicate window between them measured by value.
/// </summary>
/// <remarks>
/// <para>
/// No new F# surface is under test here and that is the point. <c>DurableRunOptions</c>,
/// <c>MaterializeDurableAsync</c>, and <c>MaterializeFromCheckpointAsync</c> are public runtime members that
/// read as they are from F#, so what these tests pin is that an F#-authored graph reaches the durability
/// machinery at all — its source declares a cursor, its checkpoints are written under the run's own identity,
/// and a second materialization of the same document continues the first.
/// </para>
/// <para>
/// <b>The crash is an F# lambda that raises.</b> The C# suite injects its failure through the Testing
/// package's fault point, which answers in the C# facade's own <c>Flow&lt;T, T&gt;</c> and therefore cannot be
/// composed into an F#-authored chain at all; the same is true of its marking sink. A raising <c>map</c> is
/// the honest substitute — it kills the attempt at a chosen element — and what it costs is stated in the run
/// report: with no commit mark in the document, the replay is measured from the stored cursor alone.
/// </para>
/// <para>
/// <b>Every number here is a value rather than a count.</b> The two attempts' element lists are compared
/// element-wise, because a count would pass for a resume that delivered the wrong elements the right number
/// of times. The stored checkpoint itself is asserted only through the public store, whose document format is
/// the runtime's business and not this suite's.
/// </para>
/// </remarks>
type DurabilityTests() =

    /// <summary>The identity every locally authored graph carries, there being no author who named it.</summary>
    static let anonymous = GraphId.Create "anonymous"

    /// <summary>Builds the durable declaration a run is started or resumed under.</summary>
    static let durable (store: Orleans.Dataflow.Testing.InMemoryCheckpointStore) (run: string) (everyElements: int) =
        Orleans.Dataflow.DurableRunOptions(
            Store = store,
            RunId = RunId.Create run,
            EveryElements = System.Nullable everyElements)

    /// <summary>Reads the commit mark the store currently holds for one durable run.</summary>
    /// <remarks>
    /// Read through the core package's own checkpoint reader — this project is a friend for exactly this
    /// kind of assertion — because what a resume restores is what was written down, and a number read off a
    /// live sink would only say what that sink believes.
    /// </remarks>
    static let storedMark (store: Orleans.Dataflow.Testing.InMemoryCheckpointStore) (run: string) =
        task {
            let! stored = store.ReadAsync(anonymous, RunId.Create run)

            Assert.True(stored.HasValue, "the store holds a checkpoint for the run")

            match Orleans.Dataflow.Runtime.LocalCheckpointDocument.TryRead stored.Value.Document with
            | true, checkpoint, _ ->
                let mark = (nonNull checkpoint).Marks |> Seq.exactlyOne

                return
                    mark.Value
                        .ToElement()
                        .GetProperty(Orleans.Dataflow.Runtime.LocalMarkingSink.CommittedMember)
                        .GetInt64()
            | false, _, violations ->
                let reasons = String.concat "; " violations

                Assert.Fail($"the stored checkpoint does not read: {reasons}")

                return 0L
        }

    /// <summary>The twelve-element graph both attempts run, with the element that kills the first.</summary>
    /// <remarks>
    /// The failing element is a parameter and the delegate is not document content, so the crashing attempt
    /// and the resumed one are the very same document — which is what makes resuming them legal at all, and
    /// is asserted rather than assumed.
    /// </remarks>
    static let committing (observed: ResizeArray<int>) (failAt: int) =
        Source.ofSeq [ 1..12 ]
        |> Source.map (fun value ->
            if value = failAt then
                raise (System.InvalidOperationException $"the attempt dies at element {failAt}")
            else
                value)
        |> Source.toSink (Sink.forEach observed.Add)

    [<Fact>]
    member _.``A durable run of an F#-authored graph writes checkpoints and a resume replays the window``() : Task =
        task {
            let store = Orleans.Dataflow.Testing.InMemoryCheckpointStore()
            let first = ResizeArray<int>()
            let second = ResizeArray<int>()

            let crashing = committing first 9
            let resumed = committing second 0

            // One document, two behaviors: a delegate never enters a document, so the resumed attempt is the
            // same graph the checkpoint was taken of.
            Assert.Equal(crashing.Fingerprint, resumed.Fingerprint)

            let! attempt = host.MaterializeDurableAsync(crashing, durable store "replay" 3, token ())

            do!
                Assert.ThrowsAsync<System.InvalidOperationException>(fun () -> attempt.Completion)
                :> Task

            do! attempt.DisposeAsync()

            // A checkpoint exists, under the run's own identity: a local graph is anonymous, so what separates
            // two durable runs is the name their author gave them.
            Assert.True(store.Holds(anonymous, RunId.Create "replay"))
            Assert.Equal(1, store.Count)

            // The attempt delivered eight elements before the ninth killed it.
            Assert.Equal<int>([ 1..8 ], first)

            let! continued = host.MaterializeFromCheckpointAsync(resumed, durable store "replay" 3, token ())

            do! continued.Completion
            do! continued.DisposeAsync()

            // The source reopened at the stored cursor, which the element bound put at six: the resumed
            // attempt starts at element seven and runs the stream out.
            Assert.Equal<int>([ 7..12 ], second)

            // The duplicate window is exactly the elements between the stored cursor and the crash — two of
            // them, by value, and not one more. Nothing is lost, because this graph holds nothing between the
            // source and its terminal.
            Assert.Equal<int>([ 7; 8 ], first |> Seq.skip 6 |> Seq.toList)
            Assert.Equal<int>([ 1..12 ], Seq.append first second |> Seq.distinct |> Seq.sort |> Seq.toList)
            Assert.Equal(14, first.Count + second.Count)
        }

    [<Fact>]
    member _.``A commit mark travels through an F#-authored graph and the resumed sink continues it``() : Task =
        task {
            let store = Orleans.Dataflow.Testing.InMemoryCheckpointStore()
            let firstCommitted = ResizeArray<int>()
            let secondCommitted = ResizeArray<int>()

            // The marking sink is the Testing package's own, reached through the tests-only bridge: the C#
            // facade value's occurrence chain is the currency both frontends share, so the mark in this
            // document is the very stage the C# suite measures with.
            let marked (committed: ResizeArray<int>) (failAt: int) =
                Source.ofSeq [ 1..12 ]
                |> Source.map (fun value ->
                    if value = failAt then
                        raise (System.InvalidOperationException $"the attempt dies at element {failAt}")
                    else
                        value)
                |> Source.toSink (
                    TestingInterop.sink (Orleans.Dataflow.Testing.TestSink.Marking<int>("mark", fun value -> committed.Add value)))

            let! attempt = host.MaterializeDurableAsync(marked firstCommitted 9, durable store "marked" 3, token ())

            do!
                Assert.ThrowsAsync<System.InvalidOperationException>(fun () -> attempt.Completion)
                :> Task

            do! attempt.DisposeAsync()

            Assert.Equal<int>([ 1..8 ], firstCommitted)

            // The stored pair, read out of the store rather than off the run: at the second capture the
            // element bound held the run at element six, the sink's callback had returned for all six, so
            // cursor and mark agree — a mark advances after its effect, and nothing here holds elements
            // between the two.
            let! storedBefore = storedMark store "marked"

            Assert.Equal(6L, storedBefore)

            let! continued =
                host.MaterializeFromCheckpointAsync(marked secondCommitted 0, durable store "marked" 3, token ())

            do! continued.Completion
            do! continued.DisposeAsync()

            Assert.Equal<int>([ 7..12 ], secondCommitted)

            // The mark is the run's number and not the attempt's: the resumed sink was handed six committed
            // elements and counted its own on top, so the last capture — at the twelfth element — stored
            // twelve. A mark that restarted with the attempt would have stored six.
            let! storedAfter = storedMark store "marked"

            Assert.Equal(12L, storedAfter)
        }

    [<Fact>]
    member _.``A durable run that declares no timing never touches the store``() : Task =
        task {
            let store = Orleans.Dataflow.Testing.InMemoryCheckpointStore()
            let observed = ResizeArray<int>()

            let options =
                Orleans.Dataflow.DurableRunOptions(Store = store, RunId = RunId.Create "untimed")

            let! run = host.MaterializeDurableAsync(committing observed 0, options, token ())

            do! run.Completion

            // The documented promise, asserted rather than assumed: a run with neither an interval nor an
            // element bound has nothing that could make a capture due.
            Assert.Equal(0, store.Count)
            Assert.Equal<int>([ 1..12 ], observed)

            do! run.DisposeAsync()
        }

    [<Fact>]
    member _.``Resuming a run the store knows nothing about is refused by name``() : Task =
        task {
            let store = Orleans.Dataflow.Testing.InMemoryCheckpointStore()
            let observed = ResizeArray<int>()

            let refused =
                Assert.ThrowsAsync<System.InvalidOperationException>(fun () ->
                    task {
                        let! run =
                            host.MaterializeFromCheckpointAsync(
                                committing observed 0,
                                durable store "never-ran" 3,
                                token ())

                        do! run.DisposeAsync()
                    }
                    :> Task)

            let! failure = refused

            Assert.Contains("never-ran", failure.Message)
            Assert.Empty observed
        }
