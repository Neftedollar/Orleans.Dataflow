namespace Orleans.Dataflow.FSharpTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Orleans.Dataflow.FSharp
open Orleans.Dataflow.FSharpTests.Fixtures
open Xunit

/// <summary>
/// The M7 invariant, read over the whole linear vocabulary: every F# operator and its C#-authored twin are
/// one document.
/// </summary>
/// <remarks>
/// <para>
/// Byte identity of the canonical document — asserted through fingerprint equality, which is a hash of
/// exactly those bytes — is what "equal frontends over one algebra" means as a test. Delegates never enter a
/// document, so the twins use different lambda instances on purpose, and an operator whose only difference
/// between the frontends is the effect an author spells it with (an F# <c>Async</c> against a
/// <c>Task</c>) is asserted to be the very same node: the document states which stage an author wrote and
/// never how the callback was written.
/// </para>
/// <para>
/// The cases are tables rather than a fact each. What a parity suite is asked at a review is "which
/// operators are not equal", and a suite that answers one operator per run makes that question cost a run
/// per operator; the assertion walks every case and names every divergence at once. Each entry is a name
/// and the two spellings, so a failure reads as the operator's own name.
/// </para>
/// </remarks>
type OperatorParityTests() =

    static let fsharpInts () = Source.ofSeq [ 1; 2; 3 ]

    static let csharpInts () = Orleans.Dataflow.Source.From([ 1; 2; 3 ])

    static let exportInt (state: int) =
        Orleans.Dataflow.Serialization.CanonicalJsonValue.Parse(string state)

    static let restoreInt (value: Orleans.Dataflow.Serialization.CanonicalJsonValue) =
        value.ToElement().GetInt32()

    [<Fact>]
    member _.``The element operators are one document from either frontend``() =
        assertParity
            [ "map",
              (fun () -> fsharpInts () |> Source.map (fun value -> value + 1) |> closeFSharp),
              (fun () -> csharpInts().Select(fun value -> value + 1) |> closeCSharp)

              "filter",
              (fun () -> fsharpInts () |> Source.filter (fun value -> value > 1) |> closeFSharp),
              (fun () -> csharpInts().Where(fun value -> value > 1) |> closeCSharp)

              "choose",
              (fun () ->
                  fsharpInts ()
                  |> Source.choose (fun value -> if value > 1 then ValueSome(value * 2) else ValueNone)
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectMany(fun value ->
                          if value > 1 then Seq.singleton (value * 2) else Seq.empty<int>)
                  |> closeCSharp)

              "chooseOption",
              (fun () ->
                  fsharpInts ()
                  |> Source.chooseOption (fun value -> if value > 1 then Some(value * 2) else None)
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectMany(fun value ->
                          if value > 1 then Seq.singleton (value * 2) else Seq.empty<int>)
                  |> closeCSharp)

              "collect",
              (fun () ->
                  fsharpInts ()
                  |> Source.collect (fun value -> seq { value; value })
                  |> closeFSharp),
              (fun () ->
                  csharpInts().SelectMany(fun value -> seq { value; value }) |> closeCSharp)

              "scan",
              (fun () -> fsharpInts () |> Source.scan 0 (fun state value -> state + value) |> closeFSharp),
              (fun () -> csharpInts().Scan(0, fun state value -> state + value) |> closeCSharp)

              "scanDurable",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanDurable 0 (fun state value -> state + value) exportInt restoreInt
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Scan(
                          0,
                          (fun state value -> state + value),
                          Func<int, Orleans.Dataflow.Serialization.CanonicalJsonValue> exportInt,
                          Func<Orleans.Dataflow.Serialization.CanonicalJsonValue, int> restoreInt)
                  |> closeCSharp)

              "take",
              (fun () -> fsharpInts () |> Source.take 2 |> closeFSharp),
              (fun () -> csharpInts().Take(2) |> closeCSharp)

              "skip",
              (fun () -> fsharpInts () |> Source.skip 2 |> closeFSharp),
              (fun () -> csharpInts().Skip(2) |> closeCSharp)

              "takeWhile",
              (fun () -> fsharpInts () |> Source.takeWhile (fun value -> value < 3) |> closeFSharp),
              (fun () -> csharpInts().TakeWhile(fun value -> value < 3) |> closeCSharp)

              "takeThrough",
              (fun () -> fsharpInts () |> Source.takeThrough (fun value -> value = 2) |> closeFSharp),
              (fun () -> csharpInts().TakeThrough(fun value -> value = 2) |> closeCSharp)

              "skipWhile",
              (fun () -> fsharpInts () |> Source.skipWhile (fun value -> value < 3) |> closeFSharp),
              (fun () -> csharpInts().SkipWhile(fun value -> value < 3) |> closeCSharp)

              "distinct",
              (fun () ->
                  fsharpInts ()
                  |> Source.distinct (Orleans.Dataflow.DistinctOptions(MaxTrackedKeys = 8))
                  |> closeFSharp),
              (fun () ->
                  csharpInts().Distinct(Orleans.Dataflow.DistinctOptions(MaxTrackedKeys = 8))
                  |> closeCSharp)

              "deduplicateConsecutive",
              (fun () -> fsharpInts () |> Source.deduplicateConsecutive |> closeFSharp),
              (fun () -> csharpInts().DeduplicateConsecutive() |> closeCSharp) ]

    [<Fact>]
    member _.``The effect-explicit mapping families are one document from either frontend``() =
        assertParity
            [ "mapTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapTask (parallelism 4) (fun value _ -> Task.FromResult(value + 1))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectAsync(
                          parallelism 4,
                          fun value (_: CancellationToken) -> Task.FromResult(value + 1))
                  |> closeCSharp)

              // The F# effect is invisible to the document: this asserts that mapAsync writes the very node
              // mapTask writes, which is the claim "how a callback is spelled is behavior" as a test.
              "mapAsync",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapAsync (parallelism 4) (fun value -> async { return value + 1 })
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectAsync(
                          parallelism 4,
                          fun value (_: CancellationToken) -> Task.FromResult(value + 1))
                  |> closeCSharp)

              "mapTaskUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapTaskUnordered (parallelism 2) (fun value _ -> Task.FromResult(value + 1))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectAsyncUnordered(
                          parallelism 2,
                          fun value (_: CancellationToken) -> Task.FromResult(value + 1))
                  |> closeCSharp)

              "mapAsyncUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapAsyncUnordered (parallelism 2) (fun value -> async { return value + 1 })
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectAsyncUnordered(
                          parallelism 2,
                          fun value (_: CancellationToken) -> Task.FromResult(value + 1))
                  |> closeCSharp)

              "mapValueTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapValueTask (parallelism 3) (fun value _ -> ValueTask<int>(value + 1))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectValueTaskAsync(
                          parallelism 3,
                          fun value (_: CancellationToken) -> ValueTask<int>(value + 1))
                  |> closeCSharp)

              "mapValueTaskUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapValueTaskUnordered (parallelism 3) (fun value _ -> ValueTask<int>(value + 1))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .SelectValueTaskAsyncUnordered(
                          parallelism 3,
                          fun value (_: CancellationToken) -> ValueTask<int>(value + 1))
                  |> closeCSharp)

              "scanTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanTask 0 (fun state value _ -> Task.FromResult(state + value))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .ScanAsync(0, fun state value (_: CancellationToken) -> Task.FromResult(state + value))
                  |> closeCSharp)

              "scanAsync",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanAsync 0 (fun state value -> async { return state + value })
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .ScanAsync(0, fun state value (_: CancellationToken) -> Task.FromResult(state + value))
                  |> closeCSharp)

              "mergeMap",
              (fun () ->
                  fsharpInts ()
                  |> Source.mergeMap (parallelism 2) (fun value -> seq { value })
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .MergeMap(parallelism 2, Func<int, IEnumerable<int>>(fun value -> seq { value }))
                  |> closeCSharp)

              "mergeMapAsyncEnumerable",
              (fun () ->
                  fsharpInts ()
                  |> Source.mergeMapAsyncEnumerable (parallelism 2) (fun value -> asyncEnumerableOf [ value ])
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .MergeMap(
                          parallelism 2,
                          Func<int, IAsyncEnumerable<int>>(fun value -> asyncEnumerableOf [ value ]))
                  |> closeCSharp) ]

    [<Fact>]
    member _.``The batching operators are one document from either frontend``() =
        assertParity
            [ "grouped",
              (fun () -> fsharpInts () |> Source.grouped 2 |> closeFSharp),
              (fun () -> csharpInts().Grouped(2) |> closeCSharp)

              "sliding",
              (fun () -> fsharpInts () |> Source.sliding 3 2 |> closeFSharp),
              (fun () -> csharpInts().Sliding(3, 2) |> closeCSharp)

              "groupedWithin",
              (fun () -> fsharpInts () |> Source.groupedWithin 4 second |> closeFSharp),
              (fun () -> csharpInts().GroupedWithin(4, second) |> closeCSharp)

              "groupedWeightedWithin",
              (fun () ->
                  fsharpInts ()
                  |> Source.groupedWeightedWithin 4 16 second (fun value -> value)
                  |> closeFSharp),
              (fun () -> csharpInts().GroupedWithin(4, 16, second, (fun value -> value)) |> closeCSharp)

              "groupBy",
              (fun () ->
                  fsharpInts ()
                  |> Source.groupBy
                      (Orleans.Dataflow.GroupByOptions(MaxActiveKeys = 4))
                      (fun value -> value % 2)
                      (Flow.map (fun value -> value * 10))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .GroupBy(
                          Orleans.Dataflow.GroupByOptions(MaxActiveKeys = 4),
                          (fun value -> value % 2),
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value * 10))
                  |> closeCSharp) ]

    [<Fact>]
    member _.``The boundary, timing and rate operators are one document from either frontend``() =
        assertParity
            [ "buffer",
              (fun () -> fsharpInts () |> Source.buffer (bounded 8) |> closeFSharp),
              (fun () -> csharpInts().Buffer(bounded 8) |> closeCSharp)

              "delay",
              (fun () -> fsharpInts () |> Source.delay second (bounded 4) |> closeFSharp),
              (fun () -> csharpInts().Delay(second, bounded 4) |> closeCSharp)

              "initialDelay",
              (fun () -> fsharpInts () |> Source.initialDelay second |> closeFSharp),
              (fun () -> csharpInts().InitialDelay(second) |> closeCSharp)

              "timeout",
              (fun () -> fsharpInts () |> Source.timeout second |> closeFSharp),
              (fun () -> csharpInts().Timeout(second) |> closeCSharp)

              "takeWithin",
              (fun () -> fsharpInts () |> Source.takeWithin second |> closeFSharp),
              (fun () -> csharpInts().TakeWithin(second) |> closeCSharp)

              "skipWithin",
              (fun () -> fsharpInts () |> Source.skipWithin second |> closeFSharp),
              (fun () -> csharpInts().SkipWithin(second) |> closeCSharp)

              // The burst is defaulted by the shared guard rather than by either frontend, so an omitted one
              // is written identically into both documents.
              "throttle",
              (fun () ->
                  fsharpInts ()
                  |> Source.throttle (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second))
                  |> closeFSharp),
              (fun () ->
                  csharpInts().Throttle(Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second))
                  |> closeCSharp)

              "throttleBy",
              (fun () ->
                  fsharpInts ()
                  |> Source.throttleBy
                      (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second, MaximumBurst = 9))
                      (fun value -> value)
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Throttle(
                          Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second, MaximumBurst = 9),
                          fun value -> value)
                  |> closeCSharp)

              "valve",
              (fun () ->
                  fsharpInts ()
                  |> Source.valve "gate" Orleans.Dataflow.ValveMode.Closed
                  |> closeFSharp),
              (fun () -> csharpInts().Valve("gate", Orleans.Dataflow.ValveMode.Closed) |> closeCSharp) ]

    [<Fact>]
    member _.``The scope operators are one document from either frontend``() =
        assertParity
            [ "supervised",
              (fun () ->
                  fsharpInts ()
                  |> Source.supervised
                      (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Resume))
                      (Flow.map (fun value -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Supervised(
                          Orleans.Dataflow.SupervisionOptions(
                              Form = Orleans.Dataflow.SupervisionForm.Resume),
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value * 2))
                  |> closeCSharp)

              "supervisedRecovering",
              (fun () ->
                  fsharpInts ()
                  |> Source.supervisedRecovering
                      (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Recover))
                      -1
                      (Flow.map (fun value -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  csharpInts()
                      .Supervised(
                          Orleans.Dataflow.SupervisionOptions(
                              Form = Orleans.Dataflow.SupervisionForm.Recover),
                          Orleans.Dataflow.Flow.For<int>().Select(fun value -> value * 2),
                          -1)
                  |> closeCSharp)

              "durable",
              (fun () ->
                  fsharpInts ()
                  |> Source.durable (Flow.map (fun value -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  csharpInts().Durable(Orleans.Dataflow.Flow.For<int>().Select(fun value -> value * 2))
                  |> closeCSharp) ]

    [<Fact>]
    member _.``The sinks are one document from either frontend``() =
        let channel () = Channel.CreateUnbounded<int>()

        assertParity
            [ "forEach",
              (fun () -> fsharpInts () |> Source.toSink (Sink.forEach ignore)),
              (fun () -> csharpInts().To(Orleans.Dataflow.Sink.ForEach<int>(fun _ -> ())))

              "forEachTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.toSink (Sink.forEachTask (parallelism 2) (fun _ _ -> Task.CompletedTask))),
              (fun () ->
                  csharpInts()
                      .To(
                          Orleans.Dataflow.Sink.ForEachAsync<int>(
                              parallelism 2,
                              fun _ (_: CancellationToken) -> Task.CompletedTask)))

              "forEachAsync",
              (fun () ->
                  fsharpInts ()
                  |> Source.toSink (Sink.forEachAsync (parallelism 2) (fun _ -> async { return () }))),
              (fun () ->
                  csharpInts()
                      .To(
                          Orleans.Dataflow.Sink.ForEachAsync<int>(
                              parallelism 2,
                              fun _ (_: CancellationToken) -> Task.CompletedTask)))

              "toChannel",
              (fun () -> fsharpInts () |> Source.toSink (Sink.toChannel (channel ()).Writer)),
              (fun () -> csharpInts().To(Orleans.Dataflow.Sink.ToChannel((channel ()).Writer)))

              "aggregateTask",
              (fun () ->
                  fsharpInts ()
                  |> resultFSharp (Sink.aggregateTask 0 (fun state value _ -> Task.FromResult(state + value)))),
              (fun () ->
                  csharpInts()
                  |> resultCSharp (
                      Orleans.Dataflow.Sink.AggregateAsync<int, int>(
                          0,
                          fun state value (_: CancellationToken) -> Task.FromResult(state + value))))

              "aggregateAsync",
              (fun () ->
                  fsharpInts ()
                  |> resultFSharp (Sink.aggregateAsync 0 (fun state value -> async { return state + value }))),
              (fun () ->
                  csharpInts()
                  |> resultCSharp (
                      Orleans.Dataflow.Sink.AggregateAsync<int, int>(
                          0,
                          fun state value (_: CancellationToken) -> Task.FromResult(state + value))))

              "first",
              (fun () -> fsharpInts () |> resultFSharp Sink.first<int>),
              (fun () -> csharpInts () |> resultCSharp (Orleans.Dataflow.Sink.First<int>()))

              // The two default-valued sinks are twinned over a reference element type, because C#'s
              // SinkWithResult<T, T?> on an unconstrained T is a nullable type F# refuses to form for a
              // struct: Sink.FirstOrDefault<int>() is not callable from F# at all under nullable reference
              // types. Element types never appear in a local document, so the parity claim is unchanged.
              "firstOrDefault",
              (fun () -> Source.ofSeq [ "a"; "b" ] |> resultFSharp Sink.firstOrDefault<string>),
              (fun () ->
                  Orleans.Dataflow.Source.From([ "a"; "b" ])
                  |> resultCSharp (Orleans.Dataflow.Sink.FirstOrDefault<string>()))

              "last",
              (fun () -> fsharpInts () |> resultFSharp Sink.last<int>),
              (fun () -> csharpInts () |> resultCSharp (Orleans.Dataflow.Sink.Last<int>()))

              "lastOrDefault",
              (fun () -> Source.ofSeq [ "a"; "b" ] |> resultFSharp Sink.lastOrDefault<string>),
              (fun () ->
                  Orleans.Dataflow.Source.From([ "a"; "b" ])
                  |> resultCSharp (Orleans.Dataflow.Sink.LastOrDefault<string>()))

              "count",
              (fun () -> fsharpInts () |> resultFSharp Sink.count<int>),
              (fun () -> csharpInts () |> resultCSharp (Orleans.Dataflow.Sink.Count<int>()))

              "collect",
              (fun () ->
                  fsharpInts ()
                  |> resultFSharp (Sink.collect (Orleans.Dataflow.CollectOptions(MaxElements = 16)))),
              (fun () ->
                  csharpInts ()
                  |> resultCSharp (
                      Orleans.Dataflow.Sink.Collect<int>(
                          Orleans.Dataflow.CollectOptions(MaxElements = 16)))) ]

    [<Fact>]
    member _.``The source constructors are one document from either frontend``() =
        let failure = InvalidOperationException "the source refuses to produce"

        assertParity
            [ "empty",
              (fun () -> Source.empty<int> |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Empty<int>() |> closeCSharp)

              "single",
              (fun () -> Source.single 7 |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Single(7) |> closeCSharp)

              "repeat",
              (fun () -> Source.repeat 3 7 |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Repeat(7, 3) |> closeCSharp)

              "range",
              (fun () -> Source.range 5 4 |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Range(5, 4) |> closeCSharp)

              "ofTask",
              (fun () -> Source.ofTask (Task.FromResult 7) |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.FromTask(Task.FromResult 7) |> closeCSharp)

              "ofFactory",
              (fun () -> Source.ofFactory (fun () -> 7) |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.FromFactory(fun () -> 7) |> closeCSharp)

              "ofTaskFactory",
              (fun () -> Source.ofTaskFactory (fun _ -> Task.FromResult 7) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.FromAsyncFactory(fun (_: CancellationToken) -> Task.FromResult 7)
                  |> closeCSharp)

              // An Async is a cold factory, so it writes the factory node rather than the hot-task one.
              "ofAsync",
              (fun () -> Source.ofAsync (async { return 7 }) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.FromAsyncFactory(fun (_: CancellationToken) -> Task.FromResult 7)
                  |> closeCSharp)

              // The element type of a failing source and of an ingress queue appears only in the answer, so
              // an annotation states it. An F# module function cannot be given explicit type arguments from
              // another assembly (FS0686) — only a generic value can, which is why Source.empty<int> above
              // reads as it does and these two do not.
              "failed",
              (fun () -> (Source.failed failure: Source<int>) |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Failed<int>(failure) |> closeCSharp)

              "never",
              (fun () -> Source.never<int> |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Never<int>() |> closeCSharp)

              "cycle",
              (fun () -> Source.cycle [ 1; 2 ] |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Cycle([ 1; 2 ]) |> closeCSharp)

              "unfold",
              (fun () ->
                  Source.unfold (fun state -> if state > 3 then ValueNone else ValueSome(state, state + 1)) 1
                  |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.Unfold(
                      1,
                      Orleans.Dataflow.UnfoldGenerator<int, int>(fun state value next ->
                          value <- state
                          next <- state + 1
                          state <= 3))
                  |> closeCSharp)

              "unfoldTask",
              (fun () ->
                  Source.unfoldTask (fun state _ -> Task.FromResult(Some(state, state + 1))) 1
                  |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.UnfoldAsync(
                      1,
                      Orleans.Dataflow.AsyncUnfoldGenerator<int, int>(fun state (_: CancellationToken) ->
                          Task.FromResult(Nullable(Orleans.Dataflow.UnfoldStep<int, int>(state, state + 1)))))
                  |> closeCSharp)

              "unfoldAsync",
              (fun () ->
                  Source.unfoldAsync (fun state -> async { return Some(state, state + 1) }) 1
                  |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.UnfoldAsync(
                      1,
                      Orleans.Dataflow.AsyncUnfoldGenerator<int, int>(fun state (_: CancellationToken) ->
                          Task.FromResult(Nullable(Orleans.Dataflow.UnfoldStep<int, int>(state, state + 1)))))
                  |> closeCSharp)

              "ofAsyncEnumerable",
              (fun () -> Source.ofAsyncEnumerable (asyncEnumerableOf [ 1; 2 ]) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.FromAsyncEnumerable(asyncEnumerableOf [ 1; 2 ]) |> closeCSharp)

              "ofChannel",
              (fun () -> Source.ofChannel (Channel.CreateUnbounded<int>().Reader) |> closeFSharp),
              (fun () ->
                  Orleans.Dataflow.Source.FromChannel(Channel.CreateUnbounded<int>().Reader) |> closeCSharp)

              "tick",
              (fun () -> Source.tick second second |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Tick(second, second) |> closeCSharp)

              "queue",
              (fun () -> (Source.queue (bounded 4) "ingress": Source<int>) |> closeFSharp),
              (fun () -> Orleans.Dataflow.Source.Queue<int>(bounded 4, "ingress") |> closeCSharp) ]

    [<Fact>]
    member _.``Every source shorthand is its flow spelling``() =
        // The shorthands are one-line delegations, so what could break is the delegation and not the
        // binding: this asserts each against Source.via over the flow of the same name, which the family
        // tables above have already asserted against C#. Everything type-changing is closed inside its own
        // thunk, so one homogeneous list covers the whole module.
        assertParity
            [ "map",
              (fun () -> fsharpInts () |> Source.map (fun value -> value + 1) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.map (fun value -> value + 1)) |> closeFSharp)

              "filter",
              (fun () -> fsharpInts () |> Source.filter (fun value -> value > 1) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.filter (fun value -> value > 1)) |> closeFSharp)

              "choose",
              (fun () -> fsharpInts () |> Source.choose (fun value -> ValueSome value) |> closeFSharp),
              (fun () ->
                  fsharpInts () |> Source.via (Flow.choose (fun value -> ValueSome value)) |> closeFSharp)

              "chooseOption",
              (fun () -> fsharpInts () |> Source.chooseOption (fun value -> Some value) |> closeFSharp),
              (fun () ->
                  fsharpInts () |> Source.via (Flow.chooseOption (fun value -> Some value)) |> closeFSharp)

              "collect",
              (fun () -> fsharpInts () |> Source.collect (fun value -> seq { value }) |> closeFSharp),
              (fun () ->
                  fsharpInts () |> Source.via (Flow.collect (fun value -> seq { value })) |> closeFSharp)

              "mergeMap",
              (fun () ->
                  fsharpInts () |> Source.mergeMap (parallelism 2) (fun value -> seq { value }) |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mergeMap (parallelism 2) (fun value -> seq { value }))
                  |> closeFSharp)

              "mergeMapAsyncEnumerable",
              (fun () ->
                  fsharpInts ()
                  |> Source.mergeMapAsyncEnumerable (parallelism 2) (fun value -> asyncEnumerableOf [ value ])
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.mergeMapAsyncEnumerable (parallelism 2) (fun value -> asyncEnumerableOf [ value ]))
                  |> closeFSharp)

              "mapTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapTask (parallelism 2) (fun value _ -> Task.FromResult value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mapTask (parallelism 2) (fun value _ -> Task.FromResult value))
                  |> closeFSharp)

              "mapTaskUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapTaskUnordered (parallelism 2) (fun value _ -> Task.FromResult value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mapTaskUnordered (parallelism 2) (fun value _ -> Task.FromResult value))
                  |> closeFSharp)

              "mapValueTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapValueTask (parallelism 2) (fun value _ -> ValueTask<int> value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mapValueTask (parallelism 2) (fun value _ -> ValueTask<int> value))
                  |> closeFSharp)

              "mapValueTaskUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapValueTaskUnordered (parallelism 2) (fun value _ -> ValueTask<int> value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.mapValueTaskUnordered (parallelism 2) (fun value _ -> ValueTask<int> value))
                  |> closeFSharp)

              "mapAsync",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapAsync (parallelism 2) (fun value -> async { return value })
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mapAsync (parallelism 2) (fun value -> async { return value }))
                  |> closeFSharp)

              "mapAsyncUnordered",
              (fun () ->
                  fsharpInts ()
                  |> Source.mapAsyncUnordered (parallelism 2) (fun value -> async { return value })
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.mapAsyncUnordered (parallelism 2) (fun value -> async { return value }))
                  |> closeFSharp)

              "scan",
              (fun () -> fsharpInts () |> Source.scan 0 (fun state value -> state + value) |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.scan 0 (fun state value -> state + value))
                  |> closeFSharp)

              "scanDurable",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanDurable 0 (fun state value -> state + value) exportInt restoreInt
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.scanDurable 0 (fun state value -> state + value) exportInt restoreInt)
                  |> closeFSharp)

              "scanTask",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanTask 0 (fun state value _ -> Task.FromResult(state + value))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.scanTask 0 (fun state value _ -> Task.FromResult(state + value)))
                  |> closeFSharp)

              "scanAsync",
              (fun () ->
                  fsharpInts ()
                  |> Source.scanAsync 0 (fun state value -> async { return state + value })
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.scanAsync 0 (fun state value -> async { return state + value }))
                  |> closeFSharp)

              "take",
              (fun () -> fsharpInts () |> Source.take 2 |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.take 2) |> closeFSharp)

              "skip",
              (fun () -> fsharpInts () |> Source.skip 2 |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.skip 2) |> closeFSharp)

              "takeWhile",
              (fun () -> fsharpInts () |> Source.takeWhile (fun value -> value < 3) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.takeWhile (fun value -> value < 3)) |> closeFSharp)

              "takeThrough",
              (fun () -> fsharpInts () |> Source.takeThrough (fun value -> value = 2) |> closeFSharp),
              (fun () ->
                  fsharpInts () |> Source.via (Flow.takeThrough (fun value -> value = 2)) |> closeFSharp)

              "skipWhile",
              (fun () -> fsharpInts () |> Source.skipWhile (fun value -> value < 3) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.skipWhile (fun value -> value < 3)) |> closeFSharp)

              "distinct",
              (fun () ->
                  fsharpInts ()
                  |> Source.distinct (Orleans.Dataflow.DistinctOptions(MaxTrackedKeys = 8))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.distinct (Orleans.Dataflow.DistinctOptions(MaxTrackedKeys = 8)))
                  |> closeFSharp)

              "deduplicateConsecutive",
              (fun () -> fsharpInts () |> Source.deduplicateConsecutive |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via Flow.deduplicateConsecutive |> closeFSharp)

              "grouped",
              (fun () -> fsharpInts () |> Source.grouped 2 |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.grouped 2) |> closeFSharp)

              "sliding",
              (fun () -> fsharpInts () |> Source.sliding 3 2 |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.sliding 3 2) |> closeFSharp)

              "groupedWithin",
              (fun () -> fsharpInts () |> Source.groupedWithin 4 second |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.groupedWithin 4 second) |> closeFSharp)

              "groupedWeightedWithin",
              (fun () ->
                  fsharpInts ()
                  |> Source.groupedWeightedWithin 4 16 second (fun value -> value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.groupedWeightedWithin 4 16 second (fun value -> value))
                  |> closeFSharp)

              "groupBy",
              (fun () ->
                  fsharpInts ()
                  |> Source.groupBy
                      (Orleans.Dataflow.GroupByOptions(MaxActiveKeys = 4))
                      (fun value -> value % 2)
                      (Flow.map (fun value -> value * 10))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.groupBy
                          (Orleans.Dataflow.GroupByOptions(MaxActiveKeys = 4))
                          (fun value -> value % 2)
                          (Flow.map (fun value -> value * 10)))
                  |> closeFSharp)

              "buffer",
              (fun () -> fsharpInts () |> Source.buffer (bounded 8) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.buffer (bounded 8)) |> closeFSharp)

              "delay",
              (fun () -> fsharpInts () |> Source.delay second (bounded 4) |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.delay second (bounded 4)) |> closeFSharp)

              "initialDelay",
              (fun () -> fsharpInts () |> Source.initialDelay second |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.initialDelay second) |> closeFSharp)

              "timeout",
              (fun () -> fsharpInts () |> Source.timeout second |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.timeout second) |> closeFSharp)

              "takeWithin",
              (fun () -> fsharpInts () |> Source.takeWithin second |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.takeWithin second) |> closeFSharp)

              "skipWithin",
              (fun () -> fsharpInts () |> Source.skipWithin second |> closeFSharp),
              (fun () -> fsharpInts () |> Source.via (Flow.skipWithin second) |> closeFSharp)

              "throttle",
              (fun () ->
                  fsharpInts ()
                  |> Source.throttle (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.throttle (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second)))
                  |> closeFSharp)

              "throttleBy",
              (fun () ->
                  fsharpInts ()
                  |> Source.throttleBy
                      (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second))
                      (fun value -> value)
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.throttleBy
                          (Orleans.Dataflow.ThrottleOptions(Elements = 5, Per = second))
                          (fun value -> value))
                  |> closeFSharp)

              "valve",
              (fun () -> fsharpInts () |> Source.valve "gate" Orleans.Dataflow.ValveMode.Open |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.valve "gate" Orleans.Dataflow.ValveMode.Open)
                  |> closeFSharp)

              "supervised",
              (fun () ->
                  fsharpInts ()
                  |> Source.supervised
                      (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Resume))
                      (Flow.map (fun value -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.supervised
                          (Orleans.Dataflow.SupervisionOptions(
                              Form = Orleans.Dataflow.SupervisionForm.Resume))
                          (Flow.map (fun value -> value * 2)))
                  |> closeFSharp)

              "supervisedRecovering",
              (fun () ->
                  fsharpInts ()
                  |> Source.supervisedRecovering
                      (Orleans.Dataflow.SupervisionOptions(Form = Orleans.Dataflow.SupervisionForm.Recover))
                      -1
                      (Flow.map (fun value -> value * 2))
                  |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (
                      Flow.supervisedRecovering
                          (Orleans.Dataflow.SupervisionOptions(
                              Form = Orleans.Dataflow.SupervisionForm.Recover))
                          -1
                          (Flow.map (fun value -> value * 2)))
                  |> closeFSharp)

              "durable",
              (fun () -> fsharpInts () |> Source.durable (Flow.map (fun value -> value * 2)) |> closeFSharp),
              (fun () ->
                  fsharpInts ()
                  |> Source.via (Flow.durable (Flow.map (fun value -> value * 2)))
                  |> closeFSharp) ]
