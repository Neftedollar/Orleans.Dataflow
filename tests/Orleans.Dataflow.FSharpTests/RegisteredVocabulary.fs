namespace Orleans.Dataflow.FSharpTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Text.Json
open System.Threading.Channels
open Orleans.Dataflow.Definition
open Orleans.Dataflow.Hosting
open Orleans.Dataflow.Identity
open Orleans.Dataflow.Serialization

/// <summary>Reads the payload of one stage against the members that stage declares.</summary>
/// <remarks>
/// <para>
/// One reader for the whole vocabulary, configured by the members a stage declares, because every stage here
/// declares a flat object of required members and nothing else. Its rules are the ones the shipped
/// conformance kit checks: the sample payload is accepted, a member the stage does not declare is refused, a
/// declared member that is missing is refused, and a declared member of the wrong kind is refused — each of
/// them naming the member in single quotes, because a diagnostic that says only that something is wrong makes
/// a deployment guess.
/// </para>
/// <para>
/// A reader never throws. An invalid payload is the expected outcome of validating a document nobody in this
/// process wrote, so it is answered with a report rather than with an exception.
/// </para>
/// </remarks>
type internal PayloadReader(declared: (string * JsonValueKind) list) =

    interface IStageParameterValidator with
        member _.Validate(parameters: CanonicalJsonValue) : IReadOnlyList<string> =
            if parameters.IsDefault || parameters.ToElement().ValueKind <> JsonValueKind.Object then
                [| "the payload is not a JSON object" |] :> IReadOnlyList<string>
            else
                let payload = parameters.ToElement()
                let violations = ResizeArray<string>()

                for name, kind in declared do
                    match payload.TryGetProperty name with
                    | true, value when value.ValueKind <> kind ->
                        violations.Add $"the member '{name}' is of the wrong kind, and it is {value.ValueKind}"
                    | true, _ -> ()
                    | false, _ -> violations.Add $"the member '{name}' is missing"

                for entry in payload.EnumerateObject() do
                    if not (declared |> List.exists (fun (name, _) -> name = entry.Name)) then
                        violations.Add $"the member '{entry.Name}' is not one this stage declares"

                violations :> IReadOnlyList<string>

/// <summary>One built provider: the factory a host registers, and what a run of it did.</summary>
/// <remarks>
/// A record rather than a class with properties, because the two observations are values a test reads and
/// nothing here has a lifecycle. Both are concurrent collections: a factory is invoked once per node per
/// materialization and a run's segments are real threads, so a plain list would be a data race rather than a
/// simplification.
/// </remarks>
type internal FSharpProvider =
    { /// <summary>The factory a host registers for this provider.</summary>
      Factory: IDataflowStageFactory

      /// <summary>Every element the resultless terminal has seen, in the order it saw them.</summary>
      Observed: ConcurrentQueue<int>

      /// <summary>The name of every node this factory was asked to build.</summary>
      Built: ConcurrentDictionary<string, int> }

/// <summary>
/// A catalog of registered stages, the factory that runs them, and the typed handles the F# registered
/// spellings are written against.
/// </summary>
/// <remarks>
/// <para>
/// This is the repository's first provider written in F#, and it exists because the F# tests cannot see the
/// C# suite's own fixtures: an assembly's internals are its friends', and the two test projects are not
/// friends. Writing one here rather than reaching for one is the better accident — it is the same exercise a
/// provider author does, through the same public SDK
/// (<see cref="T:Orleans.Dataflow.Definition.StageCatalog"/>,
/// <see cref="T:Orleans.Dataflow.Definition.IStageParameterValidator"/>, and
/// <see cref="T:Orleans.Dataflow.Hosting.IDataflowStageFactory"/>), and it is checked by the shipped
/// conformance kit rather than only by the tests that happen to use it.
/// </para>
/// <para>
/// The port names deliberately disagree with the local vocabulary's <c>in</c>, <c>out</c>, and <c>result</c>
/// wherever a test could see the difference — the source produces on <c>numbers</c>, the terminals consume on
/// <c>elements</c>, and the counting one yields on <c>total</c> — so a frontend that hard-coded the local
/// names would close a document naming ports no stage declares and the graph compiler would say so. The
/// junction ports are named so that the specification's canonical order, which is ordinal over the port text,
/// is the order an author writes the legs in.
/// </para>
/// </remarks>
module internal RegisteredVocabulary =

    /// <summary>The provider every stage of this vocabulary belongs to.</summary>
    let provider = ProviderId.Create "fsharp-test"

    /// <summary>Builds a reference to one stage of this vocabulary, at major version 1.</summary>
    let stage (name: string) : StageRef =
        StageRef.Create(provider, StageId.Create name, StageRef.FirstMajorVersion)

    /// <summary>Builds a reference to a stage no catalog here registers.</summary>
    let unknownStage () : StageRef = stage "no-such-stage"

    /// <summary>Builds a contract reference at major version 1.</summary>
    let private contract (id: string) = ContractReference.Create(ContractId.Create id, 1)

    /// <summary>Builds one input port.</summary>
    let private input (port: string) (element: string) =
        InputPortSpecification.Create(PortId.Create port, contract element)

    /// <summary>Builds one output port.</summary>
    let private output (port: string) (element: string) =
        OutputPortSpecification.Create(PortId.Create port, contract element)

    /// <summary>Builds one result port.</summary>
    let private answers (port: string) (element: string) =
        ResultPortSpecification.Create(PortId.Create port, contract element)

    /// <summary>Declares one stage: its ports, its parameter contract, and the reader of its members.</summary>
    let private declare
        (name: string)
        (inputs: InputPortSpecification list)
        (outputs: OutputPortSpecification list)
        (results: ResultPortSpecification list)
        (members: (string * JsonValueKind) list)
        =
        StageSpecification.Create(
            stage name,
            inputs,
            outputs,
            results,
            contract $"{name}-parameters",
            [],
            PayloadReader members)

    /// <summary>The payload every <c>number-source</c> occurrence in these tests carries.</summary>
    let sourceParameters = CanonicalJsonValue.Parse """{"count":4}"""

    /// <summary>The payload every <c>scale</c> occurrence in these tests carries.</summary>
    let scaleParameters = CanonicalJsonValue.Parse """{"factor":3}"""

    /// <summary>The payload every <c>label-sink</c> occurrence in these tests carries.</summary>
    let labelParameters = CanonicalJsonValue.Parse """{"tag":"seen"}"""

    /// <summary>The payload every <c>sum-sink</c> occurrence in these tests carries.</summary>
    let sumParameters = CanonicalJsonValue.Parse """{"label":"total"}"""

    /// <summary>The payload every junction occurrence in these tests carries.</summary>
    let junctionParameters = CanonicalJsonValue.Parse """{"mode":"declared"}"""

    /// <summary>A payload the <c>number-source</c> reader refuses, because it declares no such member.</summary>
    let strangeParameters = CanonicalJsonValue.Parse """{"topic":"orders"}"""

    /// <summary>The catalog a deployment would register these stages in.</summary>
    let catalog: IStageCatalog =
        StageCatalog.Create(
            [ declare "number-source" [] [ output "numbers" "number" ] [] [ "count", JsonValueKind.Number ]
              declare
                  "scale"
                  [ input "elements" "number" ]
                  [ output "scaled" "number" ]
                  []
                  [ "factor", JsonValueKind.Number ]
              declare "label-sink" [ input "elements" "number" ] [] [] [ "tag", JsonValueKind.String ]
              declare
                  "sum-sink"
                  [ input "elements" "number" ]
                  []
                  [ answers "total" "number-total" ]
                  [ "label", JsonValueKind.String ]
              declare
                  "split"
                  [ input "elements" "number" ]
                  [ output "leg-a" "number"; output "leg-b" "number" ]
                  []
                  [ "mode", JsonValueKind.String ]
              declare
                  "join"
                  [ input "part-a" "number"; input "part-b" "number" ]
                  [ output "joined" "number" ]
                  []
                  [ "mode", JsonValueKind.String ]
              declare
                  "unzip"
                  [ input "rows" "row" ]
                  [ output "leg-a" "number"; output "leg-b" "label" ]
                  []
                  [ "mode", JsonValueKind.String ]
              declare
                  "attach"
                  [ input "part-a" "number"; input "part-b" "label" ]
                  [ output "rows" "row" ]
                  []
                  [ "mode", JsonValueKind.String ] ])

    /// <summary>The declaration that <c>number@v1</c> is carried by a 32-bit integer in this process.</summary>
    let numberContract = Orleans.Dataflow.ElementContract.For<int>("number", 1)

    /// <summary>The declaration that <c>label@v1</c> is carried by a string in this process.</summary>
    let labelContract = Orleans.Dataflow.ElementContract.For<string>("label", 1)

    /// <summary>The declaration that <c>row@v1</c> is carried by a struct pair in this process.</summary>
    let rowContract = Orleans.Dataflow.ElementContract.For<struct (int * string)>("row", 1)

    /// <summary>The declaration that <c>number-total@v1</c> is carried by a 64-bit integer here.</summary>
    let totalContract = Orleans.Dataflow.ResultContract.For<int64>("number-total", 1)

    /// <summary>The handle of the registered source.</summary>
    let numberSource =
        Orleans.Dataflow.RegisteredStage.Source(catalog, stage "number-source", numberContract)

    /// <summary>The handle of the registered flow.</summary>
    let scale =
        Orleans.Dataflow.RegisteredStage.Flow(catalog, stage "scale", numberContract, numberContract)

    /// <summary>The handle of the registered terminal that declares no result.</summary>
    let labelSink = Orleans.Dataflow.RegisteredStage.Sink(catalog, stage "label-sink", numberContract)

    /// <summary>The handle of the registered terminal that declares one.</summary>
    let sumSink =
        Orleans.Dataflow.RegisteredStage.SinkWithResult(catalog, stage "sum-sink", numberContract, totalContract)

    /// <summary>The handle of the registered junction whose two legs carry one contract.</summary>
    let split =
        Orleans.Dataflow.RegisteredStage.FanOut(catalog, stage "split", numberContract, numberContract)

    /// <summary>The handle of the registered junction that joins two like streams.</summary>
    let join = Orleans.Dataflow.RegisteredStage.FanIn(catalog, stage "join", numberContract, numberContract)

    /// <summary>The handle of the registered junction whose two legs carry unlike contracts.</summary>
    let unzip =
        Orleans.Dataflow.RegisteredStage.FanOut(catalog, stage "unzip", rowContract, numberContract, labelContract)

    /// <summary>The handle of the registered junction that joins two unlike streams.</summary>
    let attach =
        Orleans.Dataflow.RegisteredStage.FanIn(catalog, stage "attach", numberContract, labelContract, rowContract)

    /// <summary>One sample payload per stage, which is what the conformance kit reads them all through.</summary>
    let samples =
        [ Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "number-source", sourceParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "scale", scaleParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "label-sink", labelParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "sum-sink", sumParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "split", junctionParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "join", junctionParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "unzip", junctionParameters)
          Orleans.Dataflow.Testing.ProviderStageSample.Create(stage "attach", junctionParameters) ]

    /// <summary>Opens the sequence one run of the number source enumerates.</summary>
    /// <remarks>
    /// A completed unbounded channel is the shortest honest asynchronous sequence this repository builds
    /// without a package that supplies one, and a fresh one per call is what makes the source enumerable once
    /// per run rather than once per graph.
    /// </remarks>
    let private numbers (count: int) : IAsyncEnumerable<objnull> =
        let channel = Channel.CreateUnbounded<objnull>()

        for value in 1..count do
            channel.Writer.TryWrite(box value) |> ignore

        channel.Writer.Complete()

        channel.Reader.ReadAllAsync()

    /// <summary>Builds the runtime half of this provider: a fresh factory and what it will record.</summary>
    /// <remarks>
    /// <para>
    /// An object expression rather than a named class, which is the F# spelling of an interface with one
    /// member and no lifecycle. It is the public
    /// <see cref="T:Orleans.Dataflow.Hosting.IDataflowStageFactory"/> — the very interface a silo registers —
    /// so this same value would run these stages in either host, and the local half of that claim is what the
    /// run tests assert.
    /// </para>
    /// <para>
    /// Everything is read from the request: how many numbers the source emits and what the scale multiplies by
    /// come from the occurrence's own payload, so two occurrences of one stage under two payloads behave
    /// differently and the two documents fingerprint differently. Nothing is read from anywhere else, because
    /// a factory receives no document, no sibling node, and no run identity.
    /// </para>
    /// <para>
    /// The dispatch is on the whole stage reference rather than on the identifier alone: a major version is
    /// compatibility identity, so a factory matching on the identifier would build version one's behavior for
    /// a document written against version two. A reference of this provider that this build does not implement
    /// is refused by throwing, which the conformance kit requires and checks.
    /// </para>
    /// </remarks>
    let newProvider () : FSharpProvider =
        let observed = ConcurrentQueue<int>()
        let built = ConcurrentDictionary<string, int>()

        let factory =
            { new IDataflowStageFactory with
                member _.Create(request: DataflowStageRequest) : DataflowStageRuntime =
                    let payload = request.Node.Parameters.ToElement()
                    let reference = request.Node.Stage
                    built[request.Node.Id.Value] <- 1

                    if reference = stage "number-source" then
                        let count = payload.GetProperty("count").GetInt32()

                        DataflowStageRuntime.Source(fun _ -> numbers count)
                    elif reference = stage "scale" then
                        let factor = payload.GetProperty("factor").GetInt32()

                        DataflowStageRuntime.Element(fun element -> box (unbox<int> element * factor))
                    elif reference = stage "label-sink" then
                        DataflowStageRuntime.Terminal(
                            (fun () -> box 0),
                            (fun state element ->
                                observed.Enqueue(unbox<int> element)
                                state),
                            null,
                            producesResult = false)
                    elif reference = stage "sum-sink" then
                        DataflowStageRuntime.Terminal(
                            (fun () -> box 0L),
                            (fun state element -> box (unbox<int64> state + int64 (unbox<int> element))),
                            null,
                            producesResult = true)
                    elif reference = stage "split" then
                        DataflowStageRuntime.Broadcast()
                    elif reference = stage "join" then
                        DataflowStageRuntime.Merge()
                    elif reference = stage "unzip" then
                        DataflowStageRuntime.Unzip(
                            [| Func<objnull, objnull>(fun row ->
                                   let struct (number, _) = unbox<struct (int * string)> row
                                   box number)
                               Func<objnull, objnull>(fun row ->
                                   let struct (_, label) = unbox<struct (int * string)> row
                                   box label) |])
                    elif reference = stage "attach" then
                        DataflowStageRuntime.Zip(fun parts ->
                            box (struct (unbox<int> parts[0], unbox<string> parts[1])))
                    else
                        raise (
                            NotSupportedException
                                $"The F# test provider does not implement the stage '{reference}'.") }

        { Factory = factory
          Observed = observed
          Built = built }

    /// <summary>Builds a host that knows this vocabulary, exactly as a deployment would declare one.</summary>
    /// <remarks>
    /// <c>AddCatalog</c> and <c>AddFactory</c> on the in-process builder, mirroring member for member what a
    /// silo writes — the one seam, two hosts fact, exercised from F#. The provider is answered as well as the
    /// host, because what a run did to the outside world is recorded on the provider and not on the host.
    /// </remarks>
    let hostWithProvider () =
        let provided = newProvider ()

        let host =
            Orleans.Dataflow.LocalDataflowHost(fun builder ->
                builder.AddCatalog(catalog).AddFactory(provider, provided.Factory) |> ignore)

        host, provided
