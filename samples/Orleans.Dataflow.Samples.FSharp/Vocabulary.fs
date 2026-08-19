namespace Orleans.Dataflow.Samples

open System.Globalization
open System.Text.Json
open Orleans.Dataflow.Definition
open Orleans.Dataflow.Identity
open Orleans.Dataflow.Serialization

/// <summary>The three registered stages the cluster scenario deploys, and the payloads they carry.</summary>
/// <remarks>
/// <para>
/// <b>Why the cluster scenario cannot use the lambda vocabulary.</b> Everything the first six scenarios
/// author is built out of delegates, and a delegate is not something a document can carry: those graphs
/// declare themselves nondeployable and a cluster refuses them by name. A pipeline is written in stages a
/// catalog resolves — a stage reference, a version, and a canonical payload — so that the silo that runs it
/// needs nothing from the process that authored it. This is that catalog, and it is the smallest one that
/// still has a source, a transformation, and a terminal with a result.
/// </para>
/// <para>
/// <b>What is here and what is not.</b> This is the definition half: which stages exist, what they carry
/// between them, and what their payloads mean. The runtime half — what each stage actually does when a silo
/// builds it — is the console application's <c>SampleStageFactory</c>, on the other side of the same seam. A
/// deployment ships both; an author writing a pipeline needs only this.
/// </para>
/// <para>
/// It lives beside the F# authorings because both frontends must name the same stages for their documents
/// to be comparable, and the console application references this library rather than the other way round.
/// A catalog is a published artifact rather than a language artifact, so there is nothing F# about it
/// beyond the file it is typed into.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module SampleVocabulary =

    /// <summary>The provider every stage here belongs to.</summary>
    let Provider = ProviderId.Create "samples"

    /// <summary>The source that emits a run of the sample order feed.</summary>
    let FeedStage = StageRef.Create(Provider, StageId.Create "order-feed", StageRef.FirstMajorVersion)

    /// <summary>The flow that settles an order into a document, applying a declared discount.</summary>
    let DiscountStage = StageRef.Create(Provider, StageId.Create "discount", StageRef.FirstMajorVersion)

    /// <summary>The terminal that counts documents worth at least a declared amount.</summary>
    let TallyStage = StageRef.Create(Provider, StageId.Create "tally", StageRef.FirstMajorVersion)

    /// <summary>The contract of the order events the feed emits.</summary>
    /// <remarks>
    /// A contract identifier and a major version, and deliberately not a CLR type name: what makes two
    /// stages connectable is that they agree on this, which is a fact a document can state and a silo in
    /// another process can check. The .NET type behind it is this sample's own business.
    /// </remarks>
    let OrderEventContract = Orleans.Dataflow.ElementContract.For<OrderEvent>("samples-order-event", 1)

    /// <summary>The contract of the accepted documents.</summary>
    let OrderDocumentContract = Orleans.Dataflow.ElementContract.For<OrderDocument>("samples-order-document", 1)

    /// <summary>The contract of the tally the terminal answers with.</summary>
    let TallyContract = Orleans.Dataflow.ResultContract.For<int64>("samples-tally", 1)

    /// <summary>The contract of the feed's payload.</summary>
    let FeedParameterContract = ContractReference.Create(ContractId.Create "samples-order-feed-parameters", 1)

    /// <summary>The contract of the discounting flow's payload.</summary>
    let DiscountParameterContract = ContractReference.Create(ContractId.Create "samples-discount-parameters", 1)

    /// <summary>The contract of the tallying terminal's payload.</summary>
    let TallyParameterContract = ContractReference.Create(ContractId.Create "samples-tally-parameters", 1)

    /// <summary>The payload member holding how many orders the feed emits.</summary>
    let CountMember = "count"

    /// <summary>The payload member holding the percentage the discounting flow takes off.</summary>
    let PercentMember = "percent"

    /// <summary>The payload member naming what the terminal is counting.</summary>
    /// <remarks>
    /// Canonical form sorts an object's members, and <c>label</c> already precedes <c>minimum-amount</c>, so
    /// what the writer below spells is what the document stores.
    /// </remarks>
    let LabelMember = "label"

    /// <summary>The payload member holding the smallest amount the terminal counts.</summary>
    let MinimumAmountMember = "minimum-amount"

    /// <summary>Builds the catalog a silo registers to run this vocabulary.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// A fresh catalog per call rather than one shared value, so that a deployment registering two silos
    /// registers two catalogs and nothing is quietly shared between them.
    /// </remarks>
    let Catalog () : StageCatalog =
        StageCatalog.Create
            [
                StageSpecification.Create(
                    FeedStage,
                    [],
                    [ OutputPortSpecification.Create(PortId.Create "out", OrderEventContract.Reference) ],
                    [],
                    FeedParameterContract,
                    []
                )
                StageSpecification.Create(
                    DiscountStage,
                    [ InputPortSpecification.Create(PortId.Create "in", OrderEventContract.Reference) ],
                    [ OutputPortSpecification.Create(PortId.Create "out", OrderDocumentContract.Reference) ],
                    [],
                    DiscountParameterContract,
                    []
                )
                StageSpecification.Create(
                    TallyStage,
                    [ InputPortSpecification.Create(PortId.Create "in", OrderDocumentContract.Reference) ],
                    [],
                    [ ResultPortSpecification.Create(PortId.Create "total", TallyContract.Reference) ],
                    TallyParameterContract,
                    []
                )
            ]

    /// <summary>The catalog the typed handles below are resolved against.</summary>
    /// <remarks>
    /// Authoring reads a catalog to check that the stage it names exists with the ports it expects, which is
    /// what turns a typo into a diagnostic at authoring time rather than a refusal at deployment time.
    /// </remarks>
    let private authoring: IStageCatalog = Catalog()

    /// <summary>The typed handle of the order feed.</summary>
    let Feed: Orleans.Dataflow.RegisteredSource<OrderEvent> =
        Orleans.Dataflow.RegisteredStage.Source(authoring, FeedStage, OrderEventContract)

    /// <summary>The typed handle of the discounting flow.</summary>
    let Discount: Orleans.Dataflow.RegisteredFlow<OrderEvent, OrderDocument> =
        Orleans.Dataflow.RegisteredStage.Flow(authoring, DiscountStage, OrderEventContract, OrderDocumentContract)

    /// <summary>The typed handle of the tallying terminal.</summary>
    let Tally: Orleans.Dataflow.RegisteredSinkWithResult<OrderDocument, int64> =
        Orleans.Dataflow.RegisteredStage.SinkWithResult(authoring, TallyStage, OrderDocumentContract, TallyContract)

    /// <summary>Writes the feed's payload.</summary>
    /// <param name="count">How many orders to emit.</param>
    /// <returns>The canonical payload.</returns>
    let FeedParameters (count: int) : CanonicalJsonValue =
        CanonicalJsonValue.Parse(
            System.String.Format(CultureInfo.InvariantCulture, "{{\"{0}\":{1}}}", CountMember, count)
        )

    /// <summary>Writes the discounting flow's payload.</summary>
    /// <param name="percent">The percentage taken off every order's amount.</param>
    /// <returns>The canonical payload.</returns>
    let DiscountParameters (percent: int) : CanonicalJsonValue =
        CanonicalJsonValue.Parse(
            System.String.Format(CultureInfo.InvariantCulture, "{{\"{0}\":{1}}}", PercentMember, percent)
        )

    /// <summary>Writes the tallying terminal's payload.</summary>
    /// <param name="label">What the terminal is counting, for whoever reads the document.</param>
    /// <param name="minimumAmount">The smallest amount a document has to be worth to be counted.</param>
    /// <returns>The canonical payload.</returns>
    let TallyParameters (label: string) (minimumAmount: int) : CanonicalJsonValue =
        CanonicalJsonValue.Parse(
            System.String.Format(
                CultureInfo.InvariantCulture,
                "{{\"{0}\":{1},\"{2}\":{3}}}",
                LabelMember,
                JsonSerializer.Serialize label,
                MinimumAmountMember,
                minimumAmount
            )
        )

    /// <summary>Opens a payload as the object every stage here declares.</summary>
    /// <param name="stage">What is being read, for the diagnostic.</param>
    /// <param name="parameters">The payload.</param>
    /// <returns>The object.</returns>
    /// <exception cref="T:System.InvalidOperationException">The payload is not an object.</exception>
    let private payloadOf (stage: string) (parameters: CanonicalJsonValue) : JsonElement =
        if parameters.IsDefault || parameters.ToElement().ValueKind <> JsonValueKind.Object then
            invalidOp
                $"The {stage} stage carries the payload {parameters}, and every stage of this provider declares a JSON object."

        parameters.ToElement()

    /// <summary>Reads how many orders the feed was asked for.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>The count.</returns>
    /// <exception cref="T:System.InvalidOperationException">The payload is not one this provider wrote.</exception>
    let ReadFeedCount (parameters: CanonicalJsonValue) : int =
        match (payloadOf "order feed" parameters).TryGetProperty CountMember with
        | true, counted ->
            match counted.TryGetInt32() with
            | true, count when count >= 0 -> count
            | _ -> invalidOp $"The order feed's '{CountMember}' is not a count of zero or more: {parameters}."
        | false, _ -> invalidOp $"The order feed's payload has no '{CountMember}': {parameters}."

    /// <summary>Reads the percentage the discounting flow takes off.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>The percentage.</returns>
    /// <exception cref="T:System.InvalidOperationException">The payload is not one this provider wrote.</exception>
    let ReadDiscountPercent (parameters: CanonicalJsonValue) : decimal =
        match (payloadOf "discount" parameters).TryGetProperty PercentMember with
        | true, percent ->
            match percent.TryGetDecimal() with
            | true, taken when taken >= 0m && taken <= 100m -> taken
            | _ -> invalidOp $"The discounting flow's '{PercentMember}' is not a percentage: {parameters}."
        | false, _ -> invalidOp $"The discounting flow's payload has no '{PercentMember}': {parameters}."

    /// <summary>Reads the smallest amount the tallying terminal counts.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>The amount.</returns>
    /// <exception cref="T:System.InvalidOperationException">The payload is not one this provider wrote.</exception>
    let ReadTallyMinimum (parameters: CanonicalJsonValue) : decimal =
        match (payloadOf "tally" parameters).TryGetProperty MinimumAmountMember with
        | true, minimum ->
            match minimum.TryGetDecimal() with
            | true, amount -> amount
            | false, _ -> invalidOp $"The tallying terminal's '{MinimumAmountMember}' is not a number: {parameters}."
        | false, _ -> invalidOp $"The tallying terminal's payload has no '{MinimumAmountMember}': {parameters}."

    /// <summary>Reads what the tallying terminal is counting.</summary>
    /// <param name="parameters">The node's payload.</param>
    /// <returns>The label.</returns>
    /// <exception cref="T:System.InvalidOperationException">The payload is not one this provider wrote.</exception>
    let ReadTallyLabel (parameters: CanonicalJsonValue) : string =
        match (payloadOf "tally" parameters).TryGetProperty LabelMember with
        | true, label when label.ValueKind = JsonValueKind.String -> nonNull (label.GetString())
        | _ -> invalidOp $"The tallying terminal's payload has no string '{LabelMember}': {parameters}."
