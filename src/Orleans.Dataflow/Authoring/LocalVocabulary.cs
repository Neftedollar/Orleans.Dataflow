using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Every definition-plane identity the local, lambda-implemented authoring vocabulary writes into a graph
/// document, in one place.
/// </summary>
/// <remarks>
/// <para>
/// The definition plane forbids CLR type names as contract identity, and a local graph is typed by C#
/// generics rather than by registered contracts. The local vocabulary is therefore deliberately blind: every
/// local port declares the single element contract <see cref="ElementContract"/>, which carries no
/// element-type information at all. Document-level contract checking is what registered stages are for; a
/// local document says only what it can honestly say, and the C# compiler is what actually rejects a
/// <c>Sink&lt;string&gt;</c> under a <c>Source&lt;int&gt;</c>.
/// </para>
/// <para>
/// Delegates, captured state, and the fold seed never appear here, because they never appear in a document
/// at all (AGENTS.md). They live in the authoring-side binding table that
/// <see cref="Orleans.Dataflow.RunnableGraph"/> carries for the future local runtime.
/// </para>
/// <para>
/// The fields are initialized in textual order, and every field that composes another one is declared after
/// it.
/// </para>
/// </remarks>
internal static class LocalVocabulary
{
    /// <summary>The prefix of every automatically allocated node identifier.</summary>
    /// <remarks>
    /// ADR 0004 fixes the spelling: an unnamed occurrence is <c>stage-0001</c>, <c>stage-0002</c>, and so
    /// on in authoring order. Positional identifiers are not edit-stable, which is why a document that
    /// contains one declares <see cref="EphemeralIdentity"/>.
    /// </remarks>
    internal const string AutoNamePrefix = "stage-";

    /// <summary>The highest position an automatically allocated node identifier can name.</summary>
    /// <remarks>
    /// The invariant this bound buys is that a document's canonical node order — ordinal over identifier
    /// text — is the authoring order of the occurrences it was built from, for every graph whose
    /// occurrences are automatically named. Four digits sort correctly against each other and five do
    /// not, so the numbering has to end somewhere; it ends here rather than silently becoming
    /// <c>stage-10000</c>, which would sort between <c>stage-0001</c> and <c>stage-0002</c> and quietly
    /// break the invariant for the one graph large enough to reach it.
    /// </remarks>
    internal const int MaxAutoNamedPosition = 9999;

    /// <summary>The numeric format that pads a position to the four digits <see cref="MaxAutoNamedPosition"/> allows.</summary>
    private const string AutoNameNumberFormat = "D4";

    /// <summary>The provider every local stage belongs to.</summary>
    internal static readonly ProviderId Provider = ProviderId.Create("local");

    /// <summary>The stage reference of a source over an in-memory sequence.</summary>
    internal static readonly StageRef FromEnumerable =
        StageRef.Create(Provider, StageId.Create("from-enumerable"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a mapping stage.</summary>
    internal static readonly StageRef Select =
        StageRef.Create(Provider, StageId.Create("select"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a filtering stage.</summary>
    internal static readonly StageRef Where =
        StageRef.Create(Provider, StageId.Create("where"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a bounded buffer.</summary>
    internal static readonly StageRef Buffer =
        StageRef.Create(Provider, StageId.Create("buffer"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of an order-preserving asynchronous mapping stage.</summary>
    internal static readonly StageRef SelectAsync =
        StageRef.Create(Provider, StageId.Create("select-async"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of an asynchronous mapping stage that emits in completion order.</summary>
    internal static readonly StageRef SelectAsyncUnordered =
        StageRef.Create(Provider, StageId.Create("select-async-unordered"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a folding sink.</summary>
    internal static readonly StageRef Fold =
        StageRef.Create(Provider, StageId.Create("fold"), StageRef.FirstMajorVersion);

    /// <summary>The stage reference of a discarding sink.</summary>
    internal static readonly StageRef Ignore =
        StageRef.Create(Provider, StageId.Create("ignore"), StageRef.FirstMajorVersion);

    /// <summary>The one element contract every local port declares.</summary>
    /// <remarks>
    /// One opaque contract for every local element type is the honest encoding of a graph whose element
    /// types exist only in the C# type system. Two local documents therefore agree on element contracts
    /// whatever their lambdas do, and a local graph's element typing is proven by the compiler, not by the
    /// graph compiler.
    /// </remarks>
    internal static readonly ContractReference ElementContract =
        ContractReference.Create(ContractId.Create("local-opaque"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a local stage whose whole behavior is a delegate declares.</summary>
    /// <remarks>
    /// Such a stage has no parameters that could be written down: its behavior is a delegate, and a
    /// delegate is never durable topology. The payload is therefore always the empty object.
    /// </remarks>
    internal static readonly ContractReference ParameterContract =
        ContractReference.Create(ContractId.Create("local-parameters"), ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract a buffer declares.</summary>
    /// <remarks>
    /// A buffer's capacity and overflow policy are configuration rather than behavior, so unlike a
    /// delegate they belong in the document. <see cref="LocalBufferParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference BufferParameterContract =
        ContractReference.Create(
            ContractId.Create("local-buffer-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The parameter contract an asynchronous mapping stage declares.</summary>
    /// <remarks>
    /// The concurrency bound is configuration and is written down; the callback is behavior and is not.
    /// <see cref="LocalParallelismParameters"/> owns the shape.
    /// </remarks>
    internal static readonly ContractReference ParallelismParameterContract =
        ContractReference.Create(
            ContractId.Create("local-parallelism-parameters"),
            ContractReference.FirstMajorVersion);

    /// <summary>The result contract the <c>result</c> port of a local fold declares.</summary>
    internal static readonly ContractReference FoldResultContract =
        ContractReference.Create(ContractId.Create("local-fold-result"), ContractReference.FirstMajorVersion);

    /// <summary>The input port name of every local stage that consumes elements.</summary>
    internal static readonly PortId InputPort = PortId.Create("in");

    /// <summary>The output port name of every local stage that produces elements.</summary>
    internal static readonly PortId OutputPort = PortId.Create("out");

    /// <summary>The result port name of a local fold.</summary>
    internal static readonly PortId ResultPort = PortId.Create("result");

    /// <summary>The parameter payload a local stage whose whole behavior is a delegate carries.</summary>
    /// <remarks>
    /// Empty because there is nothing to say, not because payloads are forbidden: the buffer and the two
    /// asynchronous stages write real ones. <see cref="LocalStageDescriptor.Parameters"/> is what decides
    /// which a given occurrence carries.
    /// </remarks>
    internal static readonly CanonicalJsonValue EmptyParameters = CanonicalJsonValue.Parse("{}");

    /// <summary>The capability token a document with automatically named occurrences declares.</summary>
    /// <remarks>
    /// This is the well-known token of ADR 0004 section 6, promoted onto
    /// <see cref="CapabilityToken"/> beside <see cref="CapabilityToken.Nondeployable"/>; the alias here
    /// keeps the vocabulary's callers reading in one place.
    /// </remarks>
    internal static readonly CapabilityToken EphemeralIdentity = CapabilityToken.EphemeralIdentity;

    /// <summary>The graph identity every locally authored, unnamed graph carries.</summary>
    /// <remarks>
    /// A <see cref="GraphDocument"/> always has an identity, and a graph built from lambdas has no author
    /// who gave it one, so every such document carries the same placeholder. That is deliberate rather than
    /// unfortunate: ADR 0004 section 4 binds a result slot to the document's
    /// <see cref="Definition.GraphFingerprint"/>, which requires two content-identical documents to be
    /// byte-identical, and a per-instance identity would defeat exactly that. Named deployable pipelines
    /// keep <see cref="GraphId"/> plus revision as their upgrade lineage; this constant is what stands in
    /// its place until they exist.
    /// </remarks>
    internal static readonly GraphId AnonymousGraph = GraphId.Create("anonymous");

    /// <summary>The revision every locally authored, unnamed graph carries.</summary>
    internal static readonly GraphRevision FirstRevision =
        GraphRevision.Create(GraphRevision.FirstRevisionNumber);

    /// <summary>Returns the stage reference an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The stage reference.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    internal static StageRef StageOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.FromEnumerable => FromEnumerable,
        LocalStageKind.Select => Select,
        LocalStageKind.Where => Where,
        LocalStageKind.Buffer => Buffer,
        LocalStageKind.SelectAsync => SelectAsync,
        LocalStageKind.SelectAsyncUnordered => SelectAsyncUnordered,
        LocalStageKind.Fold => Fold,
        LocalStageKind.Ignore => Ignore,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Returns the parameter contract an occurrence of <paramref name="kind"/> declares.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <returns>The contract reference.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a declared member.</exception>
    /// <remarks>
    /// Three shapes carry parameters and the rest carry the empty payload. The distinction is not "which
    /// stages happen to have options" but "which stages have options a document can state honestly": a
    /// capacity is a number and a concurrency bound is a number, and neither is a delegate.
    /// </remarks>
    internal static ContractReference ParameterContractOf(LocalStageKind kind) => kind switch
    {
        LocalStageKind.Buffer => BufferParameterContract,
        LocalStageKind.SelectAsync or LocalStageKind.SelectAsyncUnordered => ParallelismParameterContract,
        LocalStageKind.FromEnumerable or
            LocalStageKind.Select or
            LocalStageKind.Where or
            LocalStageKind.Fold or
            LocalStageKind.Ignore => ParameterContract,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Builds the node identifier of the occurrence at one position of an authoring chain.</summary>
    /// <param name="position">
    /// The one-based position in authoring order, which must not exceed
    /// <see cref="MaxAutoNamedPosition"/>; the caller enforces that bound before allocating anything.
    /// </param>
    /// <returns>The identifier, such as <c>stage-0001</c>.</returns>
    /// <remarks>
    /// The position is padded to four digits, so identifiers of one graph sort ordinally in the order they
    /// were authored in: unpadded, <c>stage-10</c> sorts before <c>stage-2</c>, and a document's canonical
    /// node order would stop being its authoring order at the tenth occurrence. The number is formatted
    /// with the invariant culture, so the identifier is the same text under every ambient culture; a
    /// culture with non-ASCII digits would otherwise produce a value the identifier grammar rejects.
    /// </remarks>
    internal static NodeId AutoName(int position) =>
        NodeId.Create(AutoNamePrefix + position.ToString(AutoNameNumberFormat, CultureInfo.InvariantCulture));
}
