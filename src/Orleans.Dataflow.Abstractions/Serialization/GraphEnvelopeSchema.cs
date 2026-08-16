using System.Globalization;
using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// The property vocabulary of the canonical graph document envelope and the diagnostics that name it.
/// </summary>
/// <remarks>
/// <para>
/// The envelope has a fixed schema property order defined by the format version, not an alphabetical or
/// sorted order (ADR 0003), so the writer and the reader must agree on one list of names in one order.
/// Both take the names from here, which is what makes "the reader accepts exactly what the writer emits"
/// a property of one shared definition rather than of two independent transcriptions.
/// </para>
/// <para>
/// The diagnostic builders live here for the same reason: a rejection has to name the JSON path, what was
/// found, and the rule that rejects it, and every one of those sentences is about this vocabulary.
/// </para>
/// </remarks>
internal static class GraphEnvelopeSchema
{
    /// <summary>The JSON path of the document root.</summary>
    internal const string RootPath = "$";

    /// <summary>The name of the format version property, which is always the first property.</summary>
    internal const string FormatVersion = "formatVersion";

    /// <summary>The name of the graph identity property.</summary>
    internal const string GraphIdName = "graphId";

    /// <summary>The name of the revision property.</summary>
    internal const string Revision = "revision";

    /// <summary>The name of the declared capability token array property.</summary>
    internal const string Capabilities = "capabilities";

    /// <summary>The name of the stage node array property.</summary>
    internal const string Nodes = "nodes";

    /// <summary>The name of the edge array property.</summary>
    internal const string Edges = "edges";

    /// <summary>The name of the result slot array property.</summary>
    internal const string ResultSlots = "resultSlots";

    /// <summary>The name of a stage node's identity property.</summary>
    internal const string NodeIdName = "nodeId";

    /// <summary>The name of a stage node's stage reference property.</summary>
    internal const string StageRefName = "stageRef";

    /// <summary>The name of a stage node's parameter contract property.</summary>
    internal const string ParameterContract = "parameterContract";

    /// <summary>The name of a stage node's parameter payload property.</summary>
    internal const string Parameters = "parameters";

    /// <summary>The name of a stage node's execution policy contract property.</summary>
    internal const string ExecutionPolicyContract = "executionPolicyContract";

    /// <summary>The name of a stage node's execution policy payload property.</summary>
    internal const string ExecutionPolicy = "executionPolicy";

    /// <summary>The name of a stage reference's provider property.</summary>
    internal const string ProviderIdName = "providerId";

    /// <summary>The name of a stage reference's stage property.</summary>
    internal const string StageIdName = "stageId";

    /// <summary>The name of a version-carrying reference's major version property.</summary>
    internal const string MajorVersion = "majorVersion";

    /// <summary>The name of a contract reference's contract property.</summary>
    internal const string ContractIdName = "contractId";

    /// <summary>The name of a port address's port property.</summary>
    internal const string PortIdName = "portId";

    /// <summary>The name of an edge's origin property.</summary>
    internal const string From = "from";

    /// <summary>The name of an edge's target property.</summary>
    internal const string To = "to";

    /// <summary>The name of a result slot's identity property.</summary>
    internal const string ResultSlotIdName = "resultSlotId";

    /// <summary>The name of a result slot's result contract property.</summary>
    internal const string ResultContract = "resultContract";

    /// <summary>The name of a result slot's producing port property.</summary>
    internal const string Producer = "producer";

    /// <summary>
    /// The longest run of characters a diagnostic quotes from the input before truncating it.
    /// </summary>
    /// <remarks>
    /// A rejection quotes what it found so that the reader of the message can see it, but the input is
    /// untrusted and a value can be as long as the payload limit allows. Truncating keeps a rejection
    /// from becoming a memory problem of its own.
    /// </remarks>
    internal const int MaxQuotedLength = 96;

    /// <summary>The marker appended to text that a diagnostic truncated.</summary>
    private const string TruncationMarker = "...";

    /// <summary>
    /// Shortens text that is too long to quote in a diagnostic.
    /// </summary>
    /// <param name="text">The text to quote.</param>
    /// <returns>
    /// <paramref name="text"/> when it is short enough; otherwise its first characters followed by an
    /// ellipsis.
    /// </returns>
    internal static string Truncate(string text) =>
        text.Length <= MaxQuotedLength ? text : text[..MaxQuotedLength] + TruncationMarker;

    /// <summary>
    /// Builds the path of an array element.
    /// </summary>
    /// <param name="arrayPath">The path of the array.</param>
    /// <param name="index">The zero-based element index.</param>
    /// <returns>A path of the form <c>$.nodes[2]</c>.</returns>
    internal static string ElementPath(string arrayPath, int index) =>
        string.Create(CultureInfo.InvariantCulture, $"{arrayPath}[{index}]");

    /// <summary>
    /// Builds the path of an object member.
    /// </summary>
    /// <param name="objectPath">The path of the object.</param>
    /// <param name="propertyName">The member name.</param>
    /// <returns>A path of the form <c>$.nodes[2].stageRef</c>.</returns>
    internal static string MemberPath(string objectPath, string propertyName) =>
        $"{objectPath}.{propertyName}";

    /// <summary>
    /// Builds a rejection naming the path, what was found, and the rule that rejects it.
    /// </summary>
    /// <param name="path">The JSON path of the offending construct.</param>
    /// <param name="violation">A sentence fragment describing what was found and why it is rejected.</param>
    /// <returns>The exception to throw.</returns>
    internal static GraphDocumentFormatException Violation(string path, string violation) =>
        new($"{path}: {violation}.");

    /// <summary>
    /// Builds a rejection that carries the lower-layer error it came from.
    /// </summary>
    /// <param name="path">The JSON path of the offending construct.</param>
    /// <param name="violation">A sentence fragment describing what was found and why it is rejected.</param>
    /// <param name="innerException">The lower-layer error.</param>
    /// <returns>The exception to throw.</returns>
    internal static GraphDocumentFormatException Violation(
        string path,
        string violation,
        Exception innerException) =>
        new($"{path}: {violation}.", innerException);

    /// <summary>
    /// Builds the rejection for text that is not a valid identifier of its kind.
    /// </summary>
    /// <param name="path">The JSON path of the offending string.</param>
    /// <param name="identifierName">The identifier type name, such as <c>NodeId</c>.</param>
    /// <param name="text">The rejected text.</param>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// The grammar is a rule of the identifier type, not of the envelope, so the message names the type
    /// and lets the type own the definition rather than restating a grammar that could drift.
    /// </remarks>
    internal static GraphDocumentFormatException InvalidIdentifier(
        string path,
        string identifierName,
        string text) =>
        new($"{path}: '{Truncate(text)}' is not a valid {identifierName}, and the envelope stores every identifier as its canonical text.");

    /// <summary>
    /// Builds the rejection for a version or revision number outside the range its type admits.
    /// </summary>
    /// <param name="path">The JSON path of the offending number.</param>
    /// <param name="typeName">The type that rejected the number, such as <c>GraphRevision</c>.</param>
    /// <param name="value">The rejected number.</param>
    /// <returns>The exception to throw.</returns>
    internal static GraphDocumentFormatException OutOfRangeVersion(string path, string typeName, int value) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"{path}: {value} is not a value a {typeName} admits; versions and revisions are positive integers."));

    /// <summary>
    /// Builds the rejection for a document written under a format version this library does not read.
    /// </summary>
    /// <param name="foundVersion">The version the document declares.</param>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// This rejection runs before every other rule, so its message deliberately mentions no other
    /// property: a document from the future may be entirely well formed under its own version, and
    /// reporting today's rules against it would be noise at best and misleading at worst.
    /// </remarks>
    internal static GraphDocumentFormatException UnknownFormatVersion(int foundVersion) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"{MemberPath(RootPath, FormatVersion)}: the document declares format version {foundVersion}, and this library reads format version {GraphDocument.CurrentFormatVersion} only. An unknown format version is rejected before any other rule runs, never parsed on a best-effort basis."));

    /// <summary>
    /// Builds the rejection for a decoded document that breaks a structural invariant of the model.
    /// </summary>
    /// <param name="path">The JSON path of the construct that was being built.</param>
    /// <param name="innerException">The invariant violation reported by the document model.</param>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// Every value is rebuilt through the same factory the authoring API uses, so a document that was
    /// hand-edited into an impossible shape fails on exactly the invariant an authored document would
    /// fail on. The original report is carried as the inner exception rather than being reworded, because
    /// it already names every violation it found.
    /// </remarks>
    internal static GraphDocumentFormatException StructuralViolation(string path, ArgumentException innerException) =>
        new(
            $"{path}: the decoded value breaks a structural invariant of the graph document model: {innerException.Message}",
            innerException);
}
