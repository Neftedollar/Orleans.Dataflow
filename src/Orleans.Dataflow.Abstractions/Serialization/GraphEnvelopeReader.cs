using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Reads the canonical envelope bytes of a graph document, accepting exactly what
/// <see cref="GraphEnvelopeWriter"/> emits.
/// </summary>
/// <remarks>
/// <para>
/// The reader is strict by design (ADR 0003): one document has exactly one byte form, so anything that is
/// merely equivalent JSON is a different byte form and therefore a different fingerprint. Whitespace, a
/// byte order mark, an omitted optional property, a reordered property, a non-minimal number, an escaped
/// identifier, a non-canonical payload, and trailing content are all rejected rather than normalized.
/// </para>
/// <para>
/// Minification is enforced positionally rather than by scanning for whitespace bytes, which would be
/// wrong inside embedded payload strings. Every token must begin exactly where the previous token ended,
/// or one byte later with a comma in between, which is the only shape minified JSON can take.
/// </para>
/// <para>
/// The reader checks that collections arrive in canonical order but deliberately does not check them for
/// duplicates. A duplicate is a structural invariant of the document model, and the model's own factories
/// re-enforce every such invariant when the decoded values are rebuilt, so the rule lives in exactly one
/// place and a hand-edited document fails on the same diagnostic an authored one would.
/// </para>
/// </remarks>
internal ref struct GraphEnvelopeReader
{
    /// <summary>The number of envelope containers wrapping an embedded payload.</summary>
    /// <remarks>
    /// A payload sits inside a stage node object, inside the node array, inside the document object.
    /// </remarks>
    private const int PayloadContainerDepth = 3;

    /// <summary>The deepest nesting this reader admits.</summary>
    /// <remarks>
    /// The bound is the payload limit plus the envelope containers around it, plus one level so that a
    /// payload one level past its own limit is still read and then rejected by
    /// <see cref="CanonicalJsonValue"/> with a diagnostic naming the payload rule, rather than surfacing
    /// as a bare parser error.
    /// </remarks>
    private const int MaxEnvelopeDepth = CanonicalJsonValue.MaxDepth + PayloadContainerDepth + 1;

    /// <summary>The widest decimal form of an <see cref="int"/>, <c>-2147483648</c>.</summary>
    private const int MaxInt32DigitCount = 11;

    private readonly ReadOnlySpan<byte> _input;
    private Utf8JsonReader _reader;
    private long _tokenEnd;
    private string _path;

    /// <summary>
    /// Initializes a new instance of the <see cref="GraphEnvelopeReader"/> struct.
    /// </summary>
    /// <param name="input">The candidate canonical envelope bytes.</param>
    private GraphEnvelopeReader(ReadOnlySpan<byte> input)
    {
        _input = input;
        _reader = new Utf8JsonReader(
            input,
            new JsonReaderOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = MaxEnvelopeDepth,
            });
        _tokenEnd = 0;
        _path = GraphEnvelopeSchema.RootPath;
    }

    /// <summary>
    /// Reads a graph document from canonical envelope bytes.
    /// </summary>
    /// <param name="canonicalEnvelope">The candidate bytes.</param>
    /// <returns>The decoded, structurally valid document.</returns>
    /// <exception cref="GraphDocumentFormatException">
    /// <paramref name="canonicalEnvelope"/> is not the canonical serialization of a graph document.
    /// </exception>
    internal static GraphDocument Read(ReadOnlySpan<byte> canonicalEnvelope)
    {
        EnsureNoByteOrderMark(canonicalEnvelope);

        GraphEnvelopeReader reader = new(canonicalEnvelope);

        try
        {
            return reader.ReadDocument();
        }
        catch (JsonException exception)
        {
            throw GraphEnvelopeSchema.Violation(
                reader._path,
                $"the input is not well-formed JSON within the bounds this reader admits: {exception.Message}",
                exception);
        }
    }

    /// <summary>
    /// Rejects a leading UTF-8 byte order mark.
    /// </summary>
    /// <param name="input">The candidate bytes.</param>
    /// <exception cref="GraphDocumentFormatException">The input starts with a byte order mark.</exception>
    /// <remarks>
    /// The mark is rejected rather than stripped. Stripping would make two byte strings decode to one
    /// document, and a document whose identity is the digest of its bytes cannot afford that.
    /// </remarks>
    private static void EnsureNoByteOrderMark(ReadOnlySpan<byte> input)
    {
        if (input.Length >= 3 && input[0] == 0xEF && input[1] == 0xBB && input[2] == 0xBF)
        {
            throw GraphEnvelopeSchema.Violation(
                GraphEnvelopeSchema.RootPath,
                "the input starts with a UTF-8 byte order mark, and canonical bytes are UTF-8 without one; the mark is rejected rather than stripped");
        }
    }

    /// <summary>
    /// Reads the document object.
    /// </summary>
    /// <returns>The decoded document.</returns>
    private GraphDocument ReadDocument()
    {
        const string Path = GraphEnvelopeSchema.RootPath;

        ExpectStartObject(Path);

        ExpectPropertyName(Path, GraphEnvelopeSchema.FormatVersion);
        int formatVersion = ReadInteger(GraphEnvelopeSchema.MemberPath(Path, GraphEnvelopeSchema.FormatVersion));

        if (formatVersion != GraphDocument.CurrentFormatVersion)
        {
            throw GraphEnvelopeSchema.UnknownFormatVersion(formatVersion);
        }

        ExpectPropertyName(Path, GraphEnvelopeSchema.GraphIdName);
        GraphId id = ReadGraphId(GraphEnvelopeSchema.MemberPath(Path, GraphEnvelopeSchema.GraphIdName));

        ExpectPropertyName(Path, GraphEnvelopeSchema.Revision);
        GraphRevision revision = ReadRevision(GraphEnvelopeSchema.MemberPath(Path, GraphEnvelopeSchema.Revision));

        ExpectPropertyName(Path, GraphEnvelopeSchema.Capabilities);
        CapabilityToken[] capabilities = ReadCapabilities();

        ExpectPropertyName(Path, GraphEnvelopeSchema.Nodes);
        StageNode[] nodes = ReadNodes();

        ExpectPropertyName(Path, GraphEnvelopeSchema.Edges);
        GraphEdge[] edges = ReadEdges();

        ExpectPropertyName(Path, GraphEnvelopeSchema.ResultSlots);
        ResultSlotDefinition[] resultSlots = ReadResultSlots();

        ExpectEndOfObject(Path);
        EnsureNoTrailingContent();

        return CreateDocument(id, revision, capabilities, nodes, edges, resultSlots);
    }

    /// <summary>
    /// Reads the capability token array.
    /// </summary>
    /// <returns>The decoded tokens, in the order they were stored.</returns>
    private CapabilityToken[] ReadCapabilities()
    {
        string arrayPath = GraphEnvelopeSchema.MemberPath(
            GraphEnvelopeSchema.RootPath,
            GraphEnvelopeSchema.Capabilities);

        ExpectStartArray(arrayPath);

        List<CapabilityToken> tokens = [];
        string? previousText = null;
        string? previousPath = null;

        for (int index = 0; ; index++)
        {
            string elementPath = GraphEnvelopeSchema.ElementPath(arrayPath, index);
            MoveNext(elementPath);

            if (_reader.TokenType == JsonTokenType.EndArray)
            {
                return [.. tokens];
            }

            string text = CurrentString(elementPath);

            if (!CapabilityToken.TryCreate(text, out CapabilityToken token))
            {
                throw GraphEnvelopeSchema.InvalidIdentifier(elementPath, nameof(CapabilityToken), text);
            }

            EnsureAscending(
                previousText,
                previousPath,
                text,
                elementPath,
                "capability token",
                "capabilities are stored in ordinal order of their token text");

            tokens.Add(token);
            previousText = text;
            previousPath = elementPath;
        }
    }

    /// <summary>
    /// Reads the stage node array.
    /// </summary>
    /// <returns>The decoded nodes, in the order they were stored.</returns>
    private StageNode[] ReadNodes()
    {
        string arrayPath = GraphEnvelopeSchema.MemberPath(
            GraphEnvelopeSchema.RootPath,
            GraphEnvelopeSchema.Nodes);

        ExpectStartArray(arrayPath);

        List<StageNode> nodes = [];
        string? previousText = null;
        string? previousPath = null;

        for (int index = 0; ; index++)
        {
            string elementPath = GraphEnvelopeSchema.ElementPath(arrayPath, index);
            MoveNext(elementPath);

            if (_reader.TokenType == JsonTokenType.EndArray)
            {
                return [.. nodes];
            }

            EnsureCurrentIsStartObject(elementPath);
            StageNode node = ReadNode(elementPath);

            EnsureAscending(
                previousText,
                previousPath,
                node.Id.Value,
                GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.NodeIdName),
                "node id",
                "nodes are stored in ordinal order of their node id path");

            nodes.Add(node);
            previousText = node.Id.Value;
            previousPath = GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.NodeIdName);
        }
    }

    /// <summary>
    /// Reads one stage node object, whose start token has already been read.
    /// </summary>
    /// <param name="path">The JSON path of the node.</param>
    /// <returns>The decoded node.</returns>
    private StageNode ReadNode(string path)
    {
        ExpectPropertyName(path, GraphEnvelopeSchema.NodeIdName);
        NodeId id = ReadNodeId(GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.NodeIdName));

        ExpectPropertyName(path, GraphEnvelopeSchema.StageRefName);
        StageRef stage = ReadStageRef(GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.StageRefName));

        ExpectPropertyName(path, GraphEnvelopeSchema.ParameterContract);
        ContractReference parameterContract =
            ReadContractReference(GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.ParameterContract));

        ExpectPropertyName(path, GraphEnvelopeSchema.Parameters);
        string parametersPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.Parameters);
        MoveNext(parametersPath);

        if (_reader.TokenType == JsonTokenType.Null)
        {
            throw GraphEnvelopeSchema.Violation(
                parametersPath,
                "the parameter payload is null, and every stage node carries a parameter payload");
        }

        CanonicalJsonValue parameters = ReadPayload(parametersPath);

        ExpectPropertyName(path, GraphEnvelopeSchema.ExecutionPolicyContract);
        string policyContractPath =
            GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.ExecutionPolicyContract);
        MoveNext(policyContractPath);
        ContractReference? policyContract = _reader.TokenType == JsonTokenType.Null
            ? null
            : ReadContractReferenceBody(policyContractPath);

        ExpectPropertyName(path, GraphEnvelopeSchema.ExecutionPolicy);
        string policyPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.ExecutionPolicy);
        MoveNext(policyPath);
        CanonicalJsonValue? policy = _reader.TokenType == JsonTokenType.Null
            ? null
            : ReadPayload(policyPath);

        EnsureExecutionPolicyIsPaired(path, policyContract, policy);
        ExpectEndOfObject(path);

        return CreateNode(path, id, stage, parameterContract, parameters, policyContract, policy);
    }

    /// <summary>
    /// Reads the edge array.
    /// </summary>
    /// <returns>The decoded edges, in the order they were stored.</returns>
    private GraphEdge[] ReadEdges()
    {
        string arrayPath = GraphEnvelopeSchema.MemberPath(
            GraphEnvelopeSchema.RootPath,
            GraphEnvelopeSchema.Edges);

        ExpectStartArray(arrayPath);

        List<GraphEdge> edges = [];
        GraphEdge previousEdge = default;
        string? previousPath = null;

        for (int index = 0; ; index++)
        {
            string elementPath = GraphEnvelopeSchema.ElementPath(arrayPath, index);
            MoveNext(elementPath);

            if (_reader.TokenType == JsonTokenType.EndArray)
            {
                return [.. edges];
            }

            EnsureCurrentIsStartObject(elementPath);

            ExpectPropertyName(elementPath, GraphEnvelopeSchema.From);
            PortAddress from = ReadPortAddress(
                GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.From));

            ExpectPropertyName(elementPath, GraphEnvelopeSchema.To);
            PortAddress to = ReadPortAddress(GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.To));

            ExpectEndOfObject(elementPath);

            GraphEdge edge = CreateEdge(elementPath, from, to);

            if (previousPath is not null && CompareEdges(previousEdge, edge) > 0)
            {
                throw GraphEnvelopeSchema.Violation(
                    elementPath,
                    $"the edge '{edge}' sorts before the edge '{previousEdge}' at {previousPath}, and edges are stored in ordinal order of origin node, origin port, target node, and target port");
            }

            edges.Add(edge);
            previousEdge = edge;
            previousPath = elementPath;
        }
    }

    /// <summary>
    /// Reads the result slot array.
    /// </summary>
    /// <returns>The decoded result slots, in the order they were stored.</returns>
    private ResultSlotDefinition[] ReadResultSlots()
    {
        string arrayPath = GraphEnvelopeSchema.MemberPath(
            GraphEnvelopeSchema.RootPath,
            GraphEnvelopeSchema.ResultSlots);

        ExpectStartArray(arrayPath);

        List<ResultSlotDefinition> slots = [];
        string? previousText = null;
        string? previousPath = null;

        for (int index = 0; ; index++)
        {
            string elementPath = GraphEnvelopeSchema.ElementPath(arrayPath, index);
            MoveNext(elementPath);

            if (_reader.TokenType == JsonTokenType.EndArray)
            {
                return [.. slots];
            }

            EnsureCurrentIsStartObject(elementPath);

            ExpectPropertyName(elementPath, GraphEnvelopeSchema.ResultSlotIdName);
            string slotIdPath = GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.ResultSlotIdName);
            string slotIdText = ReadString(slotIdPath);

            if (!ResultSlotId.TryCreate(slotIdText, out ResultSlotId slotId))
            {
                throw GraphEnvelopeSchema.InvalidIdentifier(slotIdPath, nameof(ResultSlotId), slotIdText);
            }

            ExpectPropertyName(elementPath, GraphEnvelopeSchema.ResultContract);
            ContractReference resultContract =
                ReadContractReference(GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.ResultContract));

            ExpectPropertyName(elementPath, GraphEnvelopeSchema.Producer);
            PortAddress producer =
                ReadPortAddress(GraphEnvelopeSchema.MemberPath(elementPath, GraphEnvelopeSchema.Producer));

            ExpectEndOfObject(elementPath);

            EnsureAscending(
                previousText,
                previousPath,
                slotIdText,
                slotIdPath,
                "result slot id",
                "result slots are stored in ordinal order of their slot id");

            // Every member has been validated as a created value, which is all this factory requires.
            slots.Add(ResultSlotDefinition.Create(slotId, resultContract, producer));
            previousText = slotIdText;
            previousPath = slotIdPath;
        }
    }

    /// <summary>
    /// Reads a stage reference object.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    /// <returns>The decoded stage reference.</returns>
    private StageRef ReadStageRef(string path)
    {
        ExpectStartObject(path);

        ExpectPropertyName(path, GraphEnvelopeSchema.ProviderIdName);
        string providerPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.ProviderIdName);
        string providerText = ReadString(providerPath);

        if (!ProviderId.TryCreate(providerText, out ProviderId provider))
        {
            throw GraphEnvelopeSchema.InvalidIdentifier(providerPath, nameof(ProviderId), providerText);
        }

        ExpectPropertyName(path, GraphEnvelopeSchema.StageIdName);
        string stagePath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.StageIdName);
        string stageText = ReadString(stagePath);

        if (!StageId.TryCreate(stageText, out StageId stage))
        {
            throw GraphEnvelopeSchema.InvalidIdentifier(stagePath, nameof(StageId), stageText);
        }

        ExpectPropertyName(path, GraphEnvelopeSchema.MajorVersion);
        string versionPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.MajorVersion);
        int majorVersion = ReadInteger(versionPath);

        ExpectEndOfObject(path);

        if (!StageRef.TryCreate(provider, stage, majorVersion, out StageRef stageRef))
        {
            throw GraphEnvelopeSchema.OutOfRangeVersion(versionPath, nameof(StageRef), majorVersion);
        }

        return stageRef;
    }

    /// <summary>
    /// Reads a contract reference object.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    /// <returns>The decoded contract reference.</returns>
    private ContractReference ReadContractReference(string path)
    {
        ExpectStartObject(path);
        return ReadContractReferenceBody(path);
    }

    /// <summary>
    /// Reads a contract reference object whose start token has already been read.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    /// <returns>The decoded contract reference.</returns>
    private ContractReference ReadContractReferenceBody(string path)
    {
        EnsureCurrentIsStartObject(path);

        ExpectPropertyName(path, GraphEnvelopeSchema.ContractIdName);
        string contractPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.ContractIdName);
        string contractText = ReadString(contractPath);

        if (!ContractId.TryCreate(contractText, out ContractId contract))
        {
            throw GraphEnvelopeSchema.InvalidIdentifier(contractPath, nameof(ContractId), contractText);
        }

        ExpectPropertyName(path, GraphEnvelopeSchema.MajorVersion);
        string versionPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.MajorVersion);
        int majorVersion = ReadInteger(versionPath);

        ExpectEndOfObject(path);

        if (!ContractReference.TryCreate(contract, majorVersion, out ContractReference reference))
        {
            throw GraphEnvelopeSchema.OutOfRangeVersion(versionPath, nameof(ContractReference), majorVersion);
        }

        return reference;
    }

    /// <summary>
    /// Reads a port address object.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    /// <returns>The decoded port address.</returns>
    private PortAddress ReadPortAddress(string path)
    {
        ExpectStartObject(path);

        ExpectPropertyName(path, GraphEnvelopeSchema.NodeIdName);
        NodeId node = ReadNodeId(GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.NodeIdName));

        ExpectPropertyName(path, GraphEnvelopeSchema.PortIdName);
        string portPath = GraphEnvelopeSchema.MemberPath(path, GraphEnvelopeSchema.PortIdName);
        string portText = ReadString(portPath);

        if (!PortId.TryCreate(portText, out PortId port))
        {
            throw GraphEnvelopeSchema.InvalidIdentifier(portPath, nameof(PortId), portText);
        }

        ExpectEndOfObject(path);

        // Both members have been validated as created values, which is all this factory requires.
        return PortAddress.Create(node, port);
    }

    /// <summary>
    /// Reads a graph identity string.
    /// </summary>
    /// <param name="path">The JSON path of the string.</param>
    /// <returns>The decoded identity.</returns>
    private GraphId ReadGraphId(string path)
    {
        string text = ReadString(path);

        return GraphId.TryCreate(text, out GraphId id)
            ? id
            : throw GraphEnvelopeSchema.InvalidIdentifier(path, nameof(GraphId), text);
    }

    /// <summary>
    /// Reads a node identity string.
    /// </summary>
    /// <param name="path">The JSON path of the string.</param>
    /// <returns>The decoded identity.</returns>
    private NodeId ReadNodeId(string path)
    {
        string text = ReadString(path);

        return NodeId.TryParse(text, out NodeId id)
            ? id
            : throw GraphEnvelopeSchema.InvalidIdentifier(path, nameof(NodeId), text);
    }

    /// <summary>
    /// Reads a revision number.
    /// </summary>
    /// <param name="path">The JSON path of the number.</param>
    /// <returns>The decoded revision.</returns>
    private GraphRevision ReadRevision(string path)
    {
        int value = ReadInteger(path);

        return GraphRevision.TryCreate(value, out GraphRevision revision)
            ? revision
            : throw GraphEnvelopeSchema.OutOfRangeVersion(path, nameof(GraphRevision), value);
    }

    /// <summary>
    /// Reads an embedded payload whose first token has already been read.
    /// </summary>
    /// <param name="path">The JSON path of the payload.</param>
    /// <returns>The decoded payload.</returns>
    /// <remarks>
    /// The payload is validated twice over: it must be canonicalizable at all, and its canonical form
    /// must be byte-identical to the bytes found in the envelope. The second check is the one that
    /// matters for identity, because a payload whose keys merely happen to parse is still a second byte
    /// form of the same document.
    /// </remarks>
    private CanonicalJsonValue ReadPayload(string path)
    {
        int start = (int)_reader.TokenStartIndex;
        _reader.Skip();
        _tokenEnd = _reader.BytesConsumed;

        ReadOnlySpan<byte> slice = _input[start..(int)_tokenEnd];
        CanonicalJsonValue payload;

        try
        {
            payload = CanonicalJsonValue.Parse(slice);
        }
        catch (ArgumentException exception)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"the embedded payload breaks a canonical JSON rule: {exception.Message}",
                exception);
        }

        if (!payload.CanonicalUtf8Bytes.Span.SequenceEqual(slice))
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"the embedded payload is not in canonical form; its canonical form is {GraphEnvelopeSchema.Truncate(payload.ToString())}");
        }

        return payload;
    }

    /// <summary>
    /// Rejects a node whose execution policy contract and payload do not agree.
    /// </summary>
    /// <param name="path">The JSON path of the node.</param>
    /// <param name="policyContract">The decoded policy contract, or <see langword="null"/>.</param>
    /// <param name="policy">The decoded policy payload, or <see langword="null"/>.</param>
    private static void EnsureExecutionPolicyIsPaired(
        string path,
        ContractReference? policyContract,
        CanonicalJsonValue? policy)
    {
        if (policyContract is null == policy is null)
        {
            return;
        }

        string presentName = policyContract is null
            ? GraphEnvelopeSchema.ExecutionPolicy
            : GraphEnvelopeSchema.ExecutionPolicyContract;

        string absentName = policyContract is null
            ? GraphEnvelopeSchema.ExecutionPolicyContract
            : GraphEnvelopeSchema.ExecutionPolicy;

        throw GraphEnvelopeSchema.Violation(
            GraphEnvelopeSchema.MemberPath(path, absentName),
            $"the value is null while {GraphEnvelopeSchema.MemberPath(path, presentName)} is not, and a node declares an execution policy contract and payload together or declares neither");
    }

    /// <summary>
    /// Compares two edges by origin node, origin port, target node, and target port, ordinally.
    /// </summary>
    /// <param name="left">The left edge.</param>
    /// <param name="right">The right edge.</param>
    /// <returns>The ordinal comparison result.</returns>
    /// <remarks>
    /// The order restates the one <see cref="GraphDocument"/> sorts by, because the reader has to decide
    /// whether stored bytes are already in that order before the document that defines it exists.
    /// </remarks>
    private static int CompareEdges(GraphEdge left, GraphEdge right)
    {
        int comparison = string.CompareOrdinal(left.From.Node.Value, right.From.Node.Value);

        if (comparison != 0)
        {
            return comparison;
        }

        comparison = string.CompareOrdinal(left.From.Port.Value, right.From.Port.Value);

        if (comparison != 0)
        {
            return comparison;
        }

        comparison = string.CompareOrdinal(left.To.Node.Value, right.To.Node.Value);

        return comparison != 0 ? comparison : string.CompareOrdinal(left.To.Port.Value, right.To.Port.Value);
    }

    /// <summary>
    /// Rejects a collection element that sorts before the element in front of it.
    /// </summary>
    /// <param name="previousText">The previous element's sort key, or <see langword="null"/> for the first element.</param>
    /// <param name="previousPath">The previous element's JSON path, or <see langword="null"/> for the first element.</param>
    /// <param name="currentText">This element's sort key.</param>
    /// <param name="currentPath">This element's JSON path.</param>
    /// <param name="subject">What the sort key is, in prose.</param>
    /// <param name="rule">The canonical order rule, in prose.</param>
    /// <remarks>
    /// Two equal keys pass this check. Uniqueness is a structural invariant of the document model, and
    /// the model's factories report it with a diagnostic that names every violation at once.
    /// </remarks>
    private static void EnsureAscending(
        string? previousText,
        string? previousPath,
        string currentText,
        string currentPath,
        string subject,
        string rule)
    {
        if (previousText is not null && string.CompareOrdinal(previousText, currentText) > 0)
        {
            throw GraphEnvelopeSchema.Violation(
                currentPath,
                $"the {subject} '{GraphEnvelopeSchema.Truncate(currentText)}' sorts before the {subject} '{GraphEnvelopeSchema.Truncate(previousText)}' at {previousPath}, and {rule}");
        }
    }

    /// <summary>
    /// Rebuilds a stage node through the model's own factory.
    /// </summary>
    /// <param name="path">The JSON path of the node.</param>
    /// <param name="id">The decoded node identity.</param>
    /// <param name="stage">The decoded stage reference.</param>
    /// <param name="parameterContract">The decoded parameter contract.</param>
    /// <param name="parameters">The decoded parameter payload.</param>
    /// <param name="policyContract">The decoded execution policy contract, or <see langword="null"/>.</param>
    /// <param name="policy">The decoded execution policy payload, or <see langword="null"/>.</param>
    /// <returns>The rebuilt node.</returns>
    private static StageNode CreateNode(
        string path,
        NodeId id,
        StageRef stage,
        ContractReference parameterContract,
        CanonicalJsonValue parameters,
        ContractReference? policyContract,
        CanonicalJsonValue? policy)
    {
        try
        {
            return policyContract is { } contract && policy is { } payload
                ? StageNode.Create(id, stage, parameterContract, parameters, contract, payload)
                : StageNode.Create(id, stage, parameterContract, parameters);
        }
        catch (ArgumentException exception)
        {
            throw GraphEnvelopeSchema.StructuralViolation(path, exception);
        }
    }

    /// <summary>
    /// Rebuilds an edge through the model's own factory.
    /// </summary>
    /// <param name="path">The JSON path of the edge.</param>
    /// <param name="from">The decoded origin port.</param>
    /// <param name="to">The decoded target port.</param>
    /// <returns>The rebuilt edge.</returns>
    private static GraphEdge CreateEdge(string path, PortAddress from, PortAddress to)
    {
        try
        {
            return GraphEdge.Create(from, to);
        }
        catch (ArgumentException exception)
        {
            throw GraphEnvelopeSchema.StructuralViolation(path, exception);
        }
    }

    /// <summary>
    /// Rebuilds the document through the model's own factory.
    /// </summary>
    /// <param name="id">The decoded graph identity.</param>
    /// <param name="revision">The decoded revision.</param>
    /// <param name="capabilities">The decoded capability tokens.</param>
    /// <param name="nodes">The decoded nodes.</param>
    /// <param name="edges">The decoded edges.</param>
    /// <param name="resultSlots">The decoded result slots.</param>
    /// <returns>The rebuilt document.</returns>
    /// <remarks>
    /// Every structural invariant of the definition model is re-enforced here, so bytes that were edited
    /// into a shape the authoring API cannot produce, such as a document with two nodes of one identity,
    /// fail on exactly the invariant an authored document would fail on.
    /// </remarks>
    private static GraphDocument CreateDocument(
        GraphId id,
        GraphRevision revision,
        CapabilityToken[] capabilities,
        StageNode[] nodes,
        GraphEdge[] edges,
        ResultSlotDefinition[] resultSlots)
    {
        try
        {
            return GraphDocument.Create(id, revision, capabilities, nodes, edges, resultSlots);
        }
        catch (ArgumentException exception)
        {
            throw GraphEnvelopeSchema.StructuralViolation(GraphEnvelopeSchema.RootPath, exception);
        }
    }

    /// <summary>
    /// Advances to the next token and enforces that the bytes carry no insignificant content.
    /// </summary>
    /// <param name="path">The JSON path the next token belongs to, reported on rejection.</param>
    /// <remarks>
    /// Minified JSON places every token either immediately after the previous one, which covers the
    /// colon a property name consumes and the brackets that open and close containers, or one byte after
    /// it with a comma in between. Anything else is insignificant whitespace or a repeated separator.
    /// </remarks>
    private void MoveNext(string path)
    {
        _path = path;

        if (!_reader.Read())
        {
            throw GraphEnvelopeSchema.Violation(path, "the input ends before the document is complete");
        }

        long start = _reader.TokenStartIndex;
        long gap = start - _tokenEnd;

        if (gap != 0 && !(gap == 1 && _input[(int)_tokenEnd] == (byte)','))
        {
            throw GraphEnvelopeSchema.Violation(path, DescribeInsignificantByte((int)_tokenEnd));
        }

        _tokenEnd = _reader.BytesConsumed;
    }

    /// <summary>
    /// Reads a string value.
    /// </summary>
    /// <param name="path">The JSON path of the value.</param>
    /// <returns>The string content.</returns>
    private string ReadString(string path)
    {
        MoveNext(path);
        return CurrentString(path);
    }

    /// <summary>
    /// Reads an integer value.
    /// </summary>
    /// <param name="path">The JSON path of the value.</param>
    /// <returns>The integer value.</returns>
    private int ReadInteger(string path)
    {
        MoveNext(path);
        return CurrentInteger(path);
    }

    /// <summary>
    /// Interprets the current token as a string.
    /// </summary>
    /// <param name="path">The JSON path of the value.</param>
    /// <returns>The string content.</returns>
    private string CurrentString(string path)
    {
        if (_reader.TokenType != JsonTokenType.String)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"a JSON string was expected but {DescribeToken()} was found");
        }

        if (_reader.ValueIsEscaped)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                "the string carries an escape sequence, and the canonical form escapes only the quotation mark, the backslash, and control characters, none of which an identifier contains");
        }

        try
        {
            return _reader.GetString() ?? string.Empty;
        }
        catch (InvalidOperationException exception)
        {
            // A JSON parser accepts invalid UTF-8 as syntax and only fails when the text is materialized.
            // That failure is a rule of the canonical form, not a broken invariant of this library, so it
            // is translated rather than allowed to escape as the parser's own error type.
            throw GraphEnvelopeSchema.Violation(
                path,
                "the string is not well-formed UTF-8 text, and canonical bytes are UTF-8; the bytes are rejected rather than substituted with a replacement character",
                exception);
        }
    }

    /// <summary>
    /// Interprets the current token as an integer in minimal decimal form.
    /// </summary>
    /// <param name="path">The JSON path of the value.</param>
    /// <returns>The integer value.</returns>
    private int CurrentInteger(string path)
    {
        if (_reader.TokenType != JsonTokenType.Number)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"a JSON number was expected but {DescribeToken()} was found");
        }

        if (!_reader.TryGetInt32(out int value))
        {
            string rawText = RawText();

            throw GraphEnvelopeSchema.Violation(
                path,
                rawText.AsSpan().ContainsAny('.', 'e', 'E')
                    ? $"the number {rawText} carries a fraction or an exponent, and the envelope models every quantity as an integer"
                    : $"the number {rawText} is outside the signed 32-bit range that versions and revisions occupy");
        }

        Span<byte> canonical = stackalloc byte[MaxInt32DigitCount];
        _ = value.TryFormat(canonical, out int written, format: default, provider: CultureInfo.InvariantCulture);

        if (!_reader.ValueSpan.SequenceEqual(canonical[..written]))
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"the number {RawText()} is not in minimal decimal form; the canonical spelling of this value is {value}"));
        }

        return value;
    }

    /// <summary>
    /// Reads the start of an object.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    private void ExpectStartObject(string path)
    {
        MoveNext(path);
        EnsureCurrentIsStartObject(path);
    }

    /// <summary>
    /// Enforces that the current token starts an object.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    private void EnsureCurrentIsStartObject(string path)
    {
        if (_reader.TokenType != JsonTokenType.StartObject)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"a JSON object was expected but {DescribeToken()} was found");
        }
    }

    /// <summary>
    /// Reads the start of an array.
    /// </summary>
    /// <param name="path">The JSON path of the array.</param>
    private void ExpectStartArray(string path)
    {
        MoveNext(path);

        if (_reader.TokenType != JsonTokenType.StartArray)
        {
            throw GraphEnvelopeSchema.Violation(
                path,
                $"a JSON array was expected but {DescribeToken()} was found");
        }
    }

    /// <summary>
    /// Reads the property name the schema fixes at this position.
    /// </summary>
    /// <param name="objectPath">The JSON path of the object being read.</param>
    /// <param name="expected">The property name the format version fixes at this position.</param>
    private void ExpectPropertyName(string objectPath, string expected)
    {
        MoveNext(objectPath);

        if (_reader.TokenType == JsonTokenType.EndObject)
        {
            throw GraphEnvelopeSchema.Violation(
                objectPath,
                $"the object ends before the property '{expected}', and the envelope writes every property of every object, in schema order, with no omitted defaults");
        }

        if (_reader.TokenType != JsonTokenType.PropertyName)
        {
            throw GraphEnvelopeSchema.Violation(
                objectPath,
                $"the property '{expected}' was expected but {DescribeToken()} was found");
        }

        if (_reader.ValueIsEscaped)
        {
            throw GraphEnvelopeSchema.Violation(
                objectPath,
                $"the property name at the position of '{expected}' carries an escape sequence, and canonical property names are written without escapes");
        }

        if (!_reader.ValueTextEquals(expected))
        {
            throw GraphEnvelopeSchema.Violation(
                objectPath,
                $"the property '{expected}' was expected but the property '{RawText()}' was found, and format version {GraphDocument.CurrentFormatVersion} fixes the property order of every object");
        }
    }

    /// <summary>
    /// Reads the end of an object and rejects any property beyond the schema.
    /// </summary>
    /// <param name="path">The JSON path of the object.</param>
    private void ExpectEndOfObject(string path)
    {
        MoveNext(path);

        if (_reader.TokenType == JsonTokenType.EndObject)
        {
            return;
        }

        throw GraphEnvelopeSchema.Violation(
            path,
            _reader.TokenType == JsonTokenType.PropertyName
                ? $"the object carries the property '{RawText()}', which format version {GraphDocument.CurrentFormatVersion} does not define at this position; an unknown property is rejected rather than ignored"
                : $"the end of the object was expected but {DescribeToken()} was found");
    }

    /// <summary>
    /// Rejects any byte beyond the document object.
    /// </summary>
    private void EnsureNoTrailingContent()
    {
        if (_tokenEnd != _input.Length)
        {
            throw GraphEnvelopeSchema.Violation(
                GraphEnvelopeSchema.RootPath,
                DescribeInsignificantByte((int)_tokenEnd) + ", and canonical bytes end with the document object");
        }
    }

    /// <summary>
    /// Describes a byte that has no place in the minified canonical form.
    /// </summary>
    /// <param name="offset">The offset of the byte.</param>
    /// <returns>A sentence fragment naming the byte and its offset.</returns>
    private readonly string DescribeInsignificantByte(int offset) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"the byte 0x{_input[offset]:x2} at offset {offset} is not part of the minified canonical form, which carries no whitespace and no repeated separator");

    /// <summary>
    /// Describes the current token for a diagnostic.
    /// </summary>
    /// <returns>A noun phrase such as <c>the number 2</c> or <c>an array</c>.</returns>
    private string DescribeToken() =>
        _reader.TokenType switch
        {
            JsonTokenType.StartObject => "an object",
            JsonTokenType.EndObject => "the end of an object",
            JsonTokenType.StartArray => "an array",
            JsonTokenType.EndArray => "the end of an array",
            JsonTokenType.PropertyName => $"the property '{RawText()}'",
            JsonTokenType.String => $"the string \"{RawText()}\"",
            JsonTokenType.Number => $"the number {RawText()}",
            JsonTokenType.True => "the value true",
            JsonTokenType.False => "the value false",
            JsonTokenType.Null => "the value null",
            _ => "no value at all",
        };

    /// <summary>
    /// Renders the raw bytes of the current token for a diagnostic.
    /// </summary>
    /// <returns>The token text, truncated when it is long.</returns>
    /// <remarks>
    /// The bytes are rendered as they appear in the input, not as they decode, so a diagnostic about an
    /// escaped or malformed value shows what is actually there. Truncation keeps a hostile input from
    /// turning a rejection into a memory problem of its own.
    /// </remarks>
    private string RawText()
    {
        ReadOnlySpan<byte> value = _reader.ValueSpan;

        return GraphEnvelopeSchema.Truncate(
            Encoding.UTF8.GetString(value[..Math.Min(value.Length, GraphEnvelopeSchema.MaxQuotedLength)]));
    }
}
