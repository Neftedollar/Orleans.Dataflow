using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Compilation;

/// <summary>
/// Checks a graph document against a stage catalog and reports every catalog rule it breaks.
/// </summary>
/// <remarks>
/// <para>
/// Structural validity is catalog-free and is already guaranteed by
/// <see cref="GraphDocument.Create"/>: identifiers are unique, every endpoint names a declared node, no
/// edge is a self-loop, and no port address carries more than one edge. This type checks the rules that
/// need a catalog, which are the ones about what the referenced stages actually declare.
/// </para>
/// <para>
/// M0 scope is validation only. Producing a runnable plan is the local runtime's job, and it arrives
/// with the milestone that can execute one; a compiler that emitted an artifact nothing could run would
/// be a contract nobody tested.
/// </para>
/// <para>
/// <see cref="Validate"/> never throws for a semantic problem. An unresolvable stage, a contract
/// mismatch, and a payload the stage rejects are all expected outcomes of checking an untrusted document
/// and are reported as diagnostics. The only exceptions it raises are for a <see langword="null"/>
/// argument and for a catalog that breaks its own contract, which is a defect in the registered catalog
/// rather than a statement about the document.
/// </para>
/// <para>
/// <b>Rules.</b> The eleven identifiers below are stable and each is documented and tested.
/// </para>
/// <list type="table">
/// <listheader><term>Rule</term><description>Meaning</description></listheader>
/// <item>
/// <term><c>unknown-stage</c></term>
/// <description>A node's stage reference resolves to nothing in the catalog.</description>
/// </item>
/// <item>
/// <term><c>unknown-output-port</c></term>
/// <description>An edge origin names a port the specification does not declare as an output.</description>
/// </item>
/// <item>
/// <term><c>unknown-input-port</c></term>
/// <description>An edge target names a port the specification does not declare as an input.</description>
/// </item>
/// <item>
/// <term><c>unknown-result-port</c></term>
/// <description>A slot producer names a port the specification does not declare as a result port.</description>
/// </item>
/// <item>
/// <term><c>element-contract-mismatch</c></term>
/// <description>An edge connects an output and an input whose element contracts differ.</description>
/// </item>
/// <item>
/// <term><c>result-contract-mismatch</c></term>
/// <description>A slot's declared contract differs from the specification's result-port contract.</description>
/// </item>
/// <item>
/// <term><c>parameter-contract-mismatch</c></term>
/// <description>A node's declared parameter contract differs from the specification's.</description>
/// </item>
/// <item>
/// <term><c>invalid-parameters</c></term>
/// <description>The specification's validator rejected the payload, one diagnostic per fragment.</description>
/// </item>
/// <item>
/// <term><c>unconnected-input-port</c></term>
/// <description>A non-optional input port has no edge.</description>
/// </item>
/// <item>
/// <term><c>unconnected-output-port</c></term>
/// <description>A non-ignorable output port has no edge.</description>
/// </item>
/// <item>
/// <term><c>undeclared-capability</c></term>
/// <description>A required capability of a used stage is not declared by the document.</description>
/// </item>
/// </list>
/// <para>
/// <b>Gating.</b> A rule is evaluated only when its own inputs are well formed, the same way structural
/// validation gates. A node whose stage does not resolve contributes exactly one diagnostic and then
/// takes part in nothing else: no port, contract, parameter, connectivity, or capability rule is
/// evaluated for it, and an edge or slot that touches it is checked only at its other, resolved end. A
/// node whose declared parameter contract does not match the specification's is reported once and its
/// payload is not handed to a validator that was written for a different contract. The report therefore
/// carries what is actually wrong rather than a cascade from one root cause.
/// </para>
/// <para>
/// <b>Order.</b> Diagnostics appear in document order, in five phases, which makes a report reproducible
/// element for element:
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// nodes, in node order: for each node either <c>unknown-stage</c>, or
/// <c>parameter-contract-mismatch</c>, or one <c>invalid-parameters</c> per fragment the validator
/// returned, in the validator's order;
/// </description>
/// </item>
/// <item>
/// <description>
/// edges, in edge order: <c>unknown-output-port</c>, then <c>unknown-input-port</c>, then
/// <c>element-contract-mismatch</c>;
/// </description>
/// </item>
/// <item>
/// <description>
/// result slots, in slot order: <c>unknown-result-port</c>, or <c>result-contract-mismatch</c>;
/// </description>
/// </item>
/// <item>
/// <description>
/// connectivity, in node order and, within a node, input ports before output ports, each in the
/// specification's canonical port order: <c>unconnected-input-port</c> and
/// <c>unconnected-output-port</c>;
/// </description>
/// </item>
/// <item>
/// <description>capabilities: one <c>undeclared-capability</c> per missing token, in ordinal token order.</description>
/// </item>
/// </list>
/// <para>
/// Connectivity is a phase of its own rather than part of the node phase because it is a statement about
/// the edges of the whole document, not about one node in isolation, and reporting it after the edge and
/// slot rules keeps every diagnostic about an element the author wrote before the ones about an element
/// the author did not write.
/// </para>
/// <para>
/// Format version is deliberately not a rule here. A <see cref="GraphDocument"/> instance always carries
/// the current version by construction, and rejecting an unknown version belongs to the reader, which is
/// where bytes of an unknown version actually arrive.
/// </para>
/// <para>
/// Execution policy contracts are not validated against specifications in M0, because a specification
/// does not yet declare which policy contracts a stage accepts. This is recorded in the design document
/// rather than silently skipped.
/// </para>
/// </remarks>
public static class GraphCompiler
{
    /// <summary>A node's stage reference resolves to nothing in the catalog.</summary>
    private const string UnknownStageRule = "unknown-stage";

    /// <summary>An edge origin names a port the specification does not declare as an output.</summary>
    private const string UnknownOutputPortRule = "unknown-output-port";

    /// <summary>An edge target names a port the specification does not declare as an input.</summary>
    private const string UnknownInputPortRule = "unknown-input-port";

    /// <summary>A slot producer names a port the specification does not declare as a result port.</summary>
    private const string UnknownResultPortRule = "unknown-result-port";

    /// <summary>An edge connects an output and an input whose element contracts differ.</summary>
    private const string ElementContractMismatchRule = "element-contract-mismatch";

    /// <summary>A slot's declared contract differs from the specification's result-port contract.</summary>
    private const string ResultContractMismatchRule = "result-contract-mismatch";

    /// <summary>A node's declared parameter contract differs from the specification's.</summary>
    private const string ParameterContractMismatchRule = "parameter-contract-mismatch";

    /// <summary>The specification's validator rejected the payload.</summary>
    private const string InvalidParametersRule = "invalid-parameters";

    /// <summary>A non-optional input port has no edge.</summary>
    private const string UnconnectedInputPortRule = "unconnected-input-port";

    /// <summary>A non-ignorable output port has no edge.</summary>
    private const string UnconnectedOutputPortRule = "unconnected-output-port";

    /// <summary>A required capability of a used stage is not declared by the document.</summary>
    private const string UndeclaredCapabilityRule = "undeclared-capability";

    /// <summary>
    /// Checks <paramref name="document"/> against <paramref name="catalog"/>.
    /// </summary>
    /// <param name="document">The structurally valid document to check.</param>
    /// <param name="catalog">The catalog its stage references are resolved through.</param>
    /// <returns>
    /// A report carrying the document and every diagnostic found, in the documented order; the report is
    /// valid exactly when the list is empty.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="document"/> or <paramref name="catalog"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The catalog broke its own contract, either by resolving a reference and then supplying no
    /// specification, or through a stage parameter validator that returned <see langword="null"/> or a
    /// blank violation fragment. That is a defect in the registered catalog, not a property of the
    /// document, so it is raised rather than reported as if the graph were at fault.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Validation is a pure function of the document and the catalog. It reads no clock, no ambient
    /// culture, and no environment, and it calls nothing but the catalog's lookup and, where a stage
    /// registers one, that stage's parameter validator. Two calls with the same arguments therefore
    /// produce equal reports, element for element, and two silos validating one document agree.
    /// </para>
    /// <para>
    /// A document with no nodes is valid against every catalog, including an empty one: it breaks no rule
    /// because it references nothing.
    /// </para>
    /// </remarks>
    public static GraphValidationReport Validate(GraphDocument document, IStageCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(catalog);

        List<GraphValidationDiagnostic> diagnostics = [];
        Dictionary<NodeId, StageSpecification> resolved = new(document.Nodes.Count);

        ValidateNodes(document, catalog, resolved, diagnostics);
        ValidateEdges(document, resolved, diagnostics);
        ValidateResultSlots(document, resolved, diagnostics);
        ValidateConnectivity(document, resolved, diagnostics);
        ValidateCapabilities(document, resolved, diagnostics);

        return new GraphValidationReport(document, diagnostics.AsReadOnly());
    }

    /// <summary>
    /// Resolves every node against the catalog and checks its parameters.
    /// </summary>
    /// <param name="document">The document being checked.</param>
    /// <param name="catalog">The catalog to resolve through.</param>
    /// <param name="resolved">
    /// The map this method fills with the specification of every node that resolved; the later phases
    /// read it and skip whatever is missing from it.
    /// </param>
    /// <param name="diagnostics">The report under construction.</param>
    private static void ValidateNodes(
        GraphDocument document,
        IStageCatalog catalog,
        Dictionary<NodeId, StageSpecification> resolved,
        List<GraphValidationDiagnostic> diagnostics)
    {
        for (int index = 0; index < document.Nodes.Count; index++)
        {
            StageNode node = document.Nodes[index];

            if (!catalog.TryGetSpecification(node.Stage, out StageSpecification? specification))
            {
                diagnostics.Add(GraphValidationDiagnostic.Create(
                    UnknownStageRule,
                    $"the node '{node.Id}' references the stage '{node.Stage}', which this catalog does not register",
                    node.Id.Value));

                continue;
            }

            // Unreachable through StageCatalog, and the interface's nullable annotation makes "resolved
            // implies a specification" an obligation the compiler enforces on every implementer that has
            // nullable reference types enabled. The rule is restated here because the catalog is a public
            // seam that a later heterogeneous or federated implementation fills, possibly from an
            // assembly under no such obligation, and a broken seam should name itself rather than surface
            // as a dereference of nothing several rules later.
            if (specification is null)
            {
                throw new InvalidOperationException(
                    $"The catalog resolved the stage '{node.Stage}' and then supplied no specification for it. A catalog that answers that a reference is registered must hand back the specification that registers it.");
            }

            resolved.Add(node.Id, specification);

            if (node.ParameterContract != specification.ParameterContract)
            {
                diagnostics.Add(GraphValidationDiagnostic.Create(
                    ParameterContractMismatchRule,
                    $"the node '{node.Id}' declares the parameter contract '{node.ParameterContract}', and the stage '{specification.Stage}' declares '{specification.ParameterContract}'",
                    node.Id.Value));

                continue;
            }

            ValidateParameters(node, specification, diagnostics);
        }
    }

    /// <summary>
    /// Runs a stage's parameter validator, when it has one, over a node's payload.
    /// </summary>
    /// <param name="node">The node whose payload is checked.</param>
    /// <param name="specification">The resolved specification whose parameter contract the node matched.</param>
    /// <param name="diagnostics">The report under construction.</param>
    /// <exception cref="InvalidOperationException">The validator broke its own contract.</exception>
    /// <remarks>
    /// The validator runs only after the declared parameter contract matched. A payload written for a
    /// different contract is already reported as a mismatch, and handing it to a check written for this
    /// contract would produce a second, derived complaint about a payload that was never meant for it.
    /// </remarks>
    private static void ValidateParameters(
        StageNode node,
        StageSpecification specification,
        List<GraphValidationDiagnostic> diagnostics)
    {
        if (specification.ParameterValidator is not { } validator)
        {
            return;
        }

        IReadOnlyList<string>? fragments = validator.Validate(node.Parameters);

        if (fragments is null)
        {
            throw BrokenValidator(
                specification,
                "it returned no list at all, and the contract is an empty list for a valid payload");
        }

        for (int index = 0; index < fragments.Count; index++)
        {
            string fragment = fragments[index];

            if (string.IsNullOrWhiteSpace(fragment))
            {
                throw BrokenValidator(
                    specification,
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"the fragment at index {index} is blank, and every fragment states one violation"));
            }

            diagnostics.Add(GraphValidationDiagnostic.Create(
                InvalidParametersRule,
                $"the node '{node.Id}' carries parameters the stage '{specification.Stage}' rejects: {fragment}",
                node.Id.Value));
        }
    }

    /// <summary>
    /// Checks every edge against the specifications of the nodes it connects.
    /// </summary>
    /// <param name="document">The document being checked.</param>
    /// <param name="resolved">The specifications of the nodes that resolved.</param>
    /// <param name="diagnostics">The report under construction.</param>
    /// <remarks>
    /// Each end is checked independently, so an edge between two unresolved nodes contributes nothing of
    /// its own and an edge with one unresolved end is still checked at the end that is known. The element
    /// contracts are compared only when both ports were found, because a contract that no port declares
    /// is not a contract to compare against.
    /// </remarks>
    private static void ValidateEdges(
        GraphDocument document,
        Dictionary<NodeId, StageSpecification> resolved,
        List<GraphValidationDiagnostic> diagnostics)
    {
        for (int index = 0; index < document.Edges.Count; index++)
        {
            GraphEdge edge = document.Edges[index];
            ContractReference originContract = default;
            ContractReference targetContract = default;

            if (resolved.TryGetValue(edge.From.Node, out StageSpecification? originSpecification))
            {
                if (TryFindOutputPort(originSpecification, edge.From.Port, out OutputPortSpecification origin))
                {
                    originContract = origin.ElementContract;
                }
                else
                {
                    diagnostics.Add(GraphValidationDiagnostic.Create(
                        UnknownOutputPortRule,
                        $"the edge from '{edge.From}' names a port the stage '{originSpecification.Stage}' does not declare as an output",
                        edge.From.ToString()));
                }
            }

            if (resolved.TryGetValue(edge.To.Node, out StageSpecification? targetSpecification))
            {
                if (TryFindInputPort(targetSpecification, edge.To.Port, out InputPortSpecification target))
                {
                    targetContract = target.ElementContract;
                }
                else
                {
                    diagnostics.Add(GraphValidationDiagnostic.Create(
                        UnknownInputPortRule,
                        $"the edge to '{edge.To}' names a port the stage '{targetSpecification.Stage}' does not declare as an input",
                        edge.To.ToString()));
                }
            }

            if (!originContract.IsDefault && !targetContract.IsDefault && originContract != targetContract)
            {
                diagnostics.Add(GraphValidationDiagnostic.Create(
                    ElementContractMismatchRule,
                    $"the edge '{edge}' leaves '{edge.From}' carrying elements of contract '{originContract}' and enters '{edge.To}', which accepts '{targetContract}'",
                    edge.ToString()));
            }
        }
    }

    /// <summary>
    /// Checks every result slot against the specification of the node that produces it.
    /// </summary>
    /// <param name="document">The document being checked.</param>
    /// <param name="resolved">The specifications of the nodes that resolved.</param>
    /// <param name="diagnostics">The report under construction.</param>
    /// <remarks>
    /// The subject of both slot rules is the slot name rather than the producing address, because two
    /// slots may deliberately share one producer and only the slot name says which of them is wrong. The
    /// address is named in the message.
    /// </remarks>
    private static void ValidateResultSlots(
        GraphDocument document,
        Dictionary<NodeId, StageSpecification> resolved,
        List<GraphValidationDiagnostic> diagnostics)
    {
        for (int index = 0; index < document.ResultSlots.Count; index++)
        {
            ResultSlotDefinition slot = document.ResultSlots[index];

            if (!resolved.TryGetValue(slot.Producer.Node, out StageSpecification? specification))
            {
                continue;
            }

            if (!TryFindResultPort(specification, slot.Producer.Port, out ResultPortSpecification port))
            {
                diagnostics.Add(GraphValidationDiagnostic.Create(
                    UnknownResultPortRule,
                    $"the result slot '{slot.Id}' is produced by '{slot.Producer}', which the stage '{specification.Stage}' does not declare as a result port",
                    slot.Id.Value));

                continue;
            }

            if (slot.ResultContract != port.ResultContract)
            {
                diagnostics.Add(GraphValidationDiagnostic.Create(
                    ResultContractMismatchRule,
                    $"the result slot '{slot.Id}' declares the result contract '{slot.ResultContract}', and the result port '{slot.Producer}' of the stage '{specification.Stage}' yields '{port.ResultContract}'",
                    slot.Id.Value));
            }
        }
    }

    /// <summary>
    /// Checks that every port a stage requires to be wired actually carries an edge.
    /// </summary>
    /// <param name="document">The document being checked.</param>
    /// <param name="resolved">The specifications of the nodes that resolved.</param>
    /// <param name="diagnostics">The report under construction.</param>
    /// <remarks>
    /// <para>
    /// The structural invariants already guarantee at most one edge per input address and at most one per
    /// output address, so "connected exactly once" reduces to "connected at all" and this phase only has
    /// to look for presence.
    /// </para>
    /// <para>
    /// An edge counts as connecting its port even when its other end is broken. The edge is in the
    /// document, and reporting the port as unconnected as well would be a second complaint about one
    /// mistake.
    /// </para>
    /// <para>
    /// Result ports are absent from this phase by design: a result is read through a slot, and nothing
    /// requires a graph to expose one.
    /// </para>
    /// </remarks>
    private static void ValidateConnectivity(
        GraphDocument document,
        Dictionary<NodeId, StageSpecification> resolved,
        List<GraphValidationDiagnostic> diagnostics)
    {
        if (resolved.Count == 0)
        {
            return;
        }

        HashSet<PortAddress> origins = [];
        HashSet<PortAddress> targets = [];

        for (int index = 0; index < document.Edges.Count; index++)
        {
            GraphEdge edge = document.Edges[index];
            origins.Add(edge.From);
            targets.Add(edge.To);
        }

        for (int index = 0; index < document.Nodes.Count; index++)
        {
            StageNode node = document.Nodes[index];

            if (!resolved.TryGetValue(node.Id, out StageSpecification? specification))
            {
                continue;
            }

            for (int portIndex = 0; portIndex < specification.InputPorts.Count; portIndex++)
            {
                InputPortSpecification port = specification.InputPorts[portIndex];

                if (port.IsOptional)
                {
                    continue;
                }

                PortAddress address = PortAddress.Create(node.Id, port.Id);

                if (!targets.Contains(address))
                {
                    diagnostics.Add(GraphValidationDiagnostic.Create(
                        UnconnectedInputPortRule,
                        $"the input port '{address}' of the stage '{specification.Stage}' is not optional, and no edge terminates at it",
                        address.ToString()));
                }
            }

            for (int portIndex = 0; portIndex < specification.OutputPorts.Count; portIndex++)
            {
                OutputPortSpecification port = specification.OutputPorts[portIndex];

                if (port.IsIgnorable)
                {
                    continue;
                }

                PortAddress address = PortAddress.Create(node.Id, port.Id);

                if (!origins.Contains(address))
                {
                    diagnostics.Add(GraphValidationDiagnostic.Create(
                        UnconnectedOutputPortRule,
                        $"the output port '{address}' of the stage '{specification.Stage}' is not ignorable, and no edge originates at it",
                        address.ToString()));
                }
            }
        }
    }

    /// <summary>
    /// Checks that the document declares every capability its stages require.
    /// </summary>
    /// <param name="document">The document being checked.</param>
    /// <param name="resolved">The specifications of the nodes that resolved.</param>
    /// <param name="diagnostics">The report under construction.</param>
    /// <remarks>
    /// The rule is one-directional: a document may declare a capability no stage requires, which is how
    /// an author states a fact about the graph that this version's stages do not express. Only the
    /// missing direction is a violation.
    /// </remarks>
    private static void ValidateCapabilities(
        GraphDocument document,
        Dictionary<NodeId, StageSpecification> resolved,
        List<GraphValidationDiagnostic> diagnostics)
    {
        HashSet<CapabilityToken> required = [];

        for (int index = 0; index < document.Nodes.Count; index++)
        {
            if (!resolved.TryGetValue(document.Nodes[index].Id, out StageSpecification? specification))
            {
                continue;
            }

            for (int tokenIndex = 0; tokenIndex < specification.RequiredCapabilities.Count; tokenIndex++)
            {
                required.Add(specification.RequiredCapabilities[tokenIndex]);
            }
        }

        if (required.Count == 0)
        {
            return;
        }

        for (int index = 0; index < document.Capabilities.Count; index++)
        {
            required.Remove(document.Capabilities[index]);
        }

        List<CapabilityToken> missing = [.. required];

        // The tokens are distinct, so ordinal order over their text is total and an unstable sort still
        // yields one deterministic order. Sorting is what keeps the report independent of the order the
        // nodes happened to contribute their requirements in.
        missing.Sort(static (left, right) => string.CompareOrdinal(left.Value, right.Value));

        for (int index = 0; index < missing.Count; index++)
        {
            CapabilityToken token = missing[index];

            diagnostics.Add(GraphValidationDiagnostic.Create(
                UndeclaredCapabilityRule,
                $"the stages of this document require the capability '{token}', which the document does not declare",
                token.Value));
        }
    }

    /// <summary>
    /// Finds an input port by name on a specification.
    /// </summary>
    /// <param name="specification">The specification to search.</param>
    /// <param name="port">The port name to find.</param>
    /// <param name="found">The declared port, or the default value when there is none.</param>
    /// <returns><see langword="true"/> when the specification declares the port as an input.</returns>
    /// <remarks>
    /// The scan is linear because a stage declares a handful of ports, and a linear scan over a few
    /// elements beats building a lookup per node. The port lists are in canonical order, so the scan also
    /// visits them in that order.
    /// </remarks>
    private static bool TryFindInputPort(
        StageSpecification specification,
        PortId port,
        out InputPortSpecification found)
    {
        for (int index = 0; index < specification.InputPorts.Count; index++)
        {
            InputPortSpecification candidate = specification.InputPorts[index];

            if (candidate.Id == port)
            {
                found = candidate;
                return true;
            }
        }

        found = default;
        return false;
    }

    /// <summary>
    /// Finds an output port by name on a specification.
    /// </summary>
    /// <param name="specification">The specification to search.</param>
    /// <param name="port">The port name to find.</param>
    /// <param name="found">The declared port, or the default value when there is none.</param>
    /// <returns><see langword="true"/> when the specification declares the port as an output.</returns>
    private static bool TryFindOutputPort(
        StageSpecification specification,
        PortId port,
        out OutputPortSpecification found)
    {
        for (int index = 0; index < specification.OutputPorts.Count; index++)
        {
            OutputPortSpecification candidate = specification.OutputPorts[index];

            if (candidate.Id == port)
            {
                found = candidate;
                return true;
            }
        }

        found = default;
        return false;
    }

    /// <summary>
    /// Finds a result port by name on a specification.
    /// </summary>
    /// <param name="specification">The specification to search.</param>
    /// <param name="port">The port name to find.</param>
    /// <param name="found">The declared port, or the default value when there is none.</param>
    /// <returns><see langword="true"/> when the specification declares the port as a result port.</returns>
    private static bool TryFindResultPort(
        StageSpecification specification,
        PortId port,
        out ResultPortSpecification found)
    {
        for (int index = 0; index < specification.ResultPorts.Count; index++)
        {
            ResultPortSpecification candidate = specification.ResultPorts[index];

            if (candidate.Id == port)
            {
                found = candidate;
                return true;
            }
        }

        found = default;
        return false;
    }

    /// <summary>
    /// Builds the error for a parameter validator that broke its own contract.
    /// </summary>
    /// <param name="specification">The specification whose validator misbehaved.</param>
    /// <param name="violation">A sentence fragment naming what the validator did.</param>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// This is raised rather than reported as a diagnostic because it is not a statement about the
    /// document: no edit to the graph could fix it, and reporting it as a graph problem would send whoever
    /// reads the report to the wrong place. The message names the stage so that the defective
    /// registration can be found.
    /// </remarks>
    private static InvalidOperationException BrokenValidator(
        StageSpecification specification,
        string violation) =>
        new($"The parameter validator registered for the stage '{specification.Stage}' broke its contract: {violation}. A validator returns one lower-case fragment per violation and an empty list for a valid payload.");
}
