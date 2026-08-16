using System.Globalization;
using System.Text;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// What one registered stage family declares about itself: its ports, its parameter contract, the
/// capabilities it requires, and an optional check on its parameter payloads.
/// </summary>
/// <remarks>
/// <para>
/// A specification is the catalog side of the boundary a <see cref="StageRef"/> crosses. Deployment code
/// registers it; graph data only names it. Nothing here can be reached by editing a document, which is
/// what keeps graph data from causing code loading (ADR 0001).
/// </para>
/// <para>
/// A specification is canonical by construction. <see cref="Create(StageRef, IEnumerable{InputPortSpecification}, IEnumerable{OutputPortSpecification}, IEnumerable{ResultPortSpecification}, ContractReference, IEnumerable{CapabilityToken})"/>
/// sorts each port list ordinally by port name and the capability tokens ordinally by text, so two
/// specifications built from the same elements in different orders are indistinguishable afterwards,
/// element for element, and serialize to identical bytes.
/// </para>
/// <para>
/// Port names are unique across the whole stage: inputs, outputs, and result ports share one namespace.
/// A diagnostic can therefore name a port without also naming its direction, and an address like
/// <c>mapper#out</c> means exactly one port of the resolved specification.
/// </para>
/// <para>
/// A specification is valid by construction. The factory reports all violations it finds at once, not
/// merely the first, in one numbered message.
/// </para>
/// <para>
/// Equality is structural over the declared shape and deliberately excludes
/// <see cref="ParameterValidator"/>. Behavior is not shape: two specifications that declare the same
/// ports, the same parameter contract, and the same capabilities describe the same stage contract, and
/// they serialize to the same bytes and share a <see cref="CatalogFingerprint"/> whatever their
/// validators do. Making equality depend on a validator instance would mean two values that agree on
/// every serialized member, and therefore on every byte of their identity, compared unequal; validator
/// behavior is a deployment concern, and this limit is stated rather than hidden.
/// </para>
/// </remarks>
public sealed record class StageSpecification
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StageSpecification"/> class.
    /// </summary>
    /// <param name="stage">The validated stage reference.</param>
    /// <param name="inputPorts">The validated, canonically ordered, read-only input ports.</param>
    /// <param name="outputPorts">The validated, canonically ordered, read-only output ports.</param>
    /// <param name="resultPorts">The validated, canonically ordered, read-only result ports.</param>
    /// <param name="parameterContract">The validated parameter contract reference.</param>
    /// <param name="requiredCapabilities">
    /// The validated, canonically ordered, read-only required capability tokens.
    /// </param>
    /// <param name="parameterValidator">The parameter validator, or <see langword="null"/>.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a specification cannot be built or
    /// amended around the factory: a <c>with</c> expression has no member it is allowed to change.
    /// </remarks>
    private StageSpecification(
        StageRef stage,
        IReadOnlyList<InputPortSpecification> inputPorts,
        IReadOnlyList<OutputPortSpecification> outputPorts,
        IReadOnlyList<ResultPortSpecification> resultPorts,
        ContractReference parameterContract,
        IReadOnlyList<CapabilityToken> requiredCapabilities,
        IStageParameterValidator? parameterValidator)
    {
        Stage = stage;
        InputPorts = inputPorts;
        OutputPorts = outputPorts;
        ResultPorts = resultPorts;
        ParameterContract = parameterContract;
        RequiredCapabilities = requiredCapabilities;
        ParameterValidator = parameterValidator;
    }

    /// <summary>
    /// Gets the reference every node that uses this stage names.
    /// </summary>
    /// <value>A created <see cref="StageRef"/>.</value>
    public StageRef Stage { get; }

    /// <summary>
    /// Gets the input ports this stage declares.
    /// </summary>
    /// <value>A read-only list in ordinal order of port name; empty when the stage consumes nothing.</value>
    public IReadOnlyList<InputPortSpecification> InputPorts { get; }

    /// <summary>
    /// Gets the output ports this stage declares.
    /// </summary>
    /// <value>A read-only list in ordinal order of port name; empty when the stage produces nothing.</value>
    public IReadOnlyList<OutputPortSpecification> OutputPorts { get; }

    /// <summary>
    /// Gets the result ports this stage declares.
    /// </summary>
    /// <value>A read-only list in ordinal order of port name; empty when the stage yields no result.</value>
    public IReadOnlyList<ResultPortSpecification> ResultPorts { get; }

    /// <summary>
    /// Gets the contract a node's parameter payload must declare to use this stage.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <remarks>
    /// A node stores its own parameter contract, so a document written against an earlier major version
    /// is reported as a mismatch by the graph compiler rather than reinterpreted under this one.
    /// </remarks>
    public ContractReference ParameterContract { get; }

    /// <summary>
    /// Gets the capability tokens a document must declare to use this stage.
    /// </summary>
    /// <value>
    /// A read-only list of distinct tokens in ordinal order of their text; empty when the stage requires
    /// nothing of its host.
    /// </value>
    public IReadOnlyList<CapabilityToken> RequiredCapabilities { get; }

    /// <summary>
    /// Gets the check this stage applies to a node's parameter payload.
    /// </summary>
    /// <value>
    /// The validator, or <see langword="null"/> when the stage accepts any payload that declares its
    /// parameter contract.
    /// </value>
    /// <remarks>
    /// The validator is behavior, so it is excluded from equality, from hashing, and from serialization:
    /// two specifications differing only here are equal values with one fingerprint. What a validator
    /// accepts is a property of the deployment that registered it, not of the stage contract the catalog
    /// publishes.
    /// </remarks>
    public IStageParameterValidator? ParameterValidator { get; }

    /// <summary>
    /// Creates a canonical, valid <see cref="StageSpecification"/> without a parameter validator.
    /// </summary>
    /// <param name="stage">The stage reference; must not be the default value.</param>
    /// <param name="inputPorts">The input ports, in any order.</param>
    /// <param name="outputPorts">The output ports, in any order.</param>
    /// <param name="resultPorts">The result ports, in any order.</param>
    /// <param name="parameterContract">The parameter contract; must not be the default value.</param>
    /// <param name="requiredCapabilities">The required capability tokens, in any order, without duplicates.</param>
    /// <returns>The validated specification, with every collection in canonical order.</returns>
    /// <exception cref="ArgumentNullException">Any sequence argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// The inputs break at least one invariant. The message is a numbered list of every violation found,
    /// so one call reports every problem rather than one problem per call.
    /// </exception>
    public static StageSpecification Create(
        StageRef stage,
        IEnumerable<InputPortSpecification> inputPorts,
        IEnumerable<OutputPortSpecification> outputPorts,
        IEnumerable<ResultPortSpecification> resultPorts,
        ContractReference parameterContract,
        IEnumerable<CapabilityToken> requiredCapabilities) =>
        CreateCore(
            stage,
            inputPorts,
            outputPorts,
            resultPorts,
            parameterContract,
            requiredCapabilities,
            parameterValidator: null);

    /// <summary>
    /// Creates a canonical, valid <see cref="StageSpecification"/> with a parameter validator.
    /// </summary>
    /// <param name="stage">The stage reference; must not be the default value.</param>
    /// <param name="inputPorts">The input ports, in any order.</param>
    /// <param name="outputPorts">The output ports, in any order.</param>
    /// <param name="resultPorts">The result ports, in any order.</param>
    /// <param name="parameterContract">The parameter contract; must not be the default value.</param>
    /// <param name="requiredCapabilities">The required capability tokens, in any order, without duplicates.</param>
    /// <param name="parameterValidator">The check to apply to parameter payloads.</param>
    /// <returns>The validated specification, with every collection in canonical order.</returns>
    /// <exception cref="ArgumentNullException">
    /// Any sequence argument or <paramref name="parameterValidator"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The inputs break at least one invariant. The message is a numbered list of every violation found.
    /// </exception>
    /// <remarks>
    /// The validator is an overload rather than a nullable parameter so that a caller cannot pass
    /// <see langword="null"/> and silently get a stage that validates nothing: a stage without a check
    /// says so by calling the other overload.
    /// </remarks>
    public static StageSpecification Create(
        StageRef stage,
        IEnumerable<InputPortSpecification> inputPorts,
        IEnumerable<OutputPortSpecification> outputPorts,
        IEnumerable<ResultPortSpecification> resultPorts,
        ContractReference parameterContract,
        IEnumerable<CapabilityToken> requiredCapabilities,
        IStageParameterValidator parameterValidator)
    {
        ArgumentNullException.ThrowIfNull(parameterValidator);

        return CreateCore(
            stage,
            inputPorts,
            outputPorts,
            resultPorts,
            parameterContract,
            requiredCapabilities,
            parameterValidator);
    }

    /// <summary>
    /// Determines whether this specification declares the same stage contract as <paramref name="other"/>.
    /// </summary>
    /// <param name="other">The specification to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when both declare the same stage, the same parameter contract, and
    /// element-wise equal port and capability collections; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The synthesized record equality would compare the collection properties by reference, which would
    /// make two independently built copies of one specification unequal. Comparison is therefore
    /// element-wise over the collections. Because construction already put them in canonical order,
    /// element-wise comparison is order-insensitive with respect to the caller's input while staying a
    /// cheap linear scan.
    /// </para>
    /// <para>
    /// <see cref="ParameterValidator"/> takes no part in the comparison. Equality is over the declared
    /// shape, which is exactly what a catalog serializes and fingerprints, so two specifications that
    /// agree on every byte of their identity are equal values even when their validators differ.
    /// </para>
    /// </remarks>
    public bool Equals(StageSpecification? other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return other is not null &&
            Stage == other.Stage &&
            ParameterContract == other.ParameterContract &&
            SequenceEquals(InputPorts, other.InputPorts) &&
            SequenceEquals(OutputPorts, other.OutputPorts) &&
            SequenceEquals(ResultPorts, other.ResultPorts) &&
            SequenceEquals(RequiredCapabilities, other.RequiredCapabilities);
    }

    /// <summary>
    /// Returns a hash code over the stage, the parameter contract, and every collection element.
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(StageSpecification)"/>.</returns>
    /// <remarks>
    /// <see cref="ParameterValidator"/> is excluded for the same reason it is excluded from equality.
    /// This is a hash-table hash, not a durable identity: <see cref="HashCode"/> is seeded per process,
    /// so the same specification hashes differently in a different process. The durable identity of a
    /// catalog is the SHA-256 of its canonical bytes, never this number.
    /// </remarks>
    public override int GetHashCode()
    {
        HashCode hash = default;

        hash.Add(Stage);
        hash.Add(ParameterContract);
        AddSequence(ref hash, InputPorts);
        AddSequence(ref hash, OutputPorts);
        AddSequence(ref hash, ResultPorts);
        AddSequence(ref hash, RequiredCapabilities);

        return hash.ToHashCode();
    }

    /// <summary>
    /// Returns a one-line diagnostic summary of this specification.
    /// </summary>
    /// <returns>Text of the form <c>orleans-core/map-async@v2 (2 in, 1 out, 1 result)</c>.</returns>
    /// <remarks>
    /// The record-synthesized <c>ToString</c> would print every port of every list; a log line has no use
    /// for that. The counts are formatted with the invariant culture so that the text is identical under
    /// every ambient culture, and the method never throws.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{Stage} ({InputPorts.Count} in, {OutputPorts.Count} out, {ResultPorts.Count} result)");

    /// <summary>
    /// Validates, orders, and builds a specification whatever its validator.
    /// </summary>
    /// <param name="stage">The candidate stage reference.</param>
    /// <param name="inputPorts">The candidate input ports.</param>
    /// <param name="outputPorts">The candidate output ports.</param>
    /// <param name="resultPorts">The candidate result ports.</param>
    /// <param name="parameterContract">The candidate parameter contract.</param>
    /// <param name="requiredCapabilities">The candidate required capability tokens.</param>
    /// <param name="parameterValidator">The validator, or <see langword="null"/>.</param>
    /// <returns>The validated specification.</returns>
    /// <exception cref="ArgumentNullException">Any sequence argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The inputs break at least one invariant.</exception>
    /// <remarks>
    /// Each sequence is enumerated exactly once and copied, so a caller may pass a lazy sequence and may
    /// keep mutating its own collection afterwards without affecting the specification.
    /// </remarks>
    private static StageSpecification CreateCore(
        StageRef stage,
        IEnumerable<InputPortSpecification> inputPorts,
        IEnumerable<OutputPortSpecification> outputPorts,
        IEnumerable<ResultPortSpecification> resultPorts,
        ContractReference parameterContract,
        IEnumerable<CapabilityToken> requiredCapabilities,
        IStageParameterValidator? parameterValidator)
    {
        ArgumentNullException.ThrowIfNull(inputPorts);
        ArgumentNullException.ThrowIfNull(outputPorts);
        ArgumentNullException.ThrowIfNull(resultPorts);
        ArgumentNullException.ThrowIfNull(requiredCapabilities);

        InputPortSpecification[] inputArray = [.. inputPorts];
        OutputPortSpecification[] outputArray = [.. outputPorts];
        ResultPortSpecification[] resultArray = [.. resultPorts];
        CapabilityToken[] capabilityArray = [.. requiredCapabilities];

        List<string> violations =
            Validate(stage, inputArray, outputArray, resultArray, parameterContract, capabilityArray);

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations(violations));
        }

        // Every sort key is unique on validated input: port names are unique across the whole stage, so
        // they are unique within each list, and capability tokens are distinct by rule. The order is
        // therefore total, and an unstable sort still yields one deterministic result for every
        // permutation of the same elements.
        Array.Sort(inputArray, static (left, right) => string.CompareOrdinal(left.Id.Value, right.Id.Value));
        Array.Sort(outputArray, static (left, right) => string.CompareOrdinal(left.Id.Value, right.Id.Value));
        Array.Sort(resultArray, static (left, right) => string.CompareOrdinal(left.Id.Value, right.Id.Value));
        Array.Sort(capabilityArray, static (left, right) => string.CompareOrdinal(left.Value, right.Value));

        return new StageSpecification(
            stage,
            Array.AsReadOnly(inputArray),
            Array.AsReadOnly(outputArray),
            Array.AsReadOnly(resultArray),
            parameterContract,
            Array.AsReadOnly(capabilityArray),
            parameterValidator);
    }

    /// <summary>
    /// Collects every invariant the candidate specification breaks.
    /// </summary>
    /// <param name="stage">The candidate stage reference.</param>
    /// <param name="inputPorts">The candidate input ports.</param>
    /// <param name="outputPorts">The candidate output ports.</param>
    /// <param name="resultPorts">The candidate result ports.</param>
    /// <param name="parameterContract">The candidate parameter contract.</param>
    /// <param name="requiredCapabilities">The candidate required capability tokens.</param>
    /// <returns>
    /// One lower-case sentence fragment per violation, in a deterministic order, or an empty list when
    /// the candidate is valid.
    /// </returns>
    /// <remarks>
    /// A rule is evaluated only when its own inputs are well formed: a default port is reported once and
    /// then left out of the name-uniqueness relation it would otherwise take part in, so the report
    /// carries no follow-on violation that would disappear on its own once the reported one is fixed.
    /// The three port lists are checked against one shared set of names, in the order the catalog envelope
    /// writes them, because inputs, outputs, and result ports share one namespace.
    /// </remarks>
    private static List<string> Validate(
        StageRef stage,
        InputPortSpecification[] inputPorts,
        OutputPortSpecification[] outputPorts,
        ResultPortSpecification[] resultPorts,
        ContractReference parameterContract,
        CapabilityToken[] requiredCapabilities)
    {
        List<string> violations = [];

        if (stage.IsDefault)
        {
            violations.Add($"the stage reference is the default {nameof(StageRef)}, which names no stage");
        }

        HashSet<PortId> declaredPorts = [];

        for (int index = 0; index < inputPorts.Length; index++)
        {
            InputPortSpecification port = inputPorts[index];

            if (port.IsDefault)
            {
                violations.Add(DescribeDefaultPort("inputPorts", index, nameof(InputPortSpecification)));
            }
            else if (!declaredPorts.Add(port.Id))
            {
                violations.Add(DescribeRepeatedPort("inputPorts", index, port.Id));
            }
        }

        for (int index = 0; index < outputPorts.Length; index++)
        {
            OutputPortSpecification port = outputPorts[index];

            if (port.IsDefault)
            {
                violations.Add(DescribeDefaultPort("outputPorts", index, nameof(OutputPortSpecification)));
            }
            else if (!declaredPorts.Add(port.Id))
            {
                violations.Add(DescribeRepeatedPort("outputPorts", index, port.Id));
            }
        }

        for (int index = 0; index < resultPorts.Length; index++)
        {
            ResultPortSpecification port = resultPorts[index];

            if (port.IsDefault)
            {
                violations.Add(DescribeDefaultPort("resultPorts", index, nameof(ResultPortSpecification)));
            }
            else if (!declaredPorts.Add(port.Id))
            {
                violations.Add(DescribeRepeatedPort("resultPorts", index, port.Id));
            }
        }

        if (parameterContract.IsDefault)
        {
            violations.Add(
                $"the parameter contract is the default {nameof(ContractReference)}, which names no contract");
        }

        HashSet<CapabilityToken> declaredCapabilities = [];

        for (int index = 0; index < requiredCapabilities.Length; index++)
        {
            CapabilityToken token = requiredCapabilities[index];

            if (token.IsDefault)
            {
                violations.Add(
                    $"requiredCapabilities[{index}] is the default {nameof(CapabilityToken)}, which names no capability");
            }
            else if (!declaredCapabilities.Add(token))
            {
                violations.Add(
                    $"requiredCapabilities[{index}] repeats the capability token '{token}', and a stage requires each token at most once");
            }
        }

        return violations;
    }

    /// <summary>Builds the message for a port supplied as its default value.</summary>
    /// <param name="listName">The name of the list the port arrived in.</param>
    /// <param name="index">The zero-based index of the port in that list.</param>
    /// <param name="typeName">The port specification type name.</param>
    /// <returns>A message naming the position and the type.</returns>
    private static string DescribeDefaultPort(string listName, int index, string typeName) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{listName}[{index}] is the default {typeName}, which declares no port");

    /// <summary>Builds the message for a port name that is already taken.</summary>
    /// <param name="listName">The name of the list the port arrived in.</param>
    /// <param name="index">The zero-based index of the port in that list.</param>
    /// <param name="port">The repeated port name.</param>
    /// <returns>A message naming the position, the name, and the one-namespace rule.</returns>
    private static string DescribeRepeatedPort(string listName, int index, PortId port) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{listName}[{index}] repeats the port id '{port}', and port ids are unique across the whole stage specification, inputs, outputs, and result ports together");

    /// <summary>
    /// Renders the collected violations as one numbered list.
    /// </summary>
    /// <param name="violations">The violations, in the order <see cref="Validate"/> found them.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    /// <remarks>
    /// The exception carries no parameter name because the invariants are relations between the
    /// arguments: an output port whose name an input port already took is not the fault of either
    /// argument alone. The numbered list is the diagnostic, and it names every offending identity.
    /// </remarks>
    private static string FormatViolations(List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The stage specification breaks {violations.Count} ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }

    /// <summary>Determines whether two lists hold equal elements in the same positions.</summary>
    /// <typeparam name="TElement">The element type.</typeparam>
    /// <param name="left">The left list.</param>
    /// <param name="right">The right list.</param>
    /// <returns><see langword="true"/> when the lists have equal length and equal elements.</returns>
    private static bool SequenceEquals<TElement>(IReadOnlyList<TElement> left, IReadOnlyList<TElement> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        EqualityComparer<TElement> comparer = EqualityComparer<TElement>.Default;

        for (int index = 0; index < left.Count; index++)
        {
            if (!comparer.Equals(left[index], right[index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Adds every element of a list to a hash code, in order.</summary>
    /// <typeparam name="TElement">The element type.</typeparam>
    /// <param name="hash">The hash code under construction.</param>
    /// <param name="elements">The elements to add.</param>
    private static void AddSequence<TElement>(ref HashCode hash, IReadOnlyList<TElement> elements)
    {
        hash.Add(elements.Count);

        for (int index = 0; index < elements.Count; index++)
        {
            hash.Add(elements[index]);
        }
    }
}
