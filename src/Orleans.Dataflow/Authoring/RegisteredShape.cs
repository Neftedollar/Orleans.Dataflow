using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The check a typed registered-stage handle passes before it exists: the stage is registered, it has the
/// linear shape the handle claims, and its ports carry the contracts the handle declares.
/// </summary>
/// <remarks>
/// <para>
/// Everything here happens once, at handle creation, and never again. That is the point of a typed handle:
/// a mismatch between what the author believes a stage is and what the catalog says it is becomes an
/// exception at the line that declares the handle, rather than a compiler diagnostic at the far end of a
/// chain or, worse, a document that only fails at deployment.
/// </para>
/// <para>
/// The report is one numbered list of every violation, in the style
/// <see cref="StageSpecification.Create(StageRef, IEnumerable{InputPortSpecification}, IEnumerable{OutputPortSpecification}, IEnumerable{ResultPortSpecification}, ContractReference, IEnumerable{CapabilityToken})"/>
/// and <see cref="GraphDocument.Create"/> already use, so one call names every problem rather than one
/// problem per call. It carries no parameter name, because a violation is a relation between the stage,
/// the catalog, and the declared contracts and belongs to no argument alone.
/// </para>
/// <para>
/// A rule is evaluated only when its own inputs are well formed. An unresolvable stage is reported alone,
/// because nothing else can be said about a stage nobody registered; a contract is compared only against a
/// port that exists, because a port count that is already wrong would otherwise contribute a second
/// complaint that disappears on its own once the first is fixed.
/// </para>
/// </remarks>
internal static class RegisteredShape
{
    /// <summary>Resolves a stage and checks it against the shape and contracts a handle claims.</summary>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="kind">The handle kind in prose, such as <c>flow</c>, for the diagnostic.</param>
    /// <param name="inputPorts">The number of input ports the kind requires.</param>
    /// <param name="outputPorts">The number of output ports the kind requires.</param>
    /// <param name="resultPorts">The number of result ports the kind requires.</param>
    /// <param name="input">
    /// The element contract the handle declares for the input port, or the default value when the kind
    /// declares no input.
    /// </param>
    /// <param name="output">
    /// The element contract the handle declares for the output port, or the default value when the kind
    /// declares no output.
    /// </param>
    /// <param name="result">
    /// The contract the handle declares for the result port, or the default value when the kind declares
    /// no result.
    /// </param>
    /// <returns>The resolved specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or the resolved stage breaks at least one of the
    /// handle's invariants.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The catalog resolved the reference and then supplied no specification, which is a defect in the
    /// registered catalog rather than a statement about these arguments.
    /// </exception>
    internal static StageSpecification Resolve(
        IStageCatalog catalog,
        StageRef stage,
        string kind,
        int inputPorts,
        int outputPorts,
        int resultPorts,
        ContractReference input,
        ContractReference output,
        ContractReference result)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        if (stage.IsDefault)
        {
            throw new ArgumentException(
                $"A registered {kind} handle requires a created {nameof(StageRef)}; the default {nameof(StageRef)} names no stage.",
                nameof(stage));
        }

        if (!catalog.TryGetSpecification(stage, out StageSpecification? specification))
        {
            throw new ArgumentException(
                Format(kind, stage, [$"the catalog does not register the stage '{stage}'"]));
        }

        // Unreachable through StageCatalog, and the interface's nullable annotation makes "resolved implies
        // a specification" an obligation the compiler enforces on every implementer that has nullable
        // reference types enabled. The rule is restated here for the same reason
        // Orleans.Dataflow.Compilation.GraphCompiler restates it: the catalog is a public seam that a later
        // federated implementation fills, and a broken seam should name itself rather than surface as a
        // dereference of nothing. It is not an ArgumentException, because it is a defect in the catalog
        // rather than a statement about the arguments this call was given.
        if (specification is null)
        {
            throw new InvalidOperationException(
                $"The catalog resolved the stage '{stage}' and then supplied no specification for it. A catalog that answers that a reference is registered must hand back the specification that registers it.");
        }

        List<string> violations = [];

        Count(violations, kind, specification.InputPorts.Count, inputPorts, "input");
        Count(violations, kind, specification.OutputPorts.Count, outputPorts, "output");
        Count(violations, kind, specification.ResultPorts.Count, resultPorts, "result");

        if (inputPorts == 1 && specification.InputPorts.Count == 1)
        {
            Contract(
                violations,
                specification.InputPorts[0].Id,
                specification.InputPorts[0].ElementContract,
                input,
                "accepts elements of");
        }

        if (outputPorts == 1 && specification.OutputPorts.Count == 1)
        {
            Contract(
                violations,
                specification.OutputPorts[0].Id,
                specification.OutputPorts[0].ElementContract,
                output,
                "produces elements of");
        }

        if (resultPorts == 1 && specification.ResultPorts.Count == 1)
        {
            Contract(
                violations,
                specification.ResultPorts[0].Id,
                specification.ResultPorts[0].ResultContract,
                result,
                "yields a result of");
        }

        return violations.Count == 0 ? specification : throw new ArgumentException(Format(kind, stage, violations));
    }

    /// <summary>Rejects a contract declaration supplied as its default value.</summary>
    /// <param name="isDefault">Whether the declaration is the default value.</param>
    /// <param name="typeName">The declaration type name, for the message.</param>
    /// <param name="role">The declaration's role in the handle, in prose.</param>
    /// <param name="parameterName">The name of the offending parameter.</param>
    /// <exception cref="ArgumentException"><paramref name="isDefault"/> is <see langword="true"/>.</exception>
    /// <remarks>
    /// A default declaration is one bad argument rather than a relation between several, so it is reported
    /// with the parameter name and before anything is resolved: a handle whose contract names nothing has
    /// no comparison to make against the catalog.
    /// </remarks>
    internal static void EnsureDeclared(bool isDefault, string typeName, string role, string parameterName)
    {
        if (isDefault)
        {
            throw new ArgumentException(
                $"A registered stage handle requires a created {typeName} for its {role}; the default {typeName} names no contract.",
                parameterName);
        }
    }

    /// <summary>Records a port-count violation, when there is one.</summary>
    /// <param name="violations">The violations collected so far.</param>
    /// <param name="kind">The handle kind in prose.</param>
    /// <param name="declared">The number of ports the specification declares.</param>
    /// <param name="required">The number of ports the handle kind requires.</param>
    /// <param name="direction">The port direction in prose.</param>
    private static void Count(
        List<string> violations,
        string kind,
        int declared,
        int required,
        string direction)
    {
        if (declared == required)
        {
            return;
        }

        violations.Add(string.Create(
            CultureInfo.InvariantCulture,
            $"the stage declares {declared} {direction} port{(declared == 1 ? string.Empty : "s")}, and a registered {kind} attaches a stage with exactly {required}"));
    }

    /// <summary>Records a port-contract violation, when there is one.</summary>
    /// <param name="violations">The violations collected so far.</param>
    /// <param name="port">The port the specification declares.</param>
    /// <param name="declared">The contract the specification declares for it.</param>
    /// <param name="claimed">The contract the handle declares for it.</param>
    /// <param name="verb">What the port does with values of that contract, in prose.</param>
    private static void Contract(
        List<string> violations,
        PortId port,
        ContractReference declared,
        ContractReference claimed,
        string verb)
    {
        if (declared == claimed)
        {
            return;
        }

        violations.Add(
            $"the port '{port}' {verb} contract '{declared}', and the handle declares '{claimed}'");
    }

    /// <summary>Renders the collected violations as one numbered list.</summary>
    /// <param name="kind">The handle kind in prose.</param>
    /// <param name="stage">The stage the handle was to be created for.</param>
    /// <param name="violations">The violations, in the order they were found.</param>
    /// <returns>A message whose first line names the handle and the count.</returns>
    private static string Format(string kind, StageRef stage, List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"A registered {kind} for '{stage}' cannot be created because it breaks {violations.Count} ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }
}
