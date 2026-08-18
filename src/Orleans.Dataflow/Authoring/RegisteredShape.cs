using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The check a typed registered-stage handle passes before it exists: the stage is registered, it has the
/// shape the handle claims, and its ports carry the contracts the handle declares.
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
/// <para>
/// The linear entry point takes one contract per side and the junction ones take a list or read the arity
/// from the specification, but the rules are the same rules said about more ports: exactly the shape the
/// handle claims, and the declared contract on every port of it.
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

    /// <summary>Resolves a stage and checks it against the junction shape and contracts a handle claims.</summary>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="kind">The handle kind in prose, such as <c>fan-out</c>, for the diagnostic.</param>
    /// <param name="inputs">
    /// The element contract the handle declares for each input port, in the specification's own port order;
    /// its length is the arity the handle claims.
    /// </param>
    /// <param name="outputs">
    /// The element contract the handle declares for each output port, in the specification's own port
    /// order; its length is the arity the handle claims.
    /// </param>
    /// <returns>The resolved specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or the resolved stage breaks at least one of the
    /// handle's invariants.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The catalog resolved the reference and then supplied no specification.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The multi-port sibling of <see cref="Resolve"/>, for the shapes whose ports carry unlike contracts,
    /// and the same rules said about several ports at once: the counts have to be exactly what the handle
    /// claims, and every port has to carry the contract the handle declared for its position.
    /// </para>
    /// <para>
    /// Position is the specification's own canonical port order, which is ordinal by port name. A
    /// specification sorts its ports at construction, so that order is the same in every process that
    /// resolves it, and it is the order the runtime wires the legs and the inputs in — one statement, read
    /// by the authoring side and by the planner alike.
    /// </para>
    /// </remarks>
    internal static StageSpecification ResolveJunction(
        IStageCatalog catalog,
        StageRef stage,
        string kind,
        IReadOnlyList<ContractReference> inputs,
        IReadOnlyList<ContractReference> outputs)
    {
        StageSpecification specification = Stage(catalog, stage, kind);
        List<string> violations = [];

        Count(violations, kind, specification.InputPorts.Count, inputs.Count, "input");
        Count(violations, kind, specification.OutputPorts.Count, outputs.Count, "output");
        Results(violations, kind, specification);

        if (specification.InputPorts.Count == inputs.Count)
        {
            for (int index = 0; index < inputs.Count; index++)
            {
                Contract(
                    violations,
                    specification.InputPorts[index].Id,
                    specification.InputPorts[index].ElementContract,
                    inputs[index],
                    "accepts elements of");
            }
        }

        if (specification.OutputPorts.Count == outputs.Count)
        {
            for (int index = 0; index < outputs.Count; index++)
            {
                Contract(
                    violations,
                    specification.OutputPorts[index].Id,
                    specification.OutputPorts[index].ElementContract,
                    outputs[index],
                    "produces elements of");
            }
        }

        return violations.Count == 0 ? specification : throw new ArgumentException(Format(kind, stage, violations));
    }

    /// <summary>Resolves a stage and checks it as a junction whose legs all carry one contract.</summary>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The element contract the handle declares for the one input port.</param>
    /// <param name="output">The element contract the handle declares for every output port.</param>
    /// <returns>The resolved specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or the resolved stage breaks at least one of the
    /// handle's invariants.
    /// </exception>
    /// <remarks>
    /// The arity is read from the specification rather than claimed by the handle, which is the one thing
    /// that differs from <see cref="ResolveJunction"/>: how many legs a junction has is a fact about the
    /// stage a provider registered, and a handle that let an author restate it would let the two disagree.
    /// What is checked is that there are at least two of them — a fan-out with one leg is a flow — and that
    /// every one carries the declared contract.
    /// </remarks>
    internal static StageSpecification ResolveFanOut(
        IStageCatalog catalog,
        StageRef stage,
        ContractReference input,
        ContractReference output)
    {
        const string Kind = "fan-out";

        StageSpecification specification = Stage(catalog, stage, Kind);
        List<string> violations = [];

        Count(violations, Kind, specification.InputPorts.Count, 1, "input");
        Arity(violations, Kind, specification.OutputPorts.Count, LocalVocabulary.MinFanOut, "output", "routes to");
        Results(violations, Kind, specification);

        if (specification.InputPorts.Count == 1)
        {
            Contract(
                violations,
                specification.InputPorts[0].Id,
                specification.InputPorts[0].ElementContract,
                input,
                "accepts elements of");
        }

        for (int index = 0; index < specification.OutputPorts.Count; index++)
        {
            Contract(
                violations,
                specification.OutputPorts[index].Id,
                specification.OutputPorts[index].ElementContract,
                output,
                "produces elements of");
        }

        return violations.Count == 0 ? specification : throw new ArgumentException(Format(Kind, stage, violations));
    }

    /// <summary>Resolves a stage and checks it as a junction whose inputs all carry one contract.</summary>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The element contract the handle declares for every input port.</param>
    /// <param name="output">The element contract the handle declares for the one output port.</param>
    /// <returns>The resolved specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or the resolved stage breaks at least one of the
    /// handle's invariants.
    /// </exception>
    /// <remarks>The mirror of <see cref="ResolveFanOut"/>, and the arity is read for the same reason.</remarks>
    internal static StageSpecification ResolveFanIn(
        IStageCatalog catalog,
        StageRef stage,
        ContractReference input,
        ContractReference output)
    {
        const string Kind = "fan-in";

        StageSpecification specification = Stage(catalog, stage, Kind);
        List<string> violations = [];

        Arity(violations, Kind, specification.InputPorts.Count, LocalVocabulary.MinFanIn, "input", "joins");
        Count(violations, Kind, specification.OutputPorts.Count, 1, "output");
        Results(violations, Kind, specification);

        for (int index = 0; index < specification.InputPorts.Count; index++)
        {
            Contract(
                violations,
                specification.InputPorts[index].Id,
                specification.InputPorts[index].ElementContract,
                input,
                "accepts elements of");
        }

        if (specification.OutputPorts.Count == 1)
        {
            Contract(
                violations,
                specification.OutputPorts[0].Id,
                specification.OutputPorts[0].ElementContract,
                output,
                "produces elements of");
        }

        return violations.Count == 0 ? specification : throw new ArgumentException(Format(Kind, stage, violations));
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

    /// <summary>Resolves a reference against a catalog, refusing anything a handle cannot be built on.</summary>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="kind">The handle kind in prose, for the diagnostic.</param>
    /// <returns>The registered specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> is the default value, or the catalog does not register it.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The catalog resolved the reference and then supplied no specification, which is a defect in the
    /// registered catalog rather than a statement about these arguments.
    /// </exception>
    /// <remarks>
    /// An unresolvable stage is reported alone, because nothing else can be said about a stage nobody
    /// registered.
    /// </remarks>
    private static StageSpecification Stage(IStageCatalog catalog, StageRef stage, string kind)
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

        // The same restatement Resolve makes and for the same reason: the catalog is a public seam a later
        // federated implementation fills, and a broken seam should name itself.
        return specification ??
            throw new InvalidOperationException(
                $"The catalog resolved the stage '{stage}' and then supplied no specification for it. A catalog that answers that a reference is registered must hand back the specification that registers it.");
    }

    /// <summary>Records a result-port violation, when there is one.</summary>
    /// <param name="violations">The violations collected so far.</param>
    /// <param name="kind">The handle kind in prose.</param>
    /// <param name="specification">The resolved specification.</param>
    /// <remarks>
    /// A junction declares no result port. A result is read from a terminal and a junction is not one, so
    /// requiring none rather than ignoring them is what keeps a stage from quietly declaring a result
    /// nothing in a graph could ever expose.
    /// </remarks>
    private static void Results(List<string> violations, string kind, StageSpecification specification)
    {
        if (specification.ResultPorts.Count == 0)
        {
            return;
        }

        violations.Add(string.Create(
            CultureInfo.InvariantCulture,
            $"the stage declares {specification.ResultPorts.Count} result port{(specification.ResultPorts.Count == 1 ? string.Empty : "s")}, and a registered {kind} declares none: a result is read from a terminal, and a junction is not one"));
    }

    /// <summary>Records a junction-arity violation, when there is one.</summary>
    /// <param name="violations">The violations collected so far.</param>
    /// <param name="kind">The handle kind in prose.</param>
    /// <param name="declared">The number of ports the specification declares on that side.</param>
    /// <param name="least">The fewest a junction of this direction may have.</param>
    /// <param name="direction">The port direction in prose.</param>
    /// <param name="verb">What the junction does with those ports, in prose.</param>
    /// <remarks>
    /// There is a lower bound and deliberately no upper one. Two is what makes a junction a junction — one
    /// leg is a chain written the long way — while the local vocabulary's ceiling of eight is a fact about
    /// the ports its own specifications declare, and a registered stage declares its own.
    /// </remarks>
    private static void Arity(
        List<string> violations,
        string kind,
        int declared,
        int least,
        string direction,
        string verb)
    {
        if (declared >= least)
        {
            return;
        }

        violations.Add(string.Create(
            CultureInfo.InvariantCulture,
            $"the stage declares {declared} {direction} port{(declared == 1 ? string.Empty : "s")}, and a registered {kind} {verb} at least {least}"));
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
