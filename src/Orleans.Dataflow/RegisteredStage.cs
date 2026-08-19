using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// The factories that turn a catalog entry into a typed authoring value.
/// </summary>
/// <remarks>
/// <para>
/// A registered stage is named in a document and resolved from a catalog, which is what makes it
/// deployable and what keeps graph data from causing code loading. What it is not, by itself,
/// is typed: a specification declares contracts, and contracts are not CLR types. Pairing a specification
/// with <see cref="ElementContract{T}"/> declarations is how a registered stage joins the same typed chain
/// a lambda stage composes in.
/// </para>
/// <para>
/// Every factory validates immediately and completely against the catalog: the stage has to be registered,
/// it has to have the shape the handle kind claims — exactly the port multiplicities a source, a flow, a
/// sink, a result-bearing sink, or a junction attaches with — and its ports have to carry the contracts the
/// declarations name. A mismatch is an <see cref="ArgumentException"/> at the line that declares the
/// handle, listing every violation at once, rather than a compiler diagnostic at the far end of a chain.
/// </para>
/// <para>
/// The junction factories are the multi-port half, and what they add is one word: <em>every</em>. A
/// junction's ports are checked one by one against the contracts the handle declares for them, in the
/// specification's own canonical port order, so a registered junction carries a real contract on every port
/// rather than an opaque one — which is what lets a branching pipeline built entirely from registered
/// stages close deployable, with no seam a compiler has to forgive and no occurrence overriding its
/// specification.
/// </para>
/// <para>
/// The factories live on a non-generic class so that type arguments are written only where they cannot be
/// inferred, which for these is only the result-bearing sink's pair.
/// </para>
/// </remarks>
public static class RegisteredStage
{
    /// <summary>Declares a registered stage as the typed start of a graph.</summary>
    /// <typeparam name="TOut">The element type the stage produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="output">The contract the stage's output port carries.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> or <paramref name="output"/> is the default value, the catalog does not
    /// register <paramref name="stage"/>, or the registered stage does not declare exactly zero input
    /// ports, one output port, and no result port carrying <paramref name="output"/>. The message is a
    /// numbered list of every violation found.
    /// </exception>
    public static RegisteredSource<TOut> Source<TOut>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TOut> output)
    {
        RegisteredShape.EnsureDeclared(output.IsDefault, nameof(ElementContract<TOut>), "output", nameof(output));

        return new RegisteredSource<TOut>(
            RegisteredShape.Resolve(
                catalog,
                stage,
                "source",
                inputPorts: 0,
                outputPorts: 1,
                resultPorts: 0,
                input: default,
                output: output.Reference,
                result: default),
            output);
    }

    /// <summary>Declares a registered stage as a typed transformation.</summary>
    /// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
    /// <typeparam name="TOut">The element type the stage produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract the stage's input port accepts.</param>
    /// <param name="output">The contract the stage's output port carries.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/>, <paramref name="input"/>, or <paramref name="output"/> is the default
    /// value, the catalog does not register <paramref name="stage"/>, or the registered stage does not
    /// declare exactly one input port, one output port, and no result port carrying those contracts. The
    /// message is a numbered list of every violation found.
    /// </exception>
    public static RegisteredFlow<TIn, TOut> Flow<TIn, TOut>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input,
        ElementContract<TOut> output)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));
        RegisteredShape.EnsureDeclared(output.IsDefault, nameof(ElementContract<TOut>), "output", nameof(output));

        return new RegisteredFlow<TIn, TOut>(
            RegisteredShape.Resolve(
                catalog,
                stage,
                "flow",
                inputPorts: 1,
                outputPorts: 1,
                resultPorts: 0,
                input: input.Reference,
                output: output.Reference,
                result: default),
            input,
            output);
    }

    /// <summary>Declares a registered stage as a typed termination that yields no result.</summary>
    /// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract the stage's input port accepts.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/> or <paramref name="input"/> is the default value, the catalog does not
    /// register <paramref name="stage"/>, or the registered stage does not declare exactly one input port,
    /// no output port, and no result port carrying <paramref name="input"/>. The message is a numbered
    /// list of every violation found.
    /// </exception>
    public static RegisteredSink<TIn> Sink<TIn>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));

        return new RegisteredSink<TIn>(
            RegisteredShape.Resolve(
                catalog,
                stage,
                "sink",
                inputPorts: 1,
                outputPorts: 0,
                resultPorts: 0,
                input: input.Reference,
                output: default,
                result: default),
            input);
    }

    /// <summary>Declares a registered stage as a typed termination that yields one result.</summary>
    /// <typeparam name="TIn">The element type the stage consumes in this process.</typeparam>
    /// <typeparam name="TResult">The type of the result the stage yields in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract the stage's input port accepts.</param>
    /// <param name="result">The contract the stage's result port yields.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/>, <paramref name="input"/>, or <paramref name="result"/> is the default
    /// value, the catalog does not register <paramref name="stage"/>, or the registered stage does not
    /// declare exactly one input port, no output port, and one result port carrying those contracts. The
    /// message is a numbered list of every violation found.
    /// </exception>
    public static RegisteredSinkWithResult<TIn, TResult> SinkWithResult<TIn, TResult>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input,
        ResultContract<TResult> result)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));
        RegisteredShape.EnsureDeclared(result.IsDefault, nameof(ResultContract<TResult>), "result", nameof(result));

        return new RegisteredSinkWithResult<TIn, TResult>(
            RegisteredShape.Resolve(
                catalog,
                stage,
                "sink with a result",
                inputPorts: 1,
                outputPorts: 0,
                resultPorts: 1,
                input: input.Reference,
                output: default,
                result: result.Reference),
            input,
            result);
    }

    /// <summary>Declares a registered stage as a typed junction that splits one stream into legs.</summary>
    /// <typeparam name="TIn">The element type the junction consumes in this process.</typeparam>
    /// <typeparam name="TOut">The element type every leg produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract the junction's input port accepts.</param>
    /// <param name="output">The contract every one of its output ports carries.</param>
    /// <returns>The typed handle, whose leg count is the stage's own.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/>, <paramref name="input"/>, or <paramref name="output"/> is the default
    /// value, the catalog does not register <paramref name="stage"/>, or the registered stage does not
    /// declare exactly one input port carrying <paramref name="input"/>, between two and eight output ports
    /// all carrying <paramref name="output"/>, and no result port. The message is a numbered list of every
    /// violation found.
    /// </exception>
    /// <remarks>
    /// The arity is read from the specification rather than asked for: how many legs a junction has is a
    /// fact about the stage a provider registered, and a handle that let an author restate it would let the
    /// two disagree. What a call has to match is <see cref="RegisteredFanOut{TIn, TOut}.Legs"/>.
    /// </remarks>
    public static RegisteredFanOut<TIn, TOut> FanOut<TIn, TOut>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input,
        ElementContract<TOut> output)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));
        RegisteredShape.EnsureDeclared(output.IsDefault, nameof(ElementContract<TOut>), "output", nameof(output));

        return new RegisteredFanOut<TIn, TOut>(
            RegisteredShape.ResolveFanOut(catalog, stage, input.Reference, output.Reference),
            input,
            output);
    }

    /// <summary>Declares a registered stage as a typed junction that splits a row into two unlike legs.</summary>
    /// <typeparam name="TIn">The element type the junction consumes in this process.</typeparam>
    /// <typeparam name="TLeft">The element type the first leg produces in this process.</typeparam>
    /// <typeparam name="TRight">The element type the second leg produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract the junction's input port accepts.</param>
    /// <param name="left">The contract its first output port carries.</param>
    /// <param name="right">The contract its second output port carries.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// Any contract argument or <paramref name="stage"/> is the default value, the catalog does not register
    /// <paramref name="stage"/>, or the registered stage does not declare exactly one input port, exactly
    /// two output ports carrying those contracts in the specification's own port order, and no result port.
    /// </exception>
    public static RegisteredFanOut<TIn, TLeft, TRight> FanOut<TIn, TLeft, TRight>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input,
        ElementContract<TLeft> left,
        ElementContract<TRight> right)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));
        RegisteredShape.EnsureDeclared(left.IsDefault, nameof(ElementContract<TLeft>), "first leg", nameof(left));
        RegisteredShape.EnsureDeclared(right.IsDefault, nameof(ElementContract<TRight>), "second leg", nameof(right));

        return new RegisteredFanOut<TIn, TLeft, TRight>(
            RegisteredShape.ResolveJunction(
                catalog,
                stage,
                "fan-out",
                [input.Reference],
                [left.Reference, right.Reference]),
            input,
            left,
            right);
    }

    /// <summary>Declares a registered stage as a typed junction that joins several streams into one.</summary>
    /// <typeparam name="TIn">The element type every input consumes in this process.</typeparam>
    /// <typeparam name="TOut">The element type the junction produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="input">The contract every one of its input ports accepts.</param>
    /// <param name="output">The contract its output port carries.</param>
    /// <returns>The typed handle, whose input count is the stage's own.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="stage"/>, <paramref name="input"/>, or <paramref name="output"/> is the default
    /// value, the catalog does not register <paramref name="stage"/>, or the registered stage does not
    /// declare between two and eight input ports all carrying <paramref name="input"/>, exactly one output
    /// port carrying <paramref name="output"/>, and no result port. The message is a numbered list of every
    /// violation found.
    /// </exception>
    /// <remarks>
    /// The arity is read from the specification for the reason a fan-out's is. What a call has to match is
    /// <see cref="RegisteredFanIn{TIn, TOut}.Inputs"/>, counting the receiver of the join.
    /// </remarks>
    public static RegisteredFanIn<TIn, TOut> FanIn<TIn, TOut>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TIn> input,
        ElementContract<TOut> output)
    {
        RegisteredShape.EnsureDeclared(input.IsDefault, nameof(ElementContract<TIn>), "input", nameof(input));
        RegisteredShape.EnsureDeclared(output.IsDefault, nameof(ElementContract<TOut>), "output", nameof(output));

        return new RegisteredFanIn<TIn, TOut>(
            RegisteredShape.ResolveFanIn(catalog, stage, input.Reference, output.Reference),
            input,
            output);
    }

    /// <summary>Declares a registered stage as a typed junction that joins two unlike streams into one.</summary>
    /// <typeparam name="TFirst">The element type the first input consumes in this process.</typeparam>
    /// <typeparam name="TSecond">The element type the second input consumes in this process.</typeparam>
    /// <typeparam name="TOut">The element type the junction produces in this process.</typeparam>
    /// <param name="catalog">The catalog the reference is resolved through.</param>
    /// <param name="stage">The reference to resolve.</param>
    /// <param name="first">The contract its first input port accepts.</param>
    /// <param name="second">The contract its second input port accepts.</param>
    /// <param name="output">The contract its output port carries.</param>
    /// <returns>The typed handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// Any contract argument or <paramref name="stage"/> is the default value, the catalog does not register
    /// <paramref name="stage"/>, or the registered stage does not declare exactly two input ports carrying
    /// those contracts in the specification's own port order, exactly one output port, and no result port.
    /// </exception>
    public static RegisteredFanIn<TFirst, TSecond, TOut> FanIn<TFirst, TSecond, TOut>(
        IStageCatalog catalog,
        StageRef stage,
        ElementContract<TFirst> first,
        ElementContract<TSecond> second,
        ElementContract<TOut> output)
    {
        RegisteredShape.EnsureDeclared(first.IsDefault, nameof(ElementContract<TFirst>), "first input", nameof(first));
        RegisteredShape.EnsureDeclared(second.IsDefault, nameof(ElementContract<TSecond>), "second input", nameof(second));
        RegisteredShape.EnsureDeclared(output.IsDefault, nameof(ElementContract<TOut>), "output", nameof(output));

        return new RegisteredFanIn<TFirst, TSecond, TOut>(
            RegisteredShape.ResolveJunction(
                catalog,
                stage,
                "fan-in",
                [first.Reference, second.Reference],
                [output.Reference]),
            first,
            second,
            output);
    }
}
