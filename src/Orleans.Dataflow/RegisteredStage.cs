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
/// deployable and what keeps graph data from causing code loading (ADR 0001). What it is not, by itself,
/// is typed: a specification declares contracts, and contracts are not CLR types. Pairing a specification
/// with <see cref="ElementContract{T}"/> declarations is how a registered stage joins the same typed chain
/// a lambda stage composes in.
/// </para>
/// <para>
/// Every factory validates immediately and completely against the catalog: the stage has to be registered,
/// it has to have the linear shape the handle kind claims — exactly the port multiplicities a source, a
/// flow, a sink, or a result-bearing sink attaches with — and its ports have to carry the contracts the
/// declarations name. A mismatch is an <see cref="ArgumentException"/> at the line that declares the
/// handle, listing every violation at once, rather than a compiler diagnostic at the far end of a chain.
/// </para>
/// <para>
/// The factories live on a non-generic class so that type arguments are written only where they cannot be
/// inferred, which for these is only the result-bearing sink's pair, per ADR 0004 section 1.
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
}
