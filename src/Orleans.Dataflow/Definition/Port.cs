using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// The factories that declare one port of a stage specification from its name and its contract.
/// </summary>
/// <remarks>
/// <para>
/// A port is a name and a contract, and a stage that declares three of them should read as three lines
/// saying so. The port specification types take the values the definition plane stores — a created
/// <see cref="PortId"/> and a <see cref="ContractReference"/> — because that is what a document and a
/// catalog envelope carry. These factories are how the same thing is written at an authoring site: the
/// name as the text an author types, and the contract as whichever of the two forms the author already
/// holds.
/// </para>
/// <para>
/// The typed overloads buy more than the missing <c>Reference</c>: an element contract and a result
/// contract are different types on purpose, so <see cref="In{T}(string, ElementContract{T})"/> cannot be
/// handed the declaration of a result and <see cref="Result{TResult}(string, ResultContract{TResult})"/>
/// cannot be handed the declaration of an element. Widening either to a <see cref="ContractReference"/>
/// loses that, which is why the untyped overloads exist beside the typed ones rather than instead of them:
/// a provider that holds only a reference — because its ports carry whatever a deployment binds to them —
/// says so by using them.
/// </para>
/// <para>
/// This is the one type of the definition namespace that ships in the authoring package, and the typed
/// overloads are why: <see cref="ElementContract{T}"/> asserts that a CLR type carries a contract, which is
/// an authoring-plane statement that the language-neutral package cannot make. Putting the class in the
/// namespace of the values it builds is what keeps <c>Port</c> in scope wherever a specification is being
/// written, in both frontends, without a second <c>using</c> or <c>open</c>.
/// </para>
/// <para>
/// Nothing here is a second way to build a port specification. Each factory is one call to the
/// corresponding <c>Create</c>, and a caller who already holds a <see cref="PortId"/> should keep calling
/// that directly.
/// </para>
/// </remarks>
public static class Port
{
    /// <summary>Declares a required input port carrying a typed element contract.</summary>
    /// <typeparam name="T">The CLR type this process binds to the contract.</typeparam>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="contract">The contract the port accepts; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="contract"/> is the
    /// default value.
    /// </exception>
    public static InputPortSpecification In<T>(string name, ElementContract<T> contract) =>
        InputPortSpecification.Create(Named(name), Declared(contract), isOptional: false);

    /// <summary>Declares an input port carrying a typed element contract, with an explicit optionality.</summary>
    /// <typeparam name="T">The CLR type this process binds to the contract.</typeparam>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="contract">The contract the port accepts; must not be the default value.</param>
    /// <param name="isOptional">
    /// <see langword="true"/> when a graph may leave the port unconnected; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="contract"/> is the
    /// default value.
    /// </exception>
    public static InputPortSpecification In<T>(string name, ElementContract<T> contract, bool isOptional) =>
        InputPortSpecification.Create(Named(name), Declared(contract), isOptional);

    /// <summary>Declares a required input port carrying a contract reference.</summary>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="elementContract">The contract the port accepts; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="elementContract"/> is
    /// the default value.
    /// </exception>
    public static InputPortSpecification In(string name, ContractReference elementContract) =>
        InputPortSpecification.Create(Named(name), elementContract, isOptional: false);

    /// <summary>Declares an input port carrying a contract reference, with an explicit optionality.</summary>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="elementContract">The contract the port accepts; must not be the default value.</param>
    /// <param name="isOptional">
    /// <see langword="true"/> when a graph may leave the port unconnected; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="elementContract"/> is
    /// the default value.
    /// </exception>
    public static InputPortSpecification In(string name, ContractReference elementContract, bool isOptional) =>
        InputPortSpecification.Create(Named(name), elementContract, isOptional);

    /// <summary>Declares an output port carrying a typed element contract, whose elements a graph consumes.</summary>
    /// <typeparam name="T">The CLR type this process binds to the contract.</typeparam>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="contract">The contract the port produces; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="contract"/> is the
    /// default value.
    /// </exception>
    public static OutputPortSpecification Out<T>(string name, ElementContract<T> contract) =>
        OutputPortSpecification.Create(Named(name), Declared(contract), isIgnorable: false);

    /// <summary>Declares an output port carrying a typed element contract, with an explicit ignorability.</summary>
    /// <typeparam name="T">The CLR type this process binds to the contract.</typeparam>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="contract">The contract the port produces; must not be the default value.</param>
    /// <param name="isIgnorable">
    /// <see langword="true"/> when the elements of the port may be dropped; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="contract"/> is the
    /// default value.
    /// </exception>
    public static OutputPortSpecification Out<T>(string name, ElementContract<T> contract, bool isIgnorable) =>
        OutputPortSpecification.Create(Named(name), Declared(contract), isIgnorable);

    /// <summary>Declares an output port carrying a contract reference, whose elements a graph consumes.</summary>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="elementContract">The contract the port produces; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="elementContract"/> is
    /// the default value.
    /// </exception>
    public static OutputPortSpecification Out(string name, ContractReference elementContract) =>
        OutputPortSpecification.Create(Named(name), elementContract, isIgnorable: false);

    /// <summary>Declares an output port carrying a contract reference, with an explicit ignorability.</summary>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="elementContract">The contract the port produces; must not be the default value.</param>
    /// <param name="isIgnorable">
    /// <see langword="true"/> when the elements of the port may be dropped; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="elementContract"/> is
    /// the default value.
    /// </exception>
    public static OutputPortSpecification Out(string name, ContractReference elementContract, bool isIgnorable) =>
        OutputPortSpecification.Create(Named(name), elementContract, isIgnorable);

    /// <summary>Declares a result port carrying a typed result contract.</summary>
    /// <typeparam name="TResult">The CLR type this process binds to the contract.</typeparam>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="contract">The contract the port yields; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="contract"/> is the
    /// default value.
    /// </exception>
    /// <remarks>
    /// A result port carries no optionality: nothing in the definition plane forces a result to be read, so
    /// there is no second overload here to say so.
    /// </remarks>
    public static ResultPortSpecification Result<TResult>(string name, ResultContract<TResult> contract) =>
        ResultPortSpecification.Create(Named(name), Declared(contract));

    /// <summary>Declares a result port carrying a contract reference.</summary>
    /// <param name="name">The port name, unique across the whole stage.</param>
    /// <param name="resultContract">The contract the port yields; must not be the default value.</param>
    /// <returns>The port specification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not a valid identifier segment, or <paramref name="resultContract"/> is
    /// the default value.
    /// </exception>
    public static ResultPortSpecification Result(string name, ContractReference resultContract) =>
        ResultPortSpecification.Create(Named(name), resultContract);

    /// <summary>Reads a port name written as text.</summary>
    /// <param name="name">The candidate name.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> breaks the segment grammar.</exception>
    /// <remarks>
    /// <see cref="PortId"/> owns the grammar and the diagnostic for breaking it, so the message is reused
    /// verbatim rather than restated; only the parameter name is corrected, because the author wrote a port
    /// name and not a <see cref="PortId"/> value. This is the rule
    /// <see cref="ElementContract.For{T}(string, int)"/> already follows for a contract identifier.
    /// </remarks>
    private static PortId Named(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        try
        {
            return PortId.Create(name);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, nameof(name), failure);
        }
    }

    /// <summary>Reads the reference of an element contract declaration.</summary>
    /// <typeparam name="T">The CLR type the declaration binds.</typeparam>
    /// <param name="contract">The declaration.</param>
    /// <returns>The reference a document carries for it.</returns>
    /// <exception cref="ArgumentException"><paramref name="contract"/> is the default value.</exception>
    /// <remarks>
    /// Reading <see cref="ElementContract{T}.Reference"/> of the default declaration is an
    /// <see cref="InvalidOperationException"/>, which is the right answer for a property and the wrong one
    /// for an argument. It is translated here so that a port declared from a contract nobody created is
    /// refused the way every other bad argument on this seam is.
    /// </remarks>
    private static ContractReference Declared<T>(ElementContract<T> contract) =>
        contract.IsDefault
            ? throw new ArgumentException(DescribeDefaultContract(nameof(ElementContract<T>)), nameof(contract))
            : contract.Reference;

    /// <summary>Reads the reference of a result contract declaration.</summary>
    /// <typeparam name="TResult">The CLR type the declaration binds.</typeparam>
    /// <param name="contract">The declaration.</param>
    /// <returns>The reference a document carries for it.</returns>
    /// <exception cref="ArgumentException"><paramref name="contract"/> is the default value.</exception>
    private static ContractReference Declared<TResult>(ResultContract<TResult> contract) =>
        contract.IsDefault
            ? throw new ArgumentException(DescribeDefaultContract(nameof(ResultContract<TResult>)), nameof(contract))
            : contract.Reference;

    /// <summary>Builds the message for a contract declaration supplied as its default value.</summary>
    /// <param name="typeName">The declaration type name.</param>
    /// <returns>A message naming the type and what its default declares.</returns>
    private static string DescribeDefaultContract(string typeName) =>
        $"A port requires a created {typeName}; the default {typeName} names no contract.";
}
