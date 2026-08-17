using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The registration surface a silo uses to say which dataflow stages it knows and who builds them.
/// </summary>
/// <remarks>
/// <para>
/// The two halves are registered separately because they answer different questions and different
/// processes need different halves. A catalog says which stages exist and what a document may say about
/// them, which is all a validator needs; a factory says what a stage does, which only a host that will run
/// the graph needs. A silo registers both, and a silo that registered a catalog without the matching
/// factory accepts a document at the coordinator and refuses it at materialization, naming the missing
/// provider.
/// </para>
/// <para>
/// Registration happens once, while the silo is being built, and the result is immutable. Nothing added
/// here can be changed by a document, which is the property the provider boundary rests on.
/// </para>
/// </remarks>
public interface IOrleansDataflowBuilder
{
    /// <summary>Registers the stages of one catalog with this silo.</summary>
    /// <param name="catalog">The catalog whose specifications this silo accepts.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Callable more than once, and the silo's catalog is the union of everything registered: a
    /// deployment composes vocabularies from several packages, and requiring one call would force it to
    /// merge them itself. Registering one stage reference twice is refused when the silo is built, because
    /// two specifications for one reference are two answers to one question rather than a merge.
    /// </remarks>
    IOrleansDataflowBuilder AddCatalog(IStageCatalog catalog);

    /// <summary>Registers the factory that builds every stage of one provider.</summary>
    /// <param name="provider">The provider whose stages this factory builds.</param>
    /// <param name="factory">The factory.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="provider"/> is the default value.</exception>
    /// <remarks>
    /// One factory per provider. Registering a provider twice is refused when the silo is built, for the
    /// same reason two catalog entries for one stage are.
    /// </remarks>
    IOrleansDataflowBuilder AddFactory(ProviderId provider, IDataflowStageFactory factory);

    /// <summary>Registers the CLR type that carries one element contract over this silo's streams.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="element">The binding, declared once and handed to the authoring side as well.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="element"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// A stream adapter needs the element type and cannot get it from a document, because a document never
    /// names a CLR type. It cannot open the stream as <see cref="object"/> either: Orleans binds one stream
    /// identity to one element type per process and refuses a second <c>GetStream</c> under a different one,
    /// which was probed rather than assumed. So the type is a registration and the document names the
    /// contract.
    /// </para>
    /// <para>
    /// <typeparamref name="T"/> must satisfy Orleans serialization, because a stream serializes what crosses
    /// it. That is checked by Orleans at first use rather than here.
    /// </para>
    /// </remarks>
    IOrleansDataflowBuilder AddStreamElement<T>(StreamElementBinding<T> element);

    /// <summary>Registers a named awaited grain call that transforms elements.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <typeparam name="TOut">The element type the call produces.</typeparam>
    /// <param name="binding">The binding, declared once and handed to the authoring side as well.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="binding"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The name is what a document may carry; the delegate is what a document may not. A document naming a
    /// call this silo does not register is refused when the run is started, with the compiler's own
    /// diagnostics naming the node and listing the calls this silo does publish.
    /// </remarks>
    IOrleansDataflowBuilder AddGrainCall<TIn, TOut>(GrainCallBinding<TIn, TOut> binding);

    /// <summary>Registers a named awaited grain call that terminates a graph.</summary>
    /// <typeparam name="TIn">The element type the call consumes.</typeparam>
    /// <param name="binding">The binding, declared once and handed to the authoring side as well.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="binding"/> is <see langword="null"/>.</exception>
    IOrleansDataflowBuilder AddGrainCallSink<TIn>(GrainCallSinkBinding<TIn> binding);

    /// <summary>Registers a named grain enumeration that heads a graph.</summary>
    /// <typeparam name="T">The element type the enumeration produces.</typeparam>
    /// <param name="source">The binding, declared once and handed to the authoring side as well.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    IOrleansDataflowBuilder AddGrainEnumerable<T>(GrainEnumerableBinding<T> source);
}
