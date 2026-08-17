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
}
