using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// What a checkpoint store hands back: the document it holds and the ETag a writer must present to replace
/// it.
/// </summary>
/// <param name="Document">The stored checkpoint, in canonical form.</param>
/// <param name="ETag">
/// The opaque version the store gave this document. A writer presents it back and is refused when the store
/// has moved on.
/// </param>
/// <remarks>
/// The two travel together because neither is useful without the other: a document read without its ETag
/// could only ever be overwritten blindly, and an ETag without its document names a version nobody has read.
/// The ETag is opaque text and never a number a caller may compare for order — a store is free to spell it
/// as a counter, a hash, or a token, and only equality means anything to a writer.
/// </remarks>
public readonly record struct StoredCheckpoint(CanonicalJsonValue Document, string ETag)
{
    private readonly CanonicalJsonValue _document = Document;
    private readonly string _etag = ETag;

    /// <summary>Gets the stored checkpoint, in canonical form.</summary>
    /// <exception cref="InvalidOperationException">This instance is the uninitialized default.</exception>
    public CanonicalJsonValue Document
    {
        get => _etag is null ? throw DefaultAccess() : _document;
        init => _document = value;
    }

    /// <summary>Gets the opaque version the store gave this document.</summary>
    /// <value>Never <see langword="null"/>.</value>
    /// <exception cref="InvalidOperationException">This instance is the uninitialized default.</exception>
    public string ETag
    {
        get => _etag ?? throw DefaultAccess();
        init => _etag = value;
    }

    private static InvalidOperationException DefaultAccess() =>
        new($"The default {nameof(StoredCheckpoint)} holds no document. A store reports an absent checkpoint as null, never as the uninitialized struct.");
}
