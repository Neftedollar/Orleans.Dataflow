namespace Orleans.Dataflow.Compilation;

/// <summary>
/// One rule a graph document breaks when it is checked against a stage catalog.
/// </summary>
/// <remarks>
/// <para>
/// A diagnostic is data about a document, not an exception: validation reports every violation it finds
/// and returns them, because a caller fixing one problem per run learns the shape of a graph one
/// rejection at a time.
/// </para>
/// <para>
/// The three members answer three different questions. <see cref="Rule"/> is the stable identifier a
/// program matches on and never changes for a given meaning; <see cref="Message"/> is the sentence a
/// person reads and may be reworded; <see cref="Subject"/> is the identity to look at in the document.
/// Programs match on the rule, people read the message, and tools navigate by the subject.
/// </para>
/// <para>
/// No member ever names a CLR type. A diagnostic is about the language-neutral model, and a document
/// written in F# has to receive the same report as the same document written in C#.
/// </para>
/// <para>
/// The set of rule identifiers is open. The graph compiler implements eleven catalog rules, and a
/// provider-supplied check may report an identifier of its own, so the factory validates that a rule is
/// present rather than that it is one of a fixed list.
/// </para>
/// </remarks>
public sealed record class GraphValidationDiagnostic
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GraphValidationDiagnostic"/> class.
    /// </summary>
    /// <param name="rule">The validated rule identifier.</param>
    /// <param name="message">The validated message.</param>
    /// <param name="subject">The validated subject, or <see langword="null"/>.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a diagnostic cannot be built or
    /// amended around <see cref="Create(string, string)"/>: a <c>with</c> expression has no member it is
    /// allowed to change.
    /// </remarks>
    private GraphValidationDiagnostic(string rule, string message, string? subject)
    {
        Rule = rule;
        Message = message;
        Subject = subject;
    }

    /// <summary>
    /// Gets the stable identifier of the rule this diagnostic reports.
    /// </summary>
    /// <value>A non-empty kebab-case identifier such as <c>unknown-stage</c>.</value>
    /// <remarks>
    /// The identifier is the part of a diagnostic a program is allowed to depend on. Rewording a message
    /// is not a breaking change; changing what a rule identifier means is.
    /// </remarks>
    public string Rule { get; }

    /// <summary>
    /// Gets the human-readable statement of what is wrong.
    /// </summary>
    /// <value>A non-empty sentence naming the offending identities and the rule they break.</value>
    public string Message { get; }

    /// <summary>
    /// Gets the text form of the identity this diagnostic is about.
    /// </summary>
    /// <value>
    /// A non-empty identity such as <c>reader</c>, <c>reader#out</c>, or <c>nondeployable</c>, or
    /// <see langword="null"/> when the rule is about the document as a whole and no single identity is
    /// the offender.
    /// </value>
    /// <remarks>
    /// The subject is text rather than a typed identity because one report mixes node identifiers, port
    /// addresses, edges, slot names, and capability tokens, and a tool that jumps to the offending place
    /// wants one field to read rather than a discriminated union to switch on.
    /// </remarks>
    public string? Subject { get; }

    /// <summary>
    /// Creates a diagnostic about the document as a whole.
    /// </summary>
    /// <param name="rule">The stable rule identifier; must not be empty or whitespace.</param>
    /// <param name="message">The human-readable statement; must not be empty or whitespace.</param>
    /// <returns>The validated diagnostic, with no subject.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="rule"/> or <paramref name="message"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="rule"/> or <paramref name="message"/> is empty or whitespace.
    /// </exception>
    public static GraphValidationDiagnostic Create(string rule, string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rule);
        ArgumentException.ThrowIfNullOrWhiteSpace(message);

        return new GraphValidationDiagnostic(rule, message, subject: null);
    }

    /// <summary>
    /// Creates a diagnostic about one identity in the document.
    /// </summary>
    /// <param name="rule">The stable rule identifier; must not be empty or whitespace.</param>
    /// <param name="message">The human-readable statement; must not be empty or whitespace.</param>
    /// <param name="subject">The offending identity's text form; must not be empty or whitespace.</param>
    /// <returns>The validated diagnostic.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">Any argument is empty or whitespace.</exception>
    /// <remarks>
    /// The subject is a separate overload rather than a nullable parameter, so a caller that has no
    /// identity to name says so by calling the other overload instead of passing
    /// <see langword="null"/> and hoping it means "none".
    /// </remarks>
    public static GraphValidationDiagnostic Create(string rule, string message, string subject)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rule);
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        return new GraphValidationDiagnostic(rule, message, subject);
    }

    /// <summary>
    /// Returns the one-line form of this diagnostic.
    /// </summary>
    /// <returns>Text of the form <c>unknown-stage: the node 'reader' references ...</c>.</returns>
    /// <remarks>
    /// The subject is deliberately absent from this text: every message already names the identities it
    /// is about, so appending the subject would repeat it. The subject exists for programs, not for this
    /// line. The method never throws.
    /// </remarks>
    public override string ToString() => $"{Rule}: {Message}";
}
