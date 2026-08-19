using System.Globalization;
using System.Reflection;
using System.Text;
using Xunit;

namespace Orleans.Dataflow.ApiSurface;

/// <summary>
/// Renders one assembly's whole public surface as deterministic text, and compares it against a baseline
/// checked in beside the test that asks for it.
/// </summary>
/// <remarks>
/// <para>
/// The <c>PublicAPI</c> analyzer records signatures and is blind to everything around them: generic
/// parameter variance, base types, implemented interfaces, and attributes are all absent from its files,
/// so <c>IIngressQueue&lt;T&gt;</c> becoming <c>IIngressQueue&lt;in T&gt;</c> or an <c>[Id(3)]</c> becoming
/// an <c>[Id(4)]</c> passes it without a word. Those are exactly the changes that break a consumer who
/// compiled against the old shape, or a silo that serialized against the old numbering. This dump records
/// them, and the F# assembly — which carries no analyzer at all — is guarded by nothing else.
/// </para>
/// <para>
/// Reading is done through <see cref="MetadataLoadContext"/> and never through
/// <see cref="Assembly.Load(AssemblyName)"/>: the surface is metadata, so nothing here needs the assembly's
/// code to run, and loading it for real would run module initializers and bind a second copy of assemblies
/// the test host has already loaded. Dependencies are resolved from the test's own output directory, which
/// is where every assembly the surface can mention has already been copied.
/// </para>
/// <para>
/// Every ordering in the output is imposed rather than inherited: types are sorted by full name and members
/// by their rendered line, both with <see cref="StringComparer.Ordinal"/>, so the same assembly renders
/// byte-identically on any machine and under any runtime's reflection ordering. Constants are rendered with
/// <see cref="CultureInfo.InvariantCulture"/> for the same reason.
/// </para>
/// </remarks>
internal static class PublicSurfaceDump
{
    /// <summary>The environment variable that rewrites a baseline instead of only reporting the diff.</summary>
    /// <remarks>
    /// Deliberately not a silent self-heal: when it is set the baseline is rewritten <em>and the test still
    /// fails</em>, naming the file it wrote. A rewrite is a deliberate act whose result belongs in a commit
    /// diff that somebody reads, and a run that rewrote a baseline must never be green — otherwise a CI job
    /// with the variable set would report a passing suite over a surface nobody approved.
    /// </remarks>
    internal const string UpdateVariable = "ORLEANS_DATAFLOW_UPDATE_API_SURFACE";

    /// <summary>The file that marks the repository root.</summary>
    private const string SolutionFileName = "Orleans.Dataflow.slnx";

    /// <summary>The greatest number of added or removed lines a failure message lists.</summary>
    private const int ReportedLineLimit = 20;

    /// <summary>Attribute names whose presence says nothing about the surface a consumer sees.</summary>
    /// <remarks>
    /// <para>
    /// Nullable annotations are the compiler's encoding of a language feature and appear on almost every
    /// member; the analyzer suppressions and the debugger hints are authoring notes. Recording any of them
    /// would make the baseline churn on edits that change nothing anybody can call.
    /// </para>
    /// <para>
    /// The state-machine attributes are left out for a sharper reason: each one names a compiler-generated
    /// type whose name carries the declaring method's <em>ordinal</em> — <c>&lt;ShutdownAsync&gt;d__21</c> —
    /// so adding one unrelated private method renumbers several of them at once. A baseline that moved for
    /// that would be a baseline nobody reads, and the fact it would be reporting is that a method is
    /// <c>async</c>, which is an implementation choice rather than something a caller binds to.
    /// </para>
    /// </remarks>
    private static readonly string[] IgnoredAttributePrefixes =
    [
        "System.Runtime.CompilerServices.Nullable",
        "System.Diagnostics.CodeAnalysis.SuppressMessage",
        "System.Diagnostics.DebuggerBrowsable",
        "System.Runtime.CompilerServices.CompilerGenerated",
        "System.Runtime.CompilerServices.AsyncStateMachine",
        "System.Runtime.CompilerServices.AsyncIteratorStateMachine",
        "System.Runtime.CompilerServices.IteratorStateMachine",
    ];

    /// <summary>
    /// Compares one assembly's public surface against its baseline and fails the test when they differ.
    /// </summary>
    /// <param name="repositoryRelativeBaseline">
    /// The baseline file's path relative to the repository root, with forward slashes.
    /// </param>
    /// <param name="assembly">The assembly to describe, named by an anchor type it declares.</param>
    /// <param name="excludedNamespace">
    /// A namespace whose types are not this library's to freeze, or <see langword="null"/> to keep them all.
    /// </param>
    /// <remarks>
    /// The assembly is located by <see cref="Assembly.Location"/> and then read from disk as metadata: the
    /// runtime copy is used only to find the file. What is compared is the whole text, not a summary of it,
    /// so a single changed character in one member's signature fails.
    /// </remarks>
    internal static void AssertMatchesBaseline(
        string repositoryRelativeBaseline,
        Assembly assembly,
        string? excludedNamespace = null)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        string baselinePath = Path.Combine(
            [RepositoryRoot(), .. repositoryRelativeBaseline.Split('/')]);
        string actual = Of(assembly.Location, AppContext.BaseDirectory, excludedNamespace);

        if (Environment.GetEnvironmentVariable(UpdateVariable) is not null)
        {
            _ = Directory.CreateDirectory(Path.GetDirectoryName(baselinePath)!);
            File.WriteAllText(baselinePath, actual);

            Assert.Fail(
                $"The baseline '{repositoryRelativeBaseline}' was rewritten because {UpdateVariable} is set. Review the diff in the commit, then re-run without the variable: a run that rewrote its own baseline is not a run that verified anything.");
        }

        if (!File.Exists(baselinePath))
        {
            Assert.Fail(
                $"The public-surface baseline '{repositoryRelativeBaseline}' was not found at '{baselinePath}'. Generate it deliberately by running this test once with {UpdateVariable}=1 and committing the file it writes.");
        }

        string expected = File.ReadAllText(baselinePath);

        if (string.Equals(expected, actual, StringComparison.Ordinal))
        {
            return;
        }

        Assert.Fail(Describe(repositoryRelativeBaseline, expected, actual));
    }

    /// <summary>Renders one assembly's public surface as deterministic text.</summary>
    /// <param name="assemblyPath">The assembly file to read.</param>
    /// <param name="probeDirectory">A directory holding the assemblies its surface can mention.</param>
    /// <param name="excludedNamespace">
    /// A namespace to leave out entirely, or <see langword="null"/> to keep every public type.
    /// </param>
    /// <returns>The surface text, with a trailing newline after every line.</returns>
    internal static string Of(string assemblyPath, string probeDirectory, string? excludedNamespace = null)
    {
        List<string> probe = [];

        foreach (string directory in new[] { Path.GetDirectoryName(Path.GetFullPath(assemblyPath))!, probeDirectory })
        {
            if (Directory.Exists(directory))
            {
                probe.AddRange(Directory.GetFiles(directory, "*.dll"));
            }
        }

        // The shared framework, so that a base type or a parameter type from it resolves to metadata rather
        // than to a name the renderer would have to guess at.
        probe.AddRange(Directory.GetFiles(Path.GetDirectoryName(typeof(object).Assembly.Location)!, "*.dll"));

        PathAssemblyResolver resolver = new(probe.Distinct(StringComparer.Ordinal));
        using MetadataLoadContext context = new(resolver, coreAssemblyName: "System.Private.CoreLib");
        Assembly assembly = context.LoadFromAssemblyPath(Path.GetFullPath(assemblyPath));

        List<Type> types = [.. assembly.GetTypes()
            .Where(type => IsVisible(type) && !IsExcluded(type, excludedNamespace))
            .OrderBy(Name, StringComparer.Ordinal)];

        StringBuilder surface = new();
        _ = surface.Append("# assembly: ").Append(assembly.GetName().Name).Append('\n');
        _ = surface.Append("# public types: ").Append(types.Count.ToString(CultureInfo.InvariantCulture)).Append('\n');

        foreach (Type type in types)
        {
            AppendType(surface, type);
        }

        return surface.ToString();
    }

    /// <summary>Appends one type's declaration and every member of it a consumer can reach.</summary>
    /// <param name="surface">The text under construction.</param>
    /// <param name="type">The type.</param>
    private static void AppendType(StringBuilder surface, Type type)
    {
        _ = surface.Append("TYPE ").Append(Kind(type)).Append(' ').Append(Name(type)).Append(Attributes(type)).Append('\n');

        // The base type is surface: a consumer inherits from it, catches it, and passes the derived type
        // where it is expected, and none of that survives the base type changing.
        if (type.BaseType is not null && type.BaseType.FullName != "System.Object" && !type.IsEnum)
        {
            _ = surface.Append("  BASE ").Append(Name(type.BaseType)).Append('\n');
        }

        foreach (string implemented in type.GetInterfaces().Select(Name).OrderBy(name => name, StringComparer.Ordinal))
        {
            _ = surface.Append("  IFACE ").Append(implemented).Append('\n');
        }

        foreach (Type parameter in type.GetGenericArguments().Where(argument => argument.IsGenericParameter))
        {
            GenericParameterAttributes flags = parameter.GenericParameterAttributes & ~GenericParameterAttributes.VarianceMask;
            List<string> constraints =
                [.. parameter.GetGenericParameterConstraints().Select(Name).OrderBy(name => name, StringComparer.Ordinal)];

            if (constraints.Count > 0 || flags != GenericParameterAttributes.None)
            {
                _ = surface.Append("  GENPARM ").Append(Variance(parameter)).Append(parameter.Name).Append(" : ")
                    .Append(string.Join(", ", constraints.Append(flags.ToString()))).Append('\n');
            }
        }

        List<string> lines = [];

        foreach (MemberInfo member in type.GetMembers(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
            | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            string? line = Render(member, type);

            if (line is not null)
            {
                lines.Add(line);
            }
        }

        lines.Sort(StringComparer.Ordinal);

        foreach (string line in lines)
        {
            _ = surface.Append(line).Append('\n');
        }
    }

    /// <summary>Renders one member, or answers <see langword="null"/> when it is not part of the surface.</summary>
    /// <param name="member">The member.</param>
    /// <param name="declaring">The type that declares it.</param>
    /// <returns>The line, or <see langword="null"/>.</returns>
    /// <remarks>
    /// Protected members count as surface because a consumer deriving from an unsealed public type reaches
    /// them; private and internal ones do not, whatever the friend grants say, because a friend is this
    /// repository and this repository is not the audience a baseline protects.
    /// </remarks>
    private static string? Render(MemberInfo member, Type declaring) => member switch
    {
        ConstructorInfo constructor when constructor.IsPublic || constructor.IsFamily || constructor.IsFamilyOrAssembly =>
            "  .ctor" + (constructor.IsPublic ? string.Empty : "(protected)")
            + "(" + string.Join(", ", constructor.GetParameters().Select(Signature)) + ")" + Attributes(constructor),

        MethodInfo method when (method.IsPublic || method.IsFamily || method.IsFamilyOrAssembly)
            && (!method.IsSpecialName || method.Name.StartsWith("op_", StringComparison.Ordinal)) =>
            "  " + (method.IsSpecialName ? "operator " : "method ")
            + (method.IsStatic ? "static " : string.Empty)
            + (method.IsPublic ? string.Empty : "protected ")
            + (method.IsAbstract ? "abstract " : method is { IsVirtual: true, IsFinal: false } ? "virtual " : string.Empty)
            + Name(method.ReturnType) + " " + method.Name
            + (method.IsGenericMethodDefinition
                ? "<" + string.Join(", ", method.GetGenericArguments().Select(argument => argument.Name)) + ">"
                : string.Empty)
            + "(" + string.Join(", ", method.GetParameters().Select(Signature)) + ")" + Attributes(method),

        PropertyInfo property => RenderProperty(property),

        FieldInfo field when field.IsPublic || field.IsFamily || field.IsFamilyOrAssembly =>
            "  field " + (field.IsStatic ? "static " : string.Empty)
            + (field.IsPublic ? string.Empty : "protected ")
            + (field.IsLiteral ? "const " : field.IsInitOnly ? "readonly " : string.Empty)
            + Name(field.FieldType) + " " + field.Name
            + (field.IsLiteral ? " = " + Constant(field.GetRawConstantValue(), declaring.IsEnum ? null : field.FieldType) : string.Empty)
            + Attributes(field),

        EventInfo declared when declared.AddMethod is not null
            && (declared.AddMethod.IsPublic || declared.AddMethod.IsFamily) =>
            "  event " + (declared.AddMethod.IsStatic ? "static " : string.Empty)
            + Name(declared.EventHandlerType!) + " " + declared.Name + Attributes(declared),

        Type nested when IsVisible(nested) => "  nested " + Name(nested),

        _ => null,
    };

    /// <summary>Renders one property when either of its accessors is reachable.</summary>
    /// <param name="property">The property.</param>
    /// <returns>The line, or <see langword="null"/> when neither accessor is surface.</returns>
    private static string? RenderProperty(PropertyInfo property)
    {
        MethodInfo? getter = property.GetMethod;
        MethodInfo? setter = property.SetMethod;
        bool readable = getter is not null && (getter.IsPublic || getter.IsFamily || getter.IsFamilyOrAssembly);
        bool writable = setter is not null && (setter.IsPublic || setter.IsFamily || setter.IsFamilyOrAssembly);

        if (!readable && !writable)
        {
            return null;
        }

        MethodInfo accessor = (getter ?? setter)!;
        string indexer = property.GetIndexParameters().Length > 0
            ? "[" + string.Join(", ", property.GetIndexParameters().Select(Signature)) + "]"
            : string.Empty;

        return "  property " + (accessor.IsStatic ? "static " : string.Empty)
            + (accessor.IsPublic ? string.Empty : "protected ")
            + (accessor.IsAbstract ? "abstract " : accessor is { IsVirtual: true, IsFinal: false } ? "virtual " : string.Empty)
            + Name(property.PropertyType) + " " + property.Name + indexer
            + " {" + (readable ? " get;" : string.Empty) + (writable ? " set;" : string.Empty) + " }"
            + Attributes(property);
    }

    /// <summary>Names the kind of one type the way a declaration would spell it.</summary>
    /// <param name="type">The type.</param>
    /// <returns>The keyword sequence.</returns>
    private static string Kind(Type type) => type switch
    {
        { IsInterface: true } => "interface",
        { IsEnum: true } => "enum",
        _ when type.BaseType?.FullName == "System.MulticastDelegate" => "delegate",
        { IsValueType: true } => "struct",
        { IsAbstract: true, IsSealed: true } => "static class",
        { IsAbstract: true } => "abstract class",
        { IsSealed: true } => "sealed class",
        _ => "class",
    };

    /// <summary>Spells the variance of one generic parameter.</summary>
    /// <param name="parameter">The parameter.</param>
    /// <returns><c>"in "</c>, <c>"out "</c>, or the empty string.</returns>
    /// <remarks>
    /// The one fact the <c>PublicAPI</c> analyzer never records and the one this guard exists for most:
    /// adding variance is compatible and removing it silently breaks every consumer who relied on it.
    /// </remarks>
    private static string Variance(Type parameter) =>
        (parameter.GenericParameterAttributes & GenericParameterAttributes.VarianceMask) switch
        {
            GenericParameterAttributes.Covariant => "out ",
            GenericParameterAttributes.Contravariant => "in ",
            _ => string.Empty,
        };

    /// <summary>Renders one type's name, with variance on the parameters of an open generic type.</summary>
    /// <param name="type">The type.</param>
    /// <returns>The name, never abbreviated and never carrying an assembly version.</returns>
    private static string Name(Type type)
    {
        if (type.IsGenericParameter)
        {
            return type.Name;
        }

        if (type.IsByRef)
        {
            return Name(type.GetElementType()!) + "&";
        }

        if (type.IsArray)
        {
            return Name(type.GetElementType()!) + "[" + new string(',', type.GetArrayRank() - 1) + "]";
        }

        if (type.IsPointer)
        {
            return Name(type.GetElementType()!) + "*";
        }

        if (type.IsConstructedGenericType || (type.IsGenericTypeDefinition && type.GetGenericArguments().Length > 0))
        {
            string bare = (type.Namespace is null ? string.Empty : type.Namespace + ".") + type.Name;
            int tick = bare.IndexOf('`', StringComparison.Ordinal);

            if (tick >= 0)
            {
                bare = bare[..tick];
            }

            IEnumerable<string> arguments = type.GetGenericArguments()
                .Select(argument => type.IsGenericTypeDefinition ? Variance(argument) + argument.Name : Name(argument));

            return bare + "<" + string.Join(", ", arguments) + ">";
        }

        return type.FullName ?? ((type.Namespace is null ? string.Empty : type.Namespace + ".") + type.Name);
    }

    /// <summary>Renders one parameter, with its modifier and its default.</summary>
    /// <param name="parameter">The parameter.</param>
    /// <returns>The rendered parameter.</returns>
    private static string Signature(ParameterInfo parameter)
    {
        string modifier = parameter.IsOut ? "out " : parameter.ParameterType.IsByRef ? "ref " : string.Empty;
        string given = parameter.HasDefaultValue
            ? " = " + Constant(parameter.RawDefaultValue, parameter.ParameterType)
            : string.Empty;

        return modifier + Name(parameter.ParameterType) + " " + (parameter.Name ?? "?") + given;
    }

    /// <summary>Renders one metadata constant so that it reads the same under every culture.</summary>
    /// <param name="value">The raw constant, which is <see langword="null"/> for a null or default one.</param>
    /// <param name="declared">The declared type, used to tell a null reference from a default value.</param>
    /// <returns>The rendered constant.</returns>
    private static string Constant(object? value, Type? declared)
    {
        if (value is null)
        {
            // Metadata spells "the default of this value type" and "the null reference" the same way, so
            // the declared type is what separates them.
            Type? bare = declared is not null && declared.IsByRef ? declared.GetElementType() : declared;

            return bare is not null && bare.IsValueType ? "default" : "null";
        }

        return value switch
        {
            string text => "\"" + text + "\"",
            char character => "'" + character + "'",
            bool flag => flag ? "true" : "false",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };
    }

    /// <summary>Renders the attributes on one member, in a fixed order.</summary>
    /// <param name="member">The member.</param>
    /// <returns>The rendered attribute list, or the empty string when there is nothing to say.</returns>
    /// <remarks>
    /// Arguments are recorded, not only names, and that is the point for the Orleans wire attributes:
    /// <c>[Id(3)]</c> becoming <c>[Id(4)]</c> renumbers a field on the wire and is invisible to a round-trip
    /// test, which serializes and deserializes with the same numbering and agrees with itself. The same
    /// applies to a <c>[CompoundTypeAlias]</c> or an <c>[Alias]</c>, which is the name a stored grain state
    /// or a queued message resolves through.
    /// </remarks>
    private static string Attributes(MemberInfo member)
    {
        List<string> rendered = [.. member.GetCustomAttributesData()
            .Where(attribute => !IsIgnored(attribute.AttributeType.FullName ?? "?"))
            .Select(Render)
            .OrderBy(text => text, StringComparer.Ordinal)];

        return rendered.Count == 0 ? string.Empty : "  [" + string.Join(",", rendered) + "]";

        static string Render(CustomAttributeData attribute)
        {
            List<string> arguments = [.. attribute.ConstructorArguments.Select(Argument)];
            arguments.AddRange(attribute.NamedArguments
                .Select(named => named.MemberName + "=" + Argument(named.TypedValue))
                .OrderBy(text => text, StringComparer.Ordinal));

            return (attribute.AttributeType.FullName ?? "?")
                + (arguments.Count == 0 ? string.Empty : "(" + string.Join(", ", arguments) + ")");
        }

        static string Argument(CustomAttributeTypedArgument argument) => argument.Value switch
        {
            null => Constant(null, argument.ArgumentType),
            Type type => "typeof(" + Name(type) + ")",
            IReadOnlyList<CustomAttributeTypedArgument> items => "[" + string.Join(", ", items.Select(Argument)) + "]",
            object value => Constant(value, argument.ArgumentType),
        };
    }

    /// <summary>Determines whether one attribute says nothing about the surface.</summary>
    /// <param name="fullName">The attribute type's full name.</param>
    /// <returns><see langword="true"/> when it is left out.</returns>
    private static bool IsIgnored(string fullName) =>
        Array.Exists(IgnoredAttributePrefixes, prefix => fullName.StartsWith(prefix, StringComparison.Ordinal));

    /// <summary>Determines whether one type is reachable from outside its assembly.</summary>
    /// <param name="type">The type.</param>
    /// <returns><see langword="true"/> when a consumer can name it.</returns>
    private static bool IsVisible(Type type) => type.IsNested
        ? (type.IsNestedPublic || type.IsNestedFamily || type.IsNestedFamORAssem) && IsVisible(type.DeclaringType!)
        : type.IsPublic;

    /// <summary>Determines whether one type sits inside an excluded namespace.</summary>
    /// <param name="type">The type.</param>
    /// <param name="excludedNamespace">The excluded namespace, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the type is left out.</returns>
    private static bool IsExcluded(Type type, string? excludedNamespace) =>
        excludedNamespace is not null
        && Name(type).StartsWith(excludedNamespace + ".", StringComparison.Ordinal);

    /// <summary>Builds the failure message for a surface that no longer matches its baseline.</summary>
    /// <param name="baseline">The baseline's repository-relative path, for the message.</param>
    /// <param name="expected">The recorded surface.</param>
    /// <param name="actual">The surface the assembly has now.</param>
    /// <returns>The message: what moved, in which direction, and what to do about it.</returns>
    /// <remarks>
    /// The two directions are not the same news, so they are reported apart. A removed line is a break — a
    /// consumer compiled against it and no longer can be — while an added line is new surface, which may be
    /// exactly what was intended and is still a thing somebody decided to ship forever.
    /// </remarks>
    private static string Describe(string baseline, string expected, string actual)
    {
        List<string> removed = [.. Missing(expected, actual)];
        List<string> added = [.. Missing(actual, expected)];

        StringBuilder message = new();
        _ = message.Append(CultureInfo.InvariantCulture, $"The public surface no longer matches '{baseline}': ")
            .Append(CultureInfo.InvariantCulture, $"{removed.Count} line(s) removed, {added.Count} line(s) added.\n");

        Append(message, "REMOVED (breaking: a consumer compiled against these)", removed);
        Append(message, "ADDED (new surface: possibly intended, still forever)", added);

        _ = message.Append('\n')
            .Append("If this change is intended, regenerate the baseline deliberately rather than editing it by hand: run this test once with ")
            .Append(UpdateVariable)
            .Append("=1, which rewrites the file and still fails, then review the rewritten file in the commit diff.");

        return message.ToString();

        static IEnumerable<string> Missing(string from, string against)
        {
            Dictionary<string, int> remaining = new(StringComparer.Ordinal);

            foreach (string line in against.Split('\n'))
            {
                remaining[line] = remaining.TryGetValue(line, out int count) ? count + 1 : 1;
            }

            foreach (string line in from.Split('\n'))
            {
                if (remaining.TryGetValue(line, out int count) && count > 0)
                {
                    remaining[line] = count - 1;

                    continue;
                }

                yield return line;
            }
        }

        static void Append(StringBuilder message, string heading, List<string> lines)
        {
            if (lines.Count == 0)
            {
                return;
            }

            _ = message.Append('\n').Append(heading).Append(":\n");

            foreach (string line in lines.Take(ReportedLineLimit))
            {
                _ = message.Append("  ").Append(line).Append('\n');
            }

            if (lines.Count > ReportedLineLimit)
            {
                _ = message.Append(CultureInfo.InvariantCulture, $"  ... and {lines.Count - ReportedLineLimit} more\n");
            }
        }
    }

    /// <summary>Finds the repository root by walking up from the test assembly's directory.</summary>
    /// <returns>The full path of the directory holding the solution file.</returns>
    /// <exception cref="InvalidOperationException">No ancestor directory holds the solution file.</exception>
    /// <remarks>
    /// The baselines live in the working tree rather than in the build output, for the reason the golden
    /// fixtures do: exactly one copy of a checked-in artifact should exist, and a copy step would put a
    /// second, stale one into the loop.
    /// </remarks>
    private static string RepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, SolutionFileName)))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException(
            $"No ancestor of '{AppContext.BaseDirectory}' holds '{SolutionFileName}', so the repository root that carries the public-surface baselines could not be located. These tests must run from a build inside the repository working tree.");
    }
}
