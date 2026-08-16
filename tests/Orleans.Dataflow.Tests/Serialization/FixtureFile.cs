using System.Globalization;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Reads the golden fixture files that live in the repository rather than in the test output.
/// </summary>
/// <remarks>
/// <para>
/// The fixtures are byte-exact artifacts under source control, and a golden test compares raw bytes
/// against them. They are deliberately read from the working tree instead of being copied to the build
/// output: a copy step would put a second, stale copy of a byte-exact artifact into the loop, and the
/// point of a golden fixture is that exactly one copy of it exists.
/// </para>
/// <para>
/// The repository root is found by walking up from the test assembly's directory to the directory that
/// holds the solution file. That keeps the lookup independent of the configuration, target framework, and
/// working directory the test host happens to run under.
/// </para>
/// </remarks>
internal static class FixtureFile
{
    /// <summary>The file that marks the repository root.</summary>
    private const string SolutionFileName = "Orleans.Dataflow.slnx";

    /// <summary>The repository-relative directory the fixtures live in.</summary>
    private static readonly string[] FixtureDirectory = ["tests", "Orleans.Dataflow.Tests", "Fixtures"];

    /// <summary>
    /// Reads a fixture file as raw bytes.
    /// </summary>
    /// <param name="fileName">The file name inside the fixture directory.</param>
    /// <returns>The exact bytes on disk, with no decoding and no line-ending translation.</returns>
    /// <exception cref="InvalidOperationException">
    /// The repository root or the fixture file cannot be found. The message names what was searched for
    /// and where.
    /// </exception>
    internal static byte[] Read(string fileName)
    {
        string path = Path.Combine([RepositoryRoot(), .. FixtureDirectory, fileName]);

        if (!File.Exists(path))
        {
            throw new InvalidOperationException(
                $"The golden fixture '{fileName}' was not found at '{path}'. Fixtures are byte-exact artifacts under source control; regenerate or restore the file rather than adjusting the test.");
        }

        return File.ReadAllBytes(path);
    }

    /// <summary>
    /// Finds the repository root by walking up from the test assembly's directory.
    /// </summary>
    /// <returns>The full path of the directory holding the solution file.</returns>
    /// <exception cref="InvalidOperationException">No ancestor directory holds the solution file.</exception>
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
            string.Create(
                CultureInfo.InvariantCulture,
                $"No ancestor of '{AppContext.BaseDirectory}' holds '{SolutionFileName}', so the repository root that carries the golden fixtures could not be located. The fixture tests must run from a build inside the repository working tree."));
    }
}
