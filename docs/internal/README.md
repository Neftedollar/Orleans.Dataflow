# Engineering records

These are not documentation. They are the design record the library was built
from: written milestone by milestone, chronological, and full of sections marked
*historical* or *design ahead of code* that describe a system that no longer
exists in that form.

They are kept because they are the argument behind the behaviour — when the
documentation says a rule holds, one of these files is usually where the rule
was decided and where the alternatives were rejected. They are the right thing
to read when you are changing the library, and the wrong thing to read when you
are using it.

**If you are using the library, start at [the documentation](../index.md).**

| File | What it records |
|---|---|
| `LOCAL-RUNTIME.md` | The in-process engine, checkpoint by checkpoint, from the first strict-pull core to the full graph runtime. Its own preamble explains how to read it. |
| `ORLEANS-RUNTIME.md` | The cluster runtime: grains, ownership epochs, durable runs, resume, and the rolling-upgrade discipline. |
| `C-SHARP-API.md` | The C# authoring surface as it was designed, with the naming and overload arguments. |
| `F-SHARP-API.md` | The F# frontend's design, and the constraints the .NET languages put on it. |
| `DEFINITION-MODEL.md` | The graph document, identities, canonical serialization, and validation. |
| `REGISTERED-STAGES.md` | The provider model: how a named stage is registered and resolved. |
| `FRAGMENT-ALGEBRA.md` | Composition and identity rebasing for reusable graph fragments. |
| `ORLEANS-NOTES.md` | Facts about Orleans itself that the design had to respect. |
| `*-previous.md` | The pre-restructure operator, adapter, and compatibility documents, kept until every fact in them has been carried into the documentation. |

The numbered decision records live in [`../architecture/`](../architecture).
They are project history and are deliberately not linked from the documentation:
a decision that matters to a reader is explained in the documentation itself,
in its own words, rather than by sending the reader to an argument written for
somebody else.
