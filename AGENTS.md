# Orleans.Dataflow agent contract

This file is the durable working agreement for automated contributors. It applies to the entire repository.

## Product boundary

- Orleans.Dataflow is an independent project. Do not couple its architecture to Orleans.SearchableStorage or Orleans.FSharp.
- Build an original Orleans-native dataflow system. Akka.NET Streams is a capability and semantics reference, not source code to port.
- C# and F# are equal public frontends over one language-neutral graph model; the C# API is designed and built first, and no C# or core decision may make the idiomatic F# API impossible.
- Keep source configuration, reusable flow configuration, and sink configuration distinct.
- Treat flows and complete graph definitions as immutable, reusable, composable values.
- Do not serialize delegates, closures, expression trees containing captured state, or language-specific function representations as durable topology.

## Pre-1.0 workflow

- The repository is explicitly work in progress until the 1.0.0 readiness criteria are met.
- Before 1.0.0, reviewed changes are committed directly to `main` in small checkpoints. Do not create pull requests unless the user changes this policy.
- Commit frequently enough that a lost session cannot erase a substantial completed unit.
- Do not create tags, GitHub releases, or publish NuGet packages before the explicit 1.0.0 release decision.
- Never add co-authors to commits.

## Engineering standard

- Source code, comments, XML documentation, tests, commit messages, and repository documentation are written in English.
- Prefer ordinary, discoverable C# and .NET patterns over clever metaprogramming.
- Public API changes require documentation and tests in the same checkpoint.
- Define completion, failure, cancellation, ordering, backpressure, buffering, and durability semantics explicitly. Do not rely on accidental runtime behavior.
- Use bounded resources by default. Any unbounded option must be explicit and documented as such.
- Do not claim production readiness, exactly-once delivery, durability, or feature parity without executable evidence.
- Inspect the current tree and relevant primary documentation before changing an established contract.

## Delegation and review

- Use lower-cost agents for bounded mechanical work such as inventories, scaffolding, formatting, and repetitive tests.
- Use stronger coding agents for implementation units that require local design decisions.
- Architecture, task decomposition, public API decisions, final code review, and release verdict remain the responsibility of the primary high-reasoning agent.
- Subagent output is input to review, not an automatic merge decision.

## Definition of a valid checkpoint

A checkpoint is ready to commit only when:

1. its scope is coherent and documented;
2. relevant tests and static checks pass;
3. the diff contains no unrelated files or generated secrets;
4. unfinished behavior is marked honestly;
5. public semantics introduced by the checkpoint are recorded in docs.
