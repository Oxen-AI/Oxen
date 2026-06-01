# Error Handling in Oxen

This document defines and describes the standards for error handling in the Oxen codebase.

As Oxen is a rapidly developing pre 1.0 codebase, not all of the code adheres to these principles.
New code, however, must adhere to these principles. And old code should be brought in-line with
this consistent design pattern on a best-effort approach


## Goals

1. All functions that could fail must return a `Result` type. Using `.unwrap()` and `.expect()` are
   generally disallowed in the code as they cause the calling code to panic. Only tests that are
   asserting properties may use `.unwrap()` and `.expect()` on `Result` and `Option` typed values.

2. In `liboxen`, make an error a structured `OxenError` variant when — and only when — a caller will
   act on it: either the error is inspected (`match`ed) somewhere in the code, or it can be returned to
   a caller of the public liboxen API. A structured variant carries the information needed to understand
   and handle the condition programmatically; its `#[error("...")]` message documents the condition.
   Idiomatic Rust encourages the use of an `enum`-defined error type with one variant per meaningful
   condition.

3. An error that is never inspected and never crosses the public liboxen API does not need a structured
   variant. Encode it as `OxenError::InternalError` with a formatted string. (`OxenError::Basic` is the
   older, less specific form of the same idea; prefer `InternalError` for these internal cases.)
   Inventing a structured variant that no caller ever matches on adds ceremony without changing
   behavior, so reserve structured variants for the cases described in Goal 2.

4. End-user facing code (the CLI and server) must have descriptive error messages that explain the
   problem clearly. When it's possible to correct the error, the user-facing error message must
   provide guidance or instructions the user can follow that will rectify the issue.

5. Do not introduce new error types when a crate's top-level error type fits. `liboxen` uses
   `OxenError` and `oxen-server` uses `OxenHttpError`. Prefer extending the top-level type — or
   wrapping a third-party error into it with a `#[from]` conversion — over defining a new module- or
   crate-local error `enum`.

6. `liboxen` library code returns `Result<T, OxenError>`; wrap more specific error types into
   `OxenError` with `#[from]` conversions. `oxen-server` translates those failures into HTTP responses
   through `OxenHttpError`: map each `OxenError` variant you want to differentiate to the API caller to a
   specific `OxenHttpError` variant (e.g. `RepoNotFound` → 404) and map everything else to
   `OxenHttpError::InternalServerError`.


## Specific Guidance for Modernization of Existing Oxen Code

When you touch existing code that uses `OxenError::Basic` or `OxenError::InternalError`, decide based on
how the error is used:

- If the error is inspected somewhere, or can be returned to a caller of the public liboxen API, convert
  it to a structured `OxenError` variant. The existing string message can be reused as the
  `#[error("...")]` on the new variant.
- Otherwise it is an internal error that no caller acts on; leave it as a string error, preferring
  `OxenError::InternalError` over the older `OxenError::Basic`.
