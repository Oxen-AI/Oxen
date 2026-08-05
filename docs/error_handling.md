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


## Checklist: adding or changing an `OxenError` variant

Defining the variant is the easy part. What gets missed is wiring it into the places that *classify*
errors — each is a separate `match` that silently falls through to a default when a new variant is
absent, so nothing fails to compile and nothing fails a test. Walk this list every time.

**1. Does it need a specific HTTP status?** `oxen-server`'s `error_response` in
`crates/oxen-server/src/errors.rs` maps `OxenError` variants to responses. A variant with no arm
falls to the catch-all and becomes **HTTP 500**. Add an arm whenever the caller should see anything
else — and note that a 500 for a caller's mistake both invites a pointless retry and reports the
request as a server fault.

**2. Does it need a hint?** `OxenError::hint` returns the "here is how to fix it" line the CLI
prints. Add an arm when there is a concrete action the user can take.

**3. Is it a "not found"?** `OxenError::is_not_found`. This one has reach beyond its name: several
client retry loops branch on it, and `is_fatal_for_retry` short-circuits on it.

**4. Will retrying ever succeed?** `OxenError::is_fatal_for_retry`. Return true for anything
deterministic. A variant that is absent here defaults to *retryable*, so a permanent failure gets
the full exponential backoff before surfacing — the user waits, and the answer does not change.

**5. Does an existing classifier already cover it?** Besides the above there are
`is_auth_error`, `is_unsupported_image_format`, and `is_image_too_large`. Adding a variant that
belongs in one of these and forgetting is the same silent-default failure.

**6. Does the variant carry what a reader needs to act?** Prefer fields over prose. An error that
says "file not found" without the path, or "version missing" without the hash, produces a report
nobody can act on. The fields cost nothing and are what turn a report into a diagnosis.

**7. Watch `#[from]` on the variant you are classifying.** If a variant is reachable by `#[from]`
conversion, a bare `?` anywhere converts into it *before* your classification code runs. A
classifier added at one call site can be bypassed entirely by a `?` at another. When adding
classification around a `#[from]` variant, find every conversion site, not just the one you are
looking at.

**8. Keep variable data out of the log message.** The log level decides what is reported as an
incident, and the message text is the grouping key for those reports. Interpolating a hash, path, or
id into the message produces a *separate* report per value rather than one for the condition. Log a
constant message with the variable as a structured field (`tracing::error!(hash = %hash, "...")`)
unless per-value grouping is genuinely what you want. Note `log::error!` has no structured fields
and always formats into the message.

**9. Match the log level to who is at fault.** `error_response` documents the convention: `error!`
for a server-side defect, `warn!` for a request the caller got wrong, `debug!` when there is no
identifying detail worth keeping. `error!` is what becomes a reported incident, so a 4xx logged at
`error!` turns ordinary client traffic into alerts.

**10. Every 5xx is logged at `error!` exactly once, by whoever holds the detail.** Nothing else
reports one: `tracing-actix-web`'s `emit_event_on_error` is disabled (see the workspace
`Cargo.toml`), so an unlogged 5xx is **silent**. Where the log goes follows from who has the error:

- The variant carries it: log in the `error_response` arm, the only place with the detail.
- The variant is empty: the arm stays silent and the failure is reported before the variant is
  constructed, either at the construction site or by the callee that returned the error.
  `OxenHttpError::InternalServerError` is this case. A log in that arm would be a contextless
  duplicate of a report that already carries the detail.

Before adding a log anywhere on a 5xx path, check both directions for one that already exists: the
callers that construct the error, and the callee that returned it. A liboxen function that logs in
an `inspect_err` has already reported the failure, so a handler that logs again on the way out
doubles it. This applies to handlers that build a 500 directly, not just to `error_response`.

**11. Test the classification. Never test the definition.**

Tests are for logic and interactions. A variant constructed directly is exactly what it was
hard-coded to be, so asserting that proves nothing and costs a test to compile, run, and maintain
forever. **Do not write tests like these:**

```rust
// Worthless: the field holds what it was just given.
let err = OxenError::NotAFile(PathBuf::from("a/b").into());
assert_eq!(err.to_string(), "Not a single file: a/b");   // restates #[error(...)]
assert!(matches!(err, OxenError::NotAFile(_)));          // restates the constructor
```

Test what the variant *causes* somewhere else:

```rust
// Worth having: exercises error_response's mapping, which has a silent default.
let status = OxenHttpError::from(OxenError::NoChanges).error_response().status();
assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
assert!(!status.is_server_error());
```

**The test to apply: does the assertion restate a line of source, or exercise a computation?**

Adding a variant to `is_not_found` is one line. A test asserting `err.is_not_found()` restates that
line and nothing else — there is no branch, no input, no interaction, only membership in a
`matches!` list. Write the line correctly and move on. The same goes for `hint` and for any other
list-shaped classifier: **these do not get their own tests.**

Beware the trap of "but it fails if I remove the fix". That proves nothing — a definitional test
fails when you delete the definition too (`assert_eq!(err.x, 1)` fails if you remove the field). It
is a necessary property of any useful test, not evidence that a test is useful. Judge by whether
there is computation to exercise.

What that leaves worth testing here is the HTTP boundary. `error_response` is a published contract:
the status a caller receives is observable behavior other systems depend on, and asserting
`!status.is_server_error()` pins the *intent* — that this is not a server fault — rather than
restating which arm was written. Keep those. Everything upstream of that boundary is internal
plumbing whose only observable effect shows up there anyway.

If a test would fail only because someone deliberately edited the one line it mirrors, it is not
earning its compile time. Delete it.

## Specific Guidance for Modernization of Existing Oxen Code

When you touch existing code that uses `OxenError::Basic` or `OxenError::InternalError`, decide based on
how the error is used:

- If the error is inspected somewhere, or can be returned to a caller of the public liboxen API, convert
  it to a structured `OxenError` variant. The existing string message can be reused as the
  `#[error("...")]` on the new variant.
- Otherwise it is an internal error that no caller acts on; leave it as a string error, preferring
  `OxenError::InternalError` over the older `OxenError::Basic`.
