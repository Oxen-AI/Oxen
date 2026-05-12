# Error Handling in Oxen

This document defines and describes the standards for error handling in the Oxen codebase.

As Oxen is a rapidly developing pre 1.0 codebase, not all of the code adheres to these principles.
New code, however, must adhere to these principles. And old code should be brought in-line with
this consistent design pattern on a best-effort approach


## Goals

1. All functions that could fail must return a `Result` type. Using `.unwrap()` and `.expect()` are
   generally disallowed in the code as they cause the calling code to panic. Only tests that are
   asserting properties may use `.unwrap()` and `.expect()` on `Result` and `Option` typed values.

2. In library code, errors must be as structured and informative as possible. All of the information
   about the source of the error must be included so that a caller has a reasonable opportunity to
   understand the error and handle it programmatically. Idiomatic Rust encourages the use of an `enum`
   defined error type to encapsulate different conditions as variants of the `enum`.

3. Errors that obfuscate the source or transform an error into a simple string message must be avoided.
   In Rust, errors must either be defined as a `struct` or an `enum` that provides descriptive variants
   that each describe a specific error condition. For `enum`-defined errors, callers of code that uses
   these errors must be able to reasonably `match` on the `Err` variant. An error that is a string
   makes such programmatic introspection and handling of errors impossible.

4. End-user facing code (the CLI and server) must have descriptive error messages that explain the
   problem clearly. When it's possible to correct the error, the user-facing error message must
   provide guidance or instructions the user can follow that will rectify the issue.

5. Errors should be defined as close to the using code as possible. Strongly prefer making error `enum`s
   that are specific to a module, package, or crate. For example, wrappers and helpers on networking
   code should use a streamlined locally defined error `enum` that is specific to _networking_ errors.
   Calling code should rely on `From` implementations to convert from one logically related set of code's
   error type into an appropriate unifying container error type.

6. For `liboxen`, the top-level unifying error type is `OxenError`. All library code must have some
   conversion(s) that allow a more specific error type to be wrapped as an `OxenError`.


## Specific Guidance for Modernization of Existing Oxen Code

The `OxenError::Basic` and `OxenError::InternalError` variants are deprecated. No new code must use these
string error representations. These throw away all useful error information and encode it as a string,
making it impossible to `match` on specific error conditions and handle them programmatically. Library users
are unable to meaningfully handle these variants.

Any refactoring that touches these uses of string-defined OxenError variants should convert them into well-defined
variants of an error `enum`. At a bare minimum, they should be translated to a new structured _variant_
on `OxenError`. The existing string message can be used as the `#[error("...")]` on the new variant.
