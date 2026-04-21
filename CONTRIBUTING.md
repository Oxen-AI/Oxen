# Contributing to Oxen

Thanks for your interest in contributing to Oxen! 🐂 This document describes what kinds of contributions we accept, how to structure them, and what to expect from the review process.

Oxen is an actively developed project with a dedicated maintainer team. External contributions are welcome, but every PR costs maintainer time to review, test, and support long-term.
We apply the guidelines below to keep that cost sustainable and the project focused.

> "Newton's 2nd law of OSS": every PR's contributor effort has an equal and opposite effort from the maintainers.

The Oxen team reserves the right to update this document from time to time, and to handle PRs on a case-by-case basis.

## What We Maintain

### Stable interfaces

- **`liboxen`** — The core Rust library is ready to build on. We welcome contributions that extend it, improve it, or add coverage around it.
- **`oxen` (CLI) and `oxen-server`** — These are **not yet ready** for external customization. We are not currently accepting contributions that add new extension points, plugin hooks, or customization surfaces to these binaries. We eventually want to stabilize these and add some support for plugins, but these integration points are still being actively developed.

### High-level design principles

Some parts of the system have invariants we will not break. For example:

- The only state in `oxen-server` is the state of the repositories it manages. The server must be able to be killed mid-flight, restarted, and recover to the same working behavior.
- The `oxen` client is also stateless. The repository is the only state.

Contributions that change this kind of high-level design principle will not be accepted without prior discussion and maintainer agreement.

## What We Accept

Good external contributions typically:

- Add features "in between the existing lines" — fitting naturally into the current architecture without introducing a new class of functionality.
- Improve test coverage.
- Add or update doc comments on new or changed code.
- Fix bugs.
- Improve ergonomics of existing APIs.

## Borderline Contributions

These may or may not be accepted — please open an issue and discuss with the Oxen team **before** writing code:

- Large pieces of work that introduce a new class of functionality.
- Anything where the design space is broad and maintainer input would materially change the approach.

## What We Don't Accept

We will likely decline PRs that:

- **Drop performance.** Oxen's value proposition is speed; regressions are not acceptable.
- **Are too large.** We aim for **500 lines of changes or less**. PRs larger than this may or may not be reviewed depending on their size; complex or large PRs may be asked to be broken up.
- **Conflict with the team's direction.** This includes features that conflict with ongoing active development, or features that are explicitly not on the team's roadmap, or anything else that conflicts with the Oxen team's planned, or actively developed, functionality.
- **Unreasonably increase maintenance burden.**
- **Have little to no real users.** New features should address concrete, demonstrated needs.
- **Can be covered by composing existing APIs.** If the functionality is already achievable with existing pieces, we generally won't add a new surface for it.
- **Are pure convenience functions.** Ergonomic improvements to existing APIs are welcome; thin convenience wrappers are not. This is a _great_ place to consider building _on top of_ oxen!
- **Change high-level design principles** without prior discussion and agreement (see "High-level design principles" above).
- **Add customization surfaces to `oxen` or `oxen-server`** (see "Stable interfaces" above).

A lot of this is a judgment call on our end. If you are unsure whether your contribution fits, please open an issue first to discuss scope before investing significant effort.

## Before You Start

1. **Open an issue** describing what you want to change and why, especially for anything beyond a small bug fix or doc update. This is the fastest way to find out whether your idea fits the team's direction before you write code. Or hop onto [Discord](https://discord.gg/s3tBEn7Ptg) to talk to an Oxen team member directly.
2. **Check open issues** to avoid duplicating work in progress.
3. **Keep the PR small and focused.** Target 500 lines of changes or less. If the work is naturally larger, propose a split in the issue before starting.

## Development Guidelines

See [`.claude/CLAUDE.md`](.claude/CLAUDE.md) and the [CI workflows](.github/workflows) for project layout, build and test commands, and coding conventions (error handling, module organization, async I/O rules, etc.). In short:

- Core functionality goes in `liboxen` first, then is exposed through the CLI and server.
- Use `cargo fmt --all` and `cargo clippy --all-targets --workspace --no-deps -- -D warnings` before submitting.
- Run tests with `bin/test-rust` (Rust) and `bin/test-rust -p` (Python).
- Follow the error handling rules: no `.unwrap()` or `.expect()` on `Result`/`Option` outside test code; propagate with `?`; add a new `#[from]` variant to `OxenError` that wraps your locally-defined errors.
- New or changed I/O code should be async (use `tokio` equivalents, or `spawn_blocking` when a dependency is sync-only).
- If your change affects Rust code that the Python bindings call into, update the Python side too.
- Update any documentation (including markdown files and doc comments) affected by your change.

## Review Process

- We review PRs as maintainer time allows.
- Large or complex PRs may be asked to be broken into smaller pieces before review.
- We may decline contributions for any of the reasons listed above, even if the code is well-written. When that happens, we'll do our best to explain why.

Thanks for helping make Oxen better. ❤️
