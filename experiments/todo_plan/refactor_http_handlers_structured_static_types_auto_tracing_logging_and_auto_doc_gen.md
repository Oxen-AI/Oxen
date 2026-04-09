# Refactor: Structured Static Types, Auto-Tracing, and Auto-Doc Generation for HTTP Handlers

## Vision

Move `oxen-server` to a model where every HTTP route is defined exactly once through typed structs. Path parameters, query parameters, and request bodies are all statically typed at the handler signature level. From those types, tracing instrumentation and OpenAPI documentation are derived automatically — no manual `path_param(&req, "...")` calls, no hand-written `#[utoipa::path(params(...))]`, and no per-handler tracing boilerplate.

## Current State

- **139 handler functions** across 32 controller files
- **119 routes** with 16 unique path parameter combinations
- Path params extracted dynamically via `path_param(&req, "name")` (string-based, runtime errors)
- 9 handlers bypass `path_param` entirely, using raw `match_info().get()` / `.query()`
- 81 of 139 handlers have `#[utoipa::path]` annotations with manually written `params(...)` sections
- 6 query structs derive `IntoParams`; 11 do not
- Some request bodies parsed from raw `String` instead of `web::Json<T>`
- No proc-macro crate exists in the workspace
- `utoipa`'s `actix_extras` feature is not enabled

## Architecture

### New crate: `crates/macros/`

A proc-macro crate providing:
- `#[derive(PathParams)]` — generates a declarative macro bridge + trait impl for tracing
- `#[oxen_instrument]` — attribute macro that replaces `#[tracing::instrument]` on handlers

### Trait: `PathParams`

```rust
/// Implemented by path parameter structs to record their fields
/// onto a tracing span.
pub trait PathParams {
    /// Record all field values onto the given span.
    /// Fields must already be declared on the span as `Empty`.
    fn record_on_span(&self, span: &tracing::Span);
}
```

### The derive + declarative macro bridge

For a struct:

```rust
#[derive(Deserialize, IntoParams, PathParams)]
pub struct RepoPath {
    /// Namespace of the repository
    #[param(example = "ox")]
    pub namespace: String,
    /// Name of the repository
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
}
```

The `PathParams` derive generates:

```rust
// 1. A declarative macro that creates a span with these specific fields
#[macro_export]
macro_rules! __path_params_span_RepoPath {
    ($name:literal) => {
        ::tracing::info_span!(
            $name,
            namespace = ::tracing::field::Empty,
            repo_name = ::tracing::field::Empty,
        )
    };
    ($name:literal, $($extra:tt)*) => {
        ::tracing::info_span!(
            $name,
            namespace = ::tracing::field::Empty,
            repo_name = ::tracing::field::Empty,
            $($extra)*
        )
    };
}

// 2. The trait impl that records values at runtime
impl PathParams for RepoPath {
    fn record_on_span(&self, span: &::tracing::Span) {
        span.record("namespace", self.namespace.as_str());
        span.record("repo_name", self.repo_name.as_str());
    }
}
```

### The `#[oxen_instrument]` attribute macro

Transforms a handler function by:
1. Scanning the signature for `web::Path<T>` arguments
2. Extracting the type name `T` (e.g., `RepoPath`)
3. Emitting an invocation of `__path_params_span_RepoPath!("function_name")` to create the span
4. Calling `path.record_on_span(&span)` to populate the fields
5. Wrapping the original function body in the span's scope

**Input:**

```rust
#[oxen_instrument]
pub async fn list_branches(
    path: web::Path<RepoPath>,
    req: HttpRequest,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let repo = get_repo(&app_data.path, &path.namespace, &path.repo_name)?;
    // ...
}
```

**Generated output:**

```rust
pub async fn list_branches(
    path: web::Path<RepoPath>,
    req: HttpRequest,
) -> Result<HttpResponse, OxenHttpError> {
    let __span = crate::__path_params_span_RepoPath!("list_branches");
    let __entered = __span.entered();
    path.record_on_span(&__span);
    {
        let app_data = app_data(&req)?;
        let repo = get_repo(&app_data.path, &path.namespace, &path.repo_name)?;
        // ...
    }
}
```

The compiler expands in order: proc macro (`#[oxen_instrument]`) -> declarative macro (`__path_params_span_RepoPath!`) -> `tracing::info_span!`. Each stage only needs tokens, not type information.

**When no `web::Path<T>` is present:** The macro falls back to `#[tracing::instrument(skip_all)]` behavior — creates a basic span with just the function name, no path param fields.

### Span field behavior

- Fields are declared as `Empty` at span creation (via the declarative macro)
- Fields are populated immediately after span creation (via `record_on_span`)
- Fields appear in **both** stderr/file logs and OTel exports (Jaeger tags)
- The span is a child of the `TracingLogger` root HTTP span
- Each handler only declares fields for its specific path parameters — no wasted empty fields

---

## Step 1: Define Path Parameter Structs and Migrate Handlers

### 1.1 Define the 16 path parameter structs

Create `crates/server/src/params/path_params.rs` with all unique combinations. Each struct derives `Deserialize` and `IntoParams` (for OpenAPI). Later in Step 2, they'll also derive `PathParams` (for tracing).

```rust
use serde::Deserialize;
use utoipa::IntoParams;

/// Base path params present on almost all repo routes:
/// `/api/repos/{namespace}/{repo_name}/...`
#[derive(Deserialize, IntoParams)]
pub struct RepoPath {
    /// Namespace of the repository
    #[param(example = "ox")]
    pub namespace: String,
    /// Name of the repository
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
}

/// Routes that operate on a specific branch:
/// `.../branches/{branch_name}`
#[derive(Deserialize, IntoParams)]
pub struct RepoBranchPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main")]
    pub branch_name: String,
}

/// Routes that operate on a specific commit:
/// `.../commits/{commit_id}`
#[derive(Deserialize, IntoParams)]
pub struct RepoCommitPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "abc123def456")]
    pub commit_id: String,
}

/// Routes that accept a generic resource path (file, branch, commit ref):
/// `.../{resource}`
#[derive(Deserialize, IntoParams)]
pub struct RepoResourcePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main/data/train/image.jpg")]
    pub resource: String,
}

/// Comparison routes with base..head format:
/// `.../compare/{base_head}/...`
#[derive(Deserialize, IntoParams)]
pub struct RepoComparePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main..feature-branch")]
    pub base_head: String,
}

/// Commit-or-branch flexible param:
/// `.../commits/{commit_or_branch}/...`
#[derive(Deserialize, IntoParams)]
pub struct RepoCommitOrBranchPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main")]
    pub commit_or_branch: String,
}

/// Workspace routes:
/// `.../workspaces/{workspace_id}`
#[derive(Deserialize, IntoParams)]
pub struct RepoWorkspacePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "ws-abc123")]
    pub workspace_id: String,
}

/// Workspace file routes:
/// `.../workspaces/{workspace_id}/files/{path}`
#[derive(Deserialize, IntoParams)]
pub struct RepoWorkspaceFilePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "ws-abc123")]
    pub workspace_id: String,
    pub path: String,
}

/// Compare with resource:
/// `.../compare/{base_head}/file/{resource}`
#[derive(Deserialize, IntoParams)]
pub struct RepoCompareResourcePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main..feature-branch")]
    pub base_head: String,
    #[param(example = "data/train/image.jpg")]
    pub resource: String,
}

/// Compare with directory:
/// `.../compare/{base_head}/dir/{dir}/entries`
#[derive(Deserialize, IntoParams)]
pub struct RepoCompareDirPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main..feature-branch")]
    pub base_head: String,
    pub dir: String,
}

/// Branch with file path:
/// `.../branches/{branch_name}/versions/{path}`
#[derive(Deserialize, IntoParams)]
pub struct RepoBranchFilePath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    #[param(example = "main")]
    pub branch_name: String,
    pub path: String,
}

/// Version metadata:
/// `.../versions/{version_id}/metadata`
#[derive(Deserialize, IntoParams)]
pub struct RepoVersionPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    pub version_id: String,
}

/// Workspace merge with branch:
/// `.../workspaces/{workspace_id}/merge/{branch}`
#[derive(Deserialize, IntoParams)]
pub struct RepoWorkspaceBranchPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    pub workspace_id: String,
    pub branch: String,
}

/// Workspace batch directory:
/// `.../workspaces/{workspace_id}/files/batch/{directory}`
#[derive(Deserialize, IntoParams)]
pub struct RepoWorkspaceBatchPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    pub workspace_id: String,
    pub directory: String,
}

/// Namespace only:
/// `/api/repos/{namespace}`
#[derive(Deserialize, IntoParams)]
pub struct NamespacePath {
    #[param(example = "ox")]
    pub namespace: String,
}

/// Compare diff ID:
/// `.../compare/data_frames/{compare_id}`
#[derive(Deserialize, IntoParams)]
pub struct RepoCompareIdPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    pub compare_id: String,
}

/// Action completion:
/// `.../action/completed/{action}`
#[derive(Deserialize, IntoParams)]
pub struct RepoActionPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
    pub action: String,
}
```

### 1.2 Migrate handlers from `path_param(&req, "...")` to `web::Path<T>`

For each handler, the migration is mechanical:

**Before:**
```rust
pub async fn index(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    // ...
}
```

**After:**
```rust
pub async fn index(
    path: web::Path<RepoPath>,
    req: HttpRequest,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let repo = get_repo(&app_data.path, &path.namespace, &path.repo_name)?;
    // ...
}
```

**Migration checklist per handler:**
1. Identify which path param struct matches the route
2. Add `path: web::Path<T>` to the function signature
3. Remove `path_param(&req, "...")` calls
4. Replace `namespace`, `repo_name`, etc. with `path.namespace`, `path.repo_name`, etc.
5. For handlers that also use `parse_resource()` (which internally calls `match_info().query("resource")`), the `resource` field is now on the struct — update `parse_resource` to accept a `&str` instead of `&HttpRequest`

**Handlers to migrate (grouped by path param struct):**

| Struct | Approx. handler count | Key controller files |
|--------|----------------------|---------------------|
| `RepoPath` | ~50 | branches, commits, data_frames, dir, file, fork, import, repositories, workspaces |
| `RepoResourcePath` | ~15 | commits (history), data_frames, dir, file, import, metadata, revisions, versions, export |
| `RepoComparePath` | ~5 | diff |
| `RepoCommitPath` | ~5 | commits |
| `RepoWorkspacePath` | ~10 | workspaces, workspaces/files, workspaces/changes |
| `RepoBranchPath` | ~5 | branches |
| `RepoCommitOrBranchPath` | ~3 | commits |
| `RepoCompareIdPath` | ~2 | diff |
| `RepoWorkspaceFilePath` | ~4 | workspaces/files, workspaces/changes |
| `NamespacePath` | ~2 | namespaces |
| Others | ~10 | various |

### 1.3 Fix the 9 handlers that bypass `path_param`

These handlers (listed earlier in our conversation) use raw `match_info().get()` or `.query()`. Migrate them to `web::Path<T>` as part of this step:

- `params.rs::parse_resource` — refactor to accept `&str` instead of extracting internally
- `oxen_version.rs::resolve` — uses `Option<&str>` for optional params; may need a dedicated struct or `Option` fields
- `namespaces.rs::show` — same optional pattern
- `repositories.rs::stats` — same optional pattern
- `commits.rs::complete` — uses `.get().unwrap()` (unsafe); replace with `web::Path<RepoCommitPath>`
- `file.rs::handle_initial_put_empty_repo` — uses `.query("resource")`
- `import.rs::handle_initial_upload_zip_empty_repo` — uses `.query("resource")`
- `workspaces/data_frames.rs::get_by_branch` — uses `.query("branch")`

**Special case: optional path params** (oxen_version, namespaces, repositories). These use `match_info().get()` which returns `Option`. Actix's `web::Path<T>` doesn't support optional path segments — all fields are required. These handlers may need to keep using `req.match_info().get()` or define separate routes (one with the param, one without). This is a design decision for the migration.

---

## Step 2: Proc-Macro Crate with `#[derive(PathParams)]` and `#[oxen_instrument]`

### 2.1 Create `crates/macros/`

```
crates/macros/
    Cargo.toml
    src/
        lib.rs          # proc-macro entry points
        path_params.rs  # #[derive(PathParams)] implementation
        instrument.rs   # #[oxen_instrument] implementation
```

**`Cargo.toml`:**
```toml
[package]
name = "oxen-macros"
version = { workspace = true }
edition = { workspace = true }

[lib]
proc-macro = true

[dependencies]
proc-macro2 = "1"
quote = "1"
syn = { version = "2", features = ["full", "parsing", "visit"] }
```

### 2.2 `#[derive(PathParams)]` implementation

**`src/path_params.rs`:**

```rust
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Fields, Data};

pub fn derive_path_params(input: DeriveInput) -> TokenStream {
    let struct_name = &input.ident;
    let macro_name = format_ident!("__path_params_span_{}", struct_name);

    // Extract field names from the struct
    let fields: Vec<_> = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => named.named.iter()
                .map(|f| f.ident.as_ref().unwrap().clone())
                .collect(),
            _ => panic!("PathParams can only be derived for structs with named fields"),
        },
        _ => panic!("PathParams can only be derived for structs"),
    };

    // Generate the span.record() calls for each field
    let record_calls = fields.iter().map(|field| {
        let field_name = field.to_string();
        quote! {
            span.record(#field_name, self.#field.as_str());
        }
    });

    // Generate field declarations for the declarative macro (as Empty)
    let empty_fields = fields.iter().map(|field| {
        let field_name = field.to_string();
        quote! {
            #field = ::tracing::field::Empty
        }
    });
    let empty_fields2 = empty_fields.clone();

    quote! {
        // The declarative macro that creates a span with these specific fields.
        // Two arms: one without extra fields, one with.
        #[macro_export]
        macro_rules! #macro_name {
            ($name:literal) => {
                ::tracing::info_span!(
                    $name,
                    #(#empty_fields),*
                )
            };
            ($name:literal, $($extra:tt)*) => {
                ::tracing::info_span!(
                    $name,
                    #(#empty_fields2),* ,
                    $($extra)*
                )
            };
        }

        impl ::oxen_macros::PathParams for #struct_name {
            fn record_on_span(&self, span: &::tracing::Span) {
                #(#record_calls)*
            }
        }
    }
}
```

**Note on `as_str()`:** The derive assumes all fields are `String`. For non-String fields (e.g., `u32`, `u16`), the derive would need to detect the type and use `.to_string()` or `tracing::field::display()`. This can be handled by checking the field type in the proc macro, or by requiring all path param fields to be `String` (which they are for URL segments).

### 2.3 `#[oxen_instrument]` implementation

**`src/instrument.rs`:**

```rust
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, FnArg, PatType, Type, TypePath, PathSegment, GenericArgument};

pub fn oxen_instrument(input: ItemFn) -> TokenStream {
    let fn_name = input.sig.ident.to_string();
    let fn_vis = &input.vis;
    let fn_sig = &input.sig;
    let fn_body = &input.block;
    let fn_attrs = &input.attrs; // preserve other attributes like #[utoipa::path]

    // Search for web::Path<T> in the function arguments
    let path_param_info = find_web_path_arg(&input);

    match path_param_info {
        Some((arg_ident, type_name)) => {
            // Construct the declarative macro name: __path_params_span_{TypeName}
            let span_macro = format_ident!("__path_params_span_{}", type_name);

            quote! {
                #(#fn_attrs)*
                #fn_vis #fn_sig {
                    let __span = crate::#span_macro!(#fn_name);
                    let __entered = __span.entered();
                    ::oxen_macros::PathParams::record_on_span(
                        &#arg_ident,
                        &__span,
                    );
                    #fn_body
                }
            }
        }
        None => {
            // No web::Path<T> found — fall back to basic span
            quote! {
                #(#fn_attrs)*
                #fn_vis #fn_sig {
                    let __span = ::tracing::info_span!(#fn_name);
                    let __entered = __span.entered();
                    #fn_body
                }
            }
        }
    }
}

/// Find an argument of type `web::Path<T>` and return (arg_name, T_name).
fn find_web_path_arg(func: &ItemFn) -> Option<(syn::Ident, syn::Ident)> {
    for arg in &func.sig.inputs {
        if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
            // Extract the argument name
            let arg_name = if let syn::Pat::Ident(pat_ident) = pat.as_ref() {
                pat_ident.ident.clone()
            } else {
                continue;
            };

            // Check if the type matches web::Path<T>
            if let Some(inner_type) = extract_web_path_inner(ty) {
                return Some((arg_name, inner_type));
            }
        }
    }
    None
}

/// If `ty` is `web::Path<T>` or `Path<T>`, return the ident of `T`.
fn extract_web_path_inner(ty: &Type) -> Option<syn::Ident> {
    if let Type::Path(TypePath { path, .. }) = ty {
        let last_segment = path.segments.last()?;
        // Check if the last segment is "Path"
        if last_segment.ident != "Path" {
            return None;
        }
        // Optionally verify the prefix is "web" (for web::Path)
        // Extract the generic argument T
        if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
            if let Some(GenericArgument::Type(Type::Path(inner))) = args.args.first() {
                return inner.path.get_ident().cloned();
            }
        }
    }
    None
}
```

### 2.4 `src/lib.rs` — proc-macro entry points

```rust
use proc_macro::TokenStream;

mod path_params;
mod instrument;

/// Trait for recording path parameter values onto a tracing span.
pub trait PathParams {
    fn record_on_span(&self, span: &tracing::Span);
}

#[proc_macro_derive(PathParams)]
pub fn derive_path_params(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    path_params::derive_path_params(input).into()
}

#[proc_macro_attribute]
pub fn oxen_instrument(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    instrument::oxen_instrument(input).into()
}
```

**Important:** The `PathParams` trait must be defined in a non-proc-macro crate (proc-macro crates can only export procedural macros). Options:
- Define the trait in `liboxen` (crates/lib) and re-export
- Define the trait in a small `oxen-macros-types` crate that both the server and the proc-macro crate depend on
- Define the trait directly in the server crate

The simplest approach: define the trait in the server crate (`crates/server/src/params/path_params.rs`) alongside the structs, and have the derive macro generate `impl crate::params::PathParams for ...`. The proc-macro crate itself only exports the derive and attribute macros.

### 2.5 Add `PathParams` derive to all structs

Update every struct from Step 1 to also derive `PathParams`:

```rust
#[derive(Deserialize, IntoParams, PathParams)]
pub struct RepoPath {
    #[param(example = "ox")]
    pub namespace: String,
    #[param(example = "ImageNet-1k")]
    pub repo_name: String,
}
```

### 2.6 Add `#[oxen_instrument]` to all handlers

Replace existing `#[tracing::instrument(skip_all)]` (or add where missing):

```rust
#[oxen_instrument]
#[utoipa::path(...)]
pub async fn list_branches(
    path: web::Path<RepoPath>,
    req: HttpRequest,
) -> Result<HttpResponse, OxenHttpError> {
    // ...
}
```

### 2.7 What appears in traces

For a request to `GET /api/repos/ox/ImageNet-1k/branches`:

**Jaeger span:**
```
HTTP request GET /api/repos/{namespace}/{repo_name}/branches     ← root (TracingLogger)
  └── list_branches  namespace=ox, repo_name=ImageNet-1k         ← handler span (oxen_instrument)
```

**Stderr log (with OXEN_FMT_SPAN=CLOSE):**
```
2026-04-09T12:00:00Z INFO list_branches{namespace=ox repo_name=ImageNet-1k}: ... 2.3ms
```

---

## Step 3: Extend to Query and Body Parameters

### 3.1 Query parameters

The same `PathParams` trait and derive can be extended to query structs. Or define a parallel `QueryParams` trait. The pattern is identical:

```rust
#[derive(Deserialize, IntoParams, QueryParams)]
pub struct PageNumQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}
```

The `QueryParams` derive generates `record_on_span` that records `page` and `page_size` (when `Some`). The `#[oxen_instrument]` macro scans for `web::Query<T>` in addition to `web::Path<T>`.

**Migration:** Most handlers already use `web::Query<T>`. The migration is just adding the derive and making sure the struct is used consistently.

### 3.2 Request bodies

For `web::Json<T>`, recording the entire body on a span is usually not desirable (too large). But specific fields could be useful. Options:
- Derive `BodyParams` that records selected fields (marked with an attribute)
- Skip body recording entirely and rely on the typed extraction for compile-time safety

Recommendation: Focus on compile-time safety (typed extraction via `web::Json<T>`) rather than tracing for request bodies. The tracing value is in path + query params.

### 3.3 Handlers currently parsing raw `String` bodies

Several handlers accept `body: String` and parse manually:
```rust
pub async fn list_missing(req: HttpRequest, body: String) -> ... {
    let data: Result<MerkleHashes, serde_json::Error> = serde_json::from_str(&body);
}
```

Migrate these to `web::Json<T>`:
```rust
pub async fn list_missing(
    req: HttpRequest,
    body: web::Json<MerkleHashes>,
) -> ... {
    let data = body.into_inner();
}
```

This gives automatic 400 responses on malformed JSON, removes manual error handling, and enables OpenAPI schema generation.

---

## Step 4: Automatic OpenAPI Documentation from Handler Signatures

### 4.1 Enable `actix_extras` feature on utoipa

**`Cargo.toml` (workspace root):**
```toml
utoipa = { version = "5", features = ["time", "actix_extras"] }
```

This enables utoipa to automatically detect `web::Path<T>` and `web::Query<T>` in handler signatures and derive the `params(...)` section of `#[utoipa::path]` from the struct's `IntoParams` implementation.

### 4.2 Simplify `#[utoipa::path]` annotations

**Before (current):**
```rust
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches",
    tag = "Branches",
    description = "List all branches in the repository.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "List of branches", body = ListBranchesResponse),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn index(req: HttpRequest) -> ... { }
```

**After:**
```rust
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches",
    tag = "Branches",
    description = "List all branches in the repository.",
    // params(...) is auto-derived from web::Path<RepoPath> + IntoParams!
    responses(
        (status = 200, description = "List of branches", body = ListBranchesResponse),
        (status = 404, description = "Repository not found")
    )
)]
#[oxen_instrument]
pub async fn index(
    path: web::Path<RepoPath>,
    req: HttpRequest,
) -> ... { }
```

The `params(...)` section is completely eliminated. Parameter descriptions come from doc comments on the struct fields. Parameter examples come from `#[param(example = "...")]` attributes on the struct fields. This information is defined once on the struct and reused across every handler that uses that struct.

### 4.3 What this achieves

| Concern | Defined where | Used by |
|---------|--------------|---------|
| Parameter names + types | Struct fields | actix extraction, utoipa docs, tracing spans |
| Parameter descriptions | Doc comments on struct fields | utoipa OpenAPI docs |
| Parameter examples | `#[param(example = "...")]` | utoipa OpenAPI docs |
| Parameter tracing | `#[derive(PathParams)]` | `#[oxen_instrument]` span recording |
| Parameter validation | `Deserialize` derive | actix extraction (automatic 400 on invalid) |

**One struct definition drives five concerns.** No duplication.

### 4.4 Add `#[utoipa::path]` to the 58 handlers missing it

With `actix_extras` and typed extractors, adding OpenAPI docs to the remaining 58 handlers is much simpler — you only need `path`, `tag`, `description`, and `responses`. The `params` section is automatic.

---

## Implementation Order

### Phase 1: Foundation (can be done incrementally, one controller at a time)
1. Create `crates/server/src/params/path_params.rs` with all 16+ structs (derive `Deserialize` + `IntoParams`)
2. Enable `actix_extras` feature on utoipa
3. Migrate handlers to `web::Path<T>`, starting with the most-used struct (`RepoPath`, ~50 handlers)
4. Remove `path_param()` calls as handlers are migrated
5. Simplify `#[utoipa::path]` params sections as handlers are migrated

### Phase 2: Tracing macros
6. Create `crates/macros/` proc-macro crate
7. Implement `#[derive(PathParams)]`
8. Implement `#[oxen_instrument]`
9. Add `PathParams` derive to all path param structs
10. Add `#[oxen_instrument]` to all handlers

### Phase 3: Query + body cleanup
11. Add `IntoParams` derive to remaining query structs (11 missing)
12. Migrate raw `String` body parsing to `web::Json<T>`
13. Extend `#[oxen_instrument]` to record query params

### Phase 4: Documentation completeness
14. Add `#[utoipa::path]` to the 58 handlers missing it
15. Remove the `path_param()` function from `params.rs` once all callers are migrated
16. Remove the 9 raw `match_info()` usages

---

## Verification

### Per-handler migration
- `cargo check -p oxen-server` after each handler change
- Run relevant integration tests via `bin/test-rust`

### Tracing verification
1. `OXEN_FMT_SPAN=CLOSE RUST_LOG=info cargo run -p oxen-server -- start`
2. Hit endpoints, verify stderr shows span fields: `list_branches{namespace=ox repo_name=ImageNet-1k}`
3. With Jaeger: verify individual tags appear on handler spans

### OpenAPI verification
1. Start server, visit Swagger UI at `/swagger-ui/`
2. Verify path parameters are documented with descriptions and examples
3. Compare against current docs to ensure no regression

### Full test suite
```bash
bin/test-rust
```

---

## Risk Mitigation

- **Incremental migration:** Each handler can be migrated independently. No big-bang refactor needed.
- **Backward compatible:** `path_param()` continues to work alongside `web::Path<T>` during migration. Both can coexist.
- **Type errors caught at compile time:** If a struct field name doesn't match the route's `{param}` placeholder, actix returns a 400 at runtime — but the struct definition makes it obvious and reviewable.
- **Proc-macro complexity:** The macros are straightforward token transformations. `syn` and `quote` are battle-tested. The declarative macro bridge is the only unusual technique.
