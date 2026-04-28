//! Attribute macro for `liboxen`'s `IntoOxenError` trait.
//!
//! Apply `#[oxen_macros::from_ox]` to an enum (typically `OxenError`) and mark
//! the foreign-error fields you want auto-converted with `#[from_ox]` on the
//! field. The macro:
//!
//! 1. Strips each `#[from_ox]` field marker and inserts `#[source]` in its
//!    place. This makes thiserror's subsequent `#[derive(thiserror::Error)]`
//!    chain `Error::source()` to the wrapped foreign error — the same
//!    behaviour that thiserror's `#[from]` provides implicitly. (We need to
//!    rewrite the input here because a `#[proc_macro_derive]` cannot modify
//!    its input; only an attribute macro can.)
//! 2. Emits `impl IntoOxenError for <FieldType>` for each marked field, which
//!    when combined with the crate-wide blanket
//!    `impl<E: IntoOxenError> From<E> for OxenError` makes `?` work both at
//!    every concrete call site and through traits whose `type Error: IntoOxenError`.
//!
//! Usage shape (apply the attribute **before** `#[derive(thiserror::Error)]`
//! so thiserror sees the rewritten enum):
//!
//! ```ignore
//! #[oxen_macros::from_ox]
//! #[derive(thiserror::Error, Debug)]
//! pub enum OxenError {
//!     #[error("IO error: {0}")]
//!     IO(#[from_ox] std::io::Error),
//!     #[error("Schema invalid: {0}")]
//!     InvalidSchema(#[from_ox] Box<Schema>),
//!     #[error("AWS error: {0}")]
//!     AwsError(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
//! }
//! ```
//!
//! ## `Box<T>` auto-unboxing
//!
//! When the marked field's syntactic type is `Box<T>`, **two** `IntoOxenError`
//! impls are emitted instead of one:
//!
//! 1. `impl IntoOxenError for Box<T>` — constructs the variant directly.
//! 2. `impl IntoOxenError for T` — wraps `self` in a fresh `Box::new(...)`
//!    before constructing the variant.
//!
//! Because the crate-wide blanket `impl<E: IntoOxenError> From<E> for OxenError`
//! turns each `IntoOxenError` impl into a corresponding `From` impl, **the same
//! `T` cannot be the inner type of two different `Box<T>` variants** — the
//! compiler will report a conflicting `IntoOxenError for T`. Pick one canonical
//! variant for `T`. The same is true if you have one variant
//! `Variant1(#[from_ox] T)` and another `Variant2(#[from_ox] Box<T>)`.
//!
//! ## When the inner-`T` impl is **not** emitted
//!
//! `IntoOxenError::into_oxen` takes `self` by value, which requires `Sized`.
//! If the inner type can't be guaranteed `Sized` from the syntactic form alone,
//! emitting the inner impl would produce a downstream compile error. The
//! auto-unbox impl is therefore emitted only when the inner type is a
//! [`syn::Type::Path`] (e.g. `MyType`, `crate::path::MyType<'a, U>`). Other
//! shapes — `dyn Trait`, `[T]`, `&T`, etc. — silently fall back to "outer impl
//! only". The outer `impl IntoOxenError for Box<...>` is still emitted in
//! every case, since `Box<...>` itself is always sized.
//!
//! ## Generated code (for the example above)
//!
//! ```ignore
//! // After macro expansion, the enum looks like:
//! #[derive(thiserror::Error, Debug)]
//! pub enum OxenError {
//!     #[error("IO error: {0}")]
//!     IO(#[source] std::io::Error),
//!     #[error("Schema invalid: {0}")]
//!     InvalidSchema(#[source] Box<Schema>),
//!     #[error("AWS error: {0}")]
//!     AwsError(#[source] Box<dyn std::error::Error + Send + Sync>),
//! }
//!
//! // Plus, alongside the rewritten enum:
//! impl crate::error::IntoOxenError for std::io::Error {
//!     fn into_oxen(self) -> OxenError { OxenError::IO(self) }
//! }
//! impl crate::error::IntoOxenError for Box<Schema> {
//!     fn into_oxen(self) -> OxenError { OxenError::InvalidSchema(self) }
//! }
//! impl crate::error::IntoOxenError for Schema {
//!     fn into_oxen(self) -> OxenError {
//!         OxenError::InvalidSchema(::std::boxed::Box::new(self))
//!     }
//! }
//! impl crate::error::IntoOxenError
//!     for Box<dyn std::error::Error + Send + Sync>
//! {
//!     fn into_oxen(self) -> OxenError { OxenError::AwsError(self) }
//! }
//! ```
//!
//! **NOTE**: This macro can **only be used from within `liboxen`** because the
//! emitted impls reference `crate::error::IntoOxenError` by absolute crate
//! path.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input, parse_quote, spanned::Spanned};

#[proc_macro_attribute]
pub fn from_ox(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let enum_name = input.ident.clone();

    let Data::Enum(data) = &mut input.data else {
        return syn::Error::new(input.span(), "`#[from_ox]` can only be applied to enums")
            .to_compile_error()
            .into();
    };

    // Reject generic enums up-front. The macro emits `impl ... for #FieldType
    // { fn into_oxen(self) -> #enum_name { #enum_name::#Variant(self) } }`, which
    // references the enum by its bare ident. For an `enum E<T> { ... }`, the
    // bare `E` isn't a valid type spelling, so the compiler would produce a
    // confusing "missing generics for enum `E`" diagnostic pointing at
    // macro-generated tokens. A clear up-front error is friendlier.
    //
    // This isn't a current bug (the only intended user, `OxenError`, has no
    // generics) — it's a guard against future misuse.
    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "`#[from_ox]` cannot be applied to enums with generic parameters",
        )
        .to_compile_error()
        .into();
    }

    let mut impls = Vec::new();
    let mut errors = Vec::new();

    for variant in &mut data.variants {
        let Fields::Unnamed(fields) = &mut variant.fields else {
            // Struct-like or unit variants can't carry `#[from_ox]`; skip.
            if let Some(attr) = find_from_ox_attr_in_fields(&variant.fields) {
                errors.push(syn::Error::new(
                    attr.span(),
                    "`#[from_ox]` is only valid on single-field tuple variants",
                ));
            }
            continue;
        };

        // Are any fields in this variant marked `#[from_ox]`?
        let any_marked = fields
            .unnamed
            .iter()
            .any(|f| f.attrs.iter().any(is_from_ox_attr));
        if !any_marked {
            continue;
        }

        if fields.unnamed.len() != 1 {
            errors.push(syn::Error::new(
                variant.span(),
                "`#[from_ox]` is only valid on single-field tuple variants",
            ));
            continue;
        }

        let variant_ident = variant.ident.clone();
        let outer_ty = fields.unnamed[0].ty.clone();

        // Rewrite the field's attributes:
        //   - drop `#[from_ox]` (its job is done — the impls are emitted below);
        //   - add `#[source]` (so thiserror's `#[derive(Error)]`, which runs
        //     after this attribute macro on the rewritten input, chains
        //     `Error::source()` to the wrapped foreign error).
        // Skip the add if `#[source]` (or `#[from]`) is already present, to
        // avoid generating a duplicate.
        let field = &mut fields.unnamed[0];
        field.attrs.retain(|a| !is_from_ox_attr(a));
        let already_has_source = field
            .attrs
            .iter()
            .any(|a| a.path().is_ident("source") || a.path().is_ident("from"));
        if !already_has_source {
            field.attrs.push(parse_quote!(#[source]));
        }

        // Emit `IntoOxenError for <FieldType>`.
        impls.push(quote! {
            impl crate::error::IntoOxenError for #outer_ty {
                fn into_oxen(self) -> #enum_name {
                    #enum_name::#variant_ident(self)
                }
            }
        });

        // If `<FieldType>` is syntactically `Box<T>` and `T` is path-shaped
        // (and so very likely sized), also emit `IntoOxenError for T` that
        // re-boxes on construction. See the module docs for the rationale on
        // restricting to path-shaped inner types.
        if let Some(inner_ty) = unbox_inner_path_type(&outer_ty) {
            impls.push(quote! {
                impl crate::error::IntoOxenError for #inner_ty {
                    fn into_oxen(self) -> #enum_name {
                        #enum_name::#variant_ident(::std::boxed::Box::new(self))
                    }
                }
            });
        }
    }

    let error_tokens = errors.into_iter().map(|e| e.to_compile_error());

    quote! {
        #(#error_tokens)*
        #input
        #(#impls)*
    }
    .into()
}

fn is_from_ox_attr(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("from_ox")
}

fn find_from_ox_attr_in_fields(fields: &Fields) -> Option<&syn::Attribute> {
    match fields {
        Fields::Named(named) => named
            .named
            .iter()
            .flat_map(|f| &f.attrs)
            .find(|a| is_from_ox_attr(a)),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .flat_map(|f| &f.attrs)
            .find(|a| is_from_ox_attr(a)),
        Fields::Unit => None,
    }
}

/// If `ty` is syntactically `Box<T>` (any path that ends in `Box` with one
/// type-argument), return the inner `T` *only if* `T` itself is a `Type::Path`.
///
/// That filter excludes shapes like `dyn Trait`, `[T]`, `&T`, tuples, etc., for
/// which an `impl IntoOxenError for T { fn into_oxen(self) }` would not compile
/// because `into_oxen` takes `self` by value (requires `Sized`).
///
/// Type aliases for `Box<...>` are not unwrapped — the macro only matches the
/// literal token `Box`. Callers that want auto-unboxing through an alias should
/// spell the field type as `Box<T>` directly.
fn unbox_inner_path_type(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(syn::TypePath { qself: None, path }) = ty else {
        return None;
    };
    let last = path.segments.last()?;
    if last.ident != "Box" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };

    let mut type_args = args.args.iter().filter_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    });
    let first = type_args.next()?;
    if type_args.next().is_some() {
        // `Box<A, B>` (e.g. custom allocator) — bail out; we only handle the
        // standard one-argument shape.
        return None;
    }

    // Only emit the inner impl for path-shaped inner types.
    if matches!(first, syn::Type::Path(_)) {
        Some(first)
    } else {
        None
    }
}
