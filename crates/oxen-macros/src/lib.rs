//! Proc-macro derive for `liboxen`'s `IntoOxenError` trait.
//!
//! On an enum variant with a single tuple field, annotate the field with
//! `#[from_ox]` to auto-generate `impl IntoOxenError for <FieldType>` that
//! constructs the enum via that variant. Mirrors the shape of `thiserror`'s
//! `#[from]`, but routes through `IntoOxenError` so the crate-wide blanket
//! `impl<E: IntoOxenError> From<E> for OxenError` stays unambiguous.
//!
//! ## `Box<T>` auto-unboxing
//!
//! When the annotated field's syntactic type is `Box<T>`, **two** impls are
//! emitted instead of one:
//!
//! 1. `impl IntoOxenError for Box<T>` — constructs the variant directly.
//! 2. `impl IntoOxenError for T` — wraps `self` in a fresh `Box::new(...)`
//!    before constructing the variant.
//!
//! This lets callers use `?` against an unboxed `T` value without manually
//! boxing at every call site, while preserving the option to construct from
//! an already-allocated `Box<T>`.
//!
//! Because the crate-wide blanket `impl<E: IntoOxenError> From<E> for OxenError`
//! turns each `IntoOxenError` impl into a corresponding `From` impl, **the same
//! `T` cannot be the inner type of two different `Box<T>` variants** — the
//! compiler will report a conflicting `IntoOxenError for T` (and a conflicting
//! `From<T> for OxenError`). Pick one canonical variant for `T`. The same is
//! true if you have one variant `Variant1(#[from_ox] T)` and another
//! `Variant2(#[from_ox] Box<T>)` — those two also conflict on `IntoOxenError for T`.
//!
//! ## When the inner-`T` impl is **not** emitted
//!
//! `IntoOxenError::into_oxen` takes `self` by value, which requires `Sized`.
//! If the inner type can't be guaranteed `Sized` from the syntactic form alone,
//! emitting the inner impl would produce a downstream compile error. To avoid
//! that, the auto-unbox impl is emitted only when the inner type is a
//! [`syn::Type::Path`] (e.g. `MyType`, `crate::path::MyType<'a, U>`). Other
//! shapes — `dyn Trait`, `[T]`, `&T`, etc. — silently fall back to "outer impl
//! only", because those cases are typically `?Sized` or otherwise wrong for
//! by-value conversion. The outer `impl IntoOxenError for Box<...>` is still
//! emitted in every case, since `Box<...>` itself is always sized.
//!
//! Example:
//!
//! ```ignore
//! #[derive(thiserror::Error, IntoOxen)]
//! pub enum OxenError {
//!     #[error("IO error: {0}")]
//!     IO(#[from_ox] std::io::Error),
//!     #[error("Schema invalid: {0}")]
//!     InvalidSchema(#[from_ox] Box<Schema>),
//!     // dyn Trait inner: only the outer Box impl is emitted.
//!     #[error("AWS error: {0}")]
//!     AwsError(#[from_ox] Box<dyn std::error::Error + Send + Sync>),
//! }
//! ```
//!
//! Generates:
//!
//! ```ignore
//! // From the IO(#[from_ox] std::io::Error) variant:
//! impl crate::error::IntoOxenError for std::io::Error {
//!     fn into_oxen(self) -> OxenError { OxenError::IO(self) }
//! }
//!
//! // From the InvalidSchema(#[from_ox] Box<Schema>) variant:
//! impl crate::error::IntoOxenError for Box<Schema> {
//!     fn into_oxen(self) -> OxenError { OxenError::InvalidSchema(self) }
//! }
//! impl crate::error::IntoOxenError for Schema {
//!     fn into_oxen(self) -> OxenError {
//!         OxenError::InvalidSchema(::std::boxed::Box::new(self))
//!     }
//! }
//!
//! // From the AwsError(#[from_ox] Box<dyn ...>) variant — outer only:
//! impl crate::error::IntoOxenError
//!     for Box<dyn std::error::Error + Send + Sync>
//! {
//!     fn into_oxen(self) -> OxenError { OxenError::AwsError(self) }
//! }
//! ```
//!
//! **NOTE**: This macro can **only be used from within `liboxen`**.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input, spanned::Spanned};

#[proc_macro_derive(IntoOxen, attributes(from_ox))]
pub fn derive_into_oxen(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;

    let Data::Enum(data) = &input.data else {
        return syn::Error::new(input.span(), "`IntoOxen` can only be derived on enums")
            .to_compile_error()
            .into();
    };

    let mut impls = Vec::new();
    let mut errors = Vec::new();

    for variant in &data.variants {
        let Fields::Unnamed(fields) = &variant.fields else {
            // Struct-like or unit variants can't carry `#[from_ox]`; skip.
            if let Some(attr) = find_from_ox_attr(&variant.fields) {
                errors.push(syn::Error::new(
                    attr.span(),
                    "`#[from_ox]` is only valid on single-field tuple variants",
                ));
            }
            continue;
        };

        let marked: Vec<_> = fields
            .unnamed
            .iter()
            .filter(|f| f.attrs.iter().any(is_from_ox_attr))
            .collect();

        if marked.is_empty() {
            continue;
        }

        if fields.unnamed.len() != 1 {
            errors.push(syn::Error::new(
                variant.span(),
                "`#[from_ox]` is only valid on single-field tuple variants",
            ));
            continue;
        }

        let variant_ident = &variant.ident;
        let outer_ty = &marked[0].ty;

        // Always emit `IntoOxenError for <FieldType>`.
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
        if let Some(inner_ty) = unbox_inner_path_type(outer_ty) {
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
        #(#impls)*
    }
    .into()
}

fn is_from_ox_attr(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("from_ox")
}

fn find_from_ox_attr(fields: &Fields) -> Option<&syn::Attribute> {
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
