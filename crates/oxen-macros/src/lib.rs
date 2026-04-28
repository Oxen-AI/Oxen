//! Proc-macro derive for `liboxen`'s `IntoOxenError` trait.
//!
//! On an enum variant with a single tuple field, annotate the field with
//! `#[from_ox]` to auto-generate `impl IntoOxenError for <FieldType>` that
//! constructs the enum via that variant. Mirrors the shape of `thiserror`'s
//! `#[from]`, but routes through `IntoOxenError` so the crate-wide blanket
//! `impl<E: IntoOxenError> From<E> for OxenError` stays unambiguous.
//!
//! Example:
//!
//! ```ignore
//! #[derive(thiserror::Error, IntoOxen)]
//! pub enum OxenError {
//!     #[error("IO error: {0}")]
//!     IO(#[from_ox] std::io::Error),
//! }
//! ```
//!
//! Generates:
//!
//! ```ignore
//! impl crate::error::IntoOxenError for std::io::Error {
//!     fn into_oxen(self) -> OxenError { OxenError::IO(self) }
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
        let ty = &marked[0].ty;

        impls.push(quote! {
            impl crate::error::IntoOxenError for #ty {
                fn into_oxen(self) -> #enum_name {
                    #enum_name::#variant_ident(self)
                }
            }
        });
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
