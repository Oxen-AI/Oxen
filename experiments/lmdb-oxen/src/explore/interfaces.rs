//! Builder pattern is decomposed into two traits: `Builder` and `Accumulator`.
//!
//! Often, a single type will implement both of these. The `Accumulator` mutates internal state,
//! e.g. adding a field or updating an existing sum. The `Builder` consumes internal state to
//! produce a final result.
//!
//! For a trival example -- integer addition:
//!
//! ```rust
//! struct SumBuilder(u32);
//!
//! impl Default for SumBuilder {
//!     fn default() -> Self {
//!         Self(0)
//!     }
//! }
//!
//! impl Accumulator<u32> for SumBuilder {
//!     fn accumulate(&mut self, x: u32) -> &mut Self {
//!         self.0 += x;
//!         self
//!     }
//! }
//!
//! impl Builder<u32> for SumBuilder {
//!     fn build(self) -> u32 {
//!         self.0
//!     }
//! }
//!
//!
//! fn main() {
//!     let mut builder = SumBuilder::default();
//!
//!     for i in 1..=10 {
//!         builder.accumulate(i);
//!     }
//!
//!     let sum = builder.build();
//!     println!("sum: {}", sum);
//!     assert_eq!(sum, 55);
//! }
//! ```

/// The "finisher" part of building: uses up accumulated state to construct an instance of `B`.
pub trait Builder<B> {
    fn build(self) -> B;
}

/// The "accumulator" part of building: ingests new data and updates the builder's state.
pub trait Accumulator<Ingest> {
    fn accumulate(&mut self, x: Ingest) -> &mut Self;
}

////////////////////////////////////

trait MapThrow: Iterator<Item = Result<Self::ActualItem, Self::Error>> + Sized {
    type ActualItem: Sized;
    type Error: Sized + std::error::Error;

    // fn collect<C: FromIterator<Self::Item>>(self) -> Result<C, Self::Error>;
    fn to_vec(self) -> Result<Vec<Self::ActualItem>, Self::Error> {
        let mut v = Vec::new();
        for item in self.into_iter() {
            let actual = item?;
            v.push(actual);
        }
        Ok(v)
    }
}

/// Provides the `.map_throw` method + implementation bridging to the [`MapThrow`], which
/// provides a way to convert into a concrete `Vec` of all successes or the first failure.
trait HasMapThrow: Iterator + Sized {
    fn map_throw<F, O, E: std::error::Error>(self, f: F) -> impl MapThrow<ActualItem = O, Error = E>
    where
        F: Fn(Self::Item) -> Result<O, E>,
        Self: Sized,
    {
        let transformed = self.into_iter().map(move |x| f(x));
        Wrapper(transformed)
    }
}

impl<A, I: Iterator<Item = A>> HasMapThrow for I {}

struct Wrapper<A, E: std::error::Error, I: Iterator<Item = Result<A, E>>>(I);

impl<A, E: std::error::Error, I: Iterator<Item = Result<A, E>>> Iterator for Wrapper<A, E, I> {
    type Item = Result<A, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<A, E: std::error::Error, I: Iterator<Item = Result<A, E>>> MapThrow for Wrapper<A, E, I> {
    type ActualItem = A;
    type Error = E;
}
