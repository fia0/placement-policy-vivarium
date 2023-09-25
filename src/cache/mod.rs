use std::time::Duration;

/// This module contains a simple cache trait.
/// Implementations for simple policies are provided.
/// Extension can be done by implementing the trait on a new struct. No actual data is stored.
use super::Block;

mod fifo;
pub use fifo::Fifo;

pub trait Cache {
    /// Check whether the cache contains a given block.
    fn contains(&self, block: &Block) -> Option<Duration>;
    /// Insert a new entry to cache. Returns eventually evicted entry.
    fn insert(&mut self, block: Block) -> Option<Block>;
}
