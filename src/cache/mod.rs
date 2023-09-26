use std::time::Duration;

/// This module contains a simple cache trait.
/// Implementations for simple policies are provided.
/// Extension can be done by implementing the trait on a new struct. No actual data is stored.
use super::Block;

mod fifo;
mod lru;
mod noop;
pub use fifo::Fifo;
pub use lru::Lru;
pub use noop::Noop;

pub trait Cache {
    /// Check whether the cache contains a given block.
    fn contains(&mut self, block: &Block) -> Option<Duration>;
    /// Reading time on backing device.
    fn write(&self) -> Duration;
    /// Insert a new entry to cache. Returns eventually evicted entry.
    fn insert(&mut self, block: Block) -> Option<Block>;
    /// Removes all elements in the cache an returns an iterator over contained elements.
    fn clear(&mut self) -> Box<dyn Iterator<Item = Block>>;
}
