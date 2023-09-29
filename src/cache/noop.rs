use std::time::Duration;

use super::Cache;

pub struct Noop {}

impl Cache for Noop {
    fn get(&mut self, _block: &crate::Block) -> Option<std::time::Duration> {
        None
    }

    fn put(&mut self, _block: crate::Block) -> std::time::Duration {
        Duration::ZERO
    }

    fn clear(&mut self) -> Box<dyn Iterator<Item = crate::Block>> {
        Box::new([].into_iter())
    }

    fn evict(&mut self) -> Option<crate::Block> {
        None
    }

    fn capacity(&self) -> usize {
        0
    }

    fn len(&self) -> usize {
        0
    }
}
