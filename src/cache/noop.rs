use std::time::Duration;

use super::Cache;

pub struct Noop {}

impl Cache for Noop {
    fn contains(&mut self, block: &crate::Block) -> Option<std::time::Duration> {
        None
    }

    fn insert(&mut self, block: crate::Block) -> Option<crate::Block> {
        None
    }

    fn write(&self) -> std::time::Duration {
        Duration::ZERO
    }

    fn clear(&mut self) -> Box<dyn Iterator<Item = crate::Block>> {
        Box::new([].into_iter())
    }
}
