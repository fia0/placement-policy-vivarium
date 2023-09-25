use crate::{Block, Device};
use std::collections::{HashMap, VecDeque};

use super::Cache;

pub struct Lru {
    entries: VecDeque<Block>,
    capacity: usize,
    on_device: Device,
}

impl Lru {
    pub fn new(capacity: usize, dev: Device) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
            on_device: dev,
        }
    }
}

impl Cache for Lru {
    fn contains(&mut self, block: &Block) -> Option<std::time::Duration> {
        if let Some(idx) = self
            .entries
            .iter()
            .enumerate()
            .find(|x| x.1 == block)
            .map(|x| x.0)
        {
            assert_eq!(self.entries.remove(idx), Some(block.to_owned()));
            self.entries.push_front(block.to_owned());
            Some(self.on_device.read())
        } else {
            None
        }
    }

    fn insert(&mut self, block: Block) -> Option<Block> {
        if self.contains(&block).is_some() {
            return None;
        }
        self.entries.push_front(block);
        if self.entries.len() > self.capacity {
            self.entries.pop_back()
        } else {
            None
        }
    }

    fn write(&self) -> std::time::Duration {
        self.on_device.write()
    }
}
