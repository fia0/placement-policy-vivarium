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
    fn get(&mut self, block: &Block) -> Option<std::time::Duration> {
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

    fn put(&mut self, block: Block) -> std::time::Duration {
        if self.get(&block).is_none() {
            self.entries.push_front(block);
        }
        self.on_device.write()
    }

    fn clear(&mut self) -> Box<dyn Iterator<Item = Block>> {
        let mut tmp = VecDeque::new();
        std::mem::swap(&mut self.entries, &mut tmp);
        Box::new(tmp.into_iter())
    }

    fn evict(&mut self) -> Option<Block> {
        self.entries.pop_back()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}
