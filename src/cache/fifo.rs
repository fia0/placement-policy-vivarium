use crate::{storage_stack::DeviceAccessParams, Block, Device};
use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use super::Cache;

#[derive(Default)]
pub struct Fifo {
    blocks: HashSet<Block>,
    queue: VecDeque<Block>,
    on_device: Device,
    capacity: usize,
}

impl Fifo {
    pub fn new(capacity: usize, dev: Device) -> Self {
        Self {
            blocks: HashSet::default(),
            queue: VecDeque::default(),
            on_device: dev,
            capacity,
        }
    }
}

impl Cache for Fifo {
    fn get(&mut self, block: &Block) -> Option<Duration> {
        self.blocks.get(block).map(|_| Duration::ZERO)
    }

    fn put(&mut self, block: Block) -> Duration {
        if !self.blocks.contains(&block) {
            self.queue.push_front(block.clone());
            self.blocks.insert(block);
        }
        Duration::ZERO
    }

    fn clear(&mut self) -> Box<dyn Iterator<Item = Block>> {
        let mut tmp = HashSet::new();
        std::mem::swap(&mut self.blocks, &mut tmp);
        self.queue.clear();
        Box::new(tmp.into_iter())
    }

    fn evict(&mut self) -> Option<Block> {
        self.queue.pop_back().map(|b| {
            self.blocks.remove(&b);
            b
        })
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn len(&self) -> usize {
        self.queue.len()
    }
}
