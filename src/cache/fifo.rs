use crate::{Block, Device};
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
    fn contains(&mut self, block: &Block) -> Option<Duration> {
        self.blocks.get(block).map(|_| self.on_device.read())
    }

    fn insert(&mut self, block: Block) -> Option<Block> {
        self.queue.push_front(block.clone());
        self.blocks.insert(block);
        if self.queue.len() > self.capacity {
            if let Some(b) = self.queue.pop_back() {
                self.blocks.remove(&b);
                return Some(b);
            }
        }
        None
    }

    fn write(&self) -> Duration {
        self.on_device.write()
    }

    fn clear(&mut self) -> Box<dyn Iterator<Item = Block>> {
        let mut tmp = HashSet::new();
        std::mem::swap(&mut self.blocks, &mut tmp);
        self.queue.clear();
        Box::new(tmp.into_iter())
    }
}
