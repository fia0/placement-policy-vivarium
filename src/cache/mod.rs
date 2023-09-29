use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, SystemTime},
};

use crate::Access;

/// This module contains a simple cache trait.
/// Implementations for simple policies are provided.
/// Extension can be done by implementing the trait on a new struct. No actual data is stored.
use super::{Block, Event};

mod fifo;
mod lru;
mod noop;
pub use fifo::Fifo;
pub use lru::Lru;
pub use noop::Noop;

pub trait Cache {
    /// Check whether the cache contains a given block.
    fn get(&mut self, block: &Block) -> Option<Duration>;
    /// Insert a new entry to cache.
    fn put(&mut self, block: Block) -> Duration;
    /// Removes all elements in the cache an returns an iterator over contained elements.
    fn clear(&mut self) -> Box<dyn Iterator<Item = Block>>;
    /// Evict the next entry.
    fn evict(&mut self) -> Option<Block>;
    /// Return the total capacity.
    fn capacity(&self) -> usize;
    /// Return the number of current entries.
    fn len(&self) -> usize;
}

// Meta logic for caches, takes cares of size requirements and interdependencies of caches
pub struct CacheLogic {
    in_eviction: HashSet<Block>,
    in_fetch: HashSet<Block>,
    cache: Box<dyn Cache>,
    queue_eviction: VecDeque<CacheMsg>,
    queue_completion: VecDeque<CacheMsg>,
}

#[derive(Debug, PartialEq)]
pub enum CacheMsg {
    Get(Block),
    Put(Block),
    ReadFinished(Block),
    WriteFinished(Block),
}

impl CacheMsg {
    pub fn is_get(&self) -> bool {
        match self {
            CacheMsg::Get(_) => true,
            _ => false,
        }
    }

    pub fn is_put(&self) -> bool {
        match self {
            CacheMsg::Put(_) => true,
            _ => false,
        }
    }

    pub fn block(&self) -> Block {
        match self {
            CacheMsg::Get(b) => *b,
            CacheMsg::Put(b) => *b,
            CacheMsg::ReadFinished(b) => *b,
            CacheMsg::WriteFinished(b) => *b,
        }
    }
}

impl CacheLogic {
    pub fn new(cache: Box<dyn Cache>) -> Self {
        Self {
            in_eviction: Default::default(),
            in_fetch: Default::default(),
            cache,
            queue_eviction: Default::default(),
            queue_completion: Default::default(),
        }
    }

    pub fn process(
        &mut self,
        msg: CacheMsg,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)> + '_> {
        match msg {
            CacheMsg::Get(block) => {
                // Check if block is already cached
                if let Some(dur) = self.cache.get(&block) {
                    Box::new(
                        [(now + dur, Event::Application(Access::Read(block)))]
                            .into_iter()
                            .chain(
                                [self
                                    .queue_eviction
                                    .pop_front()
                                    .map(|m| (now, Event::Cache(m)))]
                                .into_iter()
                                .filter_map(|e| e),
                            ),
                    )
                } else {
                    // If block is already being fetched only enqueue
                    if self.in_fetch.contains(&block) {
                        self.queue_completion.push_back(msg);
                        return Box::new([].into_iter());
                    }

                    // If necessary evict entry
                    if self.cache.len() + self.in_eviction.len() + self.in_fetch.len() + 1
                        > self.cache.capacity()
                    {
                        self.queue_eviction.push_back(msg);
                        if let Some(evicted) = self.cache.evict() {
                            // evict entry and wait for completion
                            self.in_eviction.insert(evicted);
                            Box::new([(now, Event::Storage(Access::Write(evicted)))].into_iter())
                        } else {
                            if self.cache.capacity() == 0 {
                                return Box::new(
                                    [(now, Event::Storage(Access::Read(block)))].into_iter(),
                                );
                            }
                            Box::new([].into_iter())
                        }
                    } else {
                        // Fetch block from storage
                        self.queue_completion.push_back(msg);
                        self.in_fetch.insert(block);
                        Box::new([(now, Event::Storage(Access::Read(block)))].into_iter())
                    }
                }
            }
            CacheMsg::Put(block) => {
                // If necessary evict entry
                if self.cache.len() + self.in_eviction.len() + self.in_fetch.len() + 1
                    > self.cache.capacity()
                {
                    self.queue_eviction.push_back(msg);
                    if let Some(evicted) = self.cache.evict() {
                        // evict entry and wait for completion
                        self.in_eviction.insert(evicted);
                        Box::new([(now, Event::Storage(Access::Write(evicted)))].into_iter())
                    } else {
                        if self.cache.capacity() == 0 {
                            return Box::new(
                                [(now, Event::Storage(Access::Write(block)))].into_iter(),
                            );
                        }
                        Box::new([].into_iter())
                    }
                } else {
                    let dur = self.cache.put(block);

                    return Box::new(
                        [(now + dur, Event::Application(Access::Write(block)))]
                            .into_iter()
                            .chain(
                                [self
                                    .queue_eviction
                                    .pop_front()
                                    .map(|m| (now, Event::Cache(m)))]
                                .into_iter()
                                .filter_map(|e| e),
                            ),
                    );
                }
            }
            CacheMsg::ReadFinished(block) => {
                if self.cache.capacity() == 0 {
                    return Box::new([(now, Event::Application(Access::Read(block)))].into_iter());
                }
                self.in_fetch.remove(&block);
                self.cache.put(block);
                assert!(self.cache.len() <= self.cache.capacity());
                let evs = self
                    .queue_completion
                    .iter()
                    .filter(|m| m.is_get())
                    .filter(move |m| m.block() == block)
                    .map(move |m| (now, Event::Application(Access::Read(m.block()))))
                    .collect::<Vec<_>>();
                // Search through queued messages and remove according read
                self.queue_completion
                    .retain(|m| !m.is_get() || m.block() != block);
                Box::new(
                    evs.into_iter().chain(
                        [self
                            .queue_eviction
                            .pop_front()
                            .map(|m| (now, Event::Cache(m)))]
                        .into_iter()
                        .filter_map(|e| e),
                    ),
                )
            }
            CacheMsg::WriteFinished(block) => {
                if self.cache.capacity() == 0 {
                    return Box::new([(now, Event::Application(Access::Write(block)))].into_iter());
                }
                self.in_eviction.remove(&block);
                Box::new(
                    [self
                        .queue_eviction
                        .pop_front()
                        .map(|m| (now, Event::Cache(m)))]
                    .into_iter()
                    .filter_map(|e| e),
                )
            }
        }
    }

    pub fn clear(&mut self, now: SystemTime) -> Box<dyn Iterator<Item = (SystemTime, Event)>> {
        Box::new(
            self.cache
                .clear()
                .map(move |b| (now, Event::Storage(Access::Write(b)))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_special_direct() {
        let mut cache = CacheLogic::new(Box::new(Noop {}));
        assert_eq!(
            cache
                .process(CacheMsg::Get(Block(1)), SystemTime::UNIX_EPOCH)
                .next()
                .unwrap()
                .1,
            Event::Storage(Access::Read(Block(1)))
        );
        assert_eq!(
            cache
                .process(CacheMsg::ReadFinished(Block(1)), SystemTime::UNIX_EPOCH)
                .next()
                .unwrap()
                .1,
            Event::Application(Access::Read(Block(1)))
        );
    }

    #[test]
    fn put_special_direct() {
        let mut cache = CacheLogic::new(Box::new(Noop {}));
        assert_eq!(
            cache
                .process(CacheMsg::Put(Block(1)), SystemTime::UNIX_EPOCH)
                .next()
                .unwrap()
                .1,
            Event::Storage(Access::Write(Block(1)))
        );
        assert_eq!(
            cache
                .process(CacheMsg::WriteFinished(Block(1)), SystemTime::UNIX_EPOCH)
                .next()
                .unwrap()
                .1,
            Event::Application(Access::Write(Block(1)))
        );
    }
}
