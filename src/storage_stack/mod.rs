use std::{collections::HashMap, fmt::Display, time::SystemTime};

use thiserror::Error;

use crate::{
    cache::{CacheLogic, CacheMsg},
    Access, Block, Event,
};

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct DiskId(pub usize);

impl Display for DiskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Internal Disk Id ({})", self.0))
    }
}

pub struct StorageStack<S> {
    pub blocks: HashMap<Block, DiskId>,
    pub devices: HashMap<DiskId, DeviceState>,
    pub cache: CacheLogic,
    pub state: S,
    pub blocks_on_hold: HashMap<Block, SystemTime>,
}

#[derive(PartialEq, Debug)]
pub enum StorageMsg {
    Init(Access),
    Finish(Access),
    Process(Step),
}

#[derive(PartialEq, Debug)]
pub enum Step {
    MoveInit(Block, DiskId),
    MoveReadFinished(Block, DiskId),
    MoveWriteFinished(Block),
}

mod devices;
pub use devices::{
    load_devices, Ap, Device, DeviceLatencyTable, DeviceSer, DeviceState, BLOCK_SIZE_IN_B,
    BLOCK_SIZE_IN_MB,
};

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Could not find block {block:?}")]
    InvalidBlock { block: Block },
    #[error("Could not find device {id}")]
    InvalidDevice { id: DiskId },
}

impl<S> StorageStack<S> {
    /// Act on specified block and return subsequent event.
    pub fn process(
        &mut self,
        msg: StorageMsg,
        now: SystemTime,
    ) -> Result<Box<dyn Iterator<Item = (SystemTime, Event)>>, StorageError> {
        match msg {
            StorageMsg::Init(access) => {
                // Postpone accesses to blocks which currently are being moved
                if let Some(time) = self.blocks_on_hold.get(access.block()) {
                    return Ok(Box::new(
                        [(time.clone(), Event::Storage(StorageMsg::Init(access)))].into_iter(),
                    ));
                }
                // Otherwise proceed
                let then = self.queue_access(&access, now)?;
                Ok(Box::new(
                    [(then.0, Event::Storage(StorageMsg::Finish(access)))]
                        .into_iter()
                        .chain([then].into_iter()),
                ))
            }
            StorageMsg::Finish(access) => {
                self.finish_access(&access);
                Ok(Box::new(
                    [(
                        now,
                        Event::PlacementPolicy(match access {
                            Access::Read(b) => crate::placement::PlacementMsg::Fetched(b),
                            Access::Write(b) => crate::placement::PlacementMsg::Written(b),
                        }),
                    )]
                    .into_iter(),
                ))
            }
            StorageMsg::Process(step) => {
                match step {
                    Step::MoveReadFinished(block, to_disk) => {
                        self.finish_access(&Access::Read(block));
                        *self.blocks.get_mut(&block).unwrap() = to_disk;
                        let then = self.queue_access(&Access::Write(block), now)?;
                        self.blocks_on_hold.insert(block, then.0);
                        return Ok(Box::new(
                            [(
                                then.0,
                                Event::Storage(StorageMsg::Process(Step::MoveWriteFinished(block))),
                            )]
                            .into_iter(),
                        ));
                    }
                    Step::MoveInit(block, to_disk) => {
                        let then = self.queue_access(&Access::Read(block), now)?;
                        self.blocks_on_hold.insert(block, then.0);
                        return Ok(Box::new(
                            [(
                                then.0,
                                Event::Storage(StorageMsg::Process(Step::MoveReadFinished(
                                    block, to_disk,
                                ))),
                            )]
                            .into_iter(),
                        ));
                    }
                    Step::MoveWriteFinished(block) => {
                        self.blocks_on_hold.remove(&block);
                        self.finish_access(&Access::Write(block));
                    }
                }
                Ok(Box::new([].into_iter()))
            }
        }
    }

    // /// An operation has finished and can be removed from the device queue.
    // pub fn finish(&mut self, dev: String) {
    //     self.devices.get_mut(&dev).unwrap().queue.pop_front();
    // }

    fn finish_access(&mut self, access: &Access) {
        self.devices
            .get_mut(self.blocks.get(access.block()).unwrap())
            .unwrap()
            .queue
            .pop_front();
    }

    fn queue_access(
        &mut self,
        access: &Access,
        now: SystemTime,
    ) -> Result<(SystemTime, Event), StorageError> {
        let dev = self
            .blocks
            .get(access.block())
            .ok_or(StorageError::InvalidBlock {
                block: access.block().clone(),
            })?;
        let dev_stats = self
            .devices
            .get_mut(dev)
            .ok_or(StorageError::InvalidDevice { id: dev.clone() })?;

        let pattern = if dev_stats.last_access.0.abs_diff(access.block().0) <= 1 {
            Ap::Sequential
        } else {
            Ap::Random
        };

        let until = dev_stats.reserved_until.max(now)
            + match access {
                Access::Read(_) => dev_stats.kind.read(BLOCK_SIZE_IN_B as u64, pattern),
                Access::Write(_) => dev_stats.kind.write(BLOCK_SIZE_IN_B as u64, pattern),
            };
        dev_stats.queue.push_back(access.clone());
        if dev_stats.reserved_until < now {
            dev_stats.idle_time += now.duration_since(dev_stats.reserved_until).unwrap();
        }
        dev_stats.reserved_until = until;
        dev_stats.total_req += 1;
        dev_stats.total_q += until.duration_since(now).unwrap();
        dev_stats.max_q = dev_stats.max_q.max(until.duration_since(now).unwrap());
        dev_stats.last_access = *access.block();
        Ok(match access {
            Access::Read(b) => (until, Event::Cache(CacheMsg::ReadFinished(*b))),
            Access::Write(b) => (until, Event::Cache(CacheMsg::WriteFinished(*b))),
        })
    }

    pub fn insert(&mut self, block: Block, device: DiskId) -> Option<Block> {
        let dev = self.devices.get_mut(&device).unwrap();
        if dev.free > 0 {
            dev.free = dev.free.saturating_sub(1);
            self.blocks.insert(block, device);
            return None;
        }
        Some(block)
    }
}
