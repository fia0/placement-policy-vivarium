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

#[derive(PartialEq, Debug, Clone)]
pub enum StorageMsg {
    Init(Access),
    Finish(Access),
    Process(Step),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Step {
    MoveInit(Block, DiskId),
    MoveReadFinished(Block, DiskId),
    MoveWriteFinished(Block),
}

mod devices;
pub use devices::{
    load_devices, to_device, Device, DeviceAccessParams, DeviceLatencyTable, DeviceState,
};

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Could not find block {block:?}")]
    InvalidBlock { block: Block },
    #[error("Block {block:?} can't be moved right now. ({msg:?})")]
    BlockIsBusy { block: Block, msg: StorageMsg },
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
            StorageMsg::Init(ref access) => {
                // Postpone accesses to blocks which currently are being moved
                if let Some(time) = self.blocks_on_hold.get(access.block()) {
                    return Ok(Box::new(
                        [(
                            time.clone(),
                            Event::Storage(StorageMsg::Init(access.clone())),
                        )]
                        .into_iter(),
                    ));
                }
                // Otherwise proceed
                self.queue_access(&access, now, None)
            }
            StorageMsg::Finish(access) => {
                self.finish_access(&access, now);
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
            StorageMsg::Process(ref step) => match step {
                Step::MoveReadFinished(block, to_disk) => {
                    self.finish_access(&Access::Read(*block), now);
                    *self.blocks.get_mut(&block).unwrap() = *to_disk;
                    self.queue_access(&Access::Write(*block), now, Some(*to_disk))
                }
                Step::MoveInit(block, to_disk) => {
                    // if self.blocks_on_hold.contains_key(&block) {
                    //     return Err(StorageError::BlockIsBusy {
                    //         block: *block,
                    //         msg: StorageMsg::Process(Step::MoveInit(*block, *to_disk)),
                    //     });
                    // }
                    if let Some(time) = self.blocks_on_hold.get(block) {
                        return Ok(Box::new(
                            [(time.clone(), Event::Storage(msg.clone()))].into_iter(),
                        ));
                    }
                    self.queue_access(&Access::Read(*block), now, Some(*to_disk))
                }
                Step::MoveWriteFinished(block) => {
                    self.blocks_on_hold.remove(block);
                    self.finish_access(&Access::Write(*block), now);
                    Ok(Box::new([].into_iter()))
                }
            },
        }
    }

    fn finish_access(&mut self, access: &Access, now: SystemTime) -> () {
        let dev = self
            .devices
            .get_mut(self.blocks.get(access.block()).unwrap())
            .unwrap();
        assert!(dev.current_queue_len > 0);
        dev.current_queue_len -= 1;
    }

    fn queue_access(
        &mut self,
        access: &Access,
        mut now: SystemTime,
        is_part_of_migration: Option<DiskId>,
    ) -> Result<Box<dyn Iterator<Item = (SystemTime, Event)>>, StorageError> {
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

        let origin = now;
        now = now.max(dev_stats.can_requeue_at);

        // Enqueue and immediately submit request
        let until = now
            + match access {
                Access::Read(_) => dev_stats.kind.sample(&DeviceAccessParams::read()),
                Access::Write(_) => dev_stats.kind.sample(&DeviceAccessParams::write()),
            };
        // If nothing was submitted the device was idling
        if dev_stats.reserved_until < now {
            dev_stats.idle_time += now.duration_since(dev_stats.reserved_until).unwrap();
        }
        dev_stats.reserved_until = dev_stats.reserved_until.max(until);
        dev_stats.current_queue_len += 1;
        if dev_stats.current_queue_len >= dev_stats.max_queue_len {
            dev_stats.can_requeue_at = until;
        }
        // NOTE: Use for passed time since original queue attempt
        dev_stats.max_q = dev_stats.max_q.max(until.duration_since(origin).unwrap());
        dev_stats.total_q += until.duration_since(origin).unwrap();
        // NOTE: Use for *only* IO duration excluding blocking queue.
        // dev_stats.max_q = dev_stats.max_q.max(until.duration_since(now).unwrap());
        // dev_stats.total_q += until.duration_since(now).unwrap();
        dev_stats.total_req += 1;

        Ok(match (access, is_part_of_migration) {
            (Access::Read(b), None) => Box::new(
                [
                    (until, Event::Storage(StorageMsg::Finish(access.clone()))),
                    (until, Event::Cache(CacheMsg::ReadFinished(*b))),
                ]
                .into_iter(),
            ),
            (Access::Write(b), None) => Box::new(
                [
                    (until, Event::Storage(StorageMsg::Finish(access.clone()))),
                    (until, Event::Cache(CacheMsg::WriteFinished(*b))),
                ]
                .into_iter(),
            ),
            (Access::Read(b), Some(to_disk)) => {
                self.blocks_on_hold.insert(*b, until);
                Box::new(
                    [(
                        until,
                        Event::Storage(StorageMsg::Process(Step::MoveReadFinished(*b, to_disk))),
                    )]
                    .into_iter(),
                )
            }
            (Access::Write(b), Some(to_disk)) => {
                self.blocks_on_hold.insert(*b, until);
                Box::new(
                    [(
                        until,
                        Event::Storage(StorageMsg::Process(Step::MoveWriteFinished(*b))),
                    )]
                    .into_iter(),
                )
            }
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
