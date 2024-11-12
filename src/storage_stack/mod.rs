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
    load_devices, to_device, Device, DeviceAccessParams, DeviceLatencyTable, DeviceState,
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
                if let Some(then) = self.queue_access(&access, now, None)? {
                    Ok(Box::new(
                        [(then.0, Event::Storage(StorageMsg::Finish(access)))]
                            .into_iter()
                            .chain(then.1),
                    ))
                } else {
                    Ok(Box::new([].into_iter()))
                }
            }
            StorageMsg::Finish(access) => {
                let new_events = self.finish_access(&access, now)?;
                Ok(Box::new(
                    [(
                        now,
                        Event::PlacementPolicy(match access {
                            Access::Read(b) => crate::placement::PlacementMsg::Fetched(b),
                            Access::Write(b) => crate::placement::PlacementMsg::Written(b),
                        }),
                    )]
                    .into_iter()
                    .chain(new_events),
                ))
            }
            StorageMsg::Process(step) => match step {
                Step::MoveReadFinished(block, to_disk) => {
                    let new_events = self.finish_access(&Access::Read(block), now)?;
                    *self.blocks.get_mut(&block).unwrap() = to_disk;
                    todo!();
                    // if let Some(then) = self.queue_access(&Access::Write(block), now)? {
                    //     self.blocks_on_hold.insert(block, then.0);
                    //     Ok(Box::new(
                    //         [(
                    //             then.0,
                    //             Event::Storage(StorageMsg::Process(Step::MoveWriteFinished(block))),
                    //         )]
                    //         .into_iter()
                    //         .chain(new_events),
                    //     ))
                    // }
                }
                Step::MoveInit(block, to_disk) => {
                    todo!();
                    // if let Some(then) = self.queue_access(&Access::Read(block), now)? {
                    //     self.blocks_on_hold.insert(block, then.0);
                    //     Ok(Box::new(
                    //         [(
                    //             then.0,
                    //             Event::Storage(StorageMsg::Process(Step::MoveReadFinished(
                    //                 block, to_disk,
                    //             ))),
                    //         )]
                    //         .into_iter(),
                    //     ))
                    // }
                }
                Step::MoveWriteFinished(block) => {
                    self.blocks_on_hold.remove(&block);
                    let new_events = self.finish_access(&Access::Write(block), now)?;
                    Ok(new_events)
                }
            },
        }
    }

    fn finish_access(
        &mut self,
        access: &Access,
        now: SystemTime,
    ) -> Result<Box<dyn Iterator<Item = (SystemTime, Event)>>, StorageError> {
        let dev = self
            .devices
            .get_mut(self.blocks.get(access.block()).unwrap())
            .unwrap();

        // One access finished refill if possible.
        dev.current_queue_len -= 1;

        let tmp = dev.submission_queue.pop_front().map(|a| {
            let (then, evs) = self.queue_access(&a.1, now, Some(a.0)).unwrap().unwrap();
            Box::new(
                [(then, Event::Storage(StorageMsg::Finish(a.1)))]
                    .into_iter()
                    .chain(evs),
            )
        });

        if let Some(it) = tmp {
            Ok(it)
        } else {
            Ok(Box::new([].into_iter()))
        }
    }

    fn queue_access(
        &mut self,
        access: &Access,
        now: SystemTime,
        originally_queued_at: Option<SystemTime>,
    ) -> Result<Option<(SystemTime, Box<dyn Iterator<Item = (SystemTime, Event)>>)>, StorageError>
    {
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

        // How to queue requests:
        //
        // 1. If queue is not full:
        //     - sample
        //     - enqueue
        //     - update reserved_until to minimum of finished timestamp and current value
        // 2. If queue is full:
        //     -

        if dev_stats.current_queue_len < dev_stats.max_queue_len {
            // Enqueue and immediately submit request
            let until = now
                + match access {
                    Access::Read(_) => dev_stats.kind.sample(&DeviceAccessParams::read()),
                    Access::Write(_) => dev_stats.kind.sample(&DeviceAccessParams::write()),
                };
            // If nothing was submitted the device was idling
            if dev_stats.current_queue_len == 0 {
                dev_stats.idle_time += now.duration_since(dev_stats.reserved_until).unwrap();
            }
            dev_stats.reserved_until = until;
            dev_stats.current_queue_len += 1;
            dev_stats.max_q = dev_stats.max_q.max(
                until
                    .duration_since(originally_queued_at.unwrap_or(now))
                    .unwrap(),
            );
            dev_stats.total_q += until
                .duration_since(originally_queued_at.unwrap_or(now))
                .unwrap();
            dev_stats.total_req += 1;

            Ok(Some(match access {
                Access::Read(b) => (
                    until,
                    Box::new([(until, Event::Cache(CacheMsg::ReadFinished(*b)))].into_iter()),
                ),
                Access::Write(b) => (
                    until,
                    Box::new([(until, Event::Cache(CacheMsg::WriteFinished(*b)))].into_iter()),
                ),
            }))
        } else {
            // Enqueue the access for later revision into the stack
            dev_stats.submission_queue.push_back((now, access.clone()));
            Ok(None)
        }
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
