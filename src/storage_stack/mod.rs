use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, SystemTime},
};

use thiserror::Error;

use crate::{
    cache::{CacheLogic, CacheMsg},
    Access, Block, Event,
};

pub struct StorageStack<S, P> {
    pub blocks: HashMap<Block, String>,
    pub devices: HashMap<String, DeviceState>,
    pub cache: CacheLogic,
    pub state: S,
    pub policy: P,
}

#[derive(PartialEq, Debug)]
pub enum StorageMsg {
    Init(Access),
    Finish(Access),
}

mod devices;
pub use devices::{
    load_devices, Device, DeviceLatencyTable, DeviceSer, DeviceState, BLOCK_SIZE_IN_MB,
};

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Could not find block {block:?}")]
    InvalidBlock { block: Block },
    #[error("Could not find device {id}")]
    InvalidDevice { id: String },
}

impl<S, P> StorageStack<S, P> {
    /// Act on specified block and return subsequent event.
    pub fn process(
        &mut self,
        msg: StorageMsg,
        now: SystemTime,
    ) -> Result<Box<dyn Iterator<Item = (SystemTime, Event)>>, StorageError> {
        match msg {
            StorageMsg::Init(access) => {
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

                let until = dev_stats.reserved_until.max(now)
                    + match access {
                        Access::Read(_) => dev_stats.kind.read(),
                        Access::Write(_) => dev_stats.kind.write(),
                    };
                dev_stats.queue.push_back(access.clone());
                if dev_stats.reserved_until < now {
                    dev_stats.idle_time += now.duration_since(dev_stats.reserved_until).unwrap();
                }
                dev_stats.reserved_until = until;
                dev_stats.total_req += 1;
                dev_stats.total_q += until.duration_since(now).unwrap();
                dev_stats.max_q = dev_stats.max_q.max(until.duration_since(now).unwrap());

                let msgs = [(until, Event::Storage(StorageMsg::Finish(access)))].into_iter();
                match access {
                    Access::Read(b) => Ok(Box::new(
                        msgs.chain([(until, Event::Cache(CacheMsg::ReadFinished(b)))].into_iter()),
                    )),
                    Access::Write(b) => Ok(Box::new(
                        msgs.chain([(until, Event::Cache(CacheMsg::WriteFinished(b)))].into_iter()),
                    )),
                }
            }
            StorageMsg::Finish(access) => {
                self.devices
                    .get_mut(self.blocks.get(access.block()).unwrap())
                    .unwrap()
                    .queue
                    .pop_front();
                Ok(Box::new([].into_iter()))
            }
        }
    }

    // /// An operation has finished and can be removed from the device queue.
    // pub fn finish(&mut self, dev: String) {
    //     self.devices.get_mut(&dev).unwrap().queue.pop_front();
    // }

    pub fn insert(&mut self, block: Block, device: String) -> Option<Block> {
        let dev = self.devices.get_mut(&device).unwrap();
        if dev.free > 0 {
            dev.free = dev.free.saturating_sub(1);
            self.blocks.insert(block, device);
            return None;
        }
        Some(block)
    }
}
