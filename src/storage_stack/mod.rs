use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, SystemTime},
};

use serde::Deserialize;
use strum::EnumIter;
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

impl Device {
    // All these numbers are approximations!  Numbers taken from peak
    // performance over multiple queue depths, real results are likely to be
    // worse.
    pub fn read(&self) -> Duration {
        match self {
            // 30 GiB/s peak
            Device::Intel_Optane_PMem_100 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (30f32 * 1024f32))
            }
            // 2.5 GiB/s peak
            Device::Intel_Optane_SSD_DC_P4800X => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2517f32)
            }
            Device::Samsung_983_ZET => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 3130f32),
            Device::Micron_9100_MAX => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2903f32),
            Device::Western_Digital_WD5000AAKS => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 94f32)
            }
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
        }
    }

    pub fn write(&self) -> Duration {
        match self {
            Device::Intel_Optane_PMem_100 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (16f32 * 1024f32))
            }
            Device::Intel_Optane_SSD_DC_P4800X => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2278f32)
            }
            Device::Samsung_983_ZET => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 576f32),
            Device::Micron_9100_MAX => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 1408f32),
            Device::Western_Digital_WD5000AAKS => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 38.2f32)
            }
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
        }
    }

    // /// Number of blocks a single device can at maximum hold.
    // fn capacity(&self) -> usize {
    //     match self {
    //         // 1 TB max assumed (more is possible i know)
    //         //                    TB   GB     MB
    //         Device::OptanePMem => 1 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
    //         // 1.6 TB max
    //         //                    GB     MB
    //         Device::OptaneSSD => 1600 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 3.2 TB max
    //         //                      GB     MB
    //         Device::SamsungZSSD => 3200 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 30.72 TB max
    //         //                      GB       MB
    //         Device::MicronTLCSSD => 30720 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 30 TB max assumed (there is higher)
    //         //                    TB    GB     MB
    //         Device::GenericHDD => 30 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
    //         // 32 GB max (set limitation due to impl on client)
    //         //              GB   MB
    //         Device::DRAM => 32 * 1024 / BLOCK_SIZE_IN_MB,
    //     }
    // }
}

const BLOCK_SIZE_IN_MB: usize = 4;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, EnumIter)]
#[repr(u8)]
pub enum Device {
    // 6 dimms
    Intel_Optane_PMem_100 = 0,
    Intel_Optane_SSD_DC_P4800X = 1,
    Samsung_983_ZET = 2,
    Micron_9100_MAX = 3,
    Western_Digital_WD5000AAKS = 4,
    DRAM = 5,
}

impl Default for Device {
    fn default() -> Self {
        Self::DRAM
    }
}

pub struct DeviceState {
    pub kind: Device,
    // Number of blocks currently used.
    pub free: usize,
    // Absolute number of blocks which can be stored.
    pub total: usize,
    pub reserved_until: SystemTime,
    pub queue: VecDeque<Access>,
    // Metrics
    pub max_q: Duration,
    pub total_q: Duration,
    pub total_req: usize,
}

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
        access: Access,
        now: SystemTime,
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

        let until = dev_stats.reserved_until.max(now)
            + match access {
                Access::Read(_) => dev_stats.kind.read(),
                Access::Write(_) => dev_stats.kind.write(),
            };
        dev_stats.queue.push_back(access.clone());
        dev_stats.reserved_until = until;
        dev_stats.total_req += 1;
        dev_stats.total_q += until.duration_since(now).unwrap();
        dev_stats.max_q = dev_stats.max_q.max(until.duration_since(now).unwrap());

        match access {
            Access::Read(b) => Ok(Box::new(
                [(until, Event::Cache(CacheMsg::ReadFinished(b)))].into_iter(),
            )),
            Access::Write(b) => Ok(Box::new(
                [(until, Event::Cache(CacheMsg::WriteFinished(b)))].into_iter(),
            )),
        }
    }

    /// An operation has finished and can be removed from the device queue.
    pub fn finish(&mut self, dev: String) {
        self.devices.get_mut(&dev).unwrap().queue.pop_front();
    }

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
