use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    error::Error,
    path::Path,
    time::{Duration, SystemTime},
};

use crate::Access;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

/// This file contains a definition of available storage devices.

pub const BLOCK_SIZE_IN_MB: usize = 4;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Serialize, Debug, Hash, PartialEq, Clone, EnumIter)]
pub enum Device {
    // 6 dimms
    Intel_Optane_PMem_100,
    Intel_Optane_SSD_DC_P4800X,
    Samsung_983_ZET,
    Micron_9100_MAX,
    Western_Digital_WD5000AAKS,
    DRAM,
    KIOXIA_CM7,
    Custom(String),
}

impl Default for Device {
    fn default() -> Self {
        Self::DRAM
    }
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
            Device::KIOXIA_CM7 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (11.4f32 * 1024f32))
            }
            Device::Custom(_) => todo!(),
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
            Device::Samsung_983_ZET => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 995f32),
            Device::Micron_9100_MAX => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 1408f32),
            Device::Western_Digital_WD5000AAKS => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 38.2f32)
            }
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
            Device::KIOXIA_CM7 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (4.18f32 * 1024f32))
            }
            Device::Custom(_) => todo!(),
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
    pub idle_time: Duration,
}

// How should the lookup table for performance estimation looklike?
//
// Access -> Translated to blocks -> Multiple ranges for devices predefined -> Interpolation
// 256 -> 4k -> 16k -> 64k -> 256k -> 1m -> 4m -> 16m -> 64m
//
// seq -> rnd -> (clustered?)

// static DEVICE_TABLE: DeviceLatencyTable = DeviceLatencyTable;
#[derive(Debug, Hash, PartialEq, Clone, Default)]
pub struct DeviceLatencyTable([BTreeMap<u64, Latencies>; Op::LEN as usize]);
#[derive(Debug, Hash, PartialEq, Clone, Default)]
pub struct Latencies([Duration; Ap::LEN as usize]);

impl DeviceLatencyTable {
    pub fn keys(&self) -> impl Iterator<Item = &u64> + '_ {
        self.0[Op::Read as usize].keys()
    }
}

pub fn load_devices(
    path: impl AsRef<Path>,
) -> Result<HashMap<String, DeviceLatencyTable>, Box<dyn Error>> {
    let mut devices = HashMap::new();
    for file in std::fs::read_dir(path)? {
        let file = file?;
        if file.path().is_file() {
            let mut device = DeviceLatencyTable::default();
            for record in csv::Reader::from_path(file.path())?.deserialize::<DeviceRecord>() {
                if let Ok(record) = record {
                    match device.0[record.op as usize].entry(record.block_size) {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            let mut latency = Latencies::default();
                            latency.0[record.pattern as usize] =
                                Duration::from_micros(record.avg_latency_us);
                            v.insert(latency);
                        }
                        std::collections::btree_map::Entry::Occupied(mut o) => {
                            o.get_mut().0[record.pattern as usize] =
                                Duration::from_micros(record.avg_latency_us);
                        }
                    }
                } else {
                    continue;
                }
            }
            devices.insert(
                file.path()
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                device,
            );
        }
    }
    Ok(devices)
}

#[derive(Deserialize)]
pub struct DeviceRecord {
    block_size: u64,
    blocks: u64,
    avg_latency_us: u64,
    op: Op,
    pattern: Ap,
}

#[derive(Deserialize)]
#[repr(u8)]
pub enum Op {
    #[serde(rename = "write")]
    Write = 0,
    #[serde(rename = "read")]
    Read,
    #[serde(skip)]
    LEN,
}

#[derive(Deserialize)]
#[repr(u8)]
pub enum Ap {
    #[serde(rename = "random")]
    Random = 0,
    #[serde(rename = "sequential")]
    Sequential,
    #[serde(skip)]
    LEN,
}

pub struct BlockSize(usize);
pub enum AccessMode {
    /// The previous access on this device has been on an directly neighbored
    /// location.  For spinning disks this could be on the same sector, for
    /// flash-based storage on the same chip.
    SequentialRead,
    SequentialWrite,
    /// Definitely identified random access, assumes arbitrary starting position
    /// of device interna.
    RandomRead,
    RandomWrite,
}

// TODO: How to deal with parallel queues on devices? Currently only sync
// accesses are simulated but ranged queries etc might be able to exploit sync
// accesses? Maybe measuring this with 64m blocks is already sufficient?
//
// SIDE NOTE: Measuring random read rates with fio's mmap is not reliable large
// random reads are heavily skewed with even BLOCKSIZE = SIZE being slower than
// multiple sequential smaller reads/writes. Maybe we need our own probing...

pub trait DeviceLatency {
    // Perform a lookup.
    fn access(bs: BlockSize, am: AccessMode) -> Duration;
    fn preferred_blocksize() -> BlockSize;
}
