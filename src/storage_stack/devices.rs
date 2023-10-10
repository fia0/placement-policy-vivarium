use std::time::Duration;

use serde::Deserialize;
use strum::EnumIter;

/// This file contains a definition of available storage devices.

pub const BLOCK_SIZE_IN_MB: usize = 4;

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
    KIOXIA_CM7 = 6,
}

impl Default for Device {
    fn default() -> Self {
        Self::DRAM
    }
}

// How should the lookup table for performance estimation looklike?
//
// Access -> Translated to blocks -> Multiple ranges for devices predefined -> Interpolation
// 256 -> 4k -> 16k -> 64k -> 256k -> 1m -> 4m -> 16m -> 64m
//
// seq -> rnd -> (clustered?)

static DEVICE_TABLE: DeviceLatencyTable = DeviceLatencyTable;

pub struct DeviceLatencyTable;

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
