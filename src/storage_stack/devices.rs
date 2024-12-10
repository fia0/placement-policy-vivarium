use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    path::Path,
    time::{Duration, SystemTime},
};

use crate::{Access, Block, SimError};
use rand::prelude::Distribution;
use serde::{Deserialize, Serialize};

use super::DiskId;

/// This file contains a definition of available storage devices.

pub const BLOCK_SIZE_IN_MB: usize = 4;
pub const BLOCK_SIZE_IN_B: usize = BLOCK_SIZE_IN_MB * 1024 * 1024;

#[derive(Debug)]
pub struct Device(DeviceLatencyTable);

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub struct Parameters {
    a: f64,
    b: f64,
    c: f64,
    gap: f64,
}

impl Parameters {
    pub fn calculate(&self, percentile: f64) -> Duration {
        Duration::from_nanos(
            (std::f64::consts::E.powf(self.c)
                * (self.a / ((percentile * self.gap) - 1.0)).powf(1.0 / self.b)) as u64,
        )
    }

    pub fn sample<R: rand::Rng>(&self, rng: &mut R) -> Duration {
        let smpl: f64 = rng.gen();
        self.calculate(smpl)
    }
}

pub fn to_device(
    name: &str,
    loaded_devices: &HashMap<String, DeviceLatencyTable>,
    _capacity: usize,
) -> Result<Device, SimError> {
    loaded_devices
        .get(name)
        .cloned()
        .ok_or(SimError::MissingCustomDevice(name.to_owned()))
        .map(|d| Device(d))
}

impl Default for Device {
    fn default() -> Self {
        todo!()
    }
}

impl Device {
    pub fn sample(&self, access: &DeviceAccessParams) -> Duration {
        let mut rng = rand::thread_rng();
        let pct = rand::distributions::Uniform::new(0.0, 1.0).sample(&mut rng);
        self.0 .0.get(access).unwrap().calculate(pct)
    }
}

pub struct DeviceState {
    pub name: String,
    pub kind: Device,
    // Number of blocks currently used.
    pub free: usize,
    // Absolute number of blocks which can be stored.
    pub total: usize,
    pub reserved_until: SystemTime,
    // pub submission_queue: VecDeque<(SystemTime, Access, Option<DiskId>)>,
    pub max_queue_len: usize,
    pub current_queue_len: usize,
    // Metrics
    pub max_q: Duration,
    pub total_q: Duration,
    pub total_req: usize,
    pub idle_time: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct DeviceLatencyTable(HashMap<DeviceAccessParams, Parameters>);

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct DeviceAccessParams {
    block_size: u32,
    queue_depth: u32,
    rw: u32,
    op: Op,
}

impl DeviceAccessParams {
    // FIXME: These are some default values which work for the current version
    // but not forever.
    pub fn read() -> Self {
        DeviceAccessParams {
            block_size: BLOCK_SIZE_IN_B as u32,
            queue_depth: 128,
            rw: (1.0f32).to_bits(),
            op: Op::Read,
        }
    }

    pub fn write() -> Self {
        DeviceAccessParams {
            block_size: BLOCK_SIZE_IN_B as u32,
            queue_depth: 128,
            rw: (1.0f32).to_bits(),
            op: Op::Read,
        }
    }
}

#[derive(Deserialize)]
pub struct DeviceRecord {
    blocksize: u32,
    op: Op,
    rw: f32,
    gap: f64,
    queue_depth: u32,
    a: f64,
    b: f64,
    c: f64,
}

impl DeviceRecord {
    fn to_access_params(&self) -> DeviceAccessParams {
        DeviceAccessParams {
            block_size: self.blocksize,
            queue_depth: self.queue_depth,
            rw: self.rw.to_bits(),
            op: self.op.clone(),
        }
    }

    fn to_params(&self) -> Parameters {
        Parameters {
            a: self.a,
            b: self.b,
            c: self.c,
            gap: self.gap,
        }
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
                    device
                        .0
                        .insert(record.to_access_params(), record.to_params());
                } else {
                    panic!("{:?}", record.err());
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

#[derive(Deserialize, Debug, Hash, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Op {
    #[serde(rename = "write")]
    Write = 0,
    #[serde(rename = "read")]
    Read,
}

// #[derive(Deserialize)]
// #[repr(u8)]
// pub enum Ap {
//     #[serde(rename = "random")]
//     Random = 0,
//     #[serde(rename = "sequential")]
//     Sequential,
//     #[serde(skip)]
//     LEN,
// }
