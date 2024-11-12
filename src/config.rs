use crate::{
    application::{Application, BatchApp, BatchConfig},
    cache::{Cache, CacheLogic, Fifo, Lru, Noop},
    placement::PlacementConfig,
    storage_stack::{to_device, DeviceLatencyTable, DeviceState, DiskId},
    Block, SimError,
};

use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use strum::EnumIter;

#[derive(Deserialize)]
pub struct Config {
    pub results: Results,
    pub app: App,
    pub devices: HashMap<String, DeviceConfig>,
    pub cache: Option<CacheConfig>,
    pub placement: PlacementConfig,
}

#[derive(Deserialize)]
pub struct Results {
    pub path: Option<std::path::PathBuf>,
}

impl Config {
    pub fn devices(
        &self,
        loaded_devices: &HashMap<String, DeviceLatencyTable>,
    ) -> Result<HashMap<DiskId, DeviceState>, SimError> {
        let mut map = HashMap::new();
        for (id, (_name, dev)) in self.devices.iter().enumerate() {
            map.insert(
                DiskId(id),
                DeviceState {
                    kind: to_device(&dev.kind, loaded_devices, dev.capacity)?,
                    free: dev.capacity,
                    total: dev.capacity,
                    reserved_until: std::time::UNIX_EPOCH,
                    submission_queue: VecDeque::new(),
                    max_queue_len: 128,
                    total_q: std::time::Duration::ZERO,
                    total_req: 0,
                    max_q: std::time::Duration::ZERO,
                    idle_time: std::time::Duration::ZERO,
                    current_queue_len: 0,
                },
            );
        }
        Ok(map)
    }

    pub fn cache(
        &self,
        loaded_devices: &HashMap<String, DeviceLatencyTable>,
    ) -> Result<CacheLogic, SimError> {
        Ok(CacheLogic::new(match &self.cache {
            Some(c) => c.build(loaded_devices)?,
            None => Box::new(Noop {}),
        }))
    }
}

#[derive(Deserialize, EnumIter, Debug)]
pub enum App {
    /// An application with a configurable access pattern on blocks
    Batch(BatchConfig),
}

impl App {
    pub fn build(&self) -> Box<dyn Application> {
        match self {
            App::Batch(config) => Box::new(BatchApp::new(config)),
        }
    }
}

#[derive(Deserialize)]
pub struct DeviceConfig {
    kind: String,
    capacity: usize,
}

#[derive(Deserialize)]
pub struct CacheConfig {
    algorithm: CacheAlgorithm,
    device: String,
    capacity: usize,
}

#[derive(Deserialize, PartialEq, Eq)]
pub enum CacheAlgorithm {
    Lru,
    Fifo,
    Noop,
}

impl CacheConfig {
    pub fn build(
        &self,
        loaded_devices: &HashMap<String, DeviceLatencyTable>,
    ) -> Result<Box<dyn Cache>, SimError> {
        match self.algorithm {
            CacheAlgorithm::Lru => Ok(Box::new(Lru::new(
                self.capacity,
                to_device(&self.device, loaded_devices, self.capacity)?,
            ))),
            CacheAlgorithm::Fifo => Ok(Box::new(Fifo::new(
                self.capacity,
                to_device(&self.device, loaded_devices, self.capacity)?,
            ))),
            CacheAlgorithm::Noop => Ok(Box::new(Noop {})),
        }
    }
}
