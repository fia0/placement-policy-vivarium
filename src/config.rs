use crate::{
    application::{Application, BatchApp, BatchConfig},
    cache::{Cache, CacheLogic, Fifo, Lru, Noop},
    placement::PlacementConfig,
    storage_stack::{Device, DeviceLatencyTable, DeviceState},
    Block, SimError,
};

use crate::storage_stack::DeviceSer;
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
    ) -> Result<HashMap<String, DeviceState>, SimError> {
        let mut map = HashMap::new();
        for (id, dev) in self.devices.iter() {
            map.insert(
                id.clone(),
                DeviceState {
                    kind: dev.kind.to_device(loaded_devices)?,
                    free: dev.capacity,
                    total: dev.capacity,
                    reserved_until: std::time::UNIX_EPOCH,
                    queue: VecDeque::new(),
                    total_q: std::time::Duration::ZERO,
                    total_req: 0,
                    max_q: std::time::Duration::ZERO,
                    idle_time: std::time::Duration::ZERO,
                    last_access: Block(0),
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
    kind: DeviceSer,
    capacity: usize,
}

#[derive(Deserialize)]
pub struct CacheConfig {
    algorithm: CacheAlgorithm,
    device: DeviceSer,
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
                self.device.to_device(loaded_devices)?,
            ))),
            CacheAlgorithm::Fifo => Ok(Box::new(Fifo::new(
                self.capacity,
                self.device.to_device(loaded_devices)?,
            ))),
            CacheAlgorithm::Noop => Ok(Box::new(Noop {})),
        }
    }
}
