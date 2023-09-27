use crate::{
    application::{Application, ZipfApp, ZipfConfig},
    cache::{Cache, Fifo, Lru, Noop},
    DeviceState,
};

use super::Device;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use strum::EnumIter;

#[derive(Deserialize)]
pub struct Config {
    pub app: App,
    pub devices: HashMap<String, DeviceConfig>,
    pub cache: Option<CacheConfig>,
}

impl Config {
    pub fn devices(&self) -> HashMap<String, DeviceState> {
        self.devices
            .iter()
            .map(|(id, dev)| {
                (
                    id.clone(),
                    DeviceState {
                        kind: dev.kind,
                        free: dev.capacity,
                        total: dev.capacity,
                        reserved_until: std::time::UNIX_EPOCH,
                        queue: VecDeque::new(),
                        total_q: std::time::Duration::ZERO,
                        total_req: 0,
                        max_q: std::time::Duration::ZERO,
                    },
                )
            })
            .collect()
    }

    pub fn cache(&self) -> Box<dyn Cache> {
        match &self.cache {
            Some(c) => c.build(),
            None => Box::new(Noop {}),
        }
    }
}

#[derive(Deserialize, EnumIter, Debug)]
pub enum App {
    /// An application with a zipfian distributed random access pattern on blocks
    Zipf(ZipfConfig),
}

impl App {
    pub fn build(&self) -> ZipfApp {
        match self {
            App::Zipf(config) => ZipfApp::new(config),
        }
    }
}

#[derive(Deserialize)]
pub struct DeviceConfig {
    kind: Device,
    capacity: usize,
}

#[derive(Deserialize)]
pub struct CacheConfig {
    algorithm: CacheAlgorithm,
    device: Device,
    capacity: usize,
}

#[derive(Deserialize, PartialEq, Eq)]
pub enum CacheAlgorithm {
    Lru,
    Fifo,
    Noop,
}

impl CacheConfig {
    pub fn build(&self) -> Box<dyn Cache> {
        match self.algorithm {
            CacheAlgorithm::Lru => Box::new(Lru::new(self.capacity, self.device)),
            CacheAlgorithm::Fifo => Box::new(Fifo::new(self.capacity, self.device)),
            CacheAlgorithm::Noop => Box::new(Noop {}),
        }
    }
}
