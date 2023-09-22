use crate::{
    application::{Application, ZipfApp, ZipfConfig},
    DeviceState,
};

use super::Device;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};

#[derive(Deserialize)]
pub struct Config {
    pub app: App,
    pub devices: HashMap<String, DeviceConfig>,
}

impl Config {
    pub fn devices(&self) -> HashMap<String, (Device, DeviceState)> {
        self.devices
            .iter()
            .map(|(id, dev)| {
                (
                    id.clone(),
                    (
                        dev.kind,
                        DeviceState {
                            free: dev.capacity,
                            total: dev.capacity,
                            reserved_until: std::time::UNIX_EPOCH,
                            queue: VecDeque::new(),
                        },
                    ),
                )
            })
            .collect()
    }
}

#[derive(Deserialize)]
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
