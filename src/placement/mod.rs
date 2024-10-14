use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::{result_csv::ResMsg, storage_stack::DeviceState, Block, Event};

mod frequency;
mod noop;

use crossbeam::channel::Sender;
use duration_str::deserialize_duration;
pub use frequency::FrequencyPolicy;
pub use noop::Noop;
use serde::Deserialize;

#[derive(Deserialize)]
pub enum PlacementConfig {
    Frequency {
        #[serde(deserialize_with = "deserialize_duration")]
        interval: Duration,
        reactiveness: usize,
        decay: f32,
    },
    Noop,
}

impl PlacementConfig {
    pub fn build(&self) -> Box<dyn PlacementPolicy> {
        match self {
            PlacementConfig::Frequency {
                interval,
                reactiveness,
                decay,
            } => Box::new(FrequencyPolicy::new(*interval, *reactiveness, *decay)),
            PlacementConfig::Noop => Box::new(Noop {}),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PlacementMsg {
    Fetched(Block),
    Written(Block),
    Migrate,
}

impl PlacementMsg {
    pub fn block(&self) -> &Block {
        match self {
            PlacementMsg::Fetched(block) | PlacementMsg::Written(block) => block,
            _ => unimplemented!(),
        }
    }
}

/// A policy adjusting data placement live.
pub trait PlacementPolicy {
    fn init(
        &mut self,
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)>>;
    fn update(
        &mut self,
        msg: PlacementMsg,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
        tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)>>;
    fn migrate(
        &mut self,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
        tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)>>;
}
