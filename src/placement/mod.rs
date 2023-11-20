use std::{collections::HashMap, time::SystemTime};

use crate::{storage_stack::DeviceState, Block, Event};

mod example;
mod noop;

pub use example::RecencyPolicy;
pub use noop::Noop;

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
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)>>;
    fn migrate(
        &mut self,
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)>>;
}
