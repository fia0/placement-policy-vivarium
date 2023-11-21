use std::{collections::HashMap, time::SystemTime};

use crate::{storage_stack::DeviceState, Block};

use super::{PlacementMsg, PlacementPolicy};

pub struct Noop {}

impl PlacementPolicy for Noop {
    fn init(
        &mut self,
        _devices: &HashMap<String, DeviceState>,
        _blocks: &HashMap<Block, String>,
        _now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn update(
        &mut self,
        _msg: PlacementMsg,
        _devices: &mut HashMap<String, DeviceState>,
        _blocks: &HashMap<Block, String>,
        _now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn migrate(
        &mut self,
        _devices: &mut HashMap<String, DeviceState>,
        _blocks: &HashMap<Block, String>,
        _now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }
}
