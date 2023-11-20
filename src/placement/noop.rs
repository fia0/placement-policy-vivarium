use std::{collections::HashMap, time::SystemTime};

use crate::{storage_stack::DeviceState, Block};

use super::{PlacementMsg, PlacementPolicy};

pub struct Noop {}

impl PlacementPolicy for Noop {
    fn init(
        &mut self,
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn update(
        &mut self,
        msg: PlacementMsg,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn migrate(
        &mut self,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }
}
