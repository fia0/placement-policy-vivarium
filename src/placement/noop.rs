use std::{collections::HashMap, time::SystemTime};

use crossbeam::channel::Sender;

use crate::{
    result_csv::ResMsg,
    storage_stack::{DeviceState, DiskId},
    Block,
};

use super::{PlacementMsg, PlacementPolicy};

pub struct Noop {}

impl PlacementPolicy for Noop {
    fn init(
        &mut self,
        _devices: &HashMap<DiskId, DeviceState>,
        _blocks: &HashMap<Block, DiskId>,
        _now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn update(
        &mut self,
        _msg: PlacementMsg,
        _devices: &mut HashMap<DiskId, DeviceState>,
        _blocks: &HashMap<Block, DiskId>,
        _now: SystemTime,
        _tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }

    fn migrate(
        &mut self,
        _devices: &mut HashMap<DiskId, DeviceState>,
        _blocks: &HashMap<Block, DiskId>,
        _now: SystemTime,
        _tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        Box::new([].into_iter())
    }
}
