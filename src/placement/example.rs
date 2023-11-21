use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use priority_queue::DoublePriorityQueue;

use crate::{
    storage_stack::{DeviceState, BLOCK_SIZE_IN_B, BLOCK_SIZE_IN_MB},
    Block, Event,
};

use super::{PlacementMsg, PlacementPolicy};

/// Simple Example policy.
/// Keeping track of blocks and promoting them eventually.
pub struct RecencyPolicy {
    // accesses: HashMap<Block, u64>,
    blocks: HashMap<String, DoublePriorityQueue<Block, u64>>,
    idle_disks: HashMap<String, Duration>,
    reactiveness: usize,
    interval: Duration,

    low_threshold: f32,
    high_threshold: f32,
}

impl RecencyPolicy {
    pub fn new(interval: Duration, reactiveness: usize) -> Self {
        RecencyPolicy {
            blocks: HashMap::new(),
            idle_disks: HashMap::new(),
            reactiveness,
            interval,
            low_threshold: 0.,
            high_threshold: 0.,
        }
    }
}

impl PlacementPolicy for RecencyPolicy {
    fn init(
        &mut self,
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        for dev in devices {
            self.blocks
                .insert(dev.0.clone(), DoublePriorityQueue::new());
            self.idle_disks.insert(dev.0.clone(), Duration::ZERO);
        }
        for block in blocks {
            self.blocks
                .get_mut(block.1)
                .unwrap()
                .push(block.0.clone(), 0);
        }
        Box::new(
            [(
                now + self.interval,
                Event::PlacementPolicy(PlacementMsg::Migrate),
            )]
            .into_iter(),
        )
    }

    fn update(
        &mut self,
        msg: PlacementMsg,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        match msg {
            PlacementMsg::Migrate => return self.migrate(devices, blocks, now),
            _ => {}
        }
        let block = msg.block();
        let dev = blocks.get(block).unwrap();
        self.blocks
            .get_mut(dev)
            .unwrap()
            .change_priority_by(block, |p| {
                *p += 1;
            });

        // match self.accesses.entry(block.clone()) {
        //     std::collections::hash_map::Entry::Occupied(mut occ) => *occ.get_mut() += 1,
        //     std::collections::hash_map::Entry::Vacant(vac) => {
        //         vac.insert(1);
        //     }
        // }
        Box::new([].into_iter())
    }

    fn migrate(
        &mut self,
        devices: &mut HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        // update idle disks numbers
        let mut least_idling_disks = Vec::new();
        for dev in devices.iter() {
            let idle = self.idle_disks.get_mut(dev.0).unwrap();
            least_idling_disks.push((dev.0.clone(), dev.1.idle_time.saturating_sub(*idle)));
            *idle = dev.1.idle_time;
        }
        least_idling_disks.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let mut eviction_ready_disk = Vec::new();
        for (device_id, device_state) in devices.iter() {
            if device_state.total as f32 * self.high_threshold < device_state.free as f32 {
                // Move data away from the current disk
                eviction_ready_disk.push(device_id.clone());
            }
        }

        // Cost estimation based on the obeserved frequency and potentially movement of data from the other disk.

        // Migrate b from A to B
        // Do if:
        // b_freq * (cost(B) - cost(A)) > cost(A) + cost(B)
        // Check if costs are reduced compared to costs expanded
        let mut msgs = Vec::new();
        for (disk_a, disk_idle) in least_idling_disks.iter() {
            for disk_b in least_idling_disks.iter().rev().filter(|s| s.1 > *disk_idle) {
                let mut new_blocks_a = Vec::new();
                let mut new_blocks_b = Vec::new();

                let state_a = devices.get(disk_a).unwrap();
                let cost_a = state_a
                    .kind
                    .read(BLOCK_SIZE_IN_B as u64, crate::storage_stack::Ap::Random);
                let state_b = devices.get(&disk_b.0).unwrap();
                let cost_b = state_b
                    .kind
                    .write(BLOCK_SIZE_IN_B as u64, crate::storage_stack::Ap::Random);

                for _ in 0..self.reactiveness {
                    let (_, a_block_freq) = self.blocks.get(disk_a).unwrap().peek_max().unwrap();
                    let (_, b_block_freq) = self.blocks.get(&disk_b.0).unwrap().peek_min().unwrap();

                    let state = devices.get_mut(&disk_b.0).unwrap();
                    if state.free > 0
                        && *a_block_freq as i128
                            * (cost_a.as_micros() as i128 - cost_b.as_micros() as i128)
                            > cost_a.checked_add(cost_b).unwrap().as_micros() as i128
                    {
                        // Space is available for migration and should be used
                        // Migration handled internally on storage stack
                        // Data is blocked until completion
                        let foo = self.blocks.get_mut(disk_a).unwrap();
                        if foo.is_empty() {
                            continue;
                        }
                        let (block, freq) = foo.pop_max().unwrap();
                        self.blocks.get_mut(&disk_b.0).unwrap().push(block, freq);
                        state.free -= 1;
                        let cur_disk = devices.get_mut(disk_a).unwrap();
                        cur_disk.free += 1;
                        msgs.push((
                            now,
                            Event::Storage(crate::storage_stack::StorageMsg::Process(
                                crate::storage_stack::Step::MoveInit(block, disk_b.0.clone()),
                            )),
                        ));
                    } else {
                        if self.blocks.get(disk_a).unwrap().is_empty() {
                            break;
                        }

                        if *a_block_freq as i128
                            * (cost_a.as_micros() as i128 - cost_b.as_micros() as i128)
                            - *b_block_freq as i128
                                * (cost_b.as_micros() as i128 - cost_a.as_micros() as i128)
                            > 2 * cost_a.checked_add(cost_b).unwrap().as_micros() as i128
                        {
                            let (a_block, a_block_freq) =
                                self.blocks.get_mut(disk_a).unwrap().pop_max().unwrap();
                            let queue_b = self.blocks.get_mut(&disk_b.0).unwrap();
                            let (b_block, b_block_freq) = queue_b.pop_min().unwrap();
                            // println!("Swapping blocks: {} <-> {}", a_block.0, b_block.0);
                            new_blocks_a.push((b_block, b_block_freq));
                            new_blocks_b.push((a_block, a_block_freq));
                            // queue_b.push(a_block, a_block_freq);
                            // let queue_a = self.blocks.get_mut(disk_a).unwrap();
                            // queue_a.push(b_block, b_block_freq);
                            msgs.push((
                                now,
                                Event::Storage(crate::storage_stack::StorageMsg::Process(
                                    crate::storage_stack::Step::MoveInit(a_block, disk_b.0.clone()),
                                )),
                            ));
                            msgs.push((
                                now,
                                Event::Storage(crate::storage_stack::StorageMsg::Process(
                                    crate::storage_stack::Step::MoveInit(b_block, disk_a.clone()),
                                )),
                            ));
                        }
                    }
                }
                let queue_a = self.blocks.get_mut(disk_a).unwrap();
                for b in new_blocks_a {
                    queue_a.push(b.0, b.1);
                }
                let queue_b = self.blocks.get_mut(&disk_b.0).unwrap();
                for b in new_blocks_b {
                    queue_b.push(b.0, b.1);
                }
            }
        }
        Box::new(msgs.into_iter().chain([(
            now + self.interval,
            Event::PlacementPolicy(PlacementMsg::Migrate),
        )]))
    }
}
