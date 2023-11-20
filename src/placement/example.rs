use std::{collections::HashMap, time::SystemTime};

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

    low_threshold: f32,
    high_threshold: f32,
}

impl RecencyPolicy {
    pub fn new() -> Self {
        RecencyPolicy {
            blocks: HashMap::new(),
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
        }
        for block in blocks {
            self.blocks
                .get_mut(block.1)
                .unwrap()
                .push(block.0.clone(), 0);
        }
        Box::new(
            [(
                now + std::time::Duration::from_secs(600),
                Event::PlacementPolicy(PlacementMsg::Migrate),
            )]
            .into_iter(),
        )
    }

    fn update(
        &mut self,
        msg: PlacementMsg,
        devices: &HashMap<String, DeviceState>,
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
        devices: &HashMap<String, DeviceState>,
        blocks: &HashMap<Block, String>,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        let mut eviction_ready_disk = Vec::new();
        for (device_id, device_state) in devices.iter() {
            if device_state.total as f32 * self.high_threshold < device_state.free as f32 {
                // Move data away from the current disk
                eviction_ready_disk.push(device_id.clone());
            }
        }

        let mut least_idling_disks = devices
            .iter()
            .map(|(device_id, device_state)| (device_id.clone(), device_state.idle_time))
            .collect::<Vec<_>>();
        least_idling_disks.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Cost estimation based on the obeserved frequency and potentially movement of data from the other disk.

        // Migrate b from A to B
        // Do if:
        // b_freq * (cost(B) - cost(A)) > cost(A) + cost(B)
        // Check if costs are reduced compared to costs expanded
        let mut msgs = Vec::new();
        for (disk, disk_idle) in least_idling_disks.iter() {
            let cur_disk = devices.get(disk).unwrap();
            for candidate in least_idling_disks.iter().rev().filter(|s| s.1 > *disk_idle) {
                let state = devices.get(&candidate.0).unwrap();

                for _ in 0..1000 {
                    if state.free > 0 {
                        // Space is available for migration and should be used
                        // Migration handled internally on storage stack
                        // Data is blocked until completion
                        let foo = self.blocks.get_mut(disk).unwrap();
                        if foo.is_empty() {
                            continue;
                        }
                        let (block, freq) = foo.pop_max().unwrap();
                        self.blocks.get_mut(&candidate.0).unwrap().push(block, freq);
                        msgs.push((
                            now,
                            Event::Storage(crate::storage_stack::StorageMsg::Process(
                                crate::storage_stack::Step::MoveInit(block, candidate.0.clone()),
                            )),
                        ));
                    } else {
                        let (_, a_block_freq) = self.blocks.get(disk).unwrap().peek_max().unwrap();
                        let cost_a = cur_disk
                            .kind
                            .read(BLOCK_SIZE_IN_B as u64, crate::storage_stack::Ap::Random);
                        let cost_b = state
                            .kind
                            .read(BLOCK_SIZE_IN_B as u64, crate::storage_stack::Ap::Random);

                        let bar = self.blocks.get(&candidate.0).unwrap();
                        let (_, b_block_freq) = bar.peek_min().unwrap();

                        if *a_block_freq as i128
                            * (cost_a.as_micros() as i128 - cost_b.as_micros() as i128)
                            - *b_block_freq as i128
                                * (cost_b.as_micros() as i128 - cost_a.as_micros() as i128)
                            > 2 * cost_a.checked_add(cost_b).unwrap().as_micros() as i128
                        {
                            dbg!(a_block_freq);
                            dbg!(b_block_freq);
                            let (a_block, a_block_freq) =
                                self.blocks.get_mut(disk).unwrap().pop_max().unwrap();
                            let b_disk = self.blocks.get_mut(&candidate.0).unwrap();
                            let (b_block, b_block_freq) = b_disk.pop_max().unwrap();
                            b_disk.push(a_block, a_block_freq);
                            let a_disk = self.blocks.get_mut(disk).unwrap();
                            a_disk.push(b_block, b_block_freq);
                            msgs.push((
                                now,
                                Event::Storage(crate::storage_stack::StorageMsg::Process(
                                    crate::storage_stack::Step::MoveInit(
                                        a_block,
                                        candidate.0.clone(),
                                    ),
                                )),
                            ));
                            msgs.push((
                                now,
                                Event::Storage(crate::storage_stack::StorageMsg::Process(
                                    crate::storage_stack::Step::MoveInit(b_block, disk.clone()),
                                )),
                            ));
                            // println!("Block switching possible... not implemented");
                        }
                    }
                }
            }
        }
        Box::new(msgs.into_iter().chain([(
            now + std::time::Duration::from_secs(600),
            Event::PlacementPolicy(PlacementMsg::Migrate),
        )]))
    }
}
