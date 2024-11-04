use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crossbeam::channel::Sender;
use priority_queue::DoublePriorityQueue;

use crate::{
    result_csv::{MovementInfo, ResMsg},
    storage_stack::{DeviceState, DiskId, BLOCK_SIZE_IN_B},
    Block, Event,
};

use super::{PlacementMsg, PlacementPolicy};

/// Simple Example policy.
/// Keeping track of blocks and promoting them eventually.
pub struct FrequencyPolicy {
    // accesses: HashMap<Block, u64>,
    blocks: HashMap<DiskId, DoublePriorityQueue<Block, u64>>,
    idle_disks: HashMap<DiskId, Duration>,
    reactiveness: usize,
    decay: f32,
    interval: Duration,

    _low_threshold: f32,
    _high_threshold: f32,
}

impl FrequencyPolicy {
    pub fn new(interval: Duration, reactiveness: usize, decay: f32) -> Self {
        FrequencyPolicy {
            blocks: HashMap::new(),
            idle_disks: HashMap::new(),
            reactiveness,
            interval,
            decay,
            _low_threshold: 0.,
            _high_threshold: 0.,
        }
    }
}

impl PlacementPolicy for FrequencyPolicy {
    fn init(
        &mut self,
        devices: &HashMap<DiskId, DeviceState>,
        blocks: &HashMap<Block, DiskId>,
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
        devices: &mut HashMap<DiskId, DeviceState>,
        blocks: &HashMap<Block, DiskId>,
        now: SystemTime,
        tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        match msg {
            PlacementMsg::Migrate => return self.migrate(devices, blocks, now, tx),
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
        devices: &mut HashMap<DiskId, DeviceState>,
        _blocks: &HashMap<Block, DiskId>,
        now: SystemTime,
        tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (std::time::SystemTime, crate::Event)>> {
        // update idle disks numbers
        let mut least_idling_disks = Vec::new();
        for dev in devices.iter() {
            let idle = self.idle_disks.get_mut(dev.0).unwrap();
            least_idling_disks.push((dev.0.clone(), dev.1.idle_time.saturating_sub(*idle)));
            *idle = dev.1.idle_time;
        }
        least_idling_disks.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // TODO: Utilize thresholds? For analysis of new blocks necessary, which is not implemented yet.
        // let mut eviction_ready_disk = Vec::new();
        // for (device_id, device_state) in devices.iter() {
        //     if device_state.total as f32 * self.high_threshold < device_state.free as f32 {
        //         // Move data away from the current disk
        //         eviction_ready_disk.push(device_id.clone());
        //     }
        // }

        // Cost estimation based on the obeserved frequency and potentially movement of data from the other disk.
        // Migrate a from A to B
        // Do if:
        // a_freq * (cost(A) - cost(B)) > cost(A) + cost(B)
        // Check if costs are reduced compared to costs expanded
        // Similar to a case when swapping two blocks:
        // a_freq * (cost(A) - cost(B)) + b_freq * (cost(B) - cost(A)) > 2 * cost(A) + cost(B)
        //
        // Take note, that costs are simplified and might diff between read/write.
        let mut msgs = Vec::new();
        let mut movements = Vec::new();
        for (disk_a, disk_idle) in least_idling_disks.iter() {
            for disk_b in least_idling_disks.iter().rev().filter(|s| s.1 > *disk_idle) {
                let mut new_blocks_a = Vec::new();
                let mut new_blocks_b = Vec::new();

                // FIXME: These operations should be replaced with hypotheticals for actual runs.
                let state_a = devices.get_mut(disk_a).unwrap();
                let cost_a = state_a
                    .kind
                    .read(BLOCK_SIZE_IN_B as u64, crate::storage_stack::Ap::Random);
                let state_b = devices.get_mut(&disk_b.0).unwrap();
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
                        new_blocks_b.push((block, freq));
                        // self.blocks.get_mut(&disk_b.0).unwrap().push(block, freq);
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
                            new_blocks_a.push((b_block, b_block_freq));
                            new_blocks_b.push((a_block, a_block_freq));
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
                        } else {
                            break;
                        }
                    }
                }
                let queue_a = self.blocks.get_mut(disk_a).unwrap();
                for b in new_blocks_a.iter() {
                    queue_a.push(b.0, b.1);
                }
                movements.push(MovementInfo {
                    from: disk_b.0.clone(),
                    to: disk_a.clone(),
                    size: new_blocks_a.len(),
                });
                let queue_b = self.blocks.get_mut(&disk_b.0).unwrap();
                for b in new_blocks_b.iter() {
                    queue_b.push(b.0, b.1);
                }
                movements.push(MovementInfo {
                    from: disk_a.clone(),
                    to: disk_b.0.clone(),
                    size: new_blocks_b.len(),
                });
            }
        }

        for queue in self.blocks.iter_mut() {
            for (_i, p) in queue.1.iter_mut() {
                *p = (*p as f32 * (1.0 - self.decay)) as u64;
            }
        }

        tx.send(ResMsg::Policy {
            now,
            moved: movements,
        })
        .unwrap();
        Box::new(msgs.into_iter().chain([(
            now + self.interval,
            Event::PlacementPolicy(PlacementMsg::Migrate),
        )]))
    }
}
