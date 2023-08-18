/// This project contains a rough-edge simulator description of a multi-device
/// storage stack to try out some migration policies.
///
/// Goal: Define a skeleton in which a policy is implemented as a trait object.
/// The policy can move data while an access pattern is performed to minimize
/// the total application runtime. For simplicity a device can handle only a
/// single operation at a time. Devices can perform operations parallel to one
/// another.
///
/// Problems
/// ========
///
/// Things like SSD internal parallelization and device access patterns cannot
/// be modelled.
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{Duration, SystemTime},
};

use rand::{prelude::Distribution, rngs::ThreadRng, seq::SliceRandom, Rng, SeedableRng};
use zipf::ZipfDistribution;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub enum Device {
    // 6 dimms
    OptanePMem,
    OptaneSSD,
    SamsungZSSD,
    MicronTLCSSD,
    GenericHDD,
    DRAM,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Block(usize);

#[derive(Clone)]
pub enum Access {
    Read(Block),
    Write(Block),
}

impl Access {
    pub fn generate<R>(rw: f64, dist: ZipfDistribution, rng: &mut R) -> Self
    where
        R: Rng,
    {
        let block = Block(dist.sample(rng));
        match rng.gen_bool(rw) {
            true => Self::Read(block),
            false => Self::Write(block),
        }
    }

    pub fn generate_iter<R>(
        rw: f64,
        dist: ZipfDistribution,
        rng: R,
        mut rng_rw: R,
    ) -> impl Iterator<Item = Access>
    where
        R: Rng,
    {
        dist.sample_iter(rng)
            .map(|ids| Block(ids))
            .map(move |block| match rng_rw.gen_bool(rw) {
                true => Self::Read(block),
                false => Self::Write(block),
            })
    }

    pub fn block(&self) -> &Block {
        match self {
            Access::Read(ref block) => block,
            Access::Write(ref block) => block,
        }
    }
}

const BLOCK_SIZE_IN_MB: usize = 4;

impl Device {
    // All these numbers are approximations!  Numbers taken from peak
    // performance over multiple queue depths, real results are likely to be
    // worse.
    fn read(&self) -> Duration {
        match self {
            // 30 GiB/s peak
            Device::OptanePMem => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (30f32 * 1024f32))
            }
            // 2.5 GiB/s peak
            Device::OptaneSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2517f32),
            Device::SamsungZSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 3130f32),
            Device::MicronTLCSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2903f32),
            Device::GenericHDD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 94f32),
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
        }
    }

    fn write(&self) -> Duration {
        match self {
            Device::OptanePMem => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (16f32 * 1024f32))
            }
            Device::OptaneSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2278f32),
            Device::SamsungZSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 576f32),
            Device::MicronTLCSSD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 1408f32),
            Device::GenericHDD => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 38.2f32),
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
        }
    }

    // /// Number of blocks a single device can at maximum hold.
    // fn capacity(&self) -> usize {
    //     match self {
    //         // 1 TB max assumed (more is possible i know)
    //         //                    TB   GB     MB
    //         Device::OptanePMem => 1 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
    //         // 1.6 TB max
    //         //                    GB     MB
    //         Device::OptaneSSD => 1600 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 3.2 TB max
    //         //                      GB     MB
    //         Device::SamsungZSSD => 3200 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 30.72 TB max
    //         //                      GB       MB
    //         Device::MicronTLCSSD => 30720 * 1000 / BLOCK_SIZE_IN_MB,
    //         // 30 TB max assumed (there is higher)
    //         //                    TB    GB     MB
    //         Device::GenericHDD => 30 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
    //         // 32 GB max (set limitation due to impl on client)
    //         //              GB   MB
    //         Device::DRAM => 32 * 1024 / BLOCK_SIZE_IN_MB,
    //     }
    // }
}

pub trait Policy {
    fn new() -> Self;
    fn update(&mut self, accesses: Vec<Access>) -> State;
}

pub enum Action {
    Replicate(Device),
    Migrate(Device),
    Prefetch,
}

pub struct State {
    /// Actions which are advised to be executed on the next encountered with the block.
    hints: HashMap<Block, Action>,
    /// Actions to be executed instantly when the update finished
    instant: Vec<Action>,
}

pub struct BlockState {
    location: Device,
    replicated: Option<Device>,
}

pub struct DeviceState {
    // Number of blocks currently used.
    free: usize,
    // Absolute number of blocks which can be stored.
    total: usize,
    reserved_until: SystemTime,
    queue: VecDeque<Access>,
}

pub struct StorageStack<S, P> {
    blocks: HashMap<Block, Device>,
    devices: HashMap<Device, DeviceState>,
    state: S,
    policy: P,
}

impl<S, P> StorageStack<S, P> {
    /// Act on specified block and return subsequent event.
    fn submit(&mut self, now: SystemTime, access: Access) -> (SystemTime, Event) {
        let dev = self.blocks.get(access.block()).expect("Invalid Block");
        let dev_stats = self.devices.get_mut(dev).expect("Invalid Device");

        let until = dev_stats.reserved_until.max(now)
            + match access {
                Access::Read(_) => dev.read(),
                Access::Write(_) => dev.write(),
            };
        dev_stats.queue.push_back(access.clone());
        dev_stats.reserved_until = until;

        (until, Event::Finished(now, access, *dev))
    }

    fn insert(&mut self, block: Block, device: Device) -> Option<Block> {
        let dev = self.devices.get_mut(&device).unwrap();
        if dev.free > 0 {
            dev.free = dev.free.saturating_sub(1);
            self.blocks.insert(block, device);
            return None;
        }
        Some(block)
    }
}

/// An event which is noted to happen sometime in the future.
pub enum Event {
    Submit(Access, Device),
    Finished(SystemTime, Access, Device),
}

/// Core unit of the simulation.
///
/// Simulated Phases
/// ----------------
///
/// The simulation must handle different parallel access and resource occupation
/// to reach a satisfying approximation.
pub struct PolicySimulator<S, P> {
    stack: StorageStack<S, P>,
    now: SystemTime,
    // Ordered Map, system time is priority.
    events: BTreeMap<SystemTime, Event>,
    dist: ZipfDistribution,
    rng: ThreadRng,
    total_blocks: usize,
    seed: u64,
    rw: f64,
    iteration: usize,
}

/// A number of requests to submit at once. All requests have to be finished
/// before a new batch can be issued.
const BATCH_SIZE: usize = 128;

impl<S, P> PolicySimulator<S, P> {
    fn step(&mut self) {
        // generate a series of 100 requests
        let reqs = Access::generate_iter(
            self.rw,
            self.dist.clone(),
            self.rng.clone(),
            self.rng.clone(),
        )
        .take(BATCH_SIZE);

        for req in reqs {
            let (then, ev) = self.stack.submit(self.now, req);
            self.events.insert(then, ev);
        }
        let mut write_latency = vec![];
        let mut read_latency = vec![];

        // "Wait" until all requests are finished to enter the next phase.
        while let Some((then, ev)) = self.events.pop_first() {
            match ev {
                Event::Submit(_, _) => unreachable!(),
                Event::Finished(when_issued, access, device) => {
                    // Step into the future.
                    self.now = then.clone();
                    let lat = match access {
                        Access::Read(_) => &mut write_latency,
                        Access::Write(_) => &mut read_latency,
                    };
                    lat.push(self.now.duration_since(when_issued).expect("Negative Time"));
                }
            }
        }
        // End of I/O Phase
        // TODO: Call Policy now, or do parallel messages (queue) to which a
        // policy can interject? Take oracle from Haura directly?
        // FIXME: Use propoer statistics, this is more of debug info
        println!(
            "Write: Average {}us, Max {}us",
            write_latency.iter().map(|d| d.as_micros()).sum::<u128>()
                / (write_latency.len() as u128),
            write_latency
                .iter()
                .map(|d| d.as_micros())
                .max()
                .unwrap_or(0)
        );
        println!(
            "Read: Average {}us, Max {}us",
            read_latency.iter().map(|d| d.as_micros()).sum::<u128>() / (read_latency.len() as u128),
            read_latency
                .iter()
                .map(|d| d.as_micros())
                .max()
                .unwrap_or(0)
        );
    }

    /// Distribute initial blocks in the storage stack. This is done entirely
    /// randomly with a fixed seed.
    fn prepare(&mut self) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed);
        for id in 0..self.total_blocks {
            // Try insertion.
            let mut devs = self
                .stack
                .devices
                .keys()
                .map(|e| e.clone())
                .collect::<Vec<Device>>();
            devs.shuffle(&mut rng);
            for dev in devs.iter() {
                if self.stack.insert(Block(id), dev.clone()).is_none() {
                    break;
                }
            }
        }
    }

    fn run(mut self) {
        println!("Start run");
        self.prepare();
        for idx in 0..self.iteration {
            println!("Iteration {idx}");
            self.step();
        }
        println!(
            "Runtime: {}s",
            self.now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        )
    }
}

fn main() {
    // TODO: Read config
    let sim: PolicySimulator<(), ()> = PolicySimulator {
        stack: StorageStack {
            blocks: [].into(),
            devices: [(
                Device::GenericHDD,
                DeviceState {
                    free: 1280,
                    total: 1280,
                    reserved_until: std::time::UNIX_EPOCH,
                    queue: VecDeque::new(),
                },
            )]
            .into(),
            state: (),
            policy: (),
        },
        now: std::time::UNIX_EPOCH,
        events: BTreeMap::new(),
        dist: ZipfDistribution::new(512, 0.99).unwrap(),
        rng: rand::thread_rng(),
        total_blocks: 513,
        seed: 12345,
        rw: 0.1,
        iteration: 10000,
    };
    sim.run()
}
