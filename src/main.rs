#![feature(return_position_impl_trait_in_trait)]
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
    io::Read,
    time::{Duration, SystemTime},
};

use application::{Application, ZipfApp, ZipfConfig};
use rand::{prelude::Distribution, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use serde::Deserialize;
use zipf::ZipfDistribution;

mod application;
mod config;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
#[repr(u8)]
pub enum Device {
    // 6 dimms
    Intel_Optane_PMem_100 = 0,
    Intel_Optane_SSD_DC_P4800X = 1,
    Samsung_983_ZET = 2,
    Micron_9100_MAX = 3,
    Western_Digital_WD5000AAKS = 4,
    DRAM = 5,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct Block(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Access {
    Read(Block),
    Write(Block),
}

impl Access {
    pub fn generate<R>(rw: f64, dist: &mut ZipfDistribution, rng: &mut R) -> Self
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

pub struct RandomAccessSequence<'a, R> {
    rng: &'a mut R,
    dist: &'a mut ZipfDistribution,
    rw: f64,
}

impl<'a, R: Rng> RandomAccessSequence<'a, R> {
    pub fn new(rng: &'a mut R, dist: &'a mut ZipfDistribution, rw: f64) -> Self {
        Self { rng, dist, rw }
    }
}

impl<'a, R: Rng> Iterator for RandomAccessSequence<'a, R> {
    type Item = Access;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Access::generate(self.rw, self.dist, self.rng))
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
            Device::Intel_Optane_PMem_100 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (30f32 * 1024f32))
            }
            // 2.5 GiB/s peak
            Device::Intel_Optane_SSD_DC_P4800X => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2517f32)
            }
            Device::Samsung_983_ZET => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 3130f32),
            Device::Micron_9100_MAX => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2903f32),
            Device::Western_Digital_WD5000AAKS => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 94f32)
            }
            Device::DRAM => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (90f32 * 1024f32)),
        }
    }

    fn write(&self) -> Duration {
        match self {
            Device::Intel_Optane_PMem_100 => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / (16f32 * 1024f32))
            }
            Device::Intel_Optane_SSD_DC_P4800X => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 2278f32)
            }
            Device::Samsung_983_ZET => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 576f32),
            Device::Micron_9100_MAX => Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 1408f32),
            Device::Western_Digital_WD5000AAKS => {
                Duration::from_secs_f32(BLOCK_SIZE_IN_MB as f32 / 38.2f32)
            }
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
    /// Returns the point in time when the policy is next due to be called for
    /// evaluating possible actions.
    fn due() -> SystemTime;
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
    kind: Device,
    // Number of blocks currently used.
    free: usize,
    // Absolute number of blocks which can be stored.
    total: usize,
    reserved_until: SystemTime,
    queue: VecDeque<Access>,
}

pub struct StorageStack<S, P> {
    blocks: HashMap<Block, String>,
    devices: HashMap<String, DeviceState>,
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
                Access::Read(_) => dev_stats.kind.read(),
                Access::Write(_) => dev_stats.kind.write(),
            };
        dev_stats.queue.push_back(access.clone());
        dev_stats.reserved_until = until;

        (until, Event::Finished(now, access, dev.clone()))
    }

    /// An operation has finished and can be removed from the device queue.
    fn finish(&mut self, dev: &String) {
        self.devices.get_mut(dev).unwrap().queue.pop_front();
    }

    fn insert(&mut self, block: Block, device: String) -> Option<Block> {
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
#[derive(Debug)]
pub enum Event {
    Submit(Access),
    Finished(SystemTime, Access, String),
    // // Call the placement policy once and reinject the new start time.
    // PlacementPolicy,
}

/// Core unit of the simulation.
///
/// Simulated Phases
/// ----------------
///
/// The simulation must handle different parallel access and resource occupation
/// to reach a satisfying approximation.
pub struct PolicySimulator<S, P, A: Application> {
    stack: StorageStack<S, P>,
    application: A,
    now: SystemTime,
    // Ordered Map, system time is priority.
    events: BTreeMap<SystemTime, Event>,
    rng: StdRng,
}

impl<S, P, A: Application> PolicySimulator<S, P, A> {
    /// Distribute initial blocks in the storage stack. This is done entirely
    /// randomly with a fixed seed.
    fn prepare(&mut self) {
        for block in self.application.init() {
            // Try insertion.
            let mut devs = self
                .stack
                .devices
                .keys()
                .map(|e| e.clone())
                .collect::<Vec<String>>();
            // hash key order not deterministic
            devs.sort();
            devs.shuffle(&mut self.rng);
            for dev in devs.iter() {
                if self.stack.insert(block, dev.clone()).is_none() {
                    break;
                }
            }
        }
    }

    /// Execute the main event digestion.
    fn run(mut self) {
        self.prepare();
        // Start the application
        for (idx, access) in self.application.start().enumerate() {
            self.events.insert(
                self.now + Duration::from_nanos(idx as u64),
                Event::Submit(access),
            );
        }
        while let Some((then, event)) = self.events.pop_first() {
            // Step forward to the current timestamp
            self.now = then;
            match event {
                Event::Submit(access) => {
                    let (then, ev) = self.stack.submit(self.now, access);
                    self.events.insert(then, ev);
                }
                Event::Finished(when_issued, access, device) => {
                    self.stack.finish(&device);
                    if let Some((future, accesses)) =
                        self.application.done(access, when_issued, self.now)
                    {
                        for (idx, acc) in accesses.enumerate() {
                            self.events.insert(
                                future + Duration::from_nanos(idx as u64),
                                Event::Submit(acc),
                            );
                        }
                    }
                }
            }
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

fn main() ->  {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open("config/input.toml");
    let mut content = String::new();
    file.unwrap().read_to_string(&mut content);
    let config: config::Config = toml::from_str(&content).unwrap();

    // TODO: Read config
    let sim: PolicySimulator<(), (), ZipfApp> = PolicySimulator {
        stack: StorageStack {
            blocks: [].into(),
            devices: config.devices(),
            state: (),
            policy: (),
        },
        application: config.app.build(),
        now: std::time::UNIX_EPOCH,
        events: BTreeMap::new(),
        rng: rand::rngs::StdRng::seed_from_u64(12345),
    };
    sim.run()
}
