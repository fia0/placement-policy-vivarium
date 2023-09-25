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
    path::PathBuf,
    time::{Duration, SystemTime},
};

use application::{Application, ZipfApp};
use cache::Cache;
use clap::{Parser, Subcommand};
use rand::{prelude::Distribution, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use serde::Deserialize;
use strum::{EnumIter, IntoEnumIterator};
use thiserror::Error;
use zipf::ZipfDistribution;

use crate::config::App;

mod application;
mod cache;
mod config;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, EnumIter)]
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

impl Default for Device {
    fn default() -> Self {
        Self::DRAM
    }
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

    pub fn is_read(&self) -> bool {
        match self {
            Access::Read(_) => true,
            Access::Write(_) => false,
        }
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
    cache: Box<dyn Cache>,
    state: S,
    policy: P,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Could not find block {block:?}")]
    InvalidBlock { block: Block },
    #[error("Could not find device {id}")]
    InvalidDevice { id: String },
}

impl<S, P> StorageStack<S, P> {
    /// Act on specified block and return subsequent event.
    fn submit(
        &mut self,
        now: SystemTime,
        access: Access,
    ) -> Result<(SystemTime, Event), StorageError> {
        // Check if blocks arlready contained in the cache
        match &access {
            Access::Read(b) => {
                if let Some(dur) = self.cache.contains(b) {
                    return Ok((now + dur, Event::Finished(now, access, Origin::FromCache)));
                }
            }
            Access::Write(_) => {}
        }

        let dev = self
            .blocks
            .get(access.block())
            .ok_or(StorageError::InvalidBlock {
                block: access.block().clone(),
            })?;
        let dev_stats = self
            .devices
            .get_mut(dev)
            .ok_or(StorageError::InvalidDevice { id: dev.clone() })?;

        let until = dev_stats.reserved_until.max(now)
            + match access {
                Access::Read(_) => dev_stats.kind.read(),
                Access::Write(_) => dev_stats.kind.write(),
            };
        dev_stats.queue.push_back(access.clone());
        dev_stats.reserved_until = until;

        Ok((
            until,
            Event::Finished(now, access, Origin::FromDisk(dev.clone())),
        ))
    }

    /// An operation has finished and can be removed from the device queue.
    fn finish(&mut self, dev: &Origin) {
        match dev {
            Origin::FromCache => {}
            Origin::FromDisk(dev) => {
                self.devices.get_mut(dev).unwrap().queue.pop_front();
            }
        }
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
    Finished(SystemTime, Access, Origin),
    // // Call the placement policy once and reinject the new start time.
    // PlacementPolicy,
}

#[derive(Debug)]
pub enum Origin {
    FromCache,
    FromDisk(String),
}

/// Core unit of the simulation.
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
    fn run(mut self) -> Result<(), SimError> {
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
                    let (then, ev) = self.stack.submit(self.now, access)?;
                    self.events.insert(then, ev);
                }
                Event::Finished(when_issued, access, device) => {
                    self.stack.finish(&device);
                    if access.is_read() && self.stack.cache.contains(access.block()).is_none() {
                        self.stack.cache.insert(access.block().to_owned());
                    }
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
        );
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum SimError {
    #[error("Could not open or read configuration file: {source}")]
    CouldNotOpenConfig {
        #[from]
        source: std::io::Error,
    },
    #[error("Encountered fatal storage error: {source}")]
    StorageError {
        #[from]
        source: StorageError,
    },
    #[error("Error in configuration: {source}")]
    ConfigurationError {
        #[from]
        source: toml::de::Error,
    },
    #[error("An error occured.")]
    Generic,
}

#[derive(Parser, Debug)]
struct SimCli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(about = "List all available devices.")]
    Devices,
    #[command(about = "List all available applications.")]
    Applications,
    #[command(about = "Run a storage stack simulation.")]
    Sim {
        #[arg(id = "CONFIG_PATH")]
        config: PathBuf,
    },
}

fn main() -> Result<(), SimError> {
    let args = SimCli::parse();

    match args.cmd {
        Commands::Devices => {
            // Print out all devices
            println!("Available devices:\n");
            for dev in Device::iter() {
                println!(
                    "\t{dev:?} (Read: {} ns, Write: {} ns)",
                    dev.read().as_nanos(),
                    dev.write().as_nanos()
                );
            }
            Ok(())
        }
        Commands::Applications => {
            println!("Available Applications:\n");
            for app in App::iter() {
                println!("\t{app:?}");
            }
            Ok(())
        }
        Commands::Sim { config } => {
            let mut file = std::fs::OpenOptions::new().read(true).open(config)?;
            let mut content = String::new();
            file.read_to_string(&mut content)?;
            let config: config::Config = toml::from_str(&content)?;

            let sim: PolicySimulator<(), (), ZipfApp> = PolicySimulator {
                stack: StorageStack {
                    blocks: [].into(),
                    devices: config.devices(),
                    state: (),
                    policy: (),
                    cache: Box::new(cache::Fifo::new(24, Device::DRAM)),
                },
                application: config.app.build(),
                now: std::time::UNIX_EPOCH,
                events: BTreeMap::new(),
                rng: rand::rngs::StdRng::seed_from_u64(12345),
            };
            sim.run()
        }
    }
}
