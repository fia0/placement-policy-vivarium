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
use cache::{Cache, CacheLogic};
use clap::{Parser, Subcommand};
use rand::{prelude::Distribution, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use serde::Deserialize;
use storage_stack::{StorageError, StorageStack};
use strum::{EnumIter, IntoEnumIterator};
use thiserror::Error;
use zipf::ZipfDistribution;

use crate::{cache::CacheMsg, config::App, storage_stack::Device};

mod application;
mod cache;
mod config;
mod storage_stack;

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

// /// An event which is noted to happen sometime in the future.
// #[derive(Debug)]
// pub enum Event {
//     Submit(Access, Issuer),
//     Finished(SystemTime, Access, Origin, Issuer),
//     // // Call the placement policy once and reinject the new start time.
//     // PlacementPolicy,
// }

#[derive(Debug, PartialEq)]
pub enum Event {
    Cache(CacheMsg),
    Storage(Access),
    Application(Block),
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

    /// Insert events into the event queue and avoid any kind of collision.
    fn insert_event(&mut self, pit: SystemTime, ev: Event) {
        if !self.events.contains_key(&pit) {
            self.events.insert(pit, ev);
        } else {
            let mut off = 0;
            loop {
                match self.events.entry(pit + Duration::from_nanos(off)) {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(ev);
                        break;
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
                off += 1;
            }
        }
    }

    /// Execute the main event digestion.
    fn run(mut self) -> Result<(), SimError> {
        self.prepare();
        // Start the application
        for access in self
            .application
            .start()
            .collect::<Vec<Access>>()
            .into_iter()
        {
            self.insert_event(
                self.now,
                match access {
                    Access::Read(b) => Event::Cache(CacheMsg::Get(b)),
                    Access::Write(b) => Event::Cache(CacheMsg::Put(b)),
                },
            )
        }
        while let Some((then, event)) = self.events.pop_first() {
            // Step forward to the current timestamp
            self.now = then;
            let events = match event {
                Event::Cache(msg) => self.stack.cache.process(msg, self.now),
                Event::Storage(msg) => self.stack.process(msg, self.now)?,
                Event::Application(_) => todo!(),
            };
            for (pit, ev) in events.collect::<Vec<_>>() {
                self.insert_event(pit, ev);
            }
        }

        // Clear cache
        println!("Application finished");
        for (then, ev) in self.stack.cache.clear(self.now) {
            self.insert_event(then, ev);
        }

        if let Some((k, _v)) = self.events.last_key_value() {
            self.now = *k;
        }

        println!(
            "Runtime: {}s",
            self.now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        );

        println!("Device stats:");
        for (id, dev) in self.stack.devices.iter() {
            println!(
                "\t{id}:
\t\tTotal requests: {}
\t\tAverage latency: {}us
\t\tMaximum latency: {}us",
                dev.total_req,
                dev.total_q.as_micros() / dev.total_req as u128,
                dev.max_q.as_micros()
            )
        }
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
                    cache: config.cache(),
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
