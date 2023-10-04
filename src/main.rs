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
    collections::{BTreeMap, HashMap},
    io::Read,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use application::Application;
use clap::{Parser, Subcommand};
use crossbeam::channel::Sender;
use rand::{prelude::Distribution, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

use result_csv::ResMsg;
use storage_stack::{StorageError, StorageMsg, StorageStack};
use strum::IntoEnumIterator;
use thiserror::Error;
use zipf::ZipfDistribution;

use crate::{cache::CacheMsg, config::App, storage_stack::Device};

mod application;
mod cache;
mod config;
mod result_csv;
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
    Storage(StorageMsg),
    Application(Access),
}

/// Core unit of the simulation.
pub struct PolicySimulator<S, P> {
    stack: StorageStack<S, P>,
    application: Box<dyn Application>,
    now: SystemTime,
    // Ordered Map, system time is priority.
    events: BTreeMap<SystemTime, Event>,
    rng: StdRng,
    results_td: (
        std::thread::JoinHandle<Result<(), std::io::Error>>,
        Sender<ResMsg>,
    ),
}

impl<S, P> PolicySimulator<S, P> {
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
        let range = self.events.range(pit..);
        // Avoid collision
        let mut off = 0;
        for p in range {
            let diff = p.0.duration_since(pit).unwrap().as_nanos();
            if off < diff {
                break;
            }
            off += 1;
        }
        self.events
            .insert(pit + Duration::from_nanos(off as u64), ev);
    }

    /// Execute the main event digestion.
    fn run(mut self) -> Result<(), SimError> {
        self.prepare();
        // Start the application
        for (time, ev) in self
            .application
            .start(self.now)
            .collect::<Vec<_>>()
            .into_iter()
        {
            self.insert_event(time, ev)
        }
        while let Some((then, event)) = self.events.pop_first() {
            // Step forward to the current timestamp
            self.now = then;
            let events = match event {
                Event::Cache(msg) => self.stack.cache.process(msg, self.now),
                Event::Storage(msg) => self.stack.process(msg, self.now)?,
                Event::Application(access) => {
                    self.application
                        .done(access, self.now, &mut self.results_td.1)
                }
            };
            for (pit, ev) in events.collect::<Vec<_>>() {
                self.insert_event(pit, ev);
            }
        }

        // Clear cache
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
\t\tMaximum latency: {}us,
\t\tIdle time: {}us",
                dev.total_req,
                dev.total_q
                    .as_micros()
                    .checked_div(dev.total_req as u128)
                    .unwrap_or(0),
                dev.max_q.as_micros(),
                dev.idle_time.as_micros()
            )
        }

        self.results_td.1.send(ResMsg::Done).unwrap();
        self.results_td.0.join().unwrap()?;

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

            // append suffix to avoid overwriting data
            let mut cur = 0;
            let mut results = config
                .results
                .path
                .clone()
                .unwrap_or_else(|| PathBuf::from("./results"));
            let last = results
                .file_name()
                .unwrap_or_else(|| &std::ffi::OsStr::new("results"))
                .to_str()
                .unwrap_or_else(|| "results")
                .to_string();
            loop {
                if !results.exists() {
                    break;
                }
                let mut n = last.clone();
                n.push_str(&format!("_{}", cur));
                results.set_file_name(n);
                cur += 1;
            }
            std::fs::create_dir_all(&results).unwrap();

            let sim: PolicySimulator<(), ()> = PolicySimulator {
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
                rng: rand::rngs::StdRng::seed_from_u64(1234),
                results_td: result_csv::ResultCollector::new(results)
                    .map(|(coll, tx)| (std::thread::spawn(|| coll.main()), tx))?,
            };
            sim.run()
        }
    }
}
