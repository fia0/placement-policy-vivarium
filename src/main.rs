#![feature(btree_cursors)]
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
    process::ExitCode,
    time::{Duration, SystemTime},
};

use application::Application;
use clap::{Parser, Subcommand};
use crossbeam::channel::Sender;
use indicatif::HumanBytes;
use placement::{PlacementMsg, PlacementPolicy};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use result_csv::ResMsg;
use storage_stack::{StorageError, StorageMsg, StorageStack};
use strum::IntoEnumIterator;
use thiserror::Error;

use crate::{
    cache::CacheMsg,
    config::App,
    storage_stack::{load_devices, Device, DeviceSer},
};

mod application;
mod cache;
mod config;
mod placement;
mod result_csv;
mod storage_stack;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct Block(usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Access {
    Read(Block),
    Write(Block),
}

impl Access {
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

#[derive(Debug, PartialEq)]
pub enum Event {
    Cache(CacheMsg),
    Storage(StorageMsg),
    Application(Access),
    PlacementPolicy(PlacementMsg),
    Terminate,
}

/// Core unit of the simulation.
pub struct PolicySimulator<S> {
    stack: StorageStack<S>,
    pub policy: Box<dyn PlacementPolicy>,
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

impl<S> PolicySimulator<S> {
    /// Distribute initial blocks in the storage stack. This is done entirely
    /// randomly with a fixed seed.
    fn prepare(&mut self) {
        'outer: for block in self.application.init() {
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
                    continue 'outer;
                }
            }
            panic!("No space available")
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

        // Startup migration policy
        for ev in self
            .policy
            .init(&self.stack.devices, &self.stack.blocks, self.now)
        {
            self.insert_event(ev.0, ev.1)
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
                Event::PlacementPolicy(msg) => self.policy.update(
                    msg,
                    &mut self.stack.devices,
                    &self.stack.blocks,
                    self.now,
                    &mut self.results_td.1,
                ),
                Event::Terminate => break,
            };
            for (pit, ev) in events.collect::<Vec<_>>() {
                self.insert_event(pit, ev);
            }
        }

        // Clear cache
        for (then, ev) in self.stack.cache.clear(self.now) {
            self.insert_event(then, ev);
        }

        // Wait until cache is flushed
        for (k, v) in self.events.iter().rev() {
            match v {
                Event::PlacementPolicy(_) => {}
                _ => {
                    self.now = *k;
                    break;
                }
            };
        }

        {
            let total_runtime = self.now.duration_since(std::time::UNIX_EPOCH).unwrap();
            self.results_td
                .1
                .send(ResMsg::Simulator { total_runtime })
                .unwrap();
            let mut map = HashMap::new();
            std::mem::swap(&mut self.stack.devices, &mut map);
            self.results_td
                .1
                .send(ResMsg::Device { map, total_runtime })
                .unwrap();
            self.results_td.1.send(ResMsg::Done).unwrap();
            self.results_td.0.join().unwrap()?;
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
    #[error("Custom device \"{0}\" was not found in given path.")]
    MissingCustomDevice(String),
    #[error("An error occured: {0}.")]
    Generic(String),
    #[error("An error occured: {source}")]
    Internal {
        #[from]
        source: Box<dyn std::error::Error>,
    },
}

#[derive(Parser, Debug)]
struct SimCli {
    #[command(subcommand)]
    cmd: Commands,
    #[arg(short, long, default_value_t = String::from("./additional_devices"))]
    add_device_path: String,
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

fn main() -> ExitCode {
    if let Err(e) = faux_main() {
        eprintln!("{}", e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn faux_main() -> Result<(), SimError> {
    let args = SimCli::parse();

    match args.cmd {
        Commands::Devices => {
            // Print out all devices
            println!("Available devices:\n");
            for dev in DeviceSer::iter() {
                println!("\t{dev:?}",);
            }
            for (id, dev) in load_devices(&args.add_device_path)?.iter() {
                println!(
                    "\t{id} (block sizes: {:?})",
                    dev.keys()
                        .map(|b| format!("{}", HumanBytes(*b)))
                        .collect::<Vec<_>>()
                )
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
            let custom_devices = load_devices(&args.add_device_path)?;
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

            let sim: PolicySimulator<()> = PolicySimulator {
                stack: StorageStack {
                    blocks: [].into(),
                    devices: config.devices(&custom_devices)?,
                    state: (),
                    cache: config.cache(&custom_devices)?,
                    blocks_on_hold: Default::default(),
                },
                policy: config.placement.build(),
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
