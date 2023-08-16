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
    collections::{HashMap, VecDeque},
    time::Duration,
};

use rand::{distributions::Standard, prelude::Distribution, rngs::ThreadRng, Rng};
use zipf::ZipfDistribution;

#[derive(Hash)]
pub enum Device {
    // 6 dimms
    OptanePMem,
    OptaneSSD,
    SamsungZSSD,
    MicronTLCSSD,
    GenericHDD,
    DRAM,
}

#[derive(Hash)]
pub struct Block(usize);

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

    /// Number of blocks a single device can at maximum hold.
    fn capacity(&self) -> usize {
        match self {
            // 1 TB max assumed (more is possible i know)
            //                    TB   GB     MB
            Device::OptanePMem => 1 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
            // 1.6 TB max
            //                    GB     MB
            Device::OptaneSSD => 1600 * 1000 / BLOCK_SIZE_IN_MB,
            // 3.2 TB max
            //                      GB     MB
            Device::SamsungZSSD => 3200 * 1000 / BLOCK_SIZE_IN_MB,
            // 30.72 TB max
            //                      GB       MB
            Device::MicronTLCSSD => 30720 * 1000 / BLOCK_SIZE_IN_MB,
            // 30 TB max assumed (there is higher)
            //                    TB    GB     MB
            Device::GenericHDD => 30 * 1024 * 1024 / BLOCK_SIZE_IN_MB,
            // 32 GB max (set limitation due to impl on client)
            //              GB   MB
            Device::DRAM => 32 * 1024 / BLOCK_SIZE_IN_MB,
        }
    }
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
    occupied: usize,
    queue: VecDeque<Action>,
}

pub struct StorageStack<S, P> {
    blocks: HashMap<Block, Device>,
    devices: HashMap<Device, DeviceState>,
    state: S,
    policy: P,
}

impl<S, P> StorageStack<S, P> {
    fn run(&mut self, request: impl Iterator<Item = Access>) {
        todo!()
    }
}

pub struct PolicySimulator<S, P> {
    stack: StorageStack<S, P>,
    dist: ZipfDistribution,
    rng: ThreadRng,
    rw: f64,
}

const BATCH_SIZE: usize = 100;

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
        self.stack.run(reqs);
    }
}

fn main() {
    println!("Hello, world!");
}
