use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use zipf::ZipfDistribution;

use crate::{Access, Block, RandomAccessSequence};

use super::Application;

#[derive(Deserialize)]
pub struct ZipfConfig {
    pub seed: u64,
    pub size: usize,
    pub rw: f64,
    pub theta: f64,
    pub iteration: usize,
}

/// Batch-oriented application with zipfian access pattern.
pub struct ZipfApp {
    size: usize,
    dist: ZipfDistribution,
    rng: StdRng,
    current_reqs: usize,
    rw: f64,
    write_latency: Vec<Duration>,
    read_latency: Vec<Duration>,
    iteration: usize,
    cur_iteration: usize,
}

impl ZipfApp {
    pub fn new(config: &ZipfConfig) -> Self {
        assert!(config.size > 0);
        assert!(config.theta > 0.0);
        assert!(config.iteration > 0);
        Self {
            size: config.size,
            dist: ZipfDistribution::new(config.size, config.theta).unwrap(),
            current_reqs: 0,
            rng: StdRng::seed_from_u64(config.seed),
            rw: config.rw,
            write_latency: vec![],
            read_latency: vec![],
            iteration: config.iteration,
            cur_iteration: 0,
        }
    }
}

/// A number of requests to submit at once. All requests have to be finished
/// before a new batch can be issued.
const BATCH_SIZE: usize = 128;

impl Application for ZipfApp {
    fn init(&self) -> impl Iterator<Item = Block> {
        (1..=self.size).map(|num| Block(num))
    }

    fn start(&mut self) -> impl Iterator<Item = Access> + '_ {
        self.current_reqs += BATCH_SIZE;
        RandomAccessSequence::new(&mut self.rng, &mut self.dist, self.rw)
            .take(BATCH_SIZE)
            .into_iter()
    }

    fn done(
        &mut self,
        access: Access,
        when_issued: SystemTime,
        now: SystemTime,
    ) -> Option<(SystemTime, impl Iterator<Item = Access> + '_)> {
        self.current_reqs -= 1;

        let lat = match access {
            Access::Read(_) => &mut self.read_latency,
            Access::Write(_) => &mut self.write_latency,
        };
        lat.push(now.duration_since(when_issued).expect("Negative Time"));

        if self.current_reqs == 0 && self.cur_iteration < self.iteration {
            // END OF BATCH
            // TODO: Call Policy now, or do parallel messages (queue) to which a
            // policy can interject? Take oracle from Haura directly?
            // FIXME: Use propoer statistics, this is more of debug info
            let batch_writes = self.write_latency.iter().rev().take(BATCH_SIZE);
            println!(
                "Write: Average {}us, Max {}us",
                batch_writes.clone().map(|d| d.as_micros()).sum::<u128>() / BATCH_SIZE as u128,
                batch_writes.map(|d| d.as_micros()).max().unwrap_or(0)
            );
            let batch_reads = self.read_latency.iter().rev().take(BATCH_SIZE);
            println!(
                "Read: Average {}us, Max {}us",
                batch_reads.clone().map(|d| d.as_micros()).sum::<u128>() / BATCH_SIZE as u128,
                batch_reads.map(|d| d.as_micros()).max().unwrap_or(0)
            );
            self.cur_iteration += 1;
            // Immediately start the next batch.
            Some((now, self.start()))
        } else {
            None
        }
    }
}
