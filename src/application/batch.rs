use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crossbeam::channel::Sender;
use duration_str::deserialize_duration;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{distributions::Uniform, prelude::Distribution, rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use zipf::ZipfDistribution;

use crate::{
    result_csv::{OpsInfo, ResMsg},
    Access, Block, Event,
};

use super::Application;

pub struct RandomAccessSequence<'a, R> {
    rng: &'a mut R,
    dist: &'a mut Dist,
    rw: f64,
}

impl<'a, R: Rng> RandomAccessSequence<'a, R> {
    pub fn new(rng: &'a mut R, dist: &'a mut Dist, rw: f64) -> Self {
        Self { rng, dist, rw }
    }
}

impl<'a, R: Rng> Iterator for RandomAccessSequence<'a, R> {
    type Item = Access;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Access::generate(self.rw, self.dist, self.rng))
    }
}

impl Access {
    pub fn generate<R>(rw: f64, dist: &mut Dist, rng: &mut R) -> Self
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

#[derive(Deserialize, Debug, Default)]
pub struct BatchConfig {
    pub size: usize,
    pub rw: f64,
    pub iteration: usize,
    /// A number of requests to submit at once. All requests have to be finished
    /// before a new batch can be issued.
    pub batch: usize,
    pub pattern: DistConfig,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: Duration,
}

#[derive(Deserialize, Debug)]
pub enum DistConfig {
    Zipf { theta: f64, seed: u64 },
    Uniform { seed: u64 },
    Sequential,
}

impl DistConfig {
    pub fn build(&self, size: usize) -> Dist {
        match self {
            DistConfig::Zipf { theta, .. } => {
                Dist::Zipf(ZipfDistribution::new(size, *theta).unwrap())
            }
            DistConfig::Uniform { seed: _ } => Dist::Uniform(Uniform::new(1, size)),
            DistConfig::Sequential => Dist::Sequential,
        }
    }

    pub fn seed(&self) -> Option<u64> {
        match self {
            DistConfig::Zipf { theta: _, seed } => Some(*seed),
            DistConfig::Uniform { seed } => Some(*seed),
            DistConfig::Sequential => None,
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        todo!()
    }
}

/// Batch-oriented application with configurable access pattern.
pub struct BatchApp {
    size: usize,
    dist: Dist,
    rng: StdRng,
    current_reqs: HashMap<Access, (SystemTime, usize)>,
    batch: usize,
    interval: Duration,
    rw: f64,
    write_latency: Vec<Duration>,
    read_latency: Vec<Duration>,
    iteration: usize,
    cur_iteration: usize,
    // Spinner
    spinner: ProgressBar,
}

/// A helper since the distribution trait from rand can't be made into a trait object.
pub enum Dist {
    Zipf(ZipfDistribution),
    Uniform(Uniform<usize>),
    Sequential,
}

impl Dist {
    pub fn sample<R: Rng>(&self, rng: &mut R) -> usize {
        match self {
            Dist::Zipf(zipf) => zipf.sample(rng),
            Dist::Uniform(d) => d.sample(rng),
            Dist::Sequential => unreachable!(),
        }
    }
}

impl BatchApp {
    pub fn new(config: &BatchConfig) -> Self {
        assert!(config.size > 0);
        assert!(config.iteration > 0);
        Self {
            size: config.size,
            dist: config.pattern.build(config.size),
            current_reqs: HashMap::new(),
            rng: StdRng::seed_from_u64(config.pattern.seed().unwrap_or(0)),
            interval: config.interval,
            rw: config.rw,
            batch: config.batch,
            write_latency: vec![],
            read_latency: vec![],
            iteration: config.iteration,
            cur_iteration: 0,
            spinner: ProgressBar::new(config.iteration.try_into().unwrap()).with_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}]{wide_bar}{pos:>7}/{len}|ETA in: {eta}|{per_sec}",
                )
                .unwrap(),
            ),
        }
    }
}

impl Application for BatchApp {
    fn init(&self) -> Box<dyn Iterator<Item = Block>> {
        Box::new((1..=self.size).map(|num| Block(num)))
    }

    fn start(&mut self, now: SystemTime) -> Box<dyn Iterator<Item = (SystemTime, Event)> + '_> {
        let evs = RandomAccessSequence::new(&mut self.rng, &mut self.dist, self.rw)
            .take(self.batch)
            .collect::<Vec<_>>();
        for ev in evs.iter() {
            match self.current_reqs.entry(ev.clone()) {
                std::collections::hash_map::Entry::Occupied(mut o) => o.get_mut().1 += 1,
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert((now, 1));
                }
            }
        }
        Box::new(evs.into_iter().enumerate().map(move |(off, a)| {
            (
                now + Duration::from_nanos(off as u64),
                match a {
                    Access::Read(b) => Event::Cache(crate::cache::CacheMsg::Get(b)),
                    Access::Write(b) => Event::Cache(crate::cache::CacheMsg::Put(b)),
                },
            )
        }))
    }

    fn done(
        &mut self,
        access: Access,
        now: SystemTime,
        tx: &mut Sender<ResMsg>,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)> + '_> {
        let entry = self.current_reqs.get_mut(&access).unwrap();
        let when_issued = entry.0;
        entry.1 -= 1;
        if entry.1 == 0 {
            let _ = self.current_reqs.remove(&access);
        }
        let lat = match access {
            Access::Read(_) => &mut self.read_latency,
            Access::Write(_) => &mut self.write_latency,
        };
        lat.push(now.duration_since(when_issued).expect("Negative Time"));

        if self.current_reqs.len() == 0 && self.cur_iteration + 1 < self.iteration {
            // END OF BATCH
            // TODO: Call Policy now, or do parallel messages (queue) to which a
            // policy can interject? Take oracle from Haura directly?
            let mut writes = Vec::with_capacity(self.batch);
            std::mem::swap(&mut self.write_latency, &mut writes);
            let mut reads = Vec::with_capacity(self.batch);
            std::mem::swap(&mut self.read_latency, &mut reads);
            tx.send(ResMsg::Application {
                now,
                interval: self.interval,
                writes: OpsInfo { all: writes },
                reads: OpsInfo { all: reads },
            })
            .unwrap();
            // println!(
            //     "({}) Write: Average {}us, Max {}us",
            //     self.cur_iteration,
            //     batch_writes.clone().map(|d| d.as_micros()).sum::<u128>() / self.batch as u128,
            //     batch_writes.map(|d| d.as_micros()).max().unwrap_or(0)
            // );
            // println!(
            //     "({}) Read: Average {}us, Max {}us",
            //     self.cur_iteration,
            //     batch_reads.clone().map(|d| d.as_micros()).sum::<u128>() / self.batch as u128,
            //     batch_reads.map(|d| d.as_micros()).max().unwrap_or(0)
            // );
            self.spinner.inc(1);
            self.cur_iteration += 1;
            // Immediately start the next batch.
            self.start(now + self.interval)
        } else {
            if self.current_reqs.len() == 0 {
                self.spinner.finish();
                println!("Application finished.");
                return Box::new([(now, Event::Terminate)].into_iter());
            }
            Box::new([].into_iter())
        }
    }
}
