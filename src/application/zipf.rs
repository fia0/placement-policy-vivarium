use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crossbeam::channel::Sender;
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use zipf::ZipfDistribution;

use crate::{
    result_csv::{OpsInfo, ResMsg},
    Access, Block, Event, RandomAccessSequence,
};

use super::Application;

#[derive(Deserialize, Debug, Default)]
pub struct ZipfConfig {
    pub seed: u64,
    pub size: usize,
    pub rw: f64,
    pub theta: f64,
    pub iteration: usize,
    /// A number of requests to submit at once. All requests have to be finished
    /// before a new batch can be issued.
    pub batch: usize,
}

/// Batch-oriented application with zipfian access pattern.
pub struct ZipfApp {
    size: usize,
    dist: ZipfDistribution,
    rng: StdRng,
    current_reqs: HashMap<Access, (SystemTime, usize)>,
    batch: usize,
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
            current_reqs: HashMap::new(),
            rng: StdRng::seed_from_u64(config.seed),
            rw: config.rw,
            batch: config.batch,
            write_latency: vec![],
            read_latency: vec![],
            iteration: config.iteration,
            cur_iteration: 0,
        }
    }
}

impl Application for ZipfApp {
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

        if self.current_reqs.len() == 0 && self.cur_iteration < self.iteration {
            // END OF BATCH
            // TODO: Call Policy now, or do parallel messages (queue) to which a
            // policy can interject? Take oracle from Haura directly?
            let mut writes = Vec::with_capacity(self.batch);
            std::mem::swap(&mut self.write_latency, &mut writes);
            let mut reads = Vec::with_capacity(self.batch);
            std::mem::swap(&mut self.read_latency, &mut reads);
            tx.send(ResMsg::Application {
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
            self.cur_iteration += 1;
            {
                use std::io::Write;
                write!(std::io::stdout(), ".").unwrap();
                let _ = std::io::stdout().flush();
            }
            // Immediately start the next batch.
            self.start(now)
        } else {
            if self.current_reqs.len() == 0 {
                println!("Application finished.");
            }
            Box::new([].into_iter())
        }
    }
}
