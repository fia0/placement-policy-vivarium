use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender};
use human_repr::HumanDuration;

use crate::storage_stack::DeviceState;

/// This module collects data from different parts of the program and creates
/// multiple csv files in the result directory. The results contain information
/// about the storage stack, the application, and the simulator itself.

pub enum ResMsg {
    Application {
        writes: OpsInfo,
        reads: OpsInfo,
    },
    Device {
        map: HashMap<String, DeviceState>,
        total_runtime: Duration,
    },
    Simulator {
        total_runtime: Duration,
    },
    Done,
}

pub struct OpsInfo {
    pub all: Vec<Duration>,
}

pub struct ResultCollector {
    rx: Receiver<ResMsg>,
    application: BufWriter<File>,
    devices: BufWriter<File>,
    sim: BufWriter<File>,
}

impl ResultCollector {
    pub fn new(path: PathBuf) -> Result<(Self, Sender<ResMsg>), std::io::Error> {
        let application = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(path.join("app.csv"))?,
        );
        let devices = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(path.join("devices.csv"))?,
        );
        let sim = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(path.join("simulator.csv"))?,
        );
        let (tx, rx) = crossbeam::channel::unbounded();
        Ok((
            Self {
                rx,
                application,
                devices,
                sim,
            },
            tx,
        ))
    }

    pub fn main(mut self) -> Result<(), std::io::Error> {
        for op in ["write", "read"].into_iter() {
            self.application.write_fmt(format_args!(
                "{op}_total,{op}_avg,{op}_max,{op}_p90,{op}_p95,{op}_p99,",
            ))?;
        }
        self.application.write(b"\n")?;
        self.devices.write_fmt(format_args!(
            "id,total_requests,avg_latency_ns,max_latency_ns,idle_percentage\n"
        ))?;

        while let Ok(msg) = self.rx.recv() {
            match msg {
                ResMsg::Application { writes, reads } => {
                    for mut vals in [writes, reads].into_iter() {
                        vals.all.sort();
                        let total = vals.all.len() as u128;
                        let avg = vals
                            .all
                            .iter()
                            .map(|d| d.as_micros())
                            .sum::<u128>()
                            .checked_div(total)
                            .unwrap_or(0);
                        let max = vals.all.iter().map(|d| d.as_micros()).max().unwrap_or(0);
                        self.application.write_fmt(format_args!(
                            "{},{},{},{},{},{},",
                            total,
                            avg,
                            max,
                            vals.all
                                .percentile(0.90)
                                .unwrap_or(&Duration::ZERO)
                                .as_micros(),
                            vals.all
                                .percentile(0.95)
                                .unwrap_or(&Duration::ZERO)
                                .as_micros(),
                            vals.all
                                .percentile(0.99)
                                .unwrap_or(&Duration::ZERO)
                                .as_micros(),
                        ))?;
                    }
                    self.application.write(b"\n")?;
                }
                ResMsg::Device { map, total_runtime } => {
                    println!("Device stats:");
                    for (id, dev) in map.iter() {
                        let total = dev.total_req;
                        let avg = dev.total_q.div_f32(dev.total_req as f32);
                        let max = dev.max_q;
                        let free_blocks = dev.free;
                        let total_size = dev.total;
                        let idle = (dev.idle_time.as_micros() / (total_runtime.as_micros() / 10000))
                            as f32
                            / 100f32;
                        self.devices.write_fmt(format_args!(
                            "{id},{total},{},{},{idle}\n",
                            avg.as_nanos(),
                            max.as_nanos(),
                        ))?;
                        println!(
                            "\t{id}:
\t\tTotal requests: {total}
\t\tAverage latency: {}
\t\tMaximum latency: {}
\t\tUsed: {}
\t\tSize: {total_size}
\t\tIdle time: {idle}%",
                            avg.human_duration(),
                            max.human_duration(),
                            total_size - free_blocks,
                        )
                    }
                }
                ResMsg::Simulator { total_runtime } => {
                    println!("Runtime: {}", total_runtime.human_duration());
                    self.sim
                        .write_fmt(format_args!("{}s\n", total_runtime.as_secs_f32()))?;
                }
                ResMsg::Done => break,
            }
        }
        self.application.flush()?;
        self.devices.flush()?;
        self.sim.flush()
    }
}

trait Percentile<T> {
    /// This function assuems that the given Vector is sorted.
    fn percentile(&self, p: f32) -> Option<&T>;
}

impl<T> Percentile<T> for Vec<T> {
    fn percentile(&self, p: f32) -> Option<&T> {
        // should be sufficient for the determination of this percentile
        let cut_off = (self.len() as f32 * p).ceil() as usize;
        self.get(cut_off)
    }
}
