use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender};

use crate::storage_stack::DeviceState;

/// This module collects data from different parts of the program and creates
/// multiple csv files in the result directory. The results contain information
/// about the storage stack, the application, and the simulator itself.

pub enum ResMsg {
    Application { writes: OpsInfo, reads: OpsInfo },
    Device { state: DeviceState },
    Simulator { total_runtime: Duration },
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

        while let Ok(msg) = self.rx.recv() {
            match msg {
                ResMsg::Application { writes, reads } => {
                    for mut vals in [writes, reads].into_iter() {
                        vals.all.sort();
                        let total = vals.all.len() as u128;
                        let avg = vals.all.iter().map(|d| d.as_micros()).sum::<u128>() / total;
                        let max = vals.all.iter().map(|d| d.as_micros()).max().unwrap_or(0);
                        self.application.write_fmt(format_args!(
                            "{},{},{},{},{},{},",
                            total,
                            avg,
                            max,
                            vals.all.percentile(0.90).as_micros(),
                            vals.all.percentile(0.95).as_micros(),
                            vals.all.percentile(0.99).as_micros(),
                        ))?;
                    }
                    self.application.write(b"\n")?;
                }
                ResMsg::Device { state } => todo!(),
                ResMsg::Simulator { total_runtime } => todo!(),
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
    fn percentile(&self, p: f32) -> &T;
}

impl<T> Percentile<T> for Vec<T> {
    fn percentile(&self, p: f32) -> &T {
        // should be sufficient for the determination of this percentile
        let cut_off = (self.len() as f32 * p).ceil() as usize;
        &self[cut_off]
    }
}
