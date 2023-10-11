const BLOCK_ALIGNMENT: usize = 4096;

use byte_unit::Byte;
use clap::Parser;
use colored::*;
use indicatif::HumanBytes;
use rand::prelude::*;
use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::PathBuf,
};

#[derive(Parser)]
pub struct Options {
    device_path: PathBuf,
    #[arg(short, long, default_value_t = String::from("1GiB"))]
    size: String,
    #[arg(short, long, default_values_t = vec![String::from("4KiB"),String::from("16KiB"),String::from("256KiB"),String::from("1MiB"),String::from("4MiB"),String::from("16MiB")])]
    block_sizes: Vec<String>,
    #[arg(short, long, default_value_t = String::from("./result.csv"))]
    result_path: String,
}

fn main() -> Result<(), std::io::Error> {
    let opts = Options::parse();

    // let _ = std::fs::remove_file(opts.device_path);

    assert!(!std::path::Path::new(&opts.result_path).exists());
    let mut results = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(opts.result_path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(opts.device_path)?;
    let size = Byte::from_str(opts.size)
        .map(|b| b.get_bytes())
        .unwrap_or(0);
    file.set_len(size as u64)?;

    results.write_fmt(format_args!("block_size,blocks,avg_latency_us\n"))?;
    for (op, block_size) in opts
        .block_sizes
        .iter()
        .map(|written| Byte::from_str(written).unwrap().get_bytes())
        .flat_map(|bs| {
            [
                (Mode::SequentialWrite, bs),
                (Mode::SequentialRead, bs),
                (Mode::RandomWrite, bs),
                (Mode::RandomRead, bs),
            ]
        })
    {
        let blocks = size / block_size;

        println!(
            "{}: Running benchmark with {} and {}",
            "Perpared".bold(),
            format!("{}", HumanBytes(block_size as u64)).green(),
            format!("{op}").bright_cyan()
        );
        let end = run(
            &mut file,
            op,
            blocks.try_into().unwrap(),
            block_size.try_into().unwrap(),
        )?;
        let bw = size as f32 / 1024. / 1024. / end.as_secs_f32();
        println!("{}: {op}: {} MiB/s", "Achieved".bold(), bw);
        results.write_fmt(format_args!(
            "{},{},{}\n",
            block_size,
            blocks,
            end.as_micros() / blocks
        ))?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    RandomWrite,
    RandomRead,
    SequentialWrite,
    SequentialRead,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::RandomWrite => f.write_str("Random Write"),
            Mode::RandomRead => f.write_str("Random Read"),
            Mode::SequentialWrite => f.write_str("Sequential Write"),
            Mode::SequentialRead => f.write_str("Sequential Read"),
        }
    }
}

fn run(
    map: &mut std::fs::File,
    mode: Mode,
    blocks: u64,
    block_size: usize,
) -> Result<std::time::Duration, std::io::Error> {
    let buf_layout =
        unsafe { std::alloc::Layout::from_size_align_unchecked(block_size, BLOCK_ALIGNMENT) };
    let buf: *mut [u8] = unsafe {
        std::ptr::slice_from_raw_parts_mut(std::alloc::alloc_zeroed(buf_layout), block_size)
    };

    let mut offsets = (0..blocks)
        .map(|x| x * block_size as u64)
        .collect::<Vec<_>>();
    match mode {
        Mode::RandomWrite | Mode::RandomRead => {
            let mut rng = rand::thread_rng();
            offsets.shuffle(&mut rng);
        }
        Mode::SequentialWrite | Mode::SequentialRead => {}
    }

    let now = std::time::Instant::now();
    unsafe {
        for n in offsets.iter() {
            match mode {
                Mode::RandomWrite | Mode::SequentialWrite => {
                    assert_eq!(map.write_at(&*buf, *n)?, block_size);
                }
                Mode::RandomRead | Mode::SequentialRead => {
                    assert_eq!(map.read_at(&mut *buf, *n)?, block_size);
                }
            }
        }
    }
    Ok(now.elapsed())
}
