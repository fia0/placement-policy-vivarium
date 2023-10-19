const BLOCK_ALIGNMENT: usize = 4096;

use byte_unit::Byte;
use clap::Parser;
use colored::*;
use indicatif::HumanBytes;
use rand::prelude::*;
use std::{
    error::Error,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

/// TODO: Measure PMem with appropriate library
/// TODO: Mutliple writers

#[derive(Parser)]
pub struct Options {
    device_path: PathBuf,
    #[arg(short, long, default_value_t = String::from("1GiB"))]
    size: String,
    #[arg(short, long, default_values_t = vec![String::from("4KiB"),String::from("16KiB"),String::from("256KiB"),String::from("1MiB"),String::from("4MiB"),String::from("16MiB")])]
    block_sizes: Vec<String>,
    #[arg(short = 'd', long, default_value_t = String::from("30s"))]
    sample_duration: String,
    #[arg(short, long, default_value_t = String::from("./result.csv"))]
    result_path: String,
}

fn main() -> ExitCode {
    if let Err(e) = faux_main() {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn faux_main() -> Result<(), Box<dyn Error>> {
    let opts = Options::parse();

    // let _ = std::fs::remove_file(opts.device_path);

    let sample_duration = duration_str::parse(&opts.sample_duration)?;
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
    // file.set_len(size as u64)?;

    results.write_fmt(format_args!(
        "block_size,blocks,avg_latency_us,op,pattern\n"
    ))?;
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
            "Prepared".bold(),
            format!("{}", HumanBytes(block_size as u64)).green(),
            format!("{op}").bright_cyan()
        );
        let (end, blocks) = run(
            &mut file,
            op,
            sample_duration,
            blocks.try_into().unwrap(),
            block_size.try_into().unwrap(),
        )?;
        let bw = (blocks as u128 * block_size) as f32 / 1024. / 1024. / end.as_secs_f32();
        println!("{}: {op}: {} MiB/s", "Achieved".bold(), bw);
        println!("{}: {op}: {}s", "Achieved".bold(), end.as_secs_f32());
        results.write_fmt(format_args!(
            "{},{},{},{},{}\n",
            block_size,
            blocks,
            end.as_micros() / blocks as u128,
            op.as_str_op(),
            op.as_str_pattern()
        ))?;
        std::thread::sleep(std::time::Duration::from_secs(5));
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

impl Mode {
    fn as_str_op(&self) -> &str {
        match self {
            Mode::RandomWrite | Mode::SequentialWrite => "write",
            Mode::RandomRead | Mode::SequentialRead => "read",
        }
    }

    fn as_str_pattern(&self) -> &str {
        match self {
            Mode::RandomWrite | Mode::RandomRead => "random",
            Mode::SequentialRead | Mode::SequentialWrite => "sequential",
        }
    }
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
    run_until: Duration,
    total_blocks: u64,
    block_size: usize,
) -> Result<(std::time::Duration, u64), std::io::Error> {
    let buf_layout =
        unsafe { std::alloc::Layout::from_size_align_unchecked(block_size, BLOCK_ALIGNMENT) };
    let buf: *mut [u8] = unsafe {
        std::ptr::slice_from_raw_parts_mut(std::alloc::alloc_zeroed(buf_layout), block_size)
    };

    let offsets: Box<dyn Iterator<Item = u64>>;
    match mode {
        Mode::RandomWrite | Mode::RandomRead => {
            let rng = rand::rngs::StdRng::seed_from_u64(54321);
            offsets = Box::new(
                rng.sample_iter(rand::distributions::Uniform::new(0, total_blocks))
                    .map(|x| x * block_size as u64),
            )
            // offsets.shuffle(&mut rng);
        }
        Mode::SequentialWrite | Mode::SequentialRead => {
            offsets = Box::new((0..total_blocks).map(|x| x * block_size as u64));
        }
    }

    let mut processed_blocks = 0;
    let now = std::time::Instant::now();
    unsafe {
        for n in offsets {
            match mode {
                Mode::RandomWrite | Mode::SequentialWrite => {
                    assert_eq!(map.write_at(&*buf, n)?, block_size);
                }
                Mode::RandomRead | Mode::SequentialRead => {
                    assert_eq!(map.read_at(&mut *buf, n)?, block_size);
                }
            }
            processed_blocks += 1;
            // FIXME: reduce costs
            // fetching takes around 100ns with comparisons, this might have a
            // rather large influence with 256b acccess taking only 250ns on
            // some NVM this might skew the result.
            if now.elapsed() > run_until {
                break;
            }
        }
    }
    Ok((now.elapsed(), processed_blocks))
}
