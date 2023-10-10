/// This test should eventually measure individually latencies.
const PATH: &str = "test.mmap";
const SIZE: usize = 512 * 1024 * 1024;
const BLOCK_SIZE: usize = 4 * 1024 * 1024;
const BLOCKS: usize = SIZE / BLOCK_SIZE;

use rand::prelude::*;
use std::os::unix::fs::{FileExt, OpenOptionsExt};

fn main() {
    let _ = std::fs::remove_file(PATH);

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(PATH)
        .unwrap();
    file.set_len(SIZE as u64).unwrap();

    // let mut file = MmapFileMut::create_with_options(
    //     PATH,
    //     fmmap::sync::Options::new()
    //         .len(SIZE)
    //         .custom_flags(libc::O_DIRECT | libc::MAP_SHARED),
    // )
    // .unwrap();
    // file.truncate(SIZE as u64).unwrap();

    let end = run(&mut file, Mode::SequentialWrite);
    println!(
        "Achieved Sequential Write: {} MiB/s",
        SIZE as f32 / 1024. / 1024. / end.as_secs_f32()
    );
    let end = run(&mut file, Mode::RandomWrite);
    println!(
        "Achieved Random Write: {} MiB/s",
        SIZE as f32 / 1024. / 1024. / end.as_secs_f32()
    );

    let end = run(&mut file, Mode::SequentialRead);
    println!(
        "Achieved Sequential Read: {} MiB/s",
        SIZE as f32 / 1024. / 1024. / end.as_secs_f32()
    );
    let end = run(&mut file, Mode::RandomRead);
    println!(
        "Achieved Random Read: {} MiB/s",
        SIZE as f32 / 1024. / 1024. / end.as_secs_f32()
    );
}

enum Mode {
    RandomWrite,
    RandomRead,
    SequentialWrite,
    SequentialRead,
}

fn run(map: &mut std::fs::File, mode: Mode) -> std::time::Duration {
    let buf_layout = unsafe { std::alloc::Layout::from_size_align_unchecked(BLOCK_SIZE, 4096) };
    let buf: *mut [u8] = unsafe {
        std::ptr::slice_from_raw_parts_mut(std::alloc::alloc_zeroed(buf_layout), BLOCK_SIZE)
    };

    let mut offsets = (0..BLOCKS as u64)
        .map(|x| x * BLOCK_SIZE as u64)
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
                    map.write_at(&*buf, *n);
                }
                Mode::RandomRead | Mode::SequentialRead => {
                    map.read_at(&mut *buf, *n);
                }
            }
            // file.flush().unwrap();
        }
    }
    // file.flush_range().unwrap();
    now.elapsed()
}
