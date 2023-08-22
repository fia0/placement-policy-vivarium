use rand::Rng;

use crate::{Access, Block, Event, RandomAccessSequence};
use std::time::SystemTime;

mod zipf;
pub use zipf::{ZipfApp, ZipfConfig};

/// An actor which issues and waits for accesses.
pub trait Application {
    /// An iterator over blocks which should be initially available.
    fn init(&self) -> impl Iterator<Item = Block>;
    fn start(&mut self) -> impl Iterator<Item = Access> + '_;
    /// Notify that the given access has finished. Returns the time when the
    /// next operations should start getting issued and future requests ready to be made then.
    fn done(
        &mut self,
        access: Access,
        when_issued: SystemTime,
        now: SystemTime,
    ) -> Option<(SystemTime, impl Iterator<Item = Access> + '_)>;
}
