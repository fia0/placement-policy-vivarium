use crate::{Access, Block, Event};
use std::time::SystemTime;

mod zipf;
pub use zipf::{ZipfApp, ZipfConfig};

/// An actor which issues and waits for accesses.
pub trait Application {
    /// An iterator over blocks which should be initially available.
    fn init(&self) -> Box<dyn Iterator<Item = Block>>;
    fn start(&mut self, now: SystemTime) -> Box<dyn Iterator<Item = (SystemTime, Event)> + '_>;
    /// Notify that the given access has finished. Returns the time when the
    /// next operations should start getting issued and future requests ready to be made then.
    fn done(
        &mut self,
        access: Access,
        now: SystemTime,
    ) -> Box<dyn Iterator<Item = (SystemTime, Event)> + '_>;
}
