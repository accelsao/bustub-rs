mod buffer;
mod storage;

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

pub use self::storage::disk::disk_manager;

pub mod errors {
    pub use anyhow::Error;
    pub use anyhow::Result;
}

pub fn default_logger() -> slog::Logger {
    use slog::Drain;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

const PAGE_SIZE: usize = 4096;

type PageId = u64;
