mod buffer;
mod storage;

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

pub use self::buffer::buffer_pool_manager::BufferPoolManager;
pub use self::buffer::clock_replacer::ClockReplacer;
pub use self::storage::disk::disk_manager;
pub use self::storage::page::hash_table_header_page;
pub use self::storage::page::Page;
use std::sync::atomic::AtomicU32;

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

const HEADER_PAGE_SIZE: usize = 16;
const PAGE_SIZE: usize = 4096;
const INVALID_PAGE_ID: PageId = 0;

type FrameId = u32;
type PageId = u32; // modify it will break `HashTableHeaderPage`
type AtomicPageId = AtomicU32;
type EpochId = u64;
type LogSequenceNum = u32; // modify it will break `HashTableHeaderPage`
