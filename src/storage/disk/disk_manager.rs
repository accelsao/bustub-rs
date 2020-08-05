use crate::errors::Result;
use crate::{AtomicPageId, PageId, PAGE_SIZE};
use slog::Logger;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::Ordering;

// DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
// writing of pages to and from disk, providing a logical file layer within the context of a database management system.
pub struct DiskManager {
    // filename: String,
    next_page_id: AtomicPageId,
    num_writes: u32,
    // num_flushes: u32,
    db_file: File,
    log_file: File,
    // flush_log: bool,
    logger: Logger,
}

impl DiskManager {
    // Creates a new disk manager that writes to the specified database file.
    pub fn new(filename: &str, logger: &Logger) -> Result<Self> {
        if let Some(n) = filename.rfind('.') {
            let log_name = filename[..n].to_string() + ".log";
            debug!(logger, "log_name: {:?}", log_name);

            let log_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(log_name)?;

            let db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(filename)?;

            Ok(Self {
                next_page_id: AtomicPageId::new(0),
                num_writes: 0,
                db_file,
                log_file,
                logger: logger.clone(),
            })
        } else {
            bail!("wrong file format")
        }
    }

    // Write the contents of the specified page into disk file
    pub fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.num_writes += 1;
        debug!(self.logger, "num_writes: {:?}", self.num_writes);
        self.db_file.seek(SeekFrom::Start(offset))?;
        self.db_file.write_all(page_data)?;
        self.db_file.flush()?;
        Ok(())
    }

    // Read the contents of the specified page into the given memory area
    pub fn read_page(&mut self, page_id: PageId, page_data: &mut [u8]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;

        debug!(
            self.logger,
            "offset: {}, file_len: {}",
            offset,
            self.db_file.metadata()?.len()
        );

        if offset > self.db_file.metadata()?.len() {
            bail!("I/O error reading past end of file")
        } else {
            // set read cursor to offset
            self.db_file.seek(SeekFrom::Start(offset))?;
            let n = self.db_file.read(page_data)?;
            debug!(self.logger, "[read_page]n: {:?}", n);
            if n < PAGE_SIZE {
                warn!(
                    self.logger,
                    "Read less than a page, n: {}, page_size: {}", n, PAGE_SIZE
                );
            }
        }
        Ok(())
    }

    // Write the contents of the log into disk file
    pub fn write_log(&mut self, log_data: &[u8]) -> Result<()> {
        if log_data.is_empty() {
            return Ok(());
        }

        self.log_file.write_all(log_data)?;
        self.log_file.flush()?;

        Ok(())
    }

    pub fn read_log(&mut self, log_data: &mut [u8], offset: u64) -> Result<bool> {
        if offset >= self.log_file.metadata()?.len() {
            return Ok(false);
        }
        let log_size = log_data.len();
        self.log_file.seek(SeekFrom::Start(offset))?;
        let n = self.log_file.read(log_data)?;
        if n < log_size {
            warn!(
                self.logger,
                "Read less than a page, n: {}, log_size: {}", n, log_size
            );
        }
        Ok(true)
    }

    pub fn allocate_page(&mut self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst);
        self.next_page_id.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_manager::DiskManager;
    use crate::errors::Result;
    use crate::{default_logger, PAGE_SIZE};
    use std::fs::remove_file;

    #[test]
    fn read_write_page_test() -> Result<()> {
        let logger = default_logger();

        let mut buf = vec![0u8; PAGE_SIZE];
        let mut data = vec![0u8; PAGE_SIZE];

        let filename = "target/test_read_write_page.db";

        let mut dm = DiskManager::new(filename, &logger)?;

        let test_data = b"A test string.";
        data[..test_data.len()].copy_from_slice(test_data);

        // tolerate empty read
        dm.read_page(0, &mut buf)?;
        dm.write_page(0, &data)?;
        debug!(logger, "second read");
        dm.read_page(0, &mut buf)?;
        debug!(
            logger,
            "data: {:?}",
            (data[..20].to_vec(), buf[..20].to_vec())
        );
        assert_eq!(data, buf);

        buf = vec![0u8; PAGE_SIZE];
        dm.write_page(5, &data)?;
        dm.read_page(5, &mut buf)?;

        assert_eq!(data, buf);

        remove_file(filename)?;

        Ok(())
    }

    #[test]
    fn read_write_log_test() -> Result<()> {
        let logger = default_logger();

        let mut buf = vec![0u8; 16];
        let mut data = vec![0u8; 16];

        let filename = "target/test_read_write_log.db";

        let mut dm = DiskManager::new(filename, &logger)?;

        let test_data = b"A test string.";
        data[..test_data.len()].copy_from_slice(test_data);

        // tolerate empty read
        dm.read_log(&mut buf, 0u64)?;

        dm.write_log(&data)?;

        dm.read_log(&mut buf, 0u64)?;

        assert_eq!(data, buf);

        remove_file(filename)?;

        Ok(())
    }
}
