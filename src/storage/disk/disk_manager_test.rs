use crate::disk_manager::DiskManager;
use crate::errors::Result;
use crate::{default_logger, PAGE_SIZE};
use std::fs::remove_file;

#[test]
fn read_write_page_test() -> Result<()> {
    let logger = default_logger();

    let mut buf = [0u8; PAGE_SIZE];
    let mut data = [0u8; PAGE_SIZE];

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
    assert_eq!(data.to_vec(), buf.to_vec());

    buf = [0u8; PAGE_SIZE];
    dm.write_page(5, &data)?;
    dm.read_page(5, &mut buf)?;

    assert_eq!(data.to_vec(), buf.to_vec());

    remove_file(filename)?;

    Ok(())
}

#[test]
fn read_write_log_test() -> Result<()> {
    let logger = default_logger();

    let mut buf = [0u8; 16];
    let mut data = [0u8; 16];

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
