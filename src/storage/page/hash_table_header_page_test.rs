use crate::errors::Result;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::{default_logger, BufferPoolManager, HEADER_PAGE_SIZE};
use std::fs::remove_file;

#[test]
fn header_page_sample() -> Result<()> {
    let logger = default_logger();
    let filename = "target/header_page_sample.db";

    let disk_manager = DiskManager::new(filename, &logger)?;
    let mut bpm = BufferPoolManager::new(5, disk_manager, &logger);

    let (page, page_id) = bpm.new_page()?;
    let page = page.unwrap();
    let mut page_data = page.get_data();

    // Get the data(PAGE_SIZE), and copy to [u8; 16]
    // Do somethings
    // Copy header(16) back to data(PAGE_SIZE)

    let mut header_page = [0u8; HEADER_PAGE_SIZE];
    header_page.copy_from_slice(&page_data[..HEADER_PAGE_SIZE]);

    let mut header_page = HashTableHeaderPage::from(header_page);

    for i in 0..11 {
        header_page.set_size(i);
        assert_eq!(header_page.get_size(), i);
        header_page.set_page_id(i);
        assert_eq!(header_page.get_page_id(), i);
        header_page.set_lsn(i);
        assert_eq!(header_page.get_lsn(), i);
    }

    for i in 0..10 {
        header_page.add_block_page_id(i);
        assert_eq!(header_page.get_block_page_id(i as usize).unwrap(), &i);
        assert_eq!(header_page.num_blocks() as u32, i + 1);
    }

    let header_page: [u8; HEADER_PAGE_SIZE] = header_page.into();
    page_data[..HEADER_PAGE_SIZE].copy_from_slice(&header_page);

    page.set_data(&page_data);

    bpm.unpin_page(page_id, true);

    let page = bpm.fetch_page(page_id)?.unwrap();
    let reloaded_page_data = page.get_data();

    assert_eq!(reloaded_page_data.to_vec(), page_data.to_vec());

    remove_file(filename)?;
    Ok(())
}
