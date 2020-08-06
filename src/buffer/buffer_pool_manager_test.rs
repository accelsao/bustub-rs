use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::errors::Result;
use crate::storage::disk::disk_manager::DiskManager;
use crate::{default_logger, PageId, INVALID_PAGE_ID, PAGE_SIZE};
use rand::prelude::StdRng;
use rand::RngCore;
use std::fs::remove_file;

const BUFFER_POOL_SIZE: usize = 10;

#[test]
fn test_buffer_pool_manager() -> Result<()> {
    let logger = default_logger();

    let filename = "target/test_buffer_pool_manager.db";

    let disk_manager = DiskManager::new(filename, &logger)?;
    let mut bpm = BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager, &logger);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    let (page1, page1_id) = bpm.new_page()?;

    assert!(page1.is_some());
    assert_eq!(page1_id, 1);

    let page1 = page1.unwrap();

    let mut rng: StdRng = rand::SeedableRng::seed_from_u64(42);
    let mut random_binary_data = [0u8; PAGE_SIZE];
    rng.fill_bytes(&mut random_binary_data);

    // Scenario: Once we have a page, we should be able to read and write content.
    page1.set_data(&random_binary_data);

    assert_eq!(page1.get_data().to_vec(), random_binary_data.to_vec());

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for i in 2..=BUFFER_POOL_SIZE {
        let (page, page_id) = bpm.new_page()?;
        assert!(page.is_some());
        assert_eq!(page_id, i as PageId);
    }

    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    for _ in BUFFER_POOL_SIZE + 1..=BUFFER_POOL_SIZE * 2 {
        let (page, page_id) = bpm.new_page()?;
        assert!(page.is_none());
        assert_eq!(page_id, INVALID_PAGE_ID);
    }

    // Scenario: After unpinning pages 1-5 and flushing page, data will written on disk.
    for i in 1..=5 {
        assert!(bpm.unpin_page(i, true));
        bpm.flush_page(i)?;
    }

    // Scenario: all the new page wont write back to disk
    for _ in 1..=5 {
        let (page, page_id) = bpm.new_page()?;
        assert!(page.is_some());
        bpm.unpin_page(page_id, false);
    }

    let page = bpm.fetch_page(1)?.unwrap();
    assert_eq!(page.get_data().to_vec(), random_binary_data.to_vec());
    assert!(bpm.unpin_page(1, true));

    // pinning page id 16 - 19, there would still be one page left for reading page 1.
    for i in 16..20 {
        let (page, page_id) = bpm.new_page()?;
        assert!(page.is_some());
        assert_eq!(page_id, i as PageId);
    }

    let page = bpm.fetch_page(1)?.unwrap();
    assert_eq!(page.get_data().to_vec(), random_binary_data.to_vec());

    // unpin1, and new page, fetch 1 will fails
    assert!(bpm.unpin_page(1, true));
    let (page, page_id) = bpm.new_page()?;
    assert!(page.is_some());
    assert_eq!(page_id, 20);
    let page = bpm.fetch_page(1)?;
    assert!(page.is_none());

    remove_file(filename)?;

    Ok(())
}
