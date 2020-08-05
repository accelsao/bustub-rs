use crate::buffer::clock_replacer::ClockReplacer;
use crate::buffer::replace::Replacer;
use crate::errors::Result;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::Page;
use crate::{FrameId, PageId, INVALID_PAGE_ID, PAGE_SIZE};
use slog::Logger;
use std::collections::{HashMap, LinkedList};

pub struct BufferPoolManager {
    // Mapping of frame to buffer pool pages.
    pages: HashMap<FrameId, Page>,
    // Page table for keeping track of buffer pool pages
    page_table: HashMap<PageId, FrameId>,
    replacer: ClockReplacer,
    free_list: LinkedList<FrameId>,
    #[allow(dead_code)]
    disk_manager: DiskManager,
    logger: Logger,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: DiskManager, logger: &Logger) -> Self {
        let mut free_list = LinkedList::new();

        for i in 1..=pool_size {
            free_list.push_back(i as FrameId);
        }

        Self {
            pages: HashMap::new(),
            page_table: Default::default(),
            replacer: ClockReplacer::new(pool_size),
            free_list,
            disk_manager,
            logger: logger.clone(),
        }
    }
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Option<&mut Page>> {
        let frame_id = self
            .page_table
            .get(&page_id)
            .unwrap_or_else(|| panic!("Page{} is not exists.", page_id));
        {
            let page = self
                .pages
                .get_mut(&frame_id)
                .unwrap_or_else(|| panic!("page from frame{} must exists", frame_id));
            if page.get_id() == page_id {
                page.pin();
                self.replacer.pin(*frame_id);
                return Ok(self.pages.get_mut(&frame_id));
            }
        }

        // find Replacement, update new page that read from disk.
        if let Some(frame_id) = self.find_replacement() {
            debug!(
                self.logger,
                "new mapping, page({}) -> frame({})", page_id, frame_id
            );
            let page = self
                .pages
                .get_mut(&frame_id)
                .unwrap_or_else(|| panic!("page from frame{} must exists", frame_id));
            if page.is_dirty() {
                // write to disk
                self.disk_manager
                    .write_page(page.get_id(), &page.get_data())?;
            }
            *page = Page::new(page_id);

            // read page from disk
            let mut buf = vec![0; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut buf)?;
            page.put_data(&buf);

            self.page_table.insert(page_id, frame_id);
            return Ok(self.pages.get_mut(&frame_id));
        }

        Ok(None)
    }

    pub fn flush_page(&mut self, page_id: PageId) -> Result<bool> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            if let Some(page) = self.pages.get(frame_id) {
                self.disk_manager
                    .write_page(page.get_id(), &page.get_data())?;
                debug!(self.logger, "page{} write to disk", page_id);
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> bool {
        let frame_id = self
            .page_table
            .get(&page_id)
            .unwrap_or_else(|| panic!("page_id: {} should be pinned", page_id));

        let page = self.pages.get_mut(frame_id).expect("pages should exists");
        page.mark_dirty(is_dirty);

        self.replacer.unpin(*frame_id);

        page.get_pin_count() > 0
    }
    pub fn new_page(&mut self) -> Result<(Option<&mut Page>, PageId)> {
        if let Some(frame_id) = self.find_replacement() {
            let page_id = self.disk_manager.allocate_page();

            debug!(self.logger, "page_id: {:?}", page_id);

            let new_page = Page::new(page_id);

            self.page_table.insert(page_id, frame_id);

            if let Some(old_page) = self.pages.get_mut(&frame_id) {
                if old_page.is_dirty() {
                    self.disk_manager
                        .write_page(old_page.get_id(), &old_page.get_data())?;
                }
            }

            self.pages.insert(frame_id, new_page);

            Ok((self.pages.get_mut(&frame_id), page_id))
        } else {
            Ok((None, INVALID_PAGE_ID))
        }
    }

    // find in free lists first, then replacer
    fn find_replacement(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop_front() {
            Some(frame_id)
        } else if let Some(frame_id) = self.replacer.victim() {
            Some(frame_id)
        } else {
            // all the pages in buffer pool are pinned
            None
        }
    }
}

#[cfg(test)]
mod tests {
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
        page1.put_data(&random_binary_data);

        assert_eq!(page1.get_data(), random_binary_data.to_vec());

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
        assert_eq!(page.get_data(), random_binary_data.to_vec());
        assert!(bpm.unpin_page(1, true));

        // pinning page id 16 - 19, there would still be one page left for reading page 1.
        for i in 16..20 {
            let (page, page_id) = bpm.new_page()?;
            assert!(page.is_some());
            assert_eq!(page_id, i as PageId);
        }

        let page = bpm.fetch_page(1)?.unwrap();
        assert_eq!(page.get_data(), random_binary_data.to_vec());

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
}
