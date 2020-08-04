use crate::buffer::clock_replacer::ClockReplacer;
use crate::buffer::replace::Replacer;
use crate::errors::Result;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::Page;
use crate::{FrameId, PageId, INVALID_PAGE_ID};
use slog::Logger;
use std::collections::{HashMap, LinkedList};

pub struct BufferPoolManager {
    // Number of pages in the buffer pool
    pool_size: usize,
    // Array of buffer pool pages.
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
            pool_size,
            pages: HashMap::new(),
            page_table: Default::default(),
            replacer: ClockReplacer::new(pool_size),
            free_list,
            disk_manager,
            logger: logger.clone(),
        }
    }
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Option<&mut Page>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            Ok(self.pages.get_mut(frame_id))
        } else if let Some(frame_id) = self.find_replacement() {
            let page = self
                .pages
                .get_mut(&frame_id)
                .unwrap_or_else(|| panic!("page from frame_id: {} must exists", frame_id));
            if page.is_dirty() {
                // write to disk
                self.disk_manager
                    .write_page(page.get_id(), &page.get_data())?;
            }
            *page = Page::new(page_id);
            self.page_table.insert(page_id, frame_id);
            Ok(Some(page))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_page(&self, page_id)

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
    pub fn new_page(&mut self) -> (Option<&mut Page>, PageId) {
        let page_id = self.disk_manager.allocate_page();

        debug!(self.logger, "page_id: {:?}", page_id);

        if let Some(frame_id) = self.find_replacement() {
            let page = Page::new(page_id);

            self.page_table.insert(page_id, frame_id);

            self.pages.insert(frame_id, page);

            (self.pages.get_mut(&frame_id), page_id)
        } else {
            (None, INVALID_PAGE_ID)
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
    use rand::{Rng, RngCore, SeedableRng};
    use std::borrow::BorrowMut;

    const DB_NAME: &str = "target/test.db";
    const BUFFER_POOL_SIZE: usize = 10;

    #[test]
    fn test_buffer_pool_manager() -> Result<()> {
        let logger = default_logger();
        let disk_manager = DiskManager::new(DB_NAME, &logger)?;
        let mut bpm = BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager, &logger);

        let (page1, page1_id) = bpm.new_page();

        assert!(page1.is_some());
        assert_eq!(page1_id, 1);

        let page1 = page1.unwrap();

        let mut rng: StdRng = rand::SeedableRng::seed_from_u64(42);
        let mut random_binary_data = [0u8; PAGE_SIZE];
        rng.fill_bytes(&mut random_binary_data);

        page1.put_data(&random_binary_data);

        assert_eq!(page1.get_data(), random_binary_data.to_vec());

        for i in 2..=BUFFER_POOL_SIZE {
            let (page, page_id) = bpm.new_page();
            assert!(page.is_some());
            assert_eq!(page_id, i as PageId);
        }

        for _ in BUFFER_POOL_SIZE + 1..BUFFER_POOL_SIZE * 2 {
            let (page, page_id) = bpm.new_page();
            assert!(page.is_none());
            assert_eq!(page_id, INVALID_PAGE_ID);
        }

        for i in 1..=5 {
            assert!(bpm.unpin_page(i, true));
        }

        Ok(())
    }
}
