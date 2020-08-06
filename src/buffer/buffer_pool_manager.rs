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
            let mut buf = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut buf)?;
            page.set_data(&buf);

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
