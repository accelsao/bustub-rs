pub mod hash_table_header_page;
mod table_page;

#[cfg(test)]
mod hash_table_header_page_test;

// Page Impl
use crate::{PageId, INVALID_PAGE_ID, PAGE_SIZE};

#[derive(Copy, Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
    page_id: PageId,
    pin_count: u32,
    is_dirty: bool,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            data: [0; PAGE_SIZE],
            page_id,
            pin_count: 1,
            is_dirty: false,
        }
    }
    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        self.data
    }
    pub fn set_data(&mut self, data: &[u8; PAGE_SIZE]) {
        self.data.copy_from_slice(data)
    }
    pub fn get_id(&self) -> PageId {
        self.page_id
    }
    pub fn get_pin_count(&self) -> u32 {
        self.pin_count
    }
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }
    pub fn mark_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(INVALID_PAGE_ID)
    }
}
