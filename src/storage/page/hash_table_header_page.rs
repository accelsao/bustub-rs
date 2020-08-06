use crate::{LogSequenceNum, PageId, HEADER_PAGE_SIZE};

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */
#[derive(Default)]
#[repr(C)]
pub struct HashTableHeaderPage {
    lsn: LogSequenceNum,
    size: u32,
    page_id: PageId,
    next_idx: u32,
    block_page_ids: Vec<PageId>,
}

impl HashTableHeaderPage {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get_size(&self) -> u32 {
        self.size
    }
    pub fn set_size(&mut self, size: u32) {
        self.size = size
    }
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id
    }
    pub fn set_lsn(&mut self, lsn: LogSequenceNum) {
        self.lsn = lsn;
    }
    pub fn get_lsn(&self) -> LogSequenceNum {
        self.lsn
    }
    pub fn add_block_page_id(&mut self, page_id: PageId) {
        self.block_page_ids.push(page_id)
    }
    pub fn get_block_page_id(&self, index: usize) -> Option<&PageId> {
        self.block_page_ids.get(index)
    }
    pub fn num_blocks(&self) -> usize {
        self.block_page_ids.len()
    }
}

impl From<[u8; HEADER_PAGE_SIZE]> for HashTableHeaderPage {
    fn from(page: [u8; HEADER_PAGE_SIZE]) -> Self {
        let mut lsn: [u8; 4] = Default::default();
        lsn.copy_from_slice(&page[..4]);

        let mut size: [u8; 4] = Default::default();
        size.copy_from_slice(&page[4..8]);

        let mut page_id: [u8; 4] = Default::default();
        page_id.copy_from_slice(&page[8..12]);

        let mut next_idx: [u8; 4] = Default::default();
        next_idx.copy_from_slice(&page[12..16]);

        Self {
            lsn: u32::from_ne_bytes(lsn),
            size: u32::from_ne_bytes(size),
            page_id: u32::from_ne_bytes(page_id),
            next_idx: u32::from_ne_bytes(next_idx),
            block_page_ids: vec![],
        }
    }
}

impl Into<[u8; HEADER_PAGE_SIZE]> for HashTableHeaderPage {
    fn into(self) -> [u8; HEADER_PAGE_SIZE] {
        let data = [
            self.lsn.to_ne_bytes(),
            self.size.to_ne_bytes(),
            self.page_id.to_ne_bytes(),
            self.next_idx.to_ne_bytes(),
        ]
        .concat();
        let mut slice = [0u8; 16];
        slice.copy_from_slice(&data);
        slice
    }
}
