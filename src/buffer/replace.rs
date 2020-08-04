pub trait Replace {
    fn victim(&mut self) -> Option<u32>;
    fn pin(&mut self, frame_id: u32);
    fn unpin(&mut self, frame_id: u32);
    fn size(&self) -> usize;
}
