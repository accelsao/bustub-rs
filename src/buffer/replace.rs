use crate::FrameId;

pub trait Replacer {
    fn victim(&mut self) -> Option<FrameId>;
    fn pin(&mut self, frame_id: FrameId);
    fn unpin(&mut self, frame_id: FrameId);
    fn size(&self) -> usize;
}
