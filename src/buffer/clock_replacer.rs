use crate::buffer::replace::Replacer;
use crate::{EpochId, FrameId};
use std::collections::{HashMap, VecDeque};

pub struct ClockReplacer {
    // replacer queue
    queue: VecDeque<(FrameId, EpochId)>,
    // record the latest epoch id for frame id
    recorder: HashMap<FrameId, EpochId>,
    // TODO(accelsao): what if epoch id is over u64?
    // global epoch id
    epoch_id: EpochId,
}

impl ClockReplacer {
    pub fn new(num_pages: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(num_pages),
            recorder: HashMap::new(),
            epoch_id: 0,
        }
    }
}

impl Replacer for ClockReplacer {
    // return the first frame in replacer that xid equal to the frame's latest xid
    fn victim(&mut self) -> Option<FrameId> {
        loop {
            if let Some((frame_id, xid)) = self.queue.pop_front() {
                if self.recorder.get(&frame_id) == Some(&xid) {
                    self.recorder.remove(&frame_id);
                    return Some(frame_id);
                }
            } else {
                return None;
            }
        }
    }

    fn pin(&mut self, frame_id: FrameId) {
        if self.recorder.contains_key(&frame_id) {
            self.recorder.remove(&frame_id);
        }
    }

    fn unpin(&mut self, frame_id: FrameId) {
        self.recorder.entry(frame_id).or_insert(self.epoch_id);
        if self.recorder[&frame_id] == self.epoch_id {
            self.queue.push_back((frame_id, self.epoch_id));
            self.epoch_id += 1;
        }
    }

    fn size(&self) -> usize {
        self.recorder.len()
    }
}
