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

#[cfg(test)]
mod tests {
    use crate::buffer::clock_replacer::ClockReplacer;
    use crate::buffer::replace::Replacer;

    #[test]
    fn test_clock_replacer() {
        let mut clock_replacer = ClockReplacer::new(7);
        clock_replacer.unpin(1);
        clock_replacer.unpin(2);
        clock_replacer.unpin(3);
        clock_replacer.unpin(4);
        clock_replacer.unpin(5);
        clock_replacer.unpin(6);
        clock_replacer.unpin(1);
        assert_eq!(clock_replacer.size(), 6);

        assert_eq!(clock_replacer.victim(), Some(1));
        assert_eq!(clock_replacer.victim(), Some(2));
        assert_eq!(clock_replacer.victim(), Some(3));

        clock_replacer.pin(3);
        clock_replacer.pin(4);
        assert_eq!(clock_replacer.size(), 2);

        clock_replacer.unpin(4);

        assert_eq!(clock_replacer.victim(), Some(5));
        assert_eq!(clock_replacer.victim(), Some(6));
        assert_eq!(clock_replacer.victim(), Some(4));

        assert_eq!(clock_replacer.size(), 0);
    }
}
