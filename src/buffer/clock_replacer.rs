use crate::buffer::replace::Replace;
use std::collections::{HashMap, VecDeque};

pub struct ClockReplacer {
    queue: VecDeque<(u32, u32)>,
    recorder: HashMap<u32, u32>,
    xid: u32,
}

impl ClockReplacer {
    // TODO(accelsao): remove when used
    #[allow(dead_code)]
    pub fn new(num_pages: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(num_pages),
            recorder: HashMap::new(),
            xid: 0,
        }
    }
}

impl Replace for ClockReplacer {
    fn victim(&mut self) -> Option<u32> {
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

    fn pin(&mut self, frame_id: u32) {
        if self.recorder.contains_key(&frame_id) {
            self.recorder.remove(&frame_id);
        }
    }

    fn unpin(&mut self, frame_id: u32) {
        self.recorder.entry(frame_id).or_insert(self.xid);
        if self.recorder[&frame_id] == self.xid {
            self.queue.push_back((frame_id, self.xid));
            self.xid += 1;
        }
    }

    fn size(&self) -> usize {
        self.recorder.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::clock_replacer::ClockReplacer;
    use crate::buffer::replace::Replace;

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
