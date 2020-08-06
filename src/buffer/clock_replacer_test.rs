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
