use ring_buffer::SpscRingBuffer;
use std::thread;
use waitgroup::WaitGroup;

#[test]
fn simple_fifo() {
    let buffer = SpscRingBuffer::new(5);

    assert!(buffer.try_produce(1));
    assert!(buffer.try_produce(2));

    assert_eq!(Some(1), buffer.try_consume());
    assert_eq!(Some(2), buffer.try_consume());
    assert_eq!(None, buffer.try_consume());

    assert!(buffer.try_produce(3));
    assert_eq!(Some(3), buffer.try_consume());
}

#[test]
fn full_empty() {
    let buffer = SpscRingBuffer::new(3);

    assert_eq!(None, buffer.try_consume());

    assert!(buffer.try_produce(1));
    assert!(buffer.try_produce(2));
    assert!(buffer.try_produce(3));

    assert!(!buffer.try_produce(4));

    assert_eq!(Some(1), buffer.try_consume());
    assert_eq!(Some(2), buffer.try_consume());
    assert_eq!(Some(3), buffer.try_consume());
    assert_eq!(None, buffer.try_consume());

    assert!(buffer.try_produce(4));
    assert_eq!(Some(4), buffer.try_consume());
}

#[test]
fn pub_receive() {
    let buffer = SpscRingBuffer::new(1);

    thread::scope(|scope| {
        scope.spawn(|| {
            assert!(buffer.try_produce(42));
        });

        scope.spawn(|| {
            let value = buffer.try_consume();
            assert!(value == None || value == Some(42));
        });
    });
}

#[test]
fn slot_reuse() {
    let buffer = SpscRingBuffer::new(1);

    thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..3 {
                buffer.try_produce(i);
            }
        });

        scope.spawn(|| {
            buffer.try_consume();
        });
    })
}

#[test]
fn different_slots() {
    let buffer = SpscRingBuffer::new(2);

    let wait_group = WaitGroup::new();
    wait_group.add(1);
    thread::scope(|scope| {
        scope.spawn(|| {
            assert!(buffer.try_produce(1));
            wait_group.done();
            assert!(buffer.try_produce(2));
        });

        scope.spawn(|| {
            wait_group.wait();
            assert_eq!(Some(1), buffer.try_consume());
        });
    })
}
