use ring_buffer::spsc;
use std::thread;
use waitgroup::WaitGroup;

#[test]
fn simple_fifo() {
    let (sender, receiver) = spsc::channel(5);

    assert_eq!(Ok(()), sender.try_produce(1));
    assert_eq!(Ok(()), sender.try_produce(2));

    assert_eq!(Some(1), receiver.try_consume());
    assert_eq!(Some(2), receiver.try_consume());
    assert_eq!(None, receiver.try_consume());

    assert_eq!(Ok(()), sender.try_produce(3));
    assert_eq!(Some(3), receiver.try_consume());
}

#[test]
fn full_empty() {
    let (sender, receiver) = spsc::channel(3);

    assert_eq!(None, receiver.try_consume());

    assert_eq!(Ok(()), sender.try_produce(1));
    assert_eq!(Ok(()), sender.try_produce(2));
    assert_eq!(Ok(()), sender.try_produce(3));

    assert_eq!(Err(4), sender.try_produce(4));

    assert_eq!(Some(1), receiver.try_consume());
    assert_eq!(Some(2), receiver.try_consume());
    assert_eq!(Some(3), receiver.try_consume());
    assert_eq!(None, receiver.try_consume());

    assert_eq!(Ok(()), sender.try_produce(4));
    assert_eq!(Some(4), receiver.try_consume());
}

#[test]
fn pub_receive() {
    let (sender, receiver) = spsc::channel(1);

    thread::scope(|scope| {
        scope.spawn(move || {
            assert_eq!(Ok(()), sender.try_produce(42));
        });

        scope.spawn(move || {
            let value = receiver.try_consume();
            assert!(value.is_none() || value == Some(42));
        });
    });
}

#[test]
fn slot_reuse() {
    let (sender, receiver) = spsc::channel(1);

    thread::scope(|scope| {
        scope.spawn(move || {
            for i in 0..3 {
                let _ = sender.try_produce(i);
            }
        });

        scope.spawn(move || {
            receiver.try_consume();
        });
    })
}

#[test]
fn different_slots() {
    let (sender, receiver) = spsc::channel(2);

    let wait_group = WaitGroup::new();
    thread::scope(|scope| {
        let wait_group = &wait_group;
        wait_group.add(1);

        scope.spawn(move || {
            assert_eq!(Ok(()), sender.try_produce(1));
            wait_group.done();
            assert_eq!(Ok(()), sender.try_produce(2));
        });

        scope.spawn(move || {
            wait_group.wait();
            assert_eq!(Some(1), receiver.try_consume());
        });
    })
}

#[derive(Debug, PartialEq, Eq)]
struct NotCopy {
    v: i32,
}

#[test]
fn not_copy() {
    let (sender, receiver) = spsc::channel(1);

    let wait_group = WaitGroup::new();
    thread::scope(|scope| {
        let wait_group = &wait_group;
        wait_group.add(1);

        scope.spawn(move || {
            assert_eq!(Ok(()), sender.try_produce(NotCopy { v: 0 }));
            wait_group.done();

            let mut value = NotCopy { v: 0 };
            while let Err(retry) = sender.try_produce(value) {
                value = retry
            }
        });

        scope.spawn(move || {
            wait_group.wait();
            assert_eq!(Some(NotCopy { v: 0 }), receiver.try_consume());
        });
    });
}
