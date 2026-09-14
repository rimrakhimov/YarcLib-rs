use ring_buffer::SpscRingBuffer;
use std::{thread, thread::sleep, time::Duration};

fn main() {
    let buffer = SpscRingBuffer::new(2);

    thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..100 {
                println!("{}", buffer.try_produce(i));
                sleep(Duration::from_millis(10));
            }
        });

        scope.spawn(|| {
            for _ in 0..100 {
                println!("value={:#?}", buffer.try_consume());
                sleep(Duration::from_millis(10));
            }
        });
    });
}
