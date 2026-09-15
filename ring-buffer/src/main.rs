use ring_buffer::spsc;
use std::{thread, thread::sleep, time::Duration};

fn main() {
    let (sender, receiver) = spsc::channel(2);

    let thread_1 = thread::spawn(move || {
        // let mut sender = sender;
        for i in 0..100 {
            println!("produce({i}) {:?}", sender.try_produce(i));
            sleep(Duration::from_millis(10));
        }
    });

    let thread_2 = thread::spawn(move || {
        for _ in 0..100 {
            println!("consume value={:#?}", receiver.try_consume());
            sleep(Duration::from_millis(5));
        }
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}
