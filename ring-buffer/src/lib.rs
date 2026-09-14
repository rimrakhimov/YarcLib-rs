// Single-Producer/Single-Consumer Fixed-Size Ring Buffer

use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct SpscRingBuffer<T> {
    buffer: Vec<UnsafeCell<Option<T>>>,
    tail: AtomicUsize,
    head: AtomicUsize,

    len: usize,
}

unsafe impl<T> Sync for SpscRingBuffer<T> {}

impl<T> SpscRingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let buffer_len = capacity + 1;
        let mut buffer = Vec::new();
        buffer.resize_with(buffer_len, || UnsafeCell::new(None));

        Self {
            buffer,
            tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),

            len: buffer_len,
        }
    }

    pub fn try_produce(&self, value: T) -> bool {
        let curr_head = self.head.load(Ordering::Acquire);
        let curr_tail = self.tail.load(Ordering::Relaxed);

        if self.is_full(curr_head, curr_tail) {
            return false;
        }

        let slot = &self.buffer[curr_tail];
        let slot_mut = unsafe { &mut *slot.get() };

        *slot_mut = Some(value);

        self.tail.store(self.next(curr_tail), Ordering::Release);

        true
    }

    pub fn try_consume(&self) -> Option<T> {
        let curr_head = self.head.load(Ordering::Relaxed);
        let curr_tail = self.tail.load(Ordering::Acquire);

        if self.is_empty(curr_head, curr_tail) {
            return None;
        }

        let slot = &self.buffer[curr_head];
        let slot_mut = unsafe { &mut *slot.get() };

        let value = slot_mut.take().expect("value must be written");

        self.head.store(self.next(curr_head), Ordering::Release);

        Some(value)
    }
}

impl<T> SpscRingBuffer<T> {
    fn is_full(&self, curr_head: usize, curr_tail: usize) -> bool {
        self.next(curr_tail) == curr_head
    }

    fn is_empty(&self, curr_head: usize, curr_tail: usize) -> bool {
        curr_head == curr_tail
    }

    fn next(&self, index: usize) -> usize {
        (index + 1) % self.len
    }
}
