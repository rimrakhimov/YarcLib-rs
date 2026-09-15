// Single-Producer/Single-Consumer Fixed-Size Ring Buffer

use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let buffer = Arc::new(RingBufferInner::new(capacity));
    (Sender::new(buffer.clone()), Receiver::new(buffer))
}

pub struct Sender<T> {
    inner: Arc<RingBufferInner<T>>,

    // forces the struct to be !Sync
    _marker: PhantomData<UnsafeCell<T>>,
}

impl<T> Sender<T> {
    pub fn try_produce(&self, value: T) -> Result<(), T> {
        self.inner.try_produce(value)
    }
}

impl<T> Sender<T> {
    fn new(inner: Arc<RingBufferInner<T>>) -> Self {
        Self {
            inner,
            _marker: Default::default(),
        }
    }
}

pub struct Receiver<T> {
    inner: Arc<RingBufferInner<T>>,

    // forces the struct to be !Sync
    _marker: PhantomData<UnsafeCell<T>>,
}

impl<T> Receiver<T> {
    pub fn try_consume(&self) -> Option<T> {
        self.inner.try_consume()
    }
}

impl<T> Receiver<T> {
    fn new(inner: Arc<RingBufferInner<T>>) -> Self {
        Self {
            inner,
            _marker: Default::default(),
        }
    }
}

struct RingBufferInner<T> {
    buffer: Box<[UnsafeCell<Option<T>>]>,
    tail: AtomicUsize,
    head: AtomicUsize,
}

unsafe impl<T> Sync for RingBufferInner<T> {}

impl<T> RingBufferInner<T> {
    fn new(capacity: usize) -> Self {
        let mut buffer = Vec::new();
        buffer.resize_with(capacity + 1, || UnsafeCell::new(None));

        Self {
            buffer: buffer.into_boxed_slice(),
            tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
        }
    }

    fn try_produce(&self, value: T) -> Result<(), T> {
        let curr_head = self.head.load(Ordering::Acquire);
        let curr_tail = self.tail.load(Ordering::Relaxed);

        if self.is_full(curr_head, curr_tail) {
            return Err(value);
        }

        let slot = &self.buffer[curr_tail];
        let slot_mut = unsafe { &mut *slot.get() };

        *slot_mut = Some(value);

        self.tail.store(self.next(curr_tail), Ordering::Release);

        Ok(())
    }

    fn try_consume(&self) -> Option<T> {
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

    fn is_full(&self, curr_head: usize, curr_tail: usize) -> bool {
        self.next(curr_tail) == curr_head
    }

    fn is_empty(&self, curr_head: usize, curr_tail: usize) -> bool {
        curr_head == curr_tail
    }

    fn next(&self, index: usize) -> usize {
        (index + 1) % self.buffer.len()
    }
}
