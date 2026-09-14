use std::sync::{Arc, Condvar, Mutex};

struct State {
    count: usize,
    waiters: usize,
}

pub struct WaitGroup {
    state: Mutex<State>,
    all_done: Condvar,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                count: 0,
                waiters: 0,
            }),
            all_done: Condvar::new(),
        }
    }

    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn add(&self, count: usize) {
        let mut state = self.state.lock().unwrap();
        state.count += count;
    }

    pub fn done(&self) {
        let mut state = self.state.lock().unwrap();
        state.count -= 1;
        if state.count == 0 && state.waiters > 0 {
            self.all_done.notify_all();
        }
    }

    pub fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        while state.count > 0 {
            state.waiters += 1;
            state = self.all_done.wait(state).unwrap();
            state.waiters -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        thread,
        time::Duration,
    };

    #[test]
    fn just_works() {
        let wg = WaitGroup::new();

        wg.add(1);
        wg.done();
        wg.wait();
    }

    #[test]
    fn init_zero() {
        let wg = WaitGroup::new();
        wg.wait();
    }

    #[test]
    fn add_count() {
        let wg = WaitGroup::new();

        wg.add(7);

        for _ in 0..7 {
            wg.done();
        }

        wg.wait();
    }

    #[test]
    fn wait() {
        let wg = WaitGroup::new();
        let ready = AtomicBool::new(false);

        thread::scope(|s| {
            wg.add(1);

            let producer = s.spawn(|| {
                thread::sleep(Duration::from_secs(1));
                ready.store(true, Ordering::SeqCst);
                wg.done();
            });

            wg.wait();
            assert!(ready.load(Ordering::SeqCst));

            producer.join().unwrap();
        });
    }

    #[test]
    fn multi_wait() {
        let wg = WaitGroup::new();
        let work = AtomicUsize::new(0);

        const WORKERS: usize = 3;
        const WAITERS: usize = 4;

        thread::scope(|s| {
            let mut threads = vec![];

            wg.add(WORKERS);

            for _ in 0..WAITERS {
                threads.push(s.spawn(|| {
                    wg.wait();
                    assert_eq!(work.load(Ordering::SeqCst), WORKERS)
                }));
            }

            for i in 0..WORKERS {
                let work = &work;
                let wg = &wg;
                threads.push(s.spawn(move || {
                    thread::sleep(Duration::from_millis(256 * i as u64));
                    work.fetch_add(1, Ordering::SeqCst);
                    wg.done();
                }));
            }

            for thread in threads {
                thread.join().unwrap();
            }
        });
    }

    #[test]
    fn blocking_wait() {
        let wg = WaitGroup::new();

        const WORKERS: usize = 3;

        let work = AtomicUsize::new(0);

        thread::scope(|s| {
            let mut workers = vec![];

            wg.add(WORKERS);

            for _ in 0..WORKERS {
                workers.push(s.spawn(|| {
                    thread::sleep(Duration::from_secs(1));
                    work.fetch_add(1, Ordering::SeqCst);
                    wg.done();
                }));
            }

            let timer = test_utils::cpu::ProcessCpuTimer::new();

            wg.wait();

            let spent = timer.spent();
            assert!(
                spent < Duration::from_millis(100),
                "spent {}ms",
                spent.as_millis()
            );
            assert_eq!(work.load(Ordering::SeqCst), WORKERS);

            for thread in workers {
                thread.join().unwrap();
            }
        });
    }

    #[test]
    fn cyclic() {
        let wg = WaitGroup::new();

        for _ in 0..4 {
            let flag = AtomicBool::new(false);

            thread::scope(|s| {
                wg.add(1);

                let worker = s.spawn(|| {
                    thread::sleep(Duration::from_secs(1));
                    flag.store(true, Ordering::SeqCst);
                    wg.done();
                });

                wg.wait();

                assert!(flag.load(Ordering::SeqCst));

                worker.join().unwrap();
            });
        }
    }
}
