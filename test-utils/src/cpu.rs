use cpu_time::{ProcessTime, ThreadTime};
use std::time::Duration;

pub struct ProcessCpuTimer {
    start: ProcessTime,
}

impl Default for ProcessCpuTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessCpuTimer {
    pub fn new() -> Self {
        Self {
            start: ProcessTime::now(),
        }
    }

    pub fn spent(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

pub struct ThreadCpuTimer {
    start: ThreadTime,
}

impl Default for ThreadCpuTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadCpuTimer {
    pub fn new() -> Self {
        Self {
            start: ThreadTime::now(),
        }
    }

    pub fn spent(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }
}
