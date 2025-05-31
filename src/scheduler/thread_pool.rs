// ThreadPoolScheduler - Simple thread pool scheduler
// Similar to ArangoDB's ThreadPoolScheduler.cpp

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use rayon::{ThreadPool, ThreadPoolBuilder};
use crate::common::error::{ArangoError, Result};
use super::scheduler::{Scheduler, WorkItem};

pub struct ThreadPoolScheduler {
    pool: ThreadPool,
    running: Arc<AtomicBool>,
    name: String,
}

impl ThreadPoolScheduler {
    pub fn new(num_threads: usize) -> Result<Self> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("ThreadPoolWorker-{}", i))
            .build()
            .map_err(|e| ArangoError::Other(format!("Failed to create thread pool: {}", e)))?;

        Ok(Self {
            pool,
            running: Arc::new(AtomicBool::new(false)),
            name: "ThreadPoolScheduler".to_string(),
        })
    }
}

impl Scheduler for ThreadPoolScheduler {
    fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    fn queue_size(&self) -> usize {
        0 // Rayon doesn't expose queue size
    }

    fn num_worker_threads(&self) -> usize {
        self.pool.current_num_threads()
    }

    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        if !self.is_running() {
            return Err(ArangoError::Other("ThreadPoolScheduler is not running".to_string()));
        }

        self.pool.spawn(move || {
            if let Err(e) = work.execute() {
                tracing::error!("Task execution failed: {}", e);
            }
        });

        Ok(())
    }
} 