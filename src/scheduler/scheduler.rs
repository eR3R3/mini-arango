// Scheduler - Main scheduler interface similar to ArangoDB's Scheduler.cpp
// Provides the main scheduling interface for task execution

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use parking_lot::Mutex;
use crate::common::error::{ArangoError, Result};
use super::scheduler_feature::SchedulerFeature;
use super::supervised_scheduler::SupervisedScheduler;
use super::thread_pool_scheduler::ThreadPoolScheduler;
use super::scheduler_metrics::SchedulerMetrics;

pub trait Scheduler: Send + Sync {
    fn start(&mut self) -> Result<()>;
    fn stop(&mut self) -> Result<()>;
    fn is_running(&self) -> bool;
    fn queue_size(&self) -> usize;
    fn num_worker_threads(&self) -> usize;
    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()>;
}

pub trait WorkItem: Send + 'static {
    fn execute(self: Box<Self>) -> Result<()>;
    fn priority(&self) -> u8 { 0 }
    fn name(&self) -> &str { "WorkItem" }
}

pub struct MainScheduler {
    supervised: Option<Arc<Mutex<SupervisedScheduler>>>,
    thread_pool: Option<Arc<Mutex<ThreadPoolScheduler>>>,
    metrics: Arc<SchedulerMetrics>,
    running: Arc<Mutex<bool>>,
    worker_threads: usize,
}

impl MainScheduler {
    pub fn new() -> Self {
        Self {
            supervised: None,
            thread_pool: None,
            metrics: Arc::new(SchedulerMetrics::new()),
            running: Arc::new(Mutex::new(false)),
            worker_threads: num_cpus::get(),
        }
    }

    pub fn with_worker_threads(mut self, count: usize) -> Self {
        self.worker_threads = count;
        self
    }

    pub fn initialize_supervised(&mut self) -> Result<()> {
        let scheduler = SupervisedScheduler::new(self.worker_threads)?;
        self.supervised = Some(Arc::new(Mutex::new(scheduler)));
        Ok(())
    }

    pub fn initialize_thread_pool(&mut self) -> Result<()> {
        let scheduler = ThreadPoolScheduler::new(self.worker_threads)?;
        self.thread_pool = Some(Arc::new(Mutex::new(scheduler)));
        Ok(())
    }

    pub fn metrics(&self) -> &SchedulerMetrics {
        &self.metrics
    }
}

impl Scheduler for MainScheduler {
    fn start(&mut self) -> Result<()> {
        let mut running = self.running.lock();
        if *running {
            return Err(ArangoError::Other("Scheduler is already running".to_string()));
        }

        if let Some(ref supervised) = self.supervised {
            supervised.lock().start()?;
        }

        if let Some(ref thread_pool) = self.thread_pool {
            thread_pool.lock().start()?;
        }

        *running = true;
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        let mut running = self.running.lock();
        if !*running {
            return Ok(());
        }

        if let Some(ref supervised) = self.supervised {
            supervised.lock().stop()?;
        }

        if let Some(ref thread_pool) = self.thread_pool {
            thread_pool.lock().stop()?;
        }

        *running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        *self.running.lock()
    }

    fn queue_size(&self) -> usize {
        let mut total = 0;

        if let Some(ref supervised) = self.supervised {
            total += supervised.lock().queue_size();
        }

        if let Some(ref thread_pool) = self.thread_pool {
            total += thread_pool.lock().queue_size();
        }

        total
    }

    fn num_worker_threads(&self) -> usize {
        self.worker_threads
    }

    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        if !self.is_running() {
            return Err(ArangoError::Other("Scheduler is not running".to_string()));
        }

        // Prefer supervised scheduler if available
        if let Some(ref supervised) = self.supervised {
            return supervised.lock().schedule_work(work);
        }

        if let Some(ref thread_pool) = self.thread_pool {
            return thread_pool.lock().schedule_work(work);
        }

        Err(ArangoError::Other("No scheduler available".to_string()))
    }
}

pub struct SimpleWorkItem<F>
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    task: Option<F>,
    name: String,
    priority: u8,
}

impl<F> SimpleWorkItem<F>
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    pub fn new(task: F, name: String) -> Self {
        Self {
            task: Some(task),
            name,
            priority: 0,
        }
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

impl<F> WorkItem for SimpleWorkItem<F>
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    fn execute(mut self: Box<Self>) -> Result<()> {
        if let Some(task) = self.task.take() {
            task()
        } else {
            Err(ArangoError::Other("Task already executed".to_string()))
        }
    }

    fn priority(&self) -> u8 {
        self.priority
    }

    fn name(&self) -> &str {
        &self.name
    }
} 