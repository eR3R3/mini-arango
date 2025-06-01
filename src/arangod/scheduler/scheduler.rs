use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use parking_lot::Mutex;
use crate::common::error::{ArangoError, Result};
use std::sync::RwLock;

/// Work item that can be executed by the scheduler
pub trait WorkItem: Send + Sync {
    fn execute(&self) -> Result<()>;
    fn description(&self) -> &str;
}

/// Simple work item implementation
pub struct SimpleWorkItem {
    description: String,
    task: Box<dyn Fn() -> Result<()> + Send + Sync>,
}

impl SimpleWorkItem {
    pub fn new(description: String, task: Box<dyn Fn() -> Result<()> + Send + Sync>) -> Self {
        SimpleWorkItem { description, task }
    }
}

impl WorkItem for SimpleWorkItem {
    fn execute(&self) -> Result<()> {
        (self.task)()
    }
    
    fn description(&self) -> &str {
        &self.description
    }
}

/// Basic scheduler trait
pub trait Scheduler: Send + Sync {
    /// Queue a work item for execution
    fn queue(&self, item: Box<dyn WorkItem>) -> Result<()>;
    
    /// Start the scheduler
    fn start(&self) -> Result<()>;
    
    /// Stop the scheduler
    fn stop(&self) -> Result<()>;
    
    /// Check if scheduler is running
    fn is_running(&self) -> bool;
    
    /// Get the current queue size
    fn queue_size(&self) -> usize;
    
    /// Get the number of worker threads
    fn num_worker_threads(&self) -> usize;
    
    /// Schedule work (alias for queue)
    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        self.queue(work)
    }
}

/// Thread pool based scheduler implementation
pub struct ThreadPoolScheduler {
    runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    num_threads: usize,
}

impl ThreadPoolScheduler {
    pub fn new(num_threads: usize) -> Self {
        ThreadPoolScheduler {
            runtime: Arc::new(Mutex::new(None)),
            num_threads,
        }
    }
    
    pub fn with_worker_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }
}

impl Default for ThreadPoolScheduler {
    fn default() -> Self {
        Self::new(4) // Default to 4 threads
    }
}

impl Scheduler for ThreadPoolScheduler {
    fn queue(&self, item: Box<dyn WorkItem>) -> Result<()> {
        let runtime_guard = self.runtime.lock();
        if let Some(ref runtime) = *runtime_guard {
            runtime.spawn(async move {
                if let Err(e) = item.execute() {
                    eprintln!("Work item '{}' failed: {}", item.description(), e);
                }
            });
            Ok(())
        } else {
            Err(ArangoError::internal("Thread pool not started"))
        }
    }
    
    fn start(&self) -> Result<()> {
        let mut runtime_guard = self.runtime.lock();
        if runtime_guard.is_none() {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(self.num_threads)
                .enable_all()
                .build()
                .map_err(|e| ArangoError::internal(format!("Failed to create runtime: {}", e)))?;
            *runtime_guard = Some(runtime);
        }
        Ok(())
    }
    
    fn stop(&self) -> Result<()> {
        let mut runtime_guard = self.runtime.lock();
        if let Some(runtime) = runtime_guard.take() {
            runtime.shutdown_background();
        }
        Ok(())
    }
    
    fn is_running(&self) -> bool {
        self.runtime.lock().is_some()
    }
    
    fn queue_size(&self) -> usize {
        // For simplicity, return 0 as we don't track queue size in this implementation
        0
    }
    
    fn num_worker_threads(&self) -> usize {
        self.num_threads
    }
    
    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        self.queue(work)
    }
}

/// Supervised scheduler with monitoring capabilities
pub struct SupervisedScheduler {
    thread_pool: Arc<ThreadPoolScheduler>,
    is_running: Arc<RwLock<bool>>,
}

impl SupervisedScheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        SupervisedScheduler {
            thread_pool: Arc::new(ThreadPoolScheduler::new(8)), // Default to 8 threads
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    pub fn with_worker_threads(self, num_threads: usize) -> Self {
        SupervisedScheduler {
            thread_pool: Arc::new(ThreadPoolScheduler::new(num_threads)),
            is_running: self.is_running,
        }
    }
    
    pub fn start(&self) -> Result<()> {
        let mut running = self.is_running.write().unwrap();
        if !*running {
            *running = true;
            // In a real implementation, this would start any background monitoring
            // For now, just set the flag
        }
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        let mut running = self.is_running.write().unwrap();
        if *running {
            *running = false;
            // In a real implementation, this would stop background monitoring
            // and drain any queued work
        }
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        *self.is_running.read().unwrap()
    }
}

impl Scheduler for SupervisedScheduler {
    fn queue(&self, item: Box<dyn WorkItem>) -> Result<()> {
        let runtime_guard = self.thread_pool.runtime.lock();
        if let Some(ref runtime) = *runtime_guard {
            runtime.spawn(async move {
                if let Err(e) = item.execute() {
                    eprintln!("Work item '{}' failed: {}", item.description(), e);
                }
            });
            Ok(())
        } else {
            Err(ArangoError::internal("Scheduler not started"))
        }
    }
    
    fn start(&self) -> Result<()> {
        self.thread_pool.start()
    }
    
    fn stop(&self) -> Result<()> {
        self.thread_pool.stop()
    }
    
    fn is_running(&self) -> bool {
        self.thread_pool.is_running()
    }
    
    fn queue_size(&self) -> usize {
        self.thread_pool.queue_size()
    }
    
    fn num_worker_threads(&self) -> usize {
        self.thread_pool.num_worker_threads()
    }
}

/// Main scheduler type alias
pub type MainScheduler = SupervisedScheduler;