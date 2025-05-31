// SupervisedScheduler - Advanced scheduler with work stealing and dynamic scaling
// Similar to ArangoDB's SupervisedScheduler.cpp

use std::sync::{Arc, atomic::{AtomicUsize, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use crossbeam_channel::{Sender, Receiver, unbounded, select};
use parking_lot::{Mutex, RwLock};
use crate::common::error::{ArangoError, Result};
use super::scheduler::{Scheduler, WorkItem};
use super::scheduler_metrics::SchedulerMetrics;

pub struct SupervisedScheduler {
    workers: Vec<Worker>,
    work_queue: Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>,
    shutdown_sender: Option<Sender<()>>,
    running: Arc<AtomicBool>,
    num_threads: usize,
    active_workers: Arc<AtomicUsize>,
    metrics: Arc<SchedulerMetrics>,
    work_stealing_enabled: bool,
    dynamic_scaling: bool,
}

struct Worker {
    id: usize,
    thread_handle: Option<thread::JoinHandle<()>>,
    local_queue: Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>,
    work_sender: Sender<Box<dyn WorkItem>>,
    work_receiver: Receiver<Box<dyn WorkItem>>,
}

impl SupervisedScheduler {
    pub fn new(num_threads: usize) -> Result<Self> {
        let mut workers = Vec::with_capacity(num_threads);
        let work_queue = Arc::new(Mutex::new(VecDeque::new()));
        let (shutdown_sender, shutdown_receiver) = unbounded();

        // Create workers
        for i in 0..num_threads {
            let (work_sender, work_receiver) = unbounded();
            let worker = Worker {
                id: i,
                thread_handle: None,
                local_queue: Arc::new(Mutex::new(VecDeque::new())),
                work_sender,
                work_receiver,
            };
            workers.push(worker);
        }

        Ok(Self {
            workers,
            work_queue,
            shutdown_sender: Some(shutdown_sender),
            running: Arc::new(AtomicBool::new(false)),
            num_threads,
            active_workers: Arc::new(AtomicUsize::new(0)),
            metrics: Arc::new(SchedulerMetrics::new()),
            work_stealing_enabled: true,
            dynamic_scaling: true,
        })
    }

    pub fn enable_work_stealing(&mut self, enable: bool) {
        self.work_stealing_enabled = enable;
    }

    pub fn enable_dynamic_scaling(&mut self, enable: bool) {
        self.dynamic_scaling = enable;
    }

    fn start_workers(&mut self, shutdown_receiver: Receiver<()>) -> Result<()> {
        for worker in &mut self.workers {
            let worker_id = worker.id;
            let work_receiver = worker.work_receiver.clone();
            let local_queue = worker.local_queue.clone();
            let global_queue = self.work_queue.clone();
            let shutdown_receiver = shutdown_receiver.clone();
            let running = self.running.clone();
            let active_workers = self.active_workers.clone();
            let metrics = self.metrics.clone();
            let work_stealing = self.work_stealing_enabled;

            // Get references to other workers for work stealing
            let other_queues: Vec<Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>> =
                self.workers.iter()
                    .enumerate()
                    .filter(|(i, _)| *i != worker_id)
                    .map(|(_, w)| w.local_queue.clone())
                    .collect();

            let handle = thread::Builder::new()
                .name(format!("SupervisedWorker-{}", worker_id))
                .spawn(move || {
                    Self::worker_loop(
                        worker_id,
                        work_receiver,
                        local_queue,
                        global_queue,
                        other_queues,
                        shutdown_receiver,
                        running,
                        active_workers,
                        metrics,
                        work_stealing,
                    );
                })?;

            worker.thread_handle = Some(handle);
        }

        Ok(())
    }

    fn worker_loop(
        worker_id: usize,
        work_receiver: Receiver<Box<dyn WorkItem>>,
        local_queue: Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>,
        global_queue: Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>,
        other_queues: Vec<Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>>,
        shutdown_receiver: Receiver<()>,
        running: Arc<AtomicBool>,
        active_workers: Arc<AtomicUsize>,
        metrics: Arc<SchedulerMetrics>,
        work_stealing: bool,
    ) {
        tracing::debug!("SupervisedWorker-{} started", worker_id);

        while running.load(Ordering::Relaxed) {
            let work_item = select! {
                recv(work_receiver) -> msg => {
                    match msg {
                        Ok(item) => Some(item),
                        Err(_) => break, // Channel closed
                    }
                }
                recv(shutdown_receiver) -> _ => {
                    break;
                }
                default(Duration::from_millis(1)) => {
                    // Try to get work from local queue
                    if let Some(item) = local_queue.lock().pop_front() {
                        Some(item)
                    } else if work_stealing {
                        // Try work stealing from other workers
                        Self::try_steal_work(&other_queues)
                    } else {
                        // Try global queue
                        global_queue.lock().pop_front()
                    }
                }
            };

            if let Some(work) = work_item {
                active_workers.fetch_add(1, Ordering::Relaxed);
                let start_time = Instant::now();

                let result = work.execute();

                let execution_time = start_time.elapsed();
                active_workers.fetch_sub(1, Ordering::Relaxed);

                // Update metrics
                metrics.record_task_execution(execution_time, result.is_ok());

                if let Err(e) = result {
                    tracing::error!("Task execution failed in worker {}: {}", worker_id, e);
                }
            }
        }

        tracing::debug!("SupervisedWorker-{} stopped", worker_id);
    }

    fn try_steal_work(
        other_queues: &[Arc<Mutex<VecDeque<Box<dyn WorkItem>>>>]
    ) -> Option<Box<dyn WorkItem>> {
        for queue in other_queues {
            if let Ok(mut queue_guard) = queue.try_lock() {
                if queue_guard.len() > 1 {
                    // Steal from the back to reduce contention
                    return queue_guard.pop_back();
                }
            }
        }
        None
    }

    fn distribute_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        // Find the worker with the smallest queue
        let mut min_queue_size = usize::MAX;
        let mut chosen_worker = 0;

        for (i, worker) in self.workers.iter().enumerate() {
            let queue_size = worker.local_queue.lock().len();
            if queue_size < min_queue_size {
                min_queue_size = queue_size;
                chosen_worker = i;
            }
        }

        // Send work to chosen worker
        self.workers[chosen_worker]
            .work_sender
            .send(work)
            .map_err(|_| ArangoError::Other("Failed to send work to worker".to_string()))?;

        Ok(())
    }
}

impl Scheduler for SupervisedScheduler {
    fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Err(ArangoError::Other("SupervisedScheduler is already running".to_string()));
        }

        let shutdown_receiver = self.shutdown_sender
            .as_ref()
            .unwrap()
            .clone()
            .into();

        self.start_workers(shutdown_receiver)?;
        self.running.store(true, Ordering::Relaxed);

        tracing::info!("SupervisedScheduler started with {} workers", self.num_threads);
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.running.store(false, Ordering::Relaxed);

        // Send shutdown signal
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        // Wait for all workers to finish
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread_handle.take() {
                let _ = handle.join();
            }
        }

        tracing::info!("SupervisedScheduler stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    fn queue_size(&self) -> usize {
        let global_size = self.work_queue.lock().len();
        let local_sizes: usize = self.workers
            .iter()
            .map(|w| w.local_queue.lock().len())
            .sum();

        global_size + local_sizes
    }

    fn num_worker_threads(&self) -> usize {
        self.num_threads
    }

    fn schedule_work(&self, work: Box<dyn WorkItem>) -> Result<()> {
        if !self.is_running() {
            return Err(ArangoError::Other("SupervisedScheduler is not running".to_string()));
        }

        self.distribute_work(work)?;
        self.metrics.increment_tasks_scheduled();

        Ok(())
    }
}

impl Drop for SupervisedScheduler {
    fn drop(&mut self) {
        let _ = self.stop();
    }
} 