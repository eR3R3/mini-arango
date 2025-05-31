// SchedulerFeature - Feature wrapper for scheduler
// Similar to ArangoDB's SchedulerFeature.cpp

use std::sync::Arc;
use parking_lot::Mutex;
use crate::common::error::Result;
use crate::arangod::feature_phases::{Feature, FeaturePhase};
use super::scheduler::{MainScheduler, Scheduler};

pub struct SchedulerFeature {
    scheduler: Arc<Mutex<Option<MainScheduler>>>,
    num_threads: usize,
}

impl SchedulerFeature {
    pub fn new(num_threads: usize) -> Self {
        Self {
            scheduler: Arc::new(Mutex::new(None)),
            num_threads,
        }
    }

    pub fn scheduler(&self) -> Option<Arc<Mutex<Option<MainScheduler>>>> {
        Some(self.scheduler.clone())
    }
}

impl Feature for SchedulerFeature {
    fn name(&self) -> &'static str {
        "Scheduler"
    }

    fn phase(&self) -> FeaturePhase {
        FeaturePhase::Prepare
    }

    fn prepare(&self) -> Result<()> {
        let mut scheduler = MainScheduler::new().with_worker_threads(self.num_threads);
        scheduler.initialize_supervised()?;
        *self.scheduler.lock() = Some(scheduler);
        Ok(())
    }

    fn start(&self) -> Result<()> {
        if let Some(ref mut scheduler) = *self.scheduler.lock() {
            scheduler.start()?;
        }
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        if let Some(ref mut scheduler) = *self.scheduler.lock() {
            scheduler.stop()?;
        }
        Ok(())
    }
} 