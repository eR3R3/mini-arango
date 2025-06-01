// SchedulerFeature - Feature wrapper for scheduler
// Similar to ArangoDB's SchedulerFeature.cpp

use std::sync::Arc;
use crate::common::error::Result;
use crate::arangod::feature_phases::{Feature, FeaturePhase};
use super::{SupervisedScheduler, Scheduler};

/// Scheduler feature for managing the task scheduler
pub struct SchedulerFeature {
    scheduler: Arc<SupervisedScheduler>,
    started: bool,
}

impl SchedulerFeature {
    pub fn new() -> Self {
        SchedulerFeature {
            scheduler: Arc::new(SupervisedScheduler::new()),
            started: false,
        }
    }
    
    pub fn with_worker_threads(mut self, num_threads: usize) -> Self {
        self.scheduler = Arc::new(SupervisedScheduler::new().with_worker_threads(num_threads));
        self
    }
    
    pub fn scheduler(&self) -> Arc<SupervisedScheduler> {
        Arc::clone(&self.scheduler)
    }
}

impl Feature for SchedulerFeature {
    fn name(&self) -> &'static str {
        "Scheduler"
    }
    
    fn state(&self) -> FeaturePhase {
        if self.started {
            FeaturePhase::Started
        } else {
            FeaturePhase::Prepared
        }
    }
    
    fn prepare(&mut self, _phase: FeaturePhase) -> Result<()> {
        // Scheduler preparation logic
        Ok(())
    }
    
    fn start(&mut self, _phase: FeaturePhase) -> Result<()> {
        if !self.started {
            self.scheduler.start()?;
            self.started = true;
        }
        Ok(())
    }
    
    fn stop(&mut self, _phase: FeaturePhase) -> Result<()> {
        if self.started {
            self.scheduler.stop()?;
            self.started = false;
        }
        Ok(())
    }
}

impl Default for SchedulerFeature {
    fn default() -> Self {
        Self::new()
    }
} 