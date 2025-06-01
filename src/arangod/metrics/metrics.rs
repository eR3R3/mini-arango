use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Registry for metrics collection
pub struct MetricsRegistry {
    counters: Arc<RwLock<HashMap<String, Counter>>>,
    gauges: Arc<RwLock<HashMap<String, Gauge>>>,
    histograms: Arc<RwLock<HashMap<String, Histogram>>>,
    timers: Arc<RwLock<HashMap<String, Timer>>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        MetricsRegistry {
            counters: Arc::new(RwLock::new(HashMap::new())),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
            timers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Increment a counter
    pub fn increment_counter(&self, name: &str, value: u64) {
        let mut counters = self.counters.write();
        let counter = counters.entry(name.to_string()).or_insert_with(Counter::new);
        counter.increment(value);
    }
    
    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: f64) {
        let mut gauges = self.gauges.write();
        let gauge = gauges.entry(name.to_string()).or_insert_with(Gauge::new);
        gauge.set(value);
    }
    
    /// Record a histogram value
    pub fn record_histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write();
        let histogram = histograms.entry(name.to_string()).or_insert_with(Histogram::new);
        histogram.record(value);
    }
    
    /// Start a timer
    pub fn start_timer(&self, name: &str) -> TimerHandle {
        let mut timers = self.timers.write();
        let timer = timers.entry(name.to_string()).or_insert_with(Timer::new);
        timer.start()
    }
    
    /// Get counter value
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters.read().get(name).map(|c| c.value())
    }
    
    /// Get gauge value
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.read().get(name).map(|g| g.value())
    }
    
    /// Get all metrics as a snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let counters = self.counters.read();
        let gauges = self.gauges.read();
        let histograms = self.histograms.read();
        let timers = self.timers.read();
        
        MetricsSnapshot {
            counters: counters.iter().map(|(k, v)| (k.clone(), v.value())).collect(),
            gauges: gauges.iter().map(|(k, v)| (k.clone(), v.value())).collect(),
            histograms: histograms.iter().map(|(k, v)| (k.clone(), v.snapshot())).collect(),
            timers: timers.iter().map(|(k, v)| (k.clone(), v.snapshot())).collect(),
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Counter metric
#[derive(Debug, Clone)]
pub struct Counter {
    value: u64,
}

impl Counter {
    pub fn new() -> Self {
        Counter { value: 0 }
    }
    
    pub fn increment(&mut self, value: u64) {
        self.value += value;
    }
    
    pub fn value(&self) -> u64 {
        self.value
    }
}

/// Gauge metric
#[derive(Debug, Clone)]
pub struct Gauge {
    value: f64,
}

impl Gauge {
    pub fn new() -> Self {
        Gauge { value: 0.0 }
    }
    
    pub fn set(&mut self, value: f64) {
        self.value = value;
    }
    
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// Histogram metric
#[derive(Debug, Clone)]
pub struct Histogram {
    values: Vec<f64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    pub fn new() -> Self {
        Histogram {
            values: Vec::new(),
            sum: 0.0,
            count: 0,
        }
    }
    
    pub fn record(&mut self, value: f64) {
        self.values.push(value);
        self.sum += value;
        self.count += 1;
        
        // Keep only the last 1000 values to prevent memory growth
        if self.values.len() > 1000 {
            self.values.remove(0);
        }
    }
    
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut sorted_values = self.values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let percentile = |p: f64| -> f64 {
            if sorted_values.is_empty() {
                return 0.0;
            }
            let index = ((sorted_values.len() as f64 - 1.0) * p / 100.0).round() as usize;
            sorted_values[index.min(sorted_values.len() - 1)]
        };
        
        HistogramSnapshot {
            count: self.count,
            sum: self.sum,
            mean: if self.count > 0 { self.sum / self.count as f64 } else { 0.0 },
            p50: percentile(50.0),
            p90: percentile(90.0),
            p95: percentile(95.0),
            p99: percentile(99.0),
        }
    }
}

/// Timer metric
#[derive(Debug, Clone)]
pub struct Timer {
    measurements: Vec<Duration>,
}

impl Timer {
    pub fn new() -> Self {
        Timer {
            measurements: Vec::new(),
        }
    }
    
    pub fn start(&mut self) -> TimerHandle {
        TimerHandle {
            start_time: Instant::now(),
        }
    }
    
    pub fn record(&mut self, duration: Duration) {
        self.measurements.push(duration);
        
        // Keep only the last 1000 measurements
        if self.measurements.len() > 1000 {
            self.measurements.remove(0);
        }
    }
    
    pub fn snapshot(&self) -> TimerSnapshot {
        let mut sorted_durations = self.measurements.clone();
        sorted_durations.sort();
        
        let percentile = |p: f64| -> Duration {
            if sorted_durations.is_empty() {
                return Duration::default();
            }
            let index = ((sorted_durations.len() as f64 - 1.0) * p / 100.0).round() as usize;
            sorted_durations[index.min(sorted_durations.len() - 1)]
        };
        
        let total: Duration = sorted_durations.iter().sum();
        let count = sorted_durations.len() as u64;
        
        TimerSnapshot {
            count,
            sum: total,
            mean: if count > 0 { total / count as u32 } else { Duration::default() },
            p50: percentile(50.0),
            p90: percentile(90.0),
            p95: percentile(95.0),
            p99: percentile(99.0),
        }
    }
}

/// Timer handle that automatically records duration when dropped
pub struct TimerHandle {
    start_time: Instant,
}

impl TimerHandle {
    pub fn stop(self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Snapshot of all metrics
#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
    pub histograms: HashMap<String, HistogramSnapshot>,
    pub timers: HashMap<String, TimerSnapshot>,
}

/// Histogram snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

/// Timer snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerSnapshot {
    pub count: u64,
    #[serde(with = "duration_serde")]
    pub sum: Duration,
    #[serde(with = "duration_serde")]
    pub mean: Duration,
    #[serde(with = "duration_serde")]
    pub p50: Duration,
    #[serde(with = "duration_serde")]
    pub p90: Duration,
    #[serde(with = "duration_serde")]
    pub p95: Duration,
    #[serde(with = "duration_serde")]
    pub p99: Duration,
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_nanos().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let nanos = u128::deserialize(deserializer)?;
        Ok(Duration::from_nanos(nanos as u64))
    }
} 