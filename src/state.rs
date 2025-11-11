use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use core_affinity::CoreId;
use hdrhistogram::{Histogram, SyncHistogram};
use quanta::Clock;

pub struct State {
    pub p_histogram: SyncHistogram<u64>,
    pub c_histogram: SyncHistogram<u64>,
    pub total_messages: Arc<AtomicU64>,
    pub total_handled_messages: Arc<AtomicU64>,
    pub zero_messages: Arc<AtomicU64>,
    pub handled_zero_messages: Arc<AtomicU64>,
    pub clock: Clock,
    pub start_ts: u64,
    pub core_ids: Vec<CoreId>,
}

impl State {
    pub fn new() -> Self {
        let p_histogram = Histogram::<u64>::new_with_bounds(100, 10_000_000, 3).unwrap();
        let p_histogram = SyncHistogram::from(p_histogram);
        let c_histogram = Histogram::<u64>::new_with_bounds(100, 10_000_000, 3).unwrap();
        let c_histogram = SyncHistogram::from(c_histogram);

        let totoal_messages = Arc::new(AtomicU64::new(0));
        let total_handled_messages = Arc::new(AtomicU64::new(0));

        let zero_messages = Arc::new(AtomicU64::new(0));
        let handled_zero_messages = Arc::new(AtomicU64::new(0));

        let clock = Clock::new();
        let start_ts = clock.raw();

        let core_ids = core_affinity::get_core_ids().unwrap();
        debug!(?core_ids);

        State {
            p_histogram,
            c_histogram,
            total_messages: totoal_messages,
            total_handled_messages,
            zero_messages,
            handled_zero_messages,
            clock,
            start_ts,
            core_ids,
        }
    }

    pub fn print_histogram(&mut self) {
        self.p_histogram.refresh();
        self.c_histogram.refresh();
        let p50 = (self.p_histogram.value_at_quantile(0.5) as f64) / 1000.00;
        let p90 = (self.p_histogram.value_at_quantile(0.9) as f64) / 1000.00;
        let p99 = (self.p_histogram.value_at_quantile(0.99) as f64) / 1000.00;
        let p999 = (self.p_histogram.value_at_quantile(0.999) as f64) / 1000.00;
        let max = (self.p_histogram.max() as f64) / 1000.00;
        info!(p50, p90, p99, p999, max, "process");
        let p50 = (self.c_histogram.value_at_quantile(0.5) as f64) / 1000.00;
        let p90 = (self.c_histogram.value_at_quantile(0.9) as f64) / 1000.00;
        let p99 = (self.c_histogram.value_at_quantile(0.99) as f64) / 1000.00;
        let p999 = (self.c_histogram.value_at_quantile(0.999) as f64) / 1000.00;
        let max = (self.c_histogram.max() as f64) / 1000.00;
        info!(p50, p90, p99, p999, max, "stage");

        let totoal_messages = self.total_messages.load(Ordering::SeqCst);
        let total_handled_messages = self.total_handled_messages.load(Ordering::SeqCst);
        info!(totoal_messages, total_handled_messages);

        let zero_messages = self.zero_messages.load(Ordering::SeqCst);
        let handled_zero_messages = self.handled_zero_messages.load(Ordering::SeqCst);
        info!(zero_messages, handled_zero_messages);
    }
}
