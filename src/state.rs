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
    pub stage1_h: SyncHistogram<u64>,
    pub stage2_h: SyncHistogram<u64>,
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
        let p_histogram = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3).unwrap();
        let p_histogram = SyncHistogram::from(p_histogram);
        let c_histogram = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3).unwrap();
        let c_histogram = SyncHistogram::from(c_histogram);
        let stage1_h = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3).unwrap();
        let stage1_h = SyncHistogram::from(stage1_h);
        let stage2_h = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3).unwrap();
        let stage2_h = SyncHistogram::from(stage2_h);

        let total_messages = Arc::new(AtomicU64::new(0));
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
            stage1_h,
            stage2_h,
            total_messages,
            total_handled_messages,
            zero_messages,
            handled_zero_messages,
            clock,
            start_ts,
            core_ids,
        }
    }

    pub fn print_histogram(&mut self) {
        print_histogram(&mut self.stage1_h, "stage1");
        print_histogram(&mut self.p_histogram, "process");
        print_histogram(&mut self.stage2_h, "stage2");
        print_histogram(&mut self.c_histogram, "total");

        let total_messages = self.total_messages.load(Ordering::SeqCst);
        let total_handled_messages = self.total_handled_messages.load(Ordering::SeqCst);
        info!(total_messages, total_handled_messages);

        let zero_messages = self.zero_messages.load(Ordering::SeqCst);
        let handled_zero_messages = self.handled_zero_messages.load(Ordering::SeqCst);
        info!(zero_messages, handled_zero_messages);
    }
}

fn print_histogram(h: &mut SyncHistogram<u64>, name: &str) {
    h.refresh();
    let p50 = (h.value_at_quantile(0.5) as f64) / 1000.00;
    let p90 = (h.value_at_quantile(0.9) as f64) / 1000.00;
    let p99 = (h.value_at_quantile(0.99) as f64) / 1000.00;
    let p999 = (h.value_at_quantile(0.999) as f64) / 1000.00;
    let max = (h.max() as f64) / 1000.00;
    info!(p50, p90, p99, p999, max, name);
}
