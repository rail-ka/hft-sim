use std::sync::{Arc, atomic::AtomicU64};

use core_affinity::CoreId;
use hdrhistogram::{Histogram, SyncHistogram};
use quanta::Clock;

#[derive(Clone, Default, Debug)]
pub struct MessagesCounter {
    pub total: Arc<AtomicU64>,
    pub zero: Arc<AtomicU64>,
}

pub struct State {
    pub p_histogram: SyncHistogram<u64>,
    pub c_histogram: SyncHistogram<u64>,
    pub stage1_h: SyncHistogram<u64>,
    pub stage2_h: SyncHistogram<u64>,
    pub produced: MessagesCounter,
    pub processed: MessagesCounter,
    pub delivered: MessagesCounter,
    pub clock: Clock,
    pub start_ts: u64,
    pub core_ids: Vec<CoreId>,
}

impl State {
    pub fn new() -> Self {
        let p_histogram = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3)
            .unwrap()
            .into();
        let c_histogram = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3)
            .unwrap()
            .into();
        let stage1_h = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3)
            .unwrap()
            .into();
        let stage2_h = Histogram::<u64>::new_with_bounds(200, 1_000_000, 3)
            .unwrap()
            .into();

        let clock = Clock::new();
        let start_ts = clock.raw();

        let core_ids = core_affinity::get_core_ids().unwrap();
        debug!(?core_ids);

        State {
            p_histogram,
            c_histogram,
            stage1_h,
            stage2_h,
            produced: MessagesCounter::default(),
            processed: MessagesCounter::default(),
            delivered: MessagesCounter::default(),
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
        info!(?self.produced, ?self.processed, ?self.delivered);
    }
}

fn round_value(value: u64, decimals: u32) -> f64 {
    let scaled = 10.0_f64.powi(decimals as i32);
    ((value as f64 / 1000.0) * scaled).round() / scaled
}

fn print_histogram(h: &mut SyncHistogram<u64>, name: &str) {
    h.refresh();
    let p50 = round_value(h.value_at_quantile(0.5), 2);
    let p90 = round_value(h.value_at_quantile(0.9), 2);
    let p99 = round_value(h.value_at_quantile(0.99), 2);
    let p999 = round_value(h.value_at_quantile(0.999), 1);
    let max = round_value(h.max(), 1);
    info!(p50, p90, p99, p999, max, name);
}
