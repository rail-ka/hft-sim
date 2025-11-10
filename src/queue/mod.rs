use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use crossbeam_channel::bounded;
use hdrhistogram::{Histogram, SyncHistogram};
use itertools::Itertools;
use quanta::Clock;

use crate::{
    config::Config, processor::ProcessorWorker, producer::ProducerWorker, stage1::Stage1,
    strategy::StrategyWorker,
};

pub fn run(config: Config) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules,
    } = config;

    let mut core_ids = core_affinity::get_core_ids().unwrap();
    debug!(?core_ids);
    core_ids.reverse();

    let p_histogram = Histogram::<u64>::new_with_bounds(100, 10_000_000, 3).unwrap();
    let mut p_histogram = SyncHistogram::from(p_histogram);
    let c_histogram = Histogram::<u64>::new_with_bounds(100, 10_000_000, 3).unwrap();
    let mut c_histogram = SyncHistogram::from(c_histogram);

    let zero_messages_counts = Arc::new(AtomicU64::new(0));
    let handled_zero_messages_counts = Arc::new(AtomicU64::new(0));

    let mut producers_handles = Vec::with_capacity(c_producers.count as usize);
    let mut processors_handles = Vec::with_capacity(c_processors.count as usize);
    let mut strategies_handles = Vec::with_capacity(c_strategies.count as usize);
    let mut strategies_queues = Vec::with_capacity(c_strategies.count as usize);

    let clock = Clock::new();
    // let start_ts = crate::utils::timestamp();
    let start_ts = clock.raw();

    let strategies_iter = c_strategies
        .processing_times_ns
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("strategy_").parse().unwrap();
            (id, v)
        })
        .sorted_unstable_by_key(|(id, _)| *id)
        .enumerate()
        .collect_vec();
    debug!(?strategies_iter);

    const STRATEGIES_QUEUE_CAP: usize = 30_000_000;

    for (index, (id, v)) in strategies_iter {
        assert_eq!(id, index as u64);
        let (s, r) = bounded(STRATEGIES_QUEUE_CAP);
        strategies_queues.push(s);
        let worker = StrategyWorker {
            id,
            receiver: r,
            processing_time: v,
            handled_zero_messages_counts: handled_zero_messages_counts.clone(),
            start_ts,
            clock: clock.clone(),
            p_histogram: p_histogram.recorder(),
            c_histogram: c_histogram.recorder(),
        };
        let cid = core_ids.pop();
        let handle = thread::Builder::new()
            .name(format!("strategy_{id}"))
            .spawn(move || {
                if let Some(cid) = cid {
                    let res = core_affinity::set_for_current(cid);
                    if !res {
                        warn!("cannot pin {cid:?} thread for strategy: {id}");
                    }
                }
                worker.run();
            })
            .unwrap();
        strategies_handles.push(handle);
    }

    let processors_processing_times = c_processors
        .processing_times_ns
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
            (id, v)
        })
        .sorted_unstable_by_key(|(id, _)| *id)
        .collect::<Vec<_>>();

    let mut processors_queues = Vec::with_capacity(c_processors.count as usize);

    const PROCESSORS_QUEUE_CAP: usize = 20_000_000;

    for id in 0..c_processors.count {
        let (s, r) = bounded(PROCESSORS_QUEUE_CAP);
        processors_queues.push(s);
        let worker = ProcessorWorker {
            id,
            processing_times: processors_processing_times.clone(),
            strategies: strategies_queues.clone(),
            receiver: r,
            stage2_rules: stage2_rules.clone(),
            start_ts,
            clock: clock.clone(),
        };
        let cid = core_ids.pop();
        let handle = thread::Builder::new()
            .name(format!("processor_{id}"))
            .spawn(move || {
                if let Some(cid) = cid {
                    let res = core_affinity::set_for_current(cid);
                    if !res {
                        warn!("cannot pin {cid:?} thread for processor: {id}");
                    }
                }
                worker.run();
            })
            .unwrap();
        processors_handles.push(handle);
    }
    drop(strategies_queues);

    let distribution = c_producers
        .distribution
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
            (id, v)
        })
        .collect::<Vec<_>>();

    let stage1 = Stage1::new(processors_queues, stage1_rules);

    for i in 0..c_producers.count {
        let worker = ProducerWorker {
            id: i,
            duration_secs,
            messages_per_sec: c_producers.messages_per_sec,
            distribution: distribution.clone(),
            burst_pattern: c_producers.burst_pattern.clone(),
            zero_messages_counts: zero_messages_counts.clone(),
            stage1: stage1.clone(),
            start_ts,
            clock: clock.clone(),
        };
        let cid = core_ids.pop();
        let handle = thread::Builder::new()
            .name(format!("producer_{i}"))
            .spawn(move || {
                if let Some(cid) = cid {
                    let res = core_affinity::set_for_current(cid);
                    if !res {
                        warn!("cannot pin {cid:?} thread for producer: {i}");
                    }
                }
                worker.run();
            })
            .unwrap();
        producers_handles.push(handle);
    }
    drop(stage1);

    let mut print_histogram = || {
        p_histogram.refresh();
        c_histogram.refresh();
        let p50 = (p_histogram.value_at_quantile(0.5) as f64) / 1000.00;
        let p90 = (p_histogram.value_at_quantile(0.9) as f64) / 1000.00;
        let p99 = (p_histogram.value_at_quantile(0.99) as f64) / 1000.00;
        let p999 = (p_histogram.value_at_quantile(0.999) as f64) / 1000.00;
        let max = (p_histogram.max() as f64) / 1000.00;
        info!(p50, p90, p99, p999, max, "process");
        let p50 = (c_histogram.value_at_quantile(0.5) as f64) / 1000.00;
        let p90 = (c_histogram.value_at_quantile(0.9) as f64) / 1000.00;
        let p99 = (c_histogram.value_at_quantile(0.99) as f64) / 1000.00;
        let p999 = (c_histogram.value_at_quantile(0.999) as f64) / 1000.00;
        let max = (c_histogram.max() as f64) / 1000.00;
        info!(p50, p90, p99, p999, max, "stage");
    };

    for sec in 0..config.duration_secs {
        std::thread::sleep(Duration::from_secs(1));
        info!(sec);
        print_histogram();
    }

    for j in producers_handles {
        j.join().unwrap();
    }
    for j in processors_handles {
        j.join().unwrap();
    }
    for j in strategies_handles {
        j.join().unwrap();
    }

    print_histogram();

    let zero_messages_counts = zero_messages_counts.load(Ordering::SeqCst);
    let handled_zero_messages_counts = handled_zero_messages_counts.load(Ordering::SeqCst);
    info!(zero_messages_counts, handled_zero_messages_counts);
    Ok(())
}
