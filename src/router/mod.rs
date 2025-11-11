use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use hdrhistogram::{Histogram, SyncHistogram};
use itertools::Itertools;
use quanta::Clock;

use crate::{
    config::Config,
    processor::ProcessorWorker,
    producer::ProducerWorker,
    router::{stage1::Stage1, stage2::Stage2},
    strategy::StrategyWorker,
};

pub mod stage1;
pub mod stage2;
pub mod traits;

pub fn run(config: Config) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules: _,
        stage2_rules: _,
    } = config.clone();

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

    let clock = Clock::new();
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

    let (stage2, strategies_cons, processors_prod) = Stage2::new(&config);
    let stage2_handle = stage2.run(core_ids.pop());

    let strategies_iter = strategies_iter.into_iter().zip(strategies_cons);

    for ((index, (id, v)), cons) in strategies_iter {
        assert_eq!(id, index as u64);
        let worker = StrategyWorker {
            id,
            receiver: cons,
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

    let (stage1, processors_cons, producers_prod) = Stage1::new(&config);
    let stage1_handle = stage1.run(core_ids.pop());

    let iter = processors_prod.into_iter().zip(processors_cons).enumerate();

    for (id, (sender, receiver)) in iter {
        let worker = ProcessorWorker {
            id: id as u64,
            processing_times: processors_processing_times.clone(),
            receiver,
            start_ts,
            clock: clock.clone(),
            sender,
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

    let distribution = c_producers
        .distribution
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
            (id, v)
        })
        .collect::<Vec<_>>();

    for (i, prod) in producers_prod.into_iter().enumerate() {
        let worker = ProducerWorker {
            id: i as u64,
            duration_secs,
            messages_per_sec: c_producers.messages_per_sec,
            distribution: distribution.clone(),
            burst_pattern: c_producers.burst_pattern.clone(),
            zero_messages_counts: zero_messages_counts.clone(),
            stage1: prod,
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
    stage1_handle.join().unwrap();
    for j in processors_handles {
        j.join().unwrap();
    }
    stage2_handle.join().unwrap();
    for j in strategies_handles {
        j.join().unwrap();
    }

    print_histogram();

    let zero_messages_counts = zero_messages_counts.load(Ordering::SeqCst);
    let handled_zero_messages_counts = handled_zero_messages_counts.load(Ordering::SeqCst);
    info!(zero_messages_counts, handled_zero_messages_counts);
    Ok(())
}
