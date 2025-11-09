use std::{
    env,
    fs::{self, File},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

mod config;
mod log;
mod processor;
mod producer;
mod stage1;
mod stage2;
mod strategy;
mod types;
mod utils;

#[macro_use]
extern crate tracing;

// use crossbeam_channel::bounded;
use itertools::Itertools;
use kanal::bounded;

use crate::{
    config::Config, processor::ProcessorWorker, producer::ProducerWorker, strategy::StrategyWorker,
};

fn main() {
    log::init();

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "pthread"])
        .build()
        .unwrap();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        error!("Usage: {} <config_path>", args[0]);
        std::process::exit(1);
    }
    info!("args: 0: {}, 1: {}", &args[0], &args[1]);
    let config_path = &args[1];
    let config_path = PathBuf::from(config_path.as_str()).canonicalize().unwrap();
    info!("config_path: {}", config_path.to_string_lossy());

    let config_data = fs::read_to_string(&config_path).unwrap_or_else(|err| {
        error!("Failed to read config file {:?}: {}", config_path, err);
        std::process::exit(1);
    });

    let config: config::Config = serde_json::from_str(&config_data).unwrap_or_else(|err| {
        error!("Failed to parse config: {}", err);
        std::process::exit(1);
    });

    let Config {
        scenario,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules,
    } = config;

    info!("Loaded config for scenario: {scenario}");

    let mut core_ids = core_affinity::get_core_ids().unwrap();
    debug!(?core_ids);
    core_ids.reverse();

    let zero_messages_counts = Arc::new(AtomicU64::new(0));
    let handled_zero_messages_counts = Arc::new(AtomicU64::new(0));

    let mut producers_handles = Vec::with_capacity(c_producers.count as usize);
    let mut processors_handles = Vec::with_capacity(c_processors.count as usize);
    let mut strategies_handles = Vec::with_capacity(c_strategies.count as usize);

    if c_strategies.count as usize != c_strategies.processing_times_ns.len() {
        panic!("strategies count error");
    }

    let mut strategies_queues = Vec::with_capacity(c_strategies.count as usize);

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

    const STRATEGIES_QUEUE_CAP: usize = 40_000_000;

    for (index, (id, v)) in strategies_iter {
        assert_eq!(id, index as u64);
        let (s, r) = bounded(STRATEGIES_QUEUE_CAP);
        // let (s, r) = unbounded();
        strategies_queues.push(s);
        let worker = StrategyWorker {
            id,
            receiver: r,
            processing_time: v,
            handled_zero_messages_counts: handled_zero_messages_counts.clone(),
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

    let strategies_queues = Arc::new(strategies_queues);

    const PROCESSORS_QUEUE_CAP: usize = 40_000_000;

    for id in 0..c_processors.count {
        let (s, r) = bounded(PROCESSORS_QUEUE_CAP);
        // let (s, r) = unbounded();
        processors_queues.push(s);
        let worker = ProcessorWorker {
            id,
            processing_times: processors_processing_times.clone(),
            strategies: strategies_queues.clone(),
            receiver: r,
            stage2_rules: stage2_rules.clone(),
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

    // let mut stage1_queues = stage1_rules
    //     .into_iter()
    //     .map(|rule| {
    //         let processors = rule
    //             .processors
    //             .into_iter()
    //             .map(|id| {
    //                 let queue = ArrayQueue::<Message>::new(512 * 512);
    //                 ProcessorIdQueue { id, queue }
    //             })
    //             .collect();
    //         Stage1Item {
    //             msg_type: rule.msg_type,
    //             processors,
    //         }
    //     })
    //     .collect::<Vec<_>>();
    // let mut stage2_queues = stage2_rules
    //     .into_iter()
    //     .map(|rule| {
    //         rule.strategy;
    //         rule.msg_type;
    //         rule.ordering_required;
    //         let queue = ArrayQueue::<HandledMesage>::new(512 * 512);
    //         queue
    //     })
    //     .collect::<Vec<_>>();

    // let stage1 = Stage1 { rules: Vec::new() };
    // let stage1 = Arc::new(stage1);

    let distribution = c_producers
        .distribution
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
            (id, v)
        })
        .collect::<Vec<_>>();

    let processors_queues = Arc::new(processors_queues);

    for i in 0..c_producers.count {
        // let stage = stage1.clone();
        let worker = ProducerWorker {
            id: i,
            duration_secs,
            messages_per_sec: c_producers.messages_per_sec,
            distribution: distribution.clone(),
            burst_pattern: c_producers.burst_pattern.clone(),
            processors: processors_queues.clone(),
            stage1_rules: stage1_rules.clone(),
            zero_messages_counts: zero_messages_counts.clone(),
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
    drop(processors_queues);

    for j in producers_handles {
        j.join().unwrap();
    }
    for j in processors_handles {
        j.join().unwrap();
    }
    for j in strategies_handles {
        j.join().unwrap();
    }

    let zero_messages_counts = zero_messages_counts.load(Ordering::SeqCst);
    let handled_zero_messages_counts = handled_zero_messages_counts.load(Ordering::SeqCst);
    info!(zero_messages_counts, handled_zero_messages_counts);

    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
}
