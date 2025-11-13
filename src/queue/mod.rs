use std::{thread, time::Duration};

use crossbeam_channel::bounded;
use itertools::Itertools;

use crate::{
    config::Config,
    processor::ProcessorWorker,
    producer::ProducerWorker,
    queue::{stage1::Stage1Queue, traits::QueueProcessorSender},
    state::State,
    strategy::StrategyWorker,
};

pub mod stage1;
pub mod stage2;
pub mod traits;

pub fn run(config: Config, mut state: State) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules,
    } = config;

    let mut strategies_queues = Vec::with_capacity(c_strategies.count as usize);

    const STRATEGIES_QUEUE_CAP: usize = 30_000_000;

    let strategies_handles = c_strategies
        .parse()
        .into_iter()
        .map(|(index, (id, v))| {
            assert_eq!(id, index as u64);
            let (s, r) = bounded(STRATEGIES_QUEUE_CAP);
            strategies_queues.push(s);
            let worker = StrategyWorker {
                id,
                receiver: r,
                processing_time: v,
                message_count: state.delivered.clone(),
                start_ts: state.start_ts,
                clock: state.clock.clone(),
                p_histogram: state.p_histogram.recorder(),
                c_histogram: state.c_histogram.recorder(),
            };
            let cid = state.core_ids.pop();
            thread::Builder::new()
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
                .unwrap()
        })
        .collect_vec();

    let processors_processing_times = c_processors.parse();

    let mut processors_queues = Vec::with_capacity(c_processors.count as usize);

    const PROCESSORS_QUEUE_CAP: usize = 20_000_000;

    let processors_handles = (0..c_processors.count)
        .map(|id| {
            let (s, r) = bounded(PROCESSORS_QUEUE_CAP);
            processors_queues.push(s);
            let sender = QueueProcessorSender {
                queues: strategies_queues.iter().cloned().collect(),
                rules: stage2_rules.clone(),
            };
            let worker = ProcessorWorker {
                id,
                message_count: state.processed.clone(),
                receiver: r,
                start_ts: state.start_ts,
                clock: state.clock.clone(),
                sender,
                processing_times: processors_processing_times.iter().cloned().collect(),
            };
            let cid = state.core_ids.pop();
            thread::Builder::new()
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
                .unwrap()
        })
        .collect_vec();
    drop(strategies_queues);

    let distribution = c_producers.distribution_parse();

    let stage1 = Stage1Queue::new(processors_queues, stage1_rules);

    let producers_handles = (0..c_producers.count)
        .map(|i| {
            let worker = ProducerWorker {
                id: i,
                duration_secs,
                messages_per_sec: c_producers.messages_per_sec,
                distribution: distribution.clone(),
                burst_pattern: c_producers.burst_pattern.clone(),
                message_count: state.produced.clone(),
                stage1: stage1.clone(),
                start_ts: state.start_ts,
                clock: state.clock.clone(),
            };
            let cid = state.core_ids.pop();
            thread::Builder::new()
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
                .unwrap()
        })
        .collect_vec();
    drop(stage1);

    for sec in 0..config.duration_secs {
        std::thread::sleep(Duration::from_secs(1));
        info!(sec);
        state.print_histogram();
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

    state.print_histogram();
    Ok(())
}
