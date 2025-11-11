use std::{thread, time::Duration};

use crossbeam_channel::bounded;
use itertools::Itertools;

use crate::{
    config::Config, processor::ProcessorWorker, producer::ProducerWorker,
    queue::traits::QueueProcessorSender, router::stage1::Stage1, state::State,
    strategy::StrategyWorker,
};

pub fn run(config: Config, mut state: State) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules: _,
        stage2_rules,
    } = config.clone();

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
                handled_zero_messages: state.handled_zero_messages.clone(),
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
    let (stage1, processors_cons, producers_prod) = Stage1::new(
        &config,
        state.stage1_h.recorder(),
        state.start_ts,
        state.clock.clone(),
    );
    let stage1_handle = stage1.run(state.core_ids.pop());
    let iter = processors_cons.into_iter().enumerate();

    let processors_handles = iter
        .map(|(id, receiver)| {
            let sender = QueueProcessorSender {
                queues: strategies_queues.iter().cloned().collect(),
                rules: stage2_rules.clone(),
            };
            let worker = ProcessorWorker {
                id: id as u64,
                processing_times: processors_processing_times.clone(),
                receiver,
                start_ts: state.start_ts,
                clock: state.clock.clone(),
                sender,
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

    let producers_handles = producers_prod
        .into_iter()
        .enumerate()
        .map(|(i, prod)| {
            let worker = ProducerWorker {
                id: i as u64,
                duration_secs,
                messages_per_sec: c_producers.messages_per_sec,
                distribution: distribution.clone(),
                burst_pattern: c_producers.burst_pattern.clone(),
                zero_messages: state.zero_messages.clone(),
                stage1: prod,
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

    for sec in 0..config.duration_secs {
        std::thread::sleep(Duration::from_secs(1));
        info!(sec);
        state.print_histogram();
    }

    for j in producers_handles {
        j.join().unwrap();
    }
    stage1_handle.join().unwrap();
    for j in processors_handles {
        j.join().unwrap();
    }
    for j in strategies_handles {
        j.join().unwrap();
    }

    state.print_histogram();
    Ok(())
}
