use std::{thread, time::Duration};

use crossbeam_channel::bounded;
use itertools::Itertools;

use crate::{
    config::Config, processor::ProcessorWorker, producer::ProducerWorker,
    queue::stage1::Stage1Queue, router::stage2::Stage2, state::State, strategy::StrategyWorker,
};

pub fn run(config: Config, mut state: State) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules: _,
    } = config.clone();

    let (stage2, strategies_cons, processors_prod) = Stage2::new(
        &config,
        state.stage2_h.recorder(),
        state.start_ts,
        state.clock.clone(),
    );
    let stage2_handle = stage2.run(state.core_ids.pop());

    let strategies_handles = c_strategies
        .parse()
        .into_iter()
        .zip(strategies_cons)
        .map(|((index, (id, v)), cons)| {
            assert_eq!(id, index as u64);
            let worker = StrategyWorker {
                id,
                receiver: cons,
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

    let mut processors_queues = Vec::with_capacity(c_processors.count as usize);

    const PROCESSORS_QUEUE_CAP: usize = 20_000_000;
    let iter = processors_prod.into_iter().enumerate();

    let processors_handles = iter
        .map(|(id, sender)| {
            let (s, r) = bounded(PROCESSORS_QUEUE_CAP);
            processors_queues.push(s);
            let worker = ProcessorWorker {
                id: id as u64,
                processing_times: processors_processing_times.clone(),
                receiver: r,
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
                zero_messages: state.zero_messages.clone(),
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
    stage2_handle.join().unwrap();
    for j in strategies_handles {
        j.join().unwrap();
    }

    state.print_histogram();
    Ok(())
}
