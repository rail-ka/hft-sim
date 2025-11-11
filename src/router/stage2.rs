use std::thread::{self, JoinHandle};

use crate::{
    Arr,
    config::{Config, Stage2Rule},
    types::HandledMessage,
};

use core_affinity::CoreId;
use hdrhistogram::sync::Recorder;
use quanta::Clock;
// use ringbuf::{
//     HeapRb,
//     traits::{Consumer as RConsumer, Observer, Split},
// };
use rtrb::{Consumer, Producer, RingBuffer};

pub struct Stage2 {
    pub processor_cons: Arr<Consumer<HandledMessage>>,
    pub strategy_prod: Arr<Producer<HandledMessage>>,
    pub stage2_rules: Arr<Stage2Rule>,
    pub histogram: Recorder<u64>,
    start_ts: u64,
    clock: Clock,
}

impl Stage2 {
    pub fn new(
        config: &Config,
        histogram: Recorder<u64>,
        start_ts: u64,
        clock: Clock,
    ) -> (
        Self,
        Arr<Consumer<HandledMessage>>,
        Arr<Producer<HandledMessage>>,
    ) {
        let (strategy_prod, strategy_cons): (Arr<_>, Arr<_>) = (0..config.strategies.count)
            .map(|_| RingBuffer::<HandledMessage>::new(10_000_000))
            .unzip();

        let (processor_prod, processor_cons): (Arr<_>, Arr<_>) = (0..config.processors.count)
            .map(|_| RingBuffer::<HandledMessage>::new(4_000_000))
            .unzip();

        let s = Self {
            processor_cons,
            strategy_prod,
            stage2_rules: config.stage2_rules.iter().cloned().collect(),
            histogram,
            start_ts,
            clock,
        };
        (s, strategy_cons, processor_prod)
    }

    pub fn run(self, core_id: Option<CoreId>) -> JoinHandle<()> {
        thread::Builder::new()
            .name("stage2".to_string())
            .spawn(move || {
                if let Some(cid) = core_id {
                    let res = core_affinity::set_for_current(cid);
                    if !res {
                        warn!("cannot pin {cid:?} thread for stage2");
                    }
                }
                let Self {
                    mut processor_cons,
                    mut strategy_prod,
                    stage2_rules,
                    mut histogram,
                    start_ts,
                    clock,
                } = self;
                // let (p, mut c) = HeapRb::<HandledMessage>::new(1000_000).split();
                let mut len = processor_cons.len();
                'a: loop {
                    let iter = processor_cons.iter_mut();
                    for cons in iter {
                        if cons.is_empty() {
                            if cons.is_abandoned() {
                                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
                                len -= 1;
                                if len == 0 {
                                    break 'a;
                                }
                            }
                            continue;
                        }
                        let slots = cons.slots();
                        for _ in 0..slots {
                            let msg = cons.pop().unwrap();
                            let strategy = stage2_rules
                                .iter()
                                .find(|i| i.msg_type == msg.msg.ty)
                                .unwrap()
                                .strategy;
                            let strategy_sender = &mut strategy_prod[strategy as usize];
                            strategy_sender.push(msg).unwrap();
                            let timestamp = clock.delta_as_nanos(start_ts, clock.raw());
                            histogram.saturating_record(timestamp - msg.processing_ts);
                        }
                    }
                }
                info!("stage2 stopped");
            })
            .unwrap()
    }
}
