use std::thread::{self, JoinHandle};

use crate::{Arr, config::Config, types::HandledMessage};

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
    pub msg_type_to_strategy: [u64; 8],
    pub histogram: Recorder<u64>,
    start_ts: u64,
    clock: Clock,
}

const CAPACITY: usize = 2usize.pow(16);

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
            .map(|_| RingBuffer::<HandledMessage>::new(CAPACITY))
            .unzip();

        let (processor_prod, processor_cons): (Arr<_>, Arr<_>) = (0..config.processors.count)
            .map(|_| RingBuffer::<HandledMessage>::new(CAPACITY))
            .unzip();

        // Build O(1) lookup table: msg_type -> strategy_id
        let mut msg_type_to_strategy = [0u64; 8];
        for rule in config.stage2_rules.iter() {
            msg_type_to_strategy[rule.msg_type as usize] = rule.strategy;
        }

        let s = Self {
            processor_cons,
            strategy_prod,
            msg_type_to_strategy,
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
                    msg_type_to_strategy,
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
                            let strategy = msg_type_to_strategy[msg.msg.ty as usize];
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
