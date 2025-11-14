use std::thread::{self, JoinHandle};

use core_affinity::CoreId;
use hdrhistogram::sync::Recorder;
use quanta::Clock;
use rtrb::{Consumer, Producer, RingBuffer};

use crate::{Arr, config::Config, types::Message};

pub struct Stage1 {
    pub processor_prod: Arr<Producer<Message>>,
    pub producer_cons: Arr<Consumer<Message>>,
    pub msg_type_to_processor: [u64; 8],
    pub histogram: Recorder<u64>,
    start_ts: u64,
    clock: Clock,
}

const CAPACITY: usize = 2usize.pow(16);

impl Stage1 {
    pub fn new(
        config: &Config,
        histogram: Recorder<u64>,
        start_ts: u64,
        clock: Clock,
    ) -> (Self, Arr<Consumer<Message>>, Arr<Producer<Message>>) {
        let (producer_prod, producer_cons): (Arr<_>, Arr<_>) = (0..config.producers.count)
            .map(|_| RingBuffer::<Message>::new(CAPACITY))
            .unzip();
        let (processor_prod, processor_cons): (Arr<_>, Arr<_>) = (0..config.processors.count)
            .map(|_| RingBuffer::<Message>::new(CAPACITY))
            .unzip();

        // Build O(1) lookup table: msg_type -> processor_id
        let mut msg_type_to_processor = [0u64; 8];
        for rule in config.stage1_rules.iter() {
            msg_type_to_processor[rule.msg_type as usize] = *rule.processors.first().unwrap();
        }

        let s = Self {
            msg_type_to_processor,
            processor_prod,
            producer_cons,
            histogram,
            start_ts,
            clock,
        };
        (s, processor_cons, producer_prod)
    }

    pub fn run(self, core_id: Option<CoreId>) -> JoinHandle<()> {
        thread::Builder::new()
            .name("stage1".to_string())
            .spawn(move || {
                if let Some(cid) = core_id {
                    let res = core_affinity::set_for_current(cid);
                    if !res {
                        warn!("cannot pin {cid:?} thread for stage1");
                    }
                }
                let Self {
                    mut processor_prod,
                    mut producer_cons,
                    msg_type_to_processor,
                    mut histogram,
                    start_ts,
                    clock,
                } = self;
                let mut len = producer_cons.len();
                'a: loop {
                    let iter = producer_cons.iter_mut();
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
                            let processor = msg_type_to_processor[msg.ty as usize];
                            let sender = &mut processor_prod[processor as usize];
                            sender.push(msg).unwrap();
                            let timestamp = clock.delta_as_nanos(start_ts, clock.raw());
                            histogram.saturating_record(timestamp - msg.timestamp);
                        }
                    }
                }
                info!("stage1 stopped");
            })
            .unwrap()
    }
}
