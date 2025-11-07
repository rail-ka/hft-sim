use crossbeam_channel::Receiver;
use quanta::Clock;

use crate::types::{HandledMesage, Message};

pub type SrategyReceiver = Receiver<HandledMesage>;

pub struct StrategyWorker {
    pub id: u64,
    pub receiver: SrategyReceiver,
    pub processing_time: u64,
}

impl StrategyWorker {
    pub fn run(self) {
        let Self {
            id,
            receiver,
            processing_time,
        } = self;
        let mut seq_arr = [[0u64; 8]; 8];
        let clock = Clock::new();
        let raw_start = clock.raw();

        let mut errors = 0usize;
        let mut prev = clock.raw();

        while let Ok(item) = receiver.recv() {
            let HandledMesage {
                msg:
                    Message {
                        ty,
                        producer_id,
                        seq,
                        timestamp,
                    },
                processor_id,
                processing_ts,
            } = item;

            let last = &mut seq_arr[producer_id as usize][ty as usize];
            if *last > seq {
                errors += 1;
            }
            *last = seq;

            let mut raw = clock.raw();
            let mut delta = clock.delta_as_nanos(prev, raw);

            while delta < processing_time {
                std::hint::spin_loop();
                raw = clock.raw();
                delta = clock.delta_as_nanos(prev, raw);
            }
            prev = raw;
        }
        if errors != 0 {
            let fmt = human_format::Formatter::new();
            error!(id, "{}", fmt.format(errors as f64));
        }
    }
}
