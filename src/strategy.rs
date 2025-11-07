use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crossbeam_channel::Receiver;
use quanta::Clock;

use crate::types::{HandledMesage, Message};

pub type SrategyReceiver = Receiver<HandledMesage>;

pub struct StrategyWorker {
    pub id: u64,
    pub receiver: SrategyReceiver,
    pub processing_time: u64,
    pub handled_zero_messages_counts: Arc<AtomicU64>,
}

impl StrategyWorker {
    pub fn run(self) {
        let Self {
            id,
            receiver,
            processing_time,
            handled_zero_messages_counts,
        } = self;
        let mut seq_arr = [[0u64; 8]; 8];
        let clock = Clock::new();
        let raw_start = clock.raw();

        let mut errors = 0usize;
        let mut prev = clock.raw();
        let mut total_msg = 0usize;
        let mut total_zero_msgs = 0u64;
        let mut nanos_per_sec = 0u64;

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
            total_msg += 1;
            if ty == 0 {
                total_zero_msgs += 1;
            }

            if nanos_per_sec < 1_000_000_000 {
                nanos_per_sec += delta;
            } else {
                nanos_per_sec = 0;
                let channel_len = receiver.len();
                info!(id, channel_len, total_msg, total_zero_msgs);
            }
        }
        if errors != 0 {
            let fmt = human_format::Formatter::new();
            error!(id, "{}", fmt.format(errors as f64));
        }
        handled_zero_messages_counts.fetch_add(total_zero_msgs, Ordering::SeqCst);
        info!(id, total_msg, total_zero_msgs);
    }
}
