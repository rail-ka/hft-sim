use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use quanta::Clock;

use crate::{
    traits::StrategyReceiver,
    types::{HandledMessage, Message},
};

pub struct StrategyWorker<R: StrategyReceiver> {
    pub id: u64,
    pub receiver: R,
    pub processing_time: u64,
    pub handled_zero_messages_counts: Arc<AtomicU64>,
}

impl<R: StrategyReceiver> StrategyWorker<R> {
    pub fn run(self) {
        let Self {
            id,
            receiver,
            processing_time,
            handled_zero_messages_counts,
        } = self;
        let mut seq_arr = [[0u64; 8]; 8];
        let clock = Clock::new();
        let fmt = human_format::Formatter::new();

        let mut errors = 0usize;
        let mut prev = clock.raw();
        let mut total_msg = 0usize;
        let mut total_zero_msgs = 0u64;
        let mut nanos_per_sec = 0u64;

        while let Some(item) = receiver.recv() {
            let HandledMessage {
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

            let _ = processor_id;
            let _processing_time = timestamp - processing_ts;

            let last = &mut seq_arr[producer_id as usize][ty as usize];
            if *last >= seq {
                errors += 1;
            }
            *last = seq;

            let mut raw = clock.raw();
            let mut delta = clock.delta_as_nanos(prev, raw);

            while delta < processing_time {
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
                let channel_len = fmt.format(channel_len as f64);
                let total_msg = fmt.format(total_msg as f64);
                let total_zero_msgs = fmt.format(total_zero_msgs as f64);
                info!(id, channel_len, total_msg, total_zero_msgs);
            }
        }
        if errors != 0 {
            error!(id, "{}", fmt.format(errors as f64));
        }
        handled_zero_messages_counts.fetch_add(total_zero_msgs, Ordering::SeqCst);

        let total_msg = fmt.format(total_msg as f64);
        let total_zero_msgs = fmt.format(total_zero_msgs as f64);
        info!(id, total_msg, total_zero_msgs);
    }
}
