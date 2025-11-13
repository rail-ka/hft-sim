use std::sync::atomic::Ordering;

use hdrhistogram::sync::Recorder;
use quanta::Clock;

use crate::{
    state::MessagesCounter,
    traits::StrategyReceiver,
    types::{HandledMessage, Message},
};

pub struct StrategyWorker<R: StrategyReceiver> {
    pub id: u64,
    pub receiver: R,
    pub processing_time: u64,
    pub start_ts: u64,
    pub clock: Clock,
    pub p_histogram: Recorder<u64>,
    pub c_histogram: Recorder<u64>,
    pub message_count: MessagesCounter,
}

impl<R: StrategyReceiver> StrategyWorker<R> {
    pub fn run(self) -> (u64, u64) {
        let Self {
            id,
            mut receiver,
            processing_time,
            message_count,
            start_ts,
            clock,
            mut p_histogram,
            mut c_histogram,
        } = self;
        let mut seq_arr = [[0u64; 8]; 8];
        let fmt = human_format::Formatter::new();

        let mut errors = 0usize;
        let mut prev = clock.raw();
        let mut total_msg = 0u64;
        let mut total_zero_msgs = 0u64;
        let mut nanos_per_sec = 0u64;
        let mut msg_per_sec = 0u64;
        let mut zero_msg_per_sec = 0u64;

        while let Some(item) = receiver.next() {
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
            let p_time = processing_ts - timestamp;
            p_histogram.saturating_record(p_time);

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
            msg_per_sec += 1;
            if ty == 0 {
                total_zero_msgs += 1;
                zero_msg_per_sec += 1;
            }

            if nanos_per_sec < 1_000_000_000 {
                nanos_per_sec += delta;
            } else {
                nanos_per_sec = 0;
                message_count
                    .total
                    .fetch_add(msg_per_sec, Ordering::Release);
                message_count
                    .zero
                    .fetch_add(zero_msg_per_sec, Ordering::Release);
                msg_per_sec = 0;
                zero_msg_per_sec = 0;
                let channel_len = receiver.len();
                let channel_len = fmt.format(channel_len as f64);
                let total_msg = fmt.format(total_msg as f64);
                let total_zero_msgs = fmt.format(total_zero_msgs as f64);
                info!(id, channel_len, total_msg, total_zero_msgs);
            }

            let timestamp = clock.delta_as_nanos(start_ts, clock.raw());
            c_histogram.saturating_record(timestamp - processing_ts);
        }
        if errors != 0 {
            error!(id, "{}", fmt.format(errors as f64));
        }
        info!(
            id,
            total_msg = fmt.format(total_msg as f64),
            total_zero_msgs = fmt.format(total_zero_msgs as f64)
        );
        (total_msg, total_zero_msgs)
    }
}
