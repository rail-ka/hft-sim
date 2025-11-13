use std::sync::atomic::Ordering;

use quanta::Clock;

use crate::{
    Arr,
    state::MessagesCounter,
    traits::{ProcessorReceiver, ProcessorSender},
    types::HandledMessage,
};

pub struct ProcessorWorker<S, R> {
    pub id: u64,
    pub processing_times: Arr<(u64, u64)>,
    pub receiver: R,
    pub sender: S,
    pub start_ts: u64,
    pub clock: Clock,
    pub message_count: MessagesCounter,
}

impl<S: ProcessorSender, R: ProcessorReceiver> ProcessorWorker<S, R> {
    pub fn run(self) -> (u64, u64) {
        let ProcessorWorker {
            id,
            processing_times,
            mut receiver,
            mut sender,
            start_ts,
            clock,
            message_count,
        } = self;
        let fmt = human_format::Formatter::new();
        let mut err_arr = [0u64; 8];

        let mut prev = clock.raw();

        let mut total_msg = 0u64;
        let mut zero_msg = 0u64;
        let mut msg_per_sec = 0u64;
        let mut zero_msg_per_sec = 0u64;
        let mut nanos_per_sec = 0u64;

        while let Some(msg) = receiver.next() {
            let msg_ty = msg.ty;
            let processing_time = processing_times
                .iter()
                .find(|(t, _)| *t == msg_ty)
                .unwrap()
                .1;

            let mut raw = clock.raw();
            let mut delta = clock.delta_as_nanos(prev, raw);

            while delta < processing_time {
                raw = clock.raw();
                delta = clock.delta_as_nanos(prev, raw);
            }
            prev = raw;
            let timestamp = clock.delta_as_nanos(start_ts, clock.raw());

            let msg = HandledMessage {
                msg,
                processor_id: id,
                processing_ts: timestamp,
            };
            let ok = sender.send(msg);
            if !ok {
                err_arr[msg_ty as usize] += 1;
            } else {
                total_msg += 1;
                msg_per_sec += 1;
                if msg_ty == 0 {
                    zero_msg += 1;
                    zero_msg_per_sec += 1;
                }
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
                info!(id, channel_len, total_msg);
            }
        }
        for (ty, count) in err_arr.iter().enumerate() {
            if *count != 0 {
                error!(ty, "{}", fmt.format(*count as f64));
            }
        }
        info!(id, total_msg = fmt.format(total_msg as f64));
        (total_msg, zero_msg)
    }
}
