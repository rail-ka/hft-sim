use quanta::Clock;

use crate::{
    traits::{ProcessorReceiver, ProcessorSender},
    types::HandledMessage,
};

pub struct ProcessorWorker<S, R> {
    pub id: u64,
    pub processing_times: Vec<(u64, u64)>,
    pub receiver: R,
    pub sender: S,
    pub start_ts: u64,
    pub clock: Clock,
}

impl<S: ProcessorSender, R: ProcessorReceiver> ProcessorWorker<S, R> {
    pub fn run(self) {
        let ProcessorWorker {
            id,
            processing_times,
            mut receiver,
            mut sender,
            start_ts,
            clock,
        } = self;
        let fmt = human_format::Formatter::new();
        let mut err_arr = [0u64; 8];

        let mut prev = clock.raw();

        let mut total_msg = 0usize;
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
            }
            if nanos_per_sec < 1_000_000_000 {
                nanos_per_sec += delta;
            } else {
                nanos_per_sec = 0;
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
        let total_msg = fmt.format(total_msg as f64);
        info!(id, total_msg);
    }
}
