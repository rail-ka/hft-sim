use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use quanta::Clock;

use crate::{
    config::Stage2Rule,
    types::{HandledMessage, Message},
    utils::timestamp,
};

pub type ProcessorReceiver = Receiver<Message>;
pub type SrategySender = Sender<HandledMessage>;

pub struct ProcessorWorker {
    pub id: u64,
    pub processing_times: Vec<(u64, u64)>,
    pub receiver: ProcessorReceiver,
    pub strategies: Arc<Vec<SrategySender>>,
    pub stage2_rules: Vec<Stage2Rule>,
}

impl ProcessorWorker {
    pub fn run(self) {
        let ProcessorWorker {
            id,
            processing_times,
            strategies,
            receiver,
            stage2_rules,
        } = self;
        let fmt = human_format::Formatter::new();
        let mut ts = timestamp();
        let clock = Clock::new();
        let mut err_arr = [[0u64; 8]; 8];

        let mut prev = clock.raw();

        let mut total_msg = 0usize;
        let mut nanos_per_sec = 0u64;

        while let Ok(msg) = receiver.recv() {
            let strategy = stage2_rules
                .iter()
                .find(|i| i.msg_type == msg.ty)
                .unwrap()
                .strategy;
            let strategy_sender = &strategies[strategy as usize];
            let processing_time = processing_times
                .iter()
                .find(|(t, _)| *t == msg.ty)
                .unwrap()
                .1;

            let mut raw = clock.raw();
            let mut delta = clock.delta_as_nanos(prev, raw);

            while delta < processing_time {
                raw = clock.raw();
                delta = clock.delta_as_nanos(prev, raw);
            }
            prev = raw;
            ts += delta;

            let res = strategy_sender.try_send(HandledMessage {
                msg,
                processor_id: id,
                processing_ts: ts,
            });
            if res.is_err() {
                err_arr[msg.ty as usize][strategy as usize] += 1;
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
        for (ty, inner) in err_arr.iter().enumerate() {
            for (strategy, count) in inner.iter().enumerate() {
                if *count != 0 {
                    error!(ty, strategy, "{}", fmt.format(*count as f64));
                }
            }
        }
        let total_msg = fmt.format(total_msg as f64);
        info!(id, total_msg);
    }
}
