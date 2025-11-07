use std::{sync::Arc, thread::sleep, time::Duration};

use crossbeam_channel::{Receiver, Sender};
use quanta::{Clock, IntoNanoseconds};

use crate::{
    config::Stage2Rule,
    types::{HandledMesage, Message},
    utils::timestamp,
};

// pub struct ProcessorIdQueue {
//     pub id: u64,
//     pub queue: ProcessorQueue,
// }

// pub type ProcessorQueue = ArrayQueue<Message>;
pub type ProcessorReceiver = Receiver<Message>;
pub type SrategySender = Sender<HandledMesage>;

pub struct ProcessorWorker {
    pub id: u64,
    pub processing_times: Vec<(u64, u64)>,
    pub strategies: Arc<Vec<SrategySender>>,
    pub receiver: ProcessorReceiver,
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
        let clock = Clock::new();
        let mut err_arr = [[0u64; 8]; 8];

        while let Ok(msg) = receiver.recv() {
            let strategy = stage2_rules
                .iter()
                .find(|i| i.msg_type == msg.ty)
                .unwrap()
                .strategy;
            let strategy_sender = &strategies[strategy as usize];
            let now = timestamp();
            let instant = clock.now();
            let processing_time = processing_times
                .iter()
                .find(|(t, _)| *t == msg.ty)
                .unwrap()
                .1;
            let nanos = processing_time.saturating_sub(instant.elapsed().into_nanos());
            sleep(Duration::from_nanos(nanos));
            let res = strategy_sender.try_send(HandledMesage {
                msg,
                processor_id: id,
                processing_ts: now,
            });
            if let Err(err) = res {
                err_arr[msg.ty as usize][strategy as usize] += 1;
            }
        }
        let fmt = human_format::Formatter::new();
        for (ty, inner) in err_arr.iter().enumerate() {
            for (strategy, count) in inner.iter().enumerate() {
                if *count != 0 {
                    error!(ty, strategy, "{}", fmt.format(*count as f64));
                }
            }
        }
    }
}
